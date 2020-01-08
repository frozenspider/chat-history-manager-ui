package org.fs.chm.dao.merge

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

import org.fs.chm.dao._
import org.fs.chm.dao.merge.ChatHistoryMerger._
import org.fs.chm.utility.EntityUtils._

class ChatHistoryMerger(
    masterDao: MutableChatHistoryDao,
    masterDs: Dataset,
    slaveDao: ChatHistoryDao,
    slaveDs: Dataset,
    whatToMerge: Seq[ChangedMergeOption]
) {

  /** Analyze dataset mergeability, returning map from slave chat to mismatches in order */
  def analyze: Map[Chat, Seq[Mismatch]] = {
    whatToMerge.map {
      case MergeOption.Add(sc)         => (sc, Seq.empty)
      case MergeOption.Combine(mc, sc) => (sc, analyzeCombine(mc, sc))
    }.toMap
  }

  protected[merge] def analyzeCombine(mc: Chat, sc: Chat): Seq[Mismatch] = {
    def messagesStream[T <: TaggedMessage](dao: ChatHistoryDao, chat: Chat, offset: Int): Stream[T] = {
      if (offset >= chat.msgCount) {
        Stream.empty
      } else {
        val batch = dao.scrollMessages(chat, offset, BatchSize).asInstanceOf[IndexedSeq[T]]
        batch.toStream #::: messagesStream[T](dao, chat, offset + batch.size)
      }
    }
    var mismatches = ArrayBuffer.empty[Mismatch]
    iterate(
      ((messagesStream(masterDao, mc, 0), None), (messagesStream(slaveDao, sc, 0), None)),
      IterationState.NoState,
      (mm => mismatches += mm)
    )
    mismatches.toVector
  }

  private val msgOptionOrdering = new Ordering[Option[Message]] {
    override def compare(xo: Option[Message], yo: Option[Message]): Int = {
      (xo, yo) match {
        case (None, None)                           => 0
        case (None, _)                              => 1
        case (_, None)                              => -1
        case (Some(x), Some(y)) if x.time != y.time => x.time compareTo y.time
        case (Some(x), Some(y))                     => x.id compareTo y.id
      }
    }
  }

  @tailrec
  private def iterate(
      cxt: IterationContext,
      state: IterationState,
      onMismatch: Mismatch => Unit
  ): Unit = {
    import IterationState._
    def prevMmId = cxt.prevMm map (_.id) getOrElse -1L
    def prevSmId = cxt.prevSm map (_.id) getOrElse -1L
    def mismatchAfterConflictEnd(conflict: Conflict): Mismatch = {
      val slaveMsgIds = (conflict.slaveMsgId, prevSmId)
      if (prevMmId == conflict.prevMasterMsgId) {
        // Master stream hasn't advanced
        Mismatch.Addition(lastCommonMasterMsgId = conflict.prevMasterMsgId, slaveMsgIds = slaveMsgIds)
      } else {
        assert(conflict.masterMsgId != -1)
        Mismatch.Conflict(masterMsgIds = (conflict.masterMsgId, prevMmId), slaveMsgIds = slaveMsgIds)
      }
    }

    (cxt.mmStream.headOption, cxt.smStream.headOption, state) match {
      case (None, None, NoState) =>
        // Stream ended
        ()
      case (Some(mm), None, NoState) =>
        // Rest of the master stream should be taken as-is
        ???
      case (mmOption, Some(sm), NoState) =>
        val state2 = if (mmOption contains sm) {
          // Continue matching subsequence
          NoState
        } else {
          // Conflict  started
          Conflict(mmOption map (_.id) getOrElse -1L, prevMmId, sm.id)
        }
        iterate(cxt.advance(), state2, onMismatch)
      case (None, None, conflict: Conflict) =>
        // Stream ended on conflict
        val mismatch = mismatchAfterConflictEnd(conflict)
        onMismatch(mismatch)
      case (mmOption, smOption, conflict: Conflict) =>
        val state2 = if (mmOption == smOption) {
          // Conflict ended
          val mismatch = mismatchAfterConflictEnd(conflict)
          onMismatch(mismatch)
          NoState
        } else {
          // Conflict continues
          conflict
        }
        iterate(cxt.advance(), state2, onMismatch)
    }
  }

  def merge(resolution: Map[Chat, Map[Mismatch, MismatchResolution]]): Dataset = {
    /*
     * Do the same as analyze, reuse as much as possible
     */
    ???
  }

  private type IterationContext =
    ((Stream[TaggedMessage.M], Option[TaggedMessage.M]), (Stream[TaggedMessage.S], Option[TaggedMessage.S]))

  private implicit class RichIterationContext(cxt: IterationContext) {
    def mmStream: Stream[TaggedMessage.M] = cxt._1._1
    def prevMm:   Option[TaggedMessage.M] = cxt._1._2
    def smStream: Stream[TaggedMessage.S] = cxt._2._1
    def prevSm:   Option[TaggedMessage.S] = cxt._2._2

    def advance(): IterationContext = {
      require(mmStream.nonEmpty || smStream.nonEmpty)
      msgOptionOrdering.compare(mmStream.headOption, smStream.headOption) match {
        case neg if neg < 0 => // mm < sm
          ((mmStream.tail, mmStream.headOption), (smStream, prevSm))
        case 0 =>
          ((mmStream.tail, mmStream.headOption), (smStream.tail, smStream.headOption))
        case pos if pos > 0 => // mm > sm
          ((mmStream, prevMm), (smStream.tail, smStream.headOption))
      }
    }
  }

  // Message tagged types
  private sealed trait TaggedMessage
  private object TaggedMessage {
    sealed trait MasterMessageTag extends TaggedMessage
    sealed trait SlaveMessageTag  extends TaggedMessage

    type M = Message with MasterMessageTag
    type S = Message with SlaveMessageTag
  }

  private sealed trait IterationState
  private object IterationState {
    case object NoState extends IterationState
    case class Conflict(
        masterMsgId: Long,
        prevMasterMsgId: Long,
        slaveMsgId: Long
    ) extends IterationState
  }
}

object ChatHistoryMerger {

  protected[merge] val BatchSize = 1000

  /** Represents a single general merge option: a chat that should be added or merged (or skipped if no decision) */
  sealed trait MergeOption
  sealed trait ChangedMergeOption extends MergeOption
  object MergeOption {
    case class Combine(masterChat: Chat, slaveChat: Chat) extends ChangedMergeOption
    case class Add(slaveChat: Chat)                       extends ChangedMergeOption
    case class Unchanged(masterChat: Chat)                extends MergeOption
  }

  sealed trait Mismatch
  object Mismatch {
    case class Addition(
        /** -1 if appended before first */
        lastCommonMasterMsgId: Long,
        /** First and last ID*/
        slaveMsgIds: (Long, Long)
    ) extends Mismatch

    case class Conflict(
        masterMsgIds: (Long, Long),
        slaveMsgIds: (Long, Long)
    ) extends Mismatch
  }

  sealed trait MismatchResolution
  object MismatchResolution {
    case object Apply  extends MismatchResolution
    case object Reject extends MismatchResolution
  }
}
