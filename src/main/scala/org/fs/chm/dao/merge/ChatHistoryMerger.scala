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

  /** Iterate through both master and slave streams using state machine like approach */
  @tailrec
  private def iterate(
      cxt: IterationContext,
      state: IterationState,
      onMismatch: Mismatch => Unit
  ): Unit = {
    import IterationState._
    def prevMmId = cxt.prevMm map (_.id) getOrElse -1L
    def prevSmId = cxt.prevSm map (_.id) getOrElse -1L
    def mismatchOptionAfterConflictEnd(state: StateInProgress): Option[Mismatch] = {
      state match {
        case AdditionInProgress(prevMasterMsgId, startSlaveMsgId) =>
          assert(prevMmId == prevMasterMsgId) // Master stream hasn't advanced
          Some(
            Mismatch.Addition(
              prevMasterMsgId = prevMasterMsgId,
              slaveMsgIds     = (startSlaveMsgId, prevSmId)
            )
          )
        case ConflictInProgress(startMasterMsgId, startSlaveMsgId) =>
          assert(startMasterMsgId != -1)
          Some(
            Mismatch.Conflict(
              masterMsgIds = (startMasterMsgId, prevMmId),
              slaveMsgIds  = (startSlaveMsgId, prevSmId)
            )
          )
        case RetentionInProgress(_, prevSlaveMsgId) =>
          assert(prevSmId == prevSlaveMsgId) // Slave stream hasn't advanced
          // We don't treat retention as a mismatch
          None
      }
    }

    (cxt.mmStream.headOption, cxt.smStream.headOption, state) match {

      //
      // Streams ended
      //

      case (None, None, NoState) =>
        ()
      case (None, None, state: StateInProgress) =>
        val mismatchOption = mismatchOptionAfterConflictEnd(state)
        mismatchOption foreach onMismatch

      //
      // NoState
      //

      case (Some(mm), Some(sm), NoState) if mm == sm =>
        // Matching subsequence continues
        iterate(cxt.advanceBoth(), NoState, onMismatch)
      case (Some(mm), Some(sm), NoState) if mm.id == sm.id =>
        // Conflict started
        val state2 = ConflictInProgress(mm.id, sm.id)
        iterate(cxt.advanceBoth(), state2, onMismatch)
      case (_, Some(sm), NoState) if cxt.cmpMasterSlave() > 0 =>
        // Addition started
        val state2 = AdditionInProgress(prevMmId, sm.id)
        iterate(cxt.advanceSlave(), state2, onMismatch)
      case (Some(mm), _, NoState) if cxt.cmpMasterSlave() < 0 =>
        // Retention started
        val state2 = RetentionInProgress(mm.id, prevSmId)
        iterate(cxt.advanceMaster(), state2, onMismatch)

      //
      // AdditionInProgress
      //

      case (_, Some(sm), AdditionInProgress(prevMasterMsgId, _))
          if prevMasterMsgId == prevMmId && cxt.cmpMasterSlave() > 0 =>
        // Addition continues
        iterate(cxt.advanceSlave(), state, onMismatch)
      case (_, _, state: AdditionInProgress) =>
        // Addition ended
        val mismatchOption = mismatchOptionAfterConflictEnd(state)
        mismatchOption foreach onMismatch
        iterate(cxt, NoState, onMismatch)

      //
      // RetentionInProgress
      //

      case (Some(mm), _, RetentionInProgress(_, prevSlaveMsgId))
          if prevSlaveMsgId == prevSmId && cxt.cmpMasterSlave() < 0 =>
        // Retention continues
        iterate(cxt.advanceMaster(), state, onMismatch)
      case (_, _, state: RetentionInProgress) =>
        // Retention ended
        val mismatchOption = mismatchOptionAfterConflictEnd(state)
        mismatchOption foreach onMismatch
        iterate(cxt, NoState, onMismatch)

      //
      // ConflictInProgress
      //

      case (Some(mm), Some(sm), state: ConflictInProgress) if mm != sm && mm.id == sm.id =>
        // Conflict continues
        iterate(cxt.advanceBoth(), state, onMismatch)
      case (_, _, state: ConflictInProgress) =>
        // Conflict ended
        val mismatchOption = mismatchOptionAfterConflictEnd(state)
        mismatchOption foreach onMismatch
        iterate(cxt, NoState, onMismatch)
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

    def cmpMasterSlave(): Int = {
      msgOptionOrdering.compare(mmStream.headOption, smStream.headOption)
    }

    def advanceBoth(): IterationContext = {
      ((mmStream.tail, mmStream.headOption), (smStream.tail, smStream.headOption))
    }

    def advanceMaster(): IterationContext = {
      ((mmStream.tail, mmStream.headOption), (smStream, prevSm))
    }

    def advanceSlave(): IterationContext = {
      ((mmStream, prevMm), (smStream.tail, smStream.headOption))
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

    sealed trait StateInProgress extends IterationState
    case class AdditionInProgress(
        prevMasterMsgId: Long,
        startSlaveMsgId: Long
    ) extends StateInProgress
    case class RetentionInProgress(
        startMasterMsgId: Long,
        prevSlaveMsgId: Long
    ) extends StateInProgress
    case class ConflictInProgress(
        startMasterMsgId: Long,
        startSlaveMsgId: Long
    ) extends StateInProgress
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
    case class Retain(masterChat: Chat)                   extends MergeOption
  }

  sealed trait Mismatch
  object Mismatch {
    case class Addition(
        /** -1 if appended before first */
        prevMasterMsgId: Long,
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
