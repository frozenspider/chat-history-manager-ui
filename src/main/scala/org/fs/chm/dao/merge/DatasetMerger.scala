package org.fs.chm.dao.merge

import java.io.File
import java.util.UUID

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

import org.fs.chm.dao._
import org.fs.chm.utility.IoUtils
import org.fs.utility.StopWatch
import org.slf4s.Logging

class DatasetMerger(
    masterDao: MutableChatHistoryDao,
    masterDs: Dataset,
    slaveDao: ChatHistoryDao,
    slaveDs: Dataset
) extends Logging {
  import DatasetMerger._

  /**
   * Analyze dataset mergeability, amending `ChatMergeOption.Combine` with mismatches in order.
   * Other `ChatMergeOption`s are returned unchanged.
   * Note that we can only detect conflicts if data source supports source IDs.
   */
  def analyzeChatHistoryMerge[T <: ChatMergeOption](merge: T): T = merge match {
    case merge @ ChatMergeOption.Combine(mc, sc, _) =>
      var mismatches = ArrayBuffer.empty[MessagesMergeOption]
      iterate(
        ((messagesStream(masterDao, mc, None), None), (messagesStream(slaveDao, sc, None), None)),
        IterationState.NoState,
        (mm => mismatches += mm)
      )
      mismatches.toVector
      merge.copy(messageMergeOptions = mismatches).asInstanceOf[T]
    case _ => merge
  }

  /** Stream messages, either from the beginning or from the given one (exclusive) */
  protected[merge] def messagesStream[T <: TaggedMessage](
      dao: ChatHistoryDao,
      chat: Chat,
      fromMessageOption: Option[Message with T]
  ): Stream[T] = {
    messageBatchesStream(dao, chat, fromMessageOption).flatten
  }

  /** Stream messages, either from the beginning or from the given one (exclusive) */
  protected[merge] def messageBatchesStream[T <: TaggedMessage](
      dao: ChatHistoryDao,
      chat: Chat,
      fromMessageOption: Option[Message with T]
  ): Stream[IndexedSeq[T]] = {
    val batch = fromMessageOption
      .map(from => dao.messagesAfter(chat, from, BatchSize + 1).drop(1))
      .getOrElse(dao.firstMessages(chat, BatchSize))
      .asInstanceOf[IndexedSeq[Message with T]]
    if (batch.isEmpty) {
      Stream.empty
    } else if (batch.size < BatchSize) {
      Stream(batch)
    } else {
      Stream(batch) #::: messageBatchesStream[T](dao, chat, Some(batch.last))
    }
  }

  /** Iterate through both master and slave streams using state machine like approach */
  @tailrec
  private def iterate(
      cxt: MsgIterationContext,
      state: IterationState,
      onMismatch: MessagesMergeOption => Unit
  ): Unit = {
    import IterationState._
    def mismatchAfterConflictEnd(state: StateInProgress): MessagesMergeOption = {
      state match {
        case MatchInProgress(_, startMasterMsg, _, startSlaveMsg) =>
          MessagesMergeOption.Keep(
            firstMasterMsg = startMasterMsg,
            lastMasterMsg = cxt.prevMm.get,
            firstSlaveMsgOption = Some(startSlaveMsg),
            lastSlaveMsgOption = cxt.prevSm
          )
        case RetentionInProgress(_, startMasterMsg, _) =>
          MessagesMergeOption.Keep(
            firstMasterMsg = startMasterMsg,
            lastMasterMsg = cxt.prevMm.get,
            firstSlaveMsgOption = None,
            lastSlaveMsgOption = None
          )
        case AdditionInProgress(prevMasterMsgOption, prevSlaveMsgOption, startSlaveMsg) =>
          assert(cxt.prevMm == prevMasterMsgOption) // Master stream hasn't advanced
          assert(cxt.prevSm.isDefined)
          MessagesMergeOption.Add(
            firstSlaveMsg = startSlaveMsg,
            lastSlaveMsg = cxt.prevSm.get
          )
        case ConflictInProgress(prevMasterMsgOption, startMasterMsg, prevSlaveMsgOption, startSlaveMsg) =>
          assert(cxt.prevMm.isDefined && cxt.prevSm.isDefined)
          MessagesMergeOption.Replace(
            firstMasterMsg = startMasterMsg,
            lastMasterMsg = cxt.prevMm.get,
            firstSlaveMsg = startSlaveMsg,
            lastSlaveMsg = cxt.prevSm.get
          )
      }
    }

    (cxt.mmStream.headOption, cxt.smStream.headOption, state) match {

      //
      // Streams ended
      //

      case (None, None, NoState) =>
        ()
      case (None, None, state: StateInProgress) =>
        val mismatch = mismatchAfterConflictEnd(state)
        onMismatch(mismatch)

      //
      // NoState
      //

      case (Some(mm), Some(sm), NoState) if mm =~= sm =>
        // Matching subsequence starts
        val state2 = MatchInProgress(cxt.prevMm, mm, cxt.prevSm, sm)
        iterate(cxt.advanceBoth(), state2, onMismatch)
      case (Some(mm), Some(sm), NoState) if mm.sourceIdOption.isDefined && mm.sourceIdOption == sm.sourceIdOption =>
        // Conflict started
        // (Conflicts are only detectable if data source supply source IDs)
        val state2 = ConflictInProgress(cxt.prevMm, mm, cxt.prevSm, sm)
        iterate(cxt.advanceBoth(), state2, onMismatch)
      case (_, Some(sm), NoState) if cxt.cmpMasterSlave() > 0 =>
        // Addition started
        val state2 = AdditionInProgress(cxt.prevMm, cxt.prevSm, sm)
        iterate(cxt.advanceSlave(), state2, onMismatch)
      case (Some(mm), _, NoState) if cxt.cmpMasterSlave() < 0 =>
        // Retention started
        val state2 = RetentionInProgress(cxt.prevMm, mm, cxt.prevSm)
        iterate(cxt.advanceMaster(), state2, onMismatch)

      //
      // AdditionInProgress
      //

      case (_, Some(sm), state: AdditionInProgress)
          if state.prevMasterMsgOption == cxt.prevMm && cxt.cmpMasterSlave() > 0 =>
        // Addition continues
        iterate(cxt.advanceSlave(), state, onMismatch)
      case (_, _, state: AdditionInProgress) =>
        // Addition ended
        onMismatch(mismatchAfterConflictEnd(state))
        iterate(cxt, NoState, onMismatch)

      //
      // MatchInProgress
      //

      case (Some(mm), Some(sm), state: MatchInProgress) if mm =~= sm =>
        // Matching subsequence continues
        iterate(cxt.advanceBoth(), state, onMismatch)
      case (_, _, state: MatchInProgress) =>
        // Matching subsequence ends
        onMismatch(mismatchAfterConflictEnd(state))
        iterate(cxt, NoState, onMismatch)

      //
      // RetentionInProgress
      //

      case (Some(mm), _, RetentionInProgress(_, _, prevSlaveMsgOption))
          if (cxt.prevSm == prevSlaveMsgOption) && cxt.cmpMasterSlave() < 0 =>
        // Retention continues
        iterate(cxt.advanceMaster(), state, onMismatch)
      case (_, _, state: RetentionInProgress) =>
        // Retention ended
        onMismatch(mismatchAfterConflictEnd(state))
        iterate(cxt, NoState, onMismatch)

      //
      // ConflictInProgress
      //

      case (Some(mm), Some(sm), state: ConflictInProgress) if mm !=~= sm =>
        // Conflict continues
        iterate(cxt.advanceBoth(), state, onMismatch)
      case (_, _, state: ConflictInProgress) =>
        // Conflict ended
        onMismatch(mismatchAfterConflictEnd(state))
        iterate(cxt, NoState, onMismatch)
    }
  }

  def merge(
      usersToMerge: Seq[UserMergeOption],
      chatsToMerge: Seq[ChatMergeOption]
  ): Dataset = {
    StopWatch.measureAndCall {
      try {
        masterDao.disableBackups()
        val newDs = Dataset(
          uuid = UUID.randomUUID(),
          alias = masterDs.alias + " (merged)",
          sourceType = masterDs.sourceType
        )
        masterDao.insertDataset(newDs)

        // Users
        val masterSelf = masterDao.myself(masterDs.uuid)
        require(
          usersToMerge.map(_.userToInsert).count(_.id == masterSelf.id) == 1,
          "User merges should contain exactly one self user!"
        )
        for (sourceUser <- usersToMerge) {
          val user2 = sourceUser.userToInsert.copy(dsUuid = newDs.uuid)
          masterDao.insertUser(user2, user2.id == masterSelf.id)
        }

        // Chats
        for (cmo <- chatsToMerge) {
          val chat = (cmo.slaveChatOption orElse cmo.masterChatOption).get.copy(dsUuid = newDs.uuid)
          masterDao.insertChat(chat)

          // Messages
          val messageBatches: Stream[(ChatHistoryDao, Dataset, IndexedSeq[Message])] = cmo match {
            case ChatMergeOption.Keep(mc) =>
              messageBatchesStream[TaggedMessage.M](masterDao, mc, None) map (mb => (masterDao, masterDs, mb))
            case ChatMergeOption.Add(sc) =>
              messageBatchesStream[TaggedMessage.S](slaveDao, sc, None) map (mb => (slaveDao, slaveDs, mb))
            case ChatMergeOption.Combine(mc, sc, resolution) =>
              resolution.map {
                case replace: MessagesMergeOption.Replace => replace.asAdd
                case etc                                  => etc
              }.map {
                case MessagesMergeOption.Keep(firstMasterMsg, lastMasterMsg, _, _) =>
                  takeMsgsFromBatchUntilInc(
                    IndexedSeq(firstMasterMsg) #:: messageBatchesStream(
                      masterDao,
                      cmo.masterChatOption.get,
                      Some(firstMasterMsg.asInstanceOf[TaggedMessage.M])
                    ),
                    lastMasterMsg
                  ) map (mb => (masterDao, masterDs, mb))
                case MessagesMergeOption.Add(firstSlaveMsg, lastSlaveMsg) =>
                  takeMsgsFromBatchUntilInc(
                    IndexedSeq(firstSlaveMsg) #:: messageBatchesStream(
                      slaveDao,
                      cmo.slaveChatOption.get,
                      Some(firstSlaveMsg.asInstanceOf[TaggedMessage.S])
                    ),
                    lastSlaveMsg
                  ) map (mb => (slaveDao, slaveDs, mb))
                case _ => throw new MatchError("Impossible!")
              }.toStream.flatten
          }
          val to = masterDao.dataPath(newDs.uuid)
          for ((srcDao, srcDs, mb) <- messageBatches) {
            masterDao.insertMessages(chat, mb)
            val paths    = mb.flatMap(_.paths)
            val from     = srcDao.dataPath(srcDs.uuid)
            val filesMap = paths.map(p => (new File(from, p), new File(to, p))).toMap
            val (notFound, _) =
              StopWatch.measureAndCall(IoUtils.copyAll(filesMap))((_, t) => log.info(s"Copied in $t ms"))
            notFound.foreach(nf => log.info(s"Not found: ${nf.getAbsolutePath}"))
          }
        }

        newDs
      } finally {
        masterDao.enableBackups()
      }
    } ((_, t) => log.info(s"Datasets merged in ${t} ms"))
  }

  //
  // Helpers
  //

  /** If message dates and plain strings are equal, we consider this enough */
  private val msgOrdering = new Ordering[Message] {
    override def compare(x: Message, y: Message): Int = {
      (x, y) match {
        case _ if x.time != y.time =>
          x.time compareTo y.time
        case _ if x.sourceIdOption.isDefined && y.sourceIdOption.isDefined =>
          x.sourceIdOption.get compareTo y.sourceIdOption.get
        case _ if x.plainSearchableString == y.plainSearchableString =>
          0
        case _ =>
          throw new IllegalStateException(s"Cannot compare messages $x and $y!")
      }
    }
  }

  private val msgOptionOrdering = new Ordering[Option[Message]] {
    override def compare(xo: Option[Message], yo: Option[Message]): Int = {
      (xo, yo) match {
        case (None, None)       => 0
        case (None, _)          => 1
        case (_, None)          => -1
        case (Some(x), Some(y)) => msgOrdering.compare(x, y)
      }
    }
  }

  private type MsgIterationContext =
    ((Stream[TaggedMessage.M], Option[TaggedMessage.M]), (Stream[TaggedMessage.S], Option[TaggedMessage.S]))

  private implicit class RichMsgIterationContext(cxt: MsgIterationContext) {
    def mmStream: Stream[TaggedMessage.M] = cxt._1._1
    def prevMm:   Option[TaggedMessage.M] = cxt._1._2
    def smStream: Stream[TaggedMessage.S] = cxt._2._1
    def prevSm:   Option[TaggedMessage.S] = cxt._2._2

    def cmpMasterSlave(): Int = {
      msgOptionOrdering.compare(mmStream.headOption, smStream.headOption)
    }

    def advanceBoth(): MsgIterationContext = {
      ((mmStream.tail, mmStream.headOption), (smStream.tail, smStream.headOption))
    }

    def advanceMaster(): MsgIterationContext = {
      ((mmStream.tail, mmStream.headOption), (smStream, prevSm))
    }

    def advanceSlave(): MsgIterationContext = {
      ((mmStream, prevMm), (smStream.tail, smStream.headOption))
    }
  }

  private sealed trait IterationState
  private object IterationState {
    case object NoState extends IterationState

    sealed trait StateInProgress extends IterationState
    case class MatchInProgress(
        prevMasterMsgOption: Option[TaggedMessage.M],
        startMasterMsg: TaggedMessage.M,
        prevSlaveMsgOption: Option[TaggedMessage.S],
        startSlaveMsg: TaggedMessage.S
    ) extends StateInProgress
    case class RetentionInProgress(
        prevMasterMsgOption: Option[TaggedMessage.M],
        startMasterMsg: TaggedMessage.M,
        prevSlaveMsgOption: Option[TaggedMessage.S]
    ) extends StateInProgress
    case class AdditionInProgress(
        prevMasterMsgOption: Option[TaggedMessage.M],
        prevSlaveMsgOption: Option[TaggedMessage.S],
        startSlaveMsg: TaggedMessage.S
    ) extends StateInProgress
    case class ConflictInProgress(
        prevMasterMsgOption: Option[TaggedMessage.M],
        startMasterMsg: TaggedMessage.M,
        prevSlaveMsgOption: Option[TaggedMessage.S],
        startSlaveMsg: TaggedMessage.S
    ) extends StateInProgress
  }

  private def takeMsgsFromBatchUntilInc(
      stream: Stream[IndexedSeq[Message]],
      m: Message
  ): Stream[IndexedSeq[Message]] = {
    var lastFound = false
    stream.map { mb =>
      if (!lastFound) {
        mb.takeWhile { m2 =>
          val isLast = m2 =~= m
          lastFound |= isLast
          isLast || !lastFound
        }
      } else {
        IndexedSeq.empty
      }
    }.takeWhile(_.nonEmpty)
  }
}

object DatasetMerger {
  protected[merge] val BatchSize = 1000

  /** Represents a single merge decision: a user that should be added, retained, merged (or skipped otherwise) */
  sealed abstract class UserMergeOption(val userToInsert: User)
  object UserMergeOption {
    case class Keep(masterUser: User)                     extends UserMergeOption(masterUser)
    case class Add(slaveUser: User)                       extends UserMergeOption(slaveUser)
    case class Replace(masterUser: User, slaveUser: User) extends UserMergeOption(slaveUser)
  }

  /** Represents a single merge decision: a chat that should be added, retained, merged (or skipped otherwise) */
  sealed abstract class ChatMergeOption(
     val masterChatOption: Option[Chat],
     val slaveChatOption:  Option[Chat]
  )
  object ChatMergeOption {
    case class Keep(masterChat: Chat) extends ChatMergeOption(Some(masterChat), None)
    case class Add(slaveChat: Chat)   extends ChatMergeOption(None, Some(slaveChat))
    case class Combine(
        val masterChat: Chat,
        val slaveChat: Chat,
        /** Serves as either mismatches (all options after analysis) or resolution (mismatches filtered by user) */
        val messageMergeOptions: IndexedSeq[MessagesMergeOption]
    ) extends ChatMergeOption(Some(masterChat), Some(slaveChat))
  }

  // Message tagged types
  sealed trait TaggedMessage
  object TaggedMessage {
    sealed trait MasterMessageTag extends TaggedMessage
    sealed trait SlaveMessageTag  extends TaggedMessage

    type M = Message with MasterMessageTag
    type S = Message with SlaveMessageTag
  }

  /** Represents a single merge decision: a messages that should be added, retained, merged (or skipped otherwise) */
  sealed trait MessagesMergeOption {
    def firstMasterMsgOption: Option[TaggedMessage.M]
    def lastMasterMsgOption:  Option[TaggedMessage.M]
    def firstSlaveMsgOption:  Option[TaggedMessage.S]
    def lastSlaveMsgOption:   Option[TaggedMessage.S]
  }
  object MessagesMergeOption {
    case class Keep(
        firstMasterMsg: TaggedMessage.M,
        lastMasterMsg: TaggedMessage.M,
        firstSlaveMsgOption: Option[TaggedMessage.S],
        lastSlaveMsgOption: Option[TaggedMessage.S]
    ) extends MessagesMergeOption {
      assert(firstSlaveMsgOption.isDefined == lastSlaveMsgOption.isDefined, (firstSlaveMsgOption, lastSlaveMsgOption))
      override def firstMasterMsgOption = Some(firstMasterMsg)
      override def lastMasterMsgOption  = Some(lastMasterMsg)
    }

    case class Add(
        firstSlaveMsg: TaggedMessage.S,
        lastSlaveMsg: TaggedMessage.S
    ) extends MessagesMergeOption {
      override def firstMasterMsgOption = None
      override def lastMasterMsgOption  = None
      override def firstSlaveMsgOption  = Some(firstSlaveMsg)
      override def lastSlaveMsgOption   = Some(lastSlaveMsg)
    }

    case class Replace(
        firstMasterMsg: TaggedMessage.M,
        lastMasterMsg: TaggedMessage.M,
        firstSlaveMsg: TaggedMessage.S,
        lastSlaveMsg: TaggedMessage.S
    ) extends MessagesMergeOption {
      override def firstMasterMsgOption = Some(firstMasterMsg)
      override def lastMasterMsgOption  = Some(lastMasterMsg)
      override def firstSlaveMsgOption  = Some(firstSlaveMsg)
      override def lastSlaveMsgOption   = Some(lastSlaveMsg)

      def asKeep: Keep = Keep(firstMasterMsg, lastMasterMsg, firstSlaveMsgOption, lastSlaveMsgOption)
      def asAdd:  Add  = Add(firstSlaveMsg, lastSlaveMsg)
    }
  }
}
