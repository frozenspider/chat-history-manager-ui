package org.fs.chm.dao.merge

import java.util.UUID

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

import org.fs.chm.dao._

class DatasetMerger(
    masterDao: MutableChatHistoryDao,
    masterDs: Dataset,
    slaveDao: ChatHistoryDao,
    slaveDs: Dataset
) {
  import DatasetMerger._

  /**
   * Analyze dataset mergeability, returning the map from slave chat to mismatches in order.
   * Note that we can only detect conflicts if data source supports source IDs.
   */
  def analyzeChatHistoryMerge(merge: ChatMergeOption): Seq[MessagesMergeOption] = merge match {
    case ChatMergeOption.Retain(mc) =>
      val firstOption = masterDao.firstMessages(mc, 1).headOption
      firstOption.toSeq.map { first =>
        val last = masterDao.lastMessages(mc, 1).last
        MessagesMergeOption.Retain(first.asInstanceOf[TaggedMessage.M], last.asInstanceOf[TaggedMessage.M], None, None)
      }
    case ChatMergeOption.Add(sc) =>
      val firstOption = slaveDao.firstMessages(sc, 1).headOption
      firstOption.toSeq.map { first =>
        val last = slaveDao.lastMessages(sc, 1).last
        MessagesMergeOption.Add(first.asInstanceOf[TaggedMessage.S], last.asInstanceOf[TaggedMessage.S])
      }
    case ChatMergeOption.Combine(mc, sc) =>
      var mismatches = ArrayBuffer.empty[MessagesMergeOption]
      iterate(
        ((messagesStream(masterDao, mc, None), None), (messagesStream(slaveDao, sc, None), None)),
        IterationState.NoState,
        (mm => mismatches += mm)
      )
      mismatches.toVector
  }

  // FIXME: Test!
  /** Stream messages, either from the beginning or from the given one (exclusive) */
  protected[merge] def messagesStream[T <: TaggedMessage](
      dao: ChatHistoryDao,
      chat: Chat,
      fromMessageOption: Option[Message with T]
  ): Stream[T] = {
    val batch = fromMessageOption
      .map(from => dao.messagesAfter(chat, from, BatchSize + 1).drop(1))
      .getOrElse(dao.firstMessages(chat, BatchSize))
      .asInstanceOf[IndexedSeq[Message with T]]
    if (batch.isEmpty) {
      Stream.empty
    } else if (batch.size < BatchSize) {
      batch.toStream
    } else {
      batch.toStream #::: messagesStream[T](dao, chat, Some(batch.last))
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
          MessagesMergeOption.Retain(
            firstMasterMsg = startMasterMsg,
            lastMasterMsg = cxt.prevMm.get,
            firstSlaveMsgOption = Some(startSlaveMsg),
            lastSlaveMsgOption = cxt.prevSm
          )
        case RetentionInProgress(_, startMasterMsg, _) =>
          MessagesMergeOption.Retain(
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
      chatsMergeResolutions: Map[ChatMergeOption, Seq[MessagesMergeOption]]
  ): Dataset = {
    try {
      masterDao.disableBackups()
      val newDs = Dataset(
        uuid = UUID.randomUUID(),
        alias = masterDs.alias + " (merged)",
        sourceType = masterDs.sourceType
      )

      // Users
      for (sourceUser <- usersToMerge) {
        val user2 = sourceUser.userToInsert.copy(dsUuid = newDs.uuid)
        masterDao.insertUser(user2)
      }

      // Chats
      for ((cmo, resolution) <- chatsMergeResolutions) {
        val chat = cmo.chatToInsert.copy(dsUuid = newDs.uuid)
        masterDao.insertChat(chat)

        // Messages
        resolution.map {
          case replace: MessagesMergeOption.Replace => replace.asAdd
          case etc                                  => etc
        }.foreach {
          case MessagesMergeOption.Retain(firstMasterMsg, lastMasterMsg, _, _) =>
            var lastFound = false
            val messages = firstMasterMsg #:: messagesStream(
              masterDao,
              cmo.asInstanceOf[ChatMergeOption.Retain].masterChat,
              Some(firstMasterMsg.asInstanceOf[TaggedMessage.M])
            ).takeWhile { m =>
              val isLast = m =~= lastMasterMsg
              lastFound = isLast
              isLast || !lastFound
            }
            masterDao.insertMessages(chat, messages)
          case MessagesMergeOption.Add(firstSlaveMsg, lastSlaveMsg) =>
            var lastFound = false
            val messages = firstSlaveMsg #:: messagesStream(
              slaveDao,
              cmo.asInstanceOf[ChatMergeOption.Add].slaveChat,
              Some(firstSlaveMsg.asInstanceOf[TaggedMessage.S])
            ).takeWhile { m =>
              val isLast = m =~= lastSlaveMsg
              lastFound = isLast
              isLast || !lastFound
            }
            masterDao.insertMessages(chat, messages)
          case _ => throw new MatchError("Impossible!")
        }
      }

      newDs
    } finally {
      masterDao.enableBackups()
    }
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
}

object DatasetMerger {
  protected[merge] val BatchSize = 1000

  // FIXME: Rename *Option to *Choice
  //        Rename Retain to Keep

  /** Represents a single merge decision: a user that should be added, retained, merged (or skipped otherwise) */
  sealed abstract class UserMergeOption(val userToInsert: User)
  object UserMergeOption {
    case class Retain(masterUser: User)                   extends UserMergeOption(masterUser)
    case class Add(slaveUser: User)                       extends UserMergeOption(slaveUser)
    case class Replace(masterUser: User, slaveUser: User) extends UserMergeOption(slaveUser)
  }

  /** Represents a single merge decision: a chat that should be added, retained, merged (or skipped otherwise) */
  sealed abstract class ChatMergeOption(val chatToInsert: Chat)
  object ChatMergeOption {
    case class Retain(masterChat: Chat)                   extends ChatMergeOption(masterChat)
    case class Add(slaveChat: Chat)                       extends ChatMergeOption(slaveChat)
    case class Combine(masterChat: Chat, slaveChat: Chat) extends ChatMergeOption(slaveChat)
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
    case class Retain(
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

      def asRetain: Retain = Retain(firstMasterMsg, lastMasterMsg, firstSlaveMsgOption, lastSlaveMsgOption)
      def asAdd:    Add    = Add(firstSlaveMsg, lastSlaveMsg)
    }
  }
}
