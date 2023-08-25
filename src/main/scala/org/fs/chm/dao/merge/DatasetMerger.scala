package org.fs.chm.dao.merge

import java.io.File

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

import org.fs.chm.dao.ChatHistoryDao
import org.fs.chm.dao.Entities._
import org.fs.chm.dao.MutableChatHistoryDao
import org.fs.chm.protobuf._
import org.fs.chm.utility.LangUtils._
import org.fs.utility.Imports._
import org.fs.utility.StopWatch
import org.slf4s.Logging

class DatasetMerger(
    val masterDao: MutableChatHistoryDao,
    val masterDs: Dataset,
    val slaveDao: ChatHistoryDao,
    val slaveDs: Dataset
) extends Logging {
  import DatasetMerger._

  private val masterRoot = masterDao.datasetRoot(masterDs.uuid)
  private val slaveRoot  = masterDao.datasetRoot(slaveDs.uuid)

  /**
   * Analyze dataset mergeability, amending `ChatMergeOption.Combine` with mismatches in order.
   * Other `ChatMergeOption`s are returned unchanged.
   * Note that we can only detect conflicts if data source supports source IDs.
   */
  def analyzeChatHistoryMerge[T <: ChatMergeOption](merge: T): T = merge match {
    case merge @ ChatMergeOption.Combine(mCwd, sCwd, _) =>
      var mismatches = ArrayBuffer.empty[MessagesMergeOption]
      iterate(
        MsgIterationContext(
          mmStream = messagesStream(masterDao, mCwd.chat, None),
          prevMm   = None,
          mCwd     = mCwd,
          smStream = messagesStream(slaveDao, sCwd.chat, None),
          prevSm   = None,
          sCwd     = sCwd,
        ),
        IterationState.NoState,
        (mm => mismatches += mm)
      )
      merge.copy(messageMergeOptions = mismatches.toVector).asInstanceOf[T]
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
  protected[merge] def messageBatchesStream[TM <: TaggedMessage](
      dao: ChatHistoryDao,
      chat: Chat,
      fromMessageOption: Option[TM]
  ): Stream[IndexedSeq[TM]] = {
    val batch = fromMessageOption
      .map(from => dao.messagesAfter(chat, from, BatchSize + 1).drop(1))
      .getOrElse(dao.firstMessages(chat, BatchSize))
      .asInstanceOf[IndexedSeq[TM]]
    if (batch.isEmpty) {
      Stream.empty
    } else if (batch.size < BatchSize) {
      Stream(batch)
    } else {
      Stream(batch) #::: messageBatchesStream[TM](dao, chat, Some(batch.last))
    }
  }

  private def mismatchAfterConflictEnd(cxt: MsgIterationContext,
                                       state: IterationState.StateInProgress): MessagesMergeOption = {
    import IterationState._
    state match {
      case MatchInProgress(_, startMasterMsg, _, startSlaveMsg) =>
        MessagesMergeOption.Keep(
          firstMasterMsg      = startMasterMsg,
          lastMasterMsg       = cxt.prevMm.get,
          firstSlaveMsgOption = Some(startSlaveMsg),
          lastSlaveMsgOption  = cxt.prevSm
        )
      case RetentionInProgress(_, startMasterMsg, _) =>
        MessagesMergeOption.Keep(
          firstMasterMsg      = startMasterMsg,
          lastMasterMsg       = cxt.prevMm.get,
          firstSlaveMsgOption = None,
          lastSlaveMsgOption  = None
        )
      case AdditionInProgress(prevMasterMsgOption, prevSlaveMsgOption, startSlaveMsg) =>
        assert(cxt.prevMm == prevMasterMsgOption) // Master stream hasn't advanced
        assert(cxt.prevSm.isDefined)
        MessagesMergeOption.Add(
          firstSlaveMsg = startSlaveMsg,
          lastSlaveMsg  = cxt.prevSm.get
        )
      case ConflictInProgress(prevMasterMsgOption, startMasterMsg, prevSlaveMsgOption, startSlaveMsg) =>
        assert(cxt.prevMm.isDefined && cxt.prevSm.isDefined)
        MessagesMergeOption.Replace(
          firstMasterMsg = startMasterMsg,
          lastMasterMsg  = cxt.prevMm.get,
          firstSlaveMsg  = startSlaveMsg,
          lastSlaveMsg   = cxt.prevSm.get
        )
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

    if (Thread.interrupted()) {
      throw new InterruptedException()
    }

    (cxt.mmStream.headOption, cxt.smStream.headOption, state) match {

      //
      // Streams ended
      //

      case (None, None, NoState) =>
        ()
      case (None, None, state: StateInProgress) =>
        val mismatch = mismatchAfterConflictEnd(cxt, state)
        onMismatch(mismatch)

      //
      // NoState
      //

      case (Some(mm), Some(sm), NoState) if matchesDisregardingContent(mm, cxt.mCwd, sm, cxt.sCwd) =>
        // Matching subsequence starts
        val state2 = MatchInProgress(cxt.prevMm, mm, cxt.prevSm, sm)
        iterate(cxt.advanceBoth(), state2, onMismatch)
      case (Some(mm), Some(sm), NoState)
        if mm.typed.service.flatten.flatMap(_.asMessage.sealedValueOptional.groupMigrateFrom).isDefined &&
           sm.typed.service.flatten.flatMap(_.asMessage.sealedValueOptional.groupMigrateFrom).isDefined &&
           mm.sourceIdOption.isDefined && mm.sourceIdOption == sm.sourceIdOption &&
           mm.fromId < 0x100000000L && sm.fromId > 0x100000000L &&
           (mm.copy(fromId = sm.fromId), masterRoot, cxt.mCwd) =~= (sm, slaveRoot, cxt.sCwd) =>

        // FIXME: Why does compiler report this as unreachable?!
        // Special handling for a service message mismatch which is expected when merging Telegram after 2020-10
        // We register this one conflict and proceed in clean state.
        // This is dirty but relatively easy to do.
        val singleConflictState = ConflictInProgress(cxt.prevMm, mm, cxt.prevSm, sm)
        onMismatch(mismatchAfterConflictEnd(cxt.advanceBoth(), singleConflictState))
        iterate(cxt.advanceBoth(), NoState, onMismatch)
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
        onMismatch(mismatchAfterConflictEnd(cxt, state))
        iterate(cxt, NoState, onMismatch)

      //
      // MatchInProgress
      //

      case (Some(mm), Some(sm), state: MatchInProgress) if matchesDisregardingContent(mm, cxt.mCwd, sm, cxt.sCwd) =>
        // Matching subsequence continues
        iterate(cxt.advanceBoth(), state, onMismatch)
      case (_, _, state: MatchInProgress) =>
        // Matching subsequence ends
        onMismatch(mismatchAfterConflictEnd(cxt, state))
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
        onMismatch(mismatchAfterConflictEnd(cxt, state))
        iterate(cxt, NoState, onMismatch)

      //
      // ConflictInProgress
      //

      case (Some(mm), Some(sm), state: ConflictInProgress)
          if (mm.asInstanceOf[Message], masterRoot, cxt.mCwd) !=~= (sm, slaveRoot, cxt.sCwd) =>
        // Conflict continues
        iterate(cxt.advanceBoth(), state, onMismatch)
      case (_, _, state: ConflictInProgress) =>
        // Conflict ended
        onMismatch(mismatchAfterConflictEnd(cxt, state))
        iterate(cxt, NoState, onMismatch)
    }
  }

  def merge(
      explicitUsersToMerge: Seq[UserMergeOption],
      chatsToMerge: Seq[ChatMergeOption]
  ): Dataset = {
    StopWatch.measureAndCall {
      try {
        masterDao.backup()
        masterDao.disableBackups()
        val newDs = Dataset(
          uuid       = randomUuid,
          alias      = masterDs.alias + " (merged)",
          sourceType = masterDs.sourceType
        )
        masterDao.insertDataset(newDs)

        // Account for users who were skipped from usersToMerge due to their chat merge skipped, treated as Keep
        val usersToMerge: Seq[UserMergeOption] = {
          val usersToKeep = {
            val masterUsersIdsToMerge = explicitUsersToMerge.collect {
              case UserMergeOption.Keep(mu) => mu.id
              case UserMergeOption.Replace(mu, _) => mu.id
            }.toSet

            masterDao
              .users(masterDs.uuid)
              .filter(mu => !masterUsersIdsToMerge.contains(mu.id))
              .map(UserMergeOption.Keep)
          }

          explicitUsersToMerge ++ usersToKeep
        }

        // Sanity check
        for {
          firstMasterChat <- chatsToMerge.find(_.masterCwdOption.isDefined)
          masterCwd <- firstMasterChat.masterCwdOption
        } require(masterDao.users(masterCwd.chat.dsUuid).size <= usersToMerge.size, "Not enough user merges!")

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
        val finalUsers = masterDao.users(newDs.uuid)

        // Chats
        for (cmo <- chatsToMerge) {
          val (dsRoot, chat) = {
            Seq(
              cmo.slaveCwdOption.map(cwd => (slaveDao.datasetRoot(cwd.dsUuid), cwd.chat)),
              cmo.masterCwdOption.map(cwd => (masterDao.datasetRoot(cwd.dsUuid), cwd.chat))
            ).yieldDefined.head match {
              case (f, c) =>
                val c2 = (c.tpe, c.memberIds.find(_ != masterSelf.id)) match {
                  case (ChatType.Personal, Some(userId)) =>
                    // For merged personal chats, name should match whatever user name was chosen
                    val user = finalUsers.find(_.id == userId).get
                    c.copy(nameOption = user.prettyNameOption)
                  case _ =>
                    c
                }
                (f, c2.copy(dsUuid = newDs.uuid))
            }
          }
          masterDao.insertChat(dsRoot, chat)

          // Messages
          val messageBatches: Stream[(DatasetRoot, IndexedSeq[Message])] = cmo match {
            case ChatMergeOption.Keep(mcwd) =>
              messageBatchesStream[TaggedMessage.M](masterDao, mcwd.chat, None)
                .map(_.map(fixupMessageWithMembers(mcwd, finalUsers)))
                .map(mb => (masterDao.datasetRoot(masterDs.uuid), mb))
            case ChatMergeOption.Add(scwd) =>
              messageBatchesStream[TaggedMessage.S](slaveDao, scwd.chat, None)
                .map(_.map(fixupMessageWithMembers(scwd, finalUsers)))
                .map(mb => (slaveDao.datasetRoot(slaveDs.uuid), mb))
            case ChatMergeOption.Combine(mc, sc, resolution) =>
              resolution.map {
                case MessagesMergeOption.Keep(firstMasterMsg, lastMasterMsg, _, _) =>
                  // In case of a Keep, we completely ignore slave messages.
                  batchLoadMsgsUntilInc(finalUsers, masterDao, masterDs, cmo.masterCwdOption.get, firstMasterMsg, lastMasterMsg)
                case MessagesMergeOption.Add(firstSlaveMsg, lastSlaveMsg) =>
                  batchLoadMsgsUntilInc(finalUsers, slaveDao, slaveDs, cmo.slaveCwdOption.get, firstSlaveMsg, lastSlaveMsg)
                case MessagesMergeOption.Replace(firstMasterMsg, lastMasterMsg, firstSlaveMsg, lastSlaveMsg) =>
                  // Treat exactly as Add
                  // TODO: Should we analyze content and make sure nothing is lost?
                  batchLoadMsgsUntilInc(finalUsers, slaveDao, slaveDs, cmo.slaveCwdOption.get, firstSlaveMsg, lastSlaveMsg)
                case _ => throw new MatchError("Impossible!")
              }.toStream.flatten
          }

          val toRootFile = masterDao.datasetRoot(newDs.uuid)
          for ((srcDsRoot, mb) <- messageBatches) {
            // Also copies files
            masterDao.insertMessages(srcDsRoot, chat, mb)
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

  /**
   * Treats master and slave messages as equal if their content mismatches, unless slave message has content and master message doesn't.
   * Needed for matching sequences.
   */
  private def matchesDisregardingContent(mm: TaggedMessage.M,
                                         mCwd: ChatWithDetails,
                                         sm: TaggedMessage.S,
                                         sCwd: ChatWithDetails): Boolean = {
    def hasNewContent(mc: WithPathFileOption, sc: WithPathFileOption): Boolean = {
       mc.pathFileOption(masterRoot).isEmpty && sc.pathFileOption(slaveRoot).isDefined && sc.pathFileOption(slaveRoot).get.exists
    }
    (mm.typed, sm.typed) match {
      case (mmRegular: Message.Typed.Regular, smRegular: Message.Typed.Regular) =>
        (mmRegular.value.contentOption, smRegular.value.contentOption) match {
          case (Some(mc), Some(sc)) if mc.hasPath && sc.hasPath && hasNewContent(mc, sc) =>
            // New information available, treat this as a mismatch
            false
          case _ =>
            val mmCmp = mm.copy(typed = Message.Typed.Regular(mmRegular.value.copy(contentOption = None)))
            val smCmp = sm.copy(typed = Message.Typed.Regular(smRegular.value.copy(contentOption = None)))
            (mmCmp, masterRoot, mCwd) =~= (smCmp, slaveRoot, sCwd)
        }
      case (Message.Typed.Service(Some(MessageServiceGroupEditPhoto(mmPhoto, _))),
            Message.Typed.Service(Some(MessageServiceGroupEditPhoto(smPhoto, _)))) =>
        !hasNewContent(mmPhoto, smPhoto)
      case _ =>
        (mm.asInstanceOf[Message], masterRoot, mCwd) =~= (sm, slaveRoot, sCwd)
    }
  }

  /** If message dates and plain strings are equal, we consider this enough */
  private val msgOrdering = new Ordering[Message] {
    override def compare(x: Message, y: Message): Int = {
      (x, y) match {
        case _ if x.time != y.time =>
          x.time compareTo y.time
        case _ if x.sourceIdOption.isDefined && y.sourceIdOption.isDefined =>
          x.sourceIdOption.get compareTo y.sourceIdOption.get
        case _ if x.searchableString == y.searchableString =>
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

  private case class MsgIterationContext(
    mmStream: Stream[TaggedMessage.M],
    prevMm:   Option[TaggedMessage.M],
    mCwd:     ChatWithDetails,
    smStream: Stream[TaggedMessage.S],
    prevSm:   Option[TaggedMessage.S],
    sCwd:     ChatWithDetails
  ) {
    def cmpMasterSlave(): Int = {
      msgOptionOrdering.compare(mmStream.headOption, smStream.headOption)
    }

    def advanceBoth(): MsgIterationContext =
      copy(
        mmStream = mmStream.tail,
        prevMm   = mmStream.headOption,
        smStream = smStream.tail,
        prevSm   = smStream.headOption
      )

    def advanceMaster(): MsgIterationContext =
      copy(
        mmStream = mmStream.tail,
        prevMm   = mmStream.headOption
      )

    def advanceSlave(): MsgIterationContext =
      copy(
        smStream = smStream.tail,
        prevSm   = smStream.headOption
      )
  }

  /** Fixup messages who have 'members' field, to make them comply with resolved/final user names. */
  def fixupMessageWithMembers(cwd: ChatWithDetails, finalUsers: Seq[User])(message: Message): Message = {
    def fixupMembers(members: Seq[String]): Seq[String] = {
      // Unresolved members are kept as-is.
      val resolvedUsers = cwd.resolveMembers(members)
      resolvedUsers.mapWithIndex((u, idx) =>
        finalUsers.find(fu => u.exists(_.id == fu.id)).map(_.prettyName).getOrElse(members(idx)))
    }

    def withTypedService(v: MessageService) = message.copy(typed = Message.Typed.Service(Some(v)))

    message.typed.service.flatten match {
      case Some(culprit: MessageServiceGroupCreate) =>
        withTypedService(culprit.copy(members = fixupMembers(culprit.members)))
      case Some(culprit: MessageServiceGroupInviteMembers) =>
        withTypedService(culprit.copy(members = fixupMembers(culprit.members)))
      case Some(culprit: MessageServiceGroupRemoveMembers) =>
        withTypedService(culprit.copy(members = fixupMembers(culprit.members)))
      case Some(culprit: MessageServiceGroupCall) =>
        withTypedService(culprit.copy(members = fixupMembers(culprit.members)))
      case _ =>
        message
    }
  }

  private def batchLoadMsgsUntilInc[TM <: TaggedMessage](
      finalUsers: Seq[User],
      dao: ChatHistoryDao,
      ds: Dataset,
      cwd: ChatWithDetails,
      firstMsg: TM,
      lastMsg: TM
  ): Stream[(DatasetRoot, IndexedSeq[Message])] = {
    takeMsgsFromBatchUntilInc(
      IndexedSeq(firstMsg) #:: messageBatchesStream(dao, cwd.chat, Some(firstMsg)),
      lastMsg
    ) map (mb => (dao.datasetRoot(ds.uuid), mb.map(fixupMessageWithMembers(cwd, finalUsers))))
  }

  private def takeMsgsFromBatchUntilInc[T <: TaggedMessage](
      stream: Stream[IndexedSeq[Message]],
      m: Message
  ): Stream[IndexedSeq[Message]] = {
    var lastFound = false
    stream.map { mb =>
      if (!lastFound) {
        mb.takeWhile { m2 =>
          val isLast = m2.sourceIdOption == m.sourceIdOption && m2.internalId == m.internalId
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

  /** Represents a single merge decision: a user that should be added, retained, merged (or skipped otherwise) */
  sealed abstract class UserMergeOption(val userToInsert: User)
  object UserMergeOption {
    case class Keep(masterUser: User)                     extends UserMergeOption(masterUser)
    case class Add(slaveUser: User)                       extends UserMergeOption(slaveUser)
    case class Replace(masterUser: User, slaveUser: User) extends UserMergeOption(slaveUser)
  }

  /** Represents a single merge decision: a chat that should be added, retained, merged (or skipped otherwise) */
  sealed abstract class ChatMergeOption(
     val masterCwdOption: Option[ChatWithDetails],
     val slaveCwdOption:  Option[ChatWithDetails]
  )
  object ChatMergeOption {
    case class Keep(masterCwd: ChatWithDetails) extends ChatMergeOption(Some(masterCwd), None)
    case class Add(slaveCwd: ChatWithDetails)   extends ChatMergeOption(None, Some(slaveCwd))
    case class Combine(
        masterCwd: ChatWithDetails,
        slaveCwd: ChatWithDetails,
        /** Serves as either mismatches (all options after analysis) or resolution (mismatches filtered by user) */
        messageMergeOptions: IndexedSeq[MessagesMergeOption]
    ) extends ChatMergeOption(Some(masterCwd), Some(slaveCwd))
  }

  // Message tagged types
  sealed trait MessageTag
  object MessageTag {
    sealed trait MasterMessageTag extends MessageTag
    sealed trait SlaveMessageTag  extends MessageTag
  }
  type TaggedMessage = Message with MessageTag
  object TaggedMessage {
    type M = Message with MessageTag.MasterMessageTag
    type S = Message with MessageTag.SlaveMessageTag
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
      override def firstMasterMsgOption: Option[TaggedMessage.M] = Some(firstMasterMsg)
      override def lastMasterMsgOption:  Option[TaggedMessage.M] = Some(lastMasterMsg)
    }

    case class Add(
        firstSlaveMsg: TaggedMessage.S,
        lastSlaveMsg: TaggedMessage.S
    ) extends MessagesMergeOption {
      override def firstMasterMsgOption: Option[TaggedMessage.M] = None
      override def lastMasterMsgOption:  Option[TaggedMessage.M] = None
      override def firstSlaveMsgOption:  Option[TaggedMessage.S] = Some(firstSlaveMsg)
      override def lastSlaveMsgOption:   Option[TaggedMessage.S] = Some(lastSlaveMsg)
    }

    case class Replace(
        firstMasterMsg: TaggedMessage.M,
        lastMasterMsg: TaggedMessage.M,
        firstSlaveMsg: TaggedMessage.S,
        lastSlaveMsg: TaggedMessage.S
    ) extends MessagesMergeOption {
      override def firstMasterMsgOption: Option[TaggedMessage.M] = Some(firstMasterMsg)
      override def lastMasterMsgOption:  Option[TaggedMessage.M] = Some(lastMasterMsg)
      override def firstSlaveMsgOption:  Option[TaggedMessage.S] = Some(firstSlaveMsg)
      override def lastSlaveMsgOption:   Option[TaggedMessage.S] = Some(lastSlaveMsg)

      def asKeep: Keep = Keep(firstMasterMsg, lastMasterMsg, firstSlaveMsgOption, lastSlaveMsgOption)
    }
  }
}
