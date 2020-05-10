package org.fs.chm.dao.merge

import java.util.UUID

import org.fs.chm.dao._

class DatasetMerger(
    masterDao: MutableChatHistoryDao,
    masterDs: Dataset,
    slaveDao: ChatHistoryDao,
    slaveDs: Dataset
) {
  import org.fs.chm.dao.merge.DatasetMerger._

  def merge(
      usersToMerge: Map[User, User],
      chatsToMerge: Seq[ChatMergeOption],
      chatsMergeResolutions: Seq[(Chat, Chat, Map[MessagesMismatch, MismatchResolution])]
  ): Dataset = {
    try {
      masterDao.disableBackups()
      val newDs = Dataset(
        uuid = UUID.randomUUID(),
        alias = masterDs.alias + " (merged)",
        sourceType = masterDs.sourceType
      )

      // Users
      // FIXME: Make UserMergeOption!
      val masterUsers   = masterDao.users(masterDs.uuid)
      val masterUserIds = masterUsers.map(_.id).toSet
      val slaveUsers    = slaveDao.users(slaveDs.uuid)
      val newUsers      = slaveUsers.filter(su => !masterUserIds.contains(su.id))
      for (sourceUser <- (masterUsers ++ newUsers)) {
        val finalUser = usersToMerge.getOrElse(sourceUser, sourceUser).copy(dsUuid = newDs.uuid)
        masterDao.insertUser(finalUser)
      }

      // Chats
      chatsToMerge foreach {
        // FIXME: We also want retains!
        case ChatMergeOption.Retain(mc) =>
          masterDao.insertChat(mc.copy(dsUuid = newDs.uuid))
        case ChatMergeOption.Add(sc) =>
          masterDao.insertChat(sc.copy(dsUuid = newDs.uuid))
        case ChatMergeOption.Combine(mc, sc) =>
          // We're not interested in master chat here
          masterDao.insertChat(sc.copy(dsUuid = newDs.uuid))
      }

      // Messages
      chatsMergeResolutions foreach {
        case (mc, sc, resolutions) => ??? // merger.mergeChats(newDs, mc, sc, resolutions)
      }

      newDs
    } finally {
      masterDao.enableBackups()
    }
  }
}

object DatasetMerger {

  /** Represents a single general merge option: a chat that should be added or merged (or skipped if no decision) */
  sealed trait ChatMergeOption
  sealed trait ChangedChatMergeOption extends ChatMergeOption
  object ChatMergeOption {
    case class Combine(masterChat: Chat, slaveChat: Chat) extends ChangedChatMergeOption
    case class Add(slaveChat: Chat)                       extends ChangedChatMergeOption
    case class Retain(masterChat: Chat)                   extends ChatMergeOption
  }

  /** Represents a single general merge option: a chat that should be added or merged (or skipped if no decision) */
  sealed trait UserMergeOption
  object UserMergeOption {
    case class Combine(masterUser: User, slaveChat: User) extends UserMergeOption
    case class Add(slaveUser: User)                       extends UserMergeOption
    case class Retain(masterUser: User)                   extends UserMergeOption
  }

  // Message tagged types
  sealed trait TaggedMessage
  object TaggedMessage {
    sealed trait MasterMessageTag extends TaggedMessage
    sealed trait SlaveMessageTag  extends TaggedMessage

    type M = Message with MasterMessageTag
    type S = Message with SlaveMessageTag
  }

  sealed trait MessagesMismatch {
    def prevMasterMsgOption: Option[TaggedMessage.M]
    def firstMasterMsgOption: Option[TaggedMessage.M]
    def lastMasterMsgOption: Option[TaggedMessage.M]
    def nextMasterMsgOption: Option[TaggedMessage.M]
    protected def slaveMsgs: (TaggedMessage.S, TaggedMessage.S)
    def prevSlaveMsgOption: Option[TaggedMessage.S]
    def firstSlaveMsg: TaggedMessage.S = slaveMsgs._1
    def lastSlaveMsg:  TaggedMessage.S = slaveMsgs._2
    def nextSlaveMsgOption: Option[TaggedMessage.S]
  }

  object MessagesMismatch {
    case class Addition(
        prevMasterMsgOption: Option[TaggedMessage.M],
        nextMasterMsgOption: Option[TaggedMessage.M],
        prevSlaveMsgOption: Option[TaggedMessage.S],
        /** First and last */
        slaveMsgs: (TaggedMessage.S, TaggedMessage.S),
        nextSlaveMsgOption: Option[TaggedMessage.S]
    ) extends MessagesMismatch {
      override def firstMasterMsgOption = None
      override def lastMasterMsgOption  = None
    }

    case class Conflict(
        prevMasterMsgOption: Option[TaggedMessage.M],
        masterMsgs: (TaggedMessage.M, TaggedMessage.M),
        nextMasterMsgOption: Option[TaggedMessage.M],
        prevSlaveMsgOption: Option[TaggedMessage.S],
        /** First and last */
        slaveMsgs: (TaggedMessage.S, TaggedMessage.S),
        nextSlaveMsgOption: Option[TaggedMessage.S]
    ) extends MessagesMismatch {
      def firstMasterMsg: TaggedMessage.M = masterMsgs._1
      def lastMasterMsg:  TaggedMessage.M = masterMsgs._2
      override def firstMasterMsgOption = Some(firstMasterMsg)
      override def lastMasterMsgOption  = Some(lastMasterMsg)
    }
  }

  sealed trait MismatchResolution
  object MismatchResolution {
    case object Apply  extends MismatchResolution
    case object Reject extends MismatchResolution
  }
}
