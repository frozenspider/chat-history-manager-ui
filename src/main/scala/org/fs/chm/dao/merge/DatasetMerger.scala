package org.fs.chm.dao.merge

import org.fs.chm.dao.Entities._
import org.fs.chm.dao.MutableChatHistoryDao
import org.fs.chm.protobuf._
import org.fs.chm.utility.Logging

trait DatasetMerger extends Logging {
  import DatasetMerger._

  /**
   * Analyze dataset mergeability, amending `ChatMergeOption.Combine` with mismatches in order.
   * Other `ChatMergeOption`s are returned unchanged.
   * Note that we can only detect conflicts if data source supports source IDs.
   */
  def analyze(masterCwd: ChatWithDetails, slaveCwd: ChatWithDetails, title: String): IndexedSeq[MessagesMergeDiff]

  def merge(explicitUsersToMerge: Seq[UserMergeOption],
            chatsToMerge: Seq[ResolvedChatMergeOption],
            newDao: MutableChatHistoryDao): Dataset

}

object DatasetMerger {
  /** Represents a single merge decision: a user that should be added, retained, merged (or skipped otherwise) */
  sealed abstract class UserMergeOption(val userToInsert: User)

  object UserMergeOption {
    case class Keep(masterUser: User) extends UserMergeOption(masterUser)

    case class Add(slaveUser: User) extends UserMergeOption(slaveUser)

    case class Replace(masterUser: User, slaveUser: User) extends UserMergeOption(slaveUser)
  }

  /** Represents a single merge decision: a chat that should be added, retained, merged (or skipped otherwise) */
  sealed trait ChatMergeOption {
    def masterCwdOption: Option[ChatWithDetails]

    def slaveCwdOption: Option[ChatWithDetails]

    def title: String = {
      val cwd = if (slaveCwdOption.isDefined) slaveCwdOption.get else masterCwdOption.get
      s"'${cwd.chat.nameOrUnnamed}' (${cwd.chat.msgCount} messages)"
    }
  }

  sealed trait SelectedChatMergeOption extends ChatMergeOption

  sealed trait AnalyzedChatMergeOption extends ChatMergeOption

  sealed trait ResolvedChatMergeOption extends ChatMergeOption

  sealed abstract class AbstractChatMergeOption(
     override val masterCwdOption: Option[ChatWithDetails],
     override val slaveCwdOption: Option[ChatWithDetails]
   ) extends ChatMergeOption

  object ChatMergeOption {
    case class Keep(masterCwd: ChatWithDetails) extends AbstractChatMergeOption(Some(masterCwd), None)
      with SelectedChatMergeOption with AnalyzedChatMergeOption with ResolvedChatMergeOption

    case class Add(slaveCwd: ChatWithDetails) extends AbstractChatMergeOption(None, Some(slaveCwd))
      with SelectedChatMergeOption with AnalyzedChatMergeOption with ResolvedChatMergeOption

    case class SelectedCombine(
      masterCwd: ChatWithDetails,
      slaveCwd: ChatWithDetails,
    ) extends AbstractChatMergeOption(Some(masterCwd), Some(slaveCwd)) with SelectedChatMergeOption {
      def analyzed(analysis: IndexedSeq[MessagesMergeDiff]): AnalyzedCombine =
        AnalyzedCombine(masterCwd, slaveCwd, analysis)
    }

    case class AnalyzedCombine(
      masterCwd: ChatWithDetails,
      slaveCwd: ChatWithDetails,
      analysis: IndexedSeq[MessagesMergeDiff]
    ) extends AbstractChatMergeOption(Some(masterCwd), Some(slaveCwd)) with AnalyzedChatMergeOption {
      def resolved(resoluion: IndexedSeq[MessagesMergeDecision]): ResolvedCombine =
        ResolvedCombine(masterCwd, slaveCwd, resoluion)

      def resolveAsIs: ResolvedCombine =
        resolved(analysis)
    }

    case class ResolvedCombine(
      masterCwd: ChatWithDetails,
      slaveCwd: ChatWithDetails,
      resoluion: IndexedSeq[MessagesMergeDecision]
    ) extends AbstractChatMergeOption(Some(masterCwd), Some(slaveCwd)) with ResolvedChatMergeOption
  }

  // Message tagged types
  sealed trait MessageTag

  object MessageTag {
    sealed trait MasterMessageTag extends MessageTag

    sealed trait SlaveMessageTag extends MessageTag
  }

  type TaggedMessage = Message with MessageTag

  object TaggedMessage {
    type M = Message with MessageTag.MasterMessageTag
    type S = Message with MessageTag.SlaveMessageTag
  }

  /** Represents a single merge resolution decision - same as `MessagesMergeDiff`, but with `DontReplace` added */
  sealed trait MessagesMergeDecision {
    def firstMasterMsgOption: Option[TaggedMessage.M]

    def lastMasterMsgOption: Option[TaggedMessage.M]

    def firstSlaveMsgOption: Option[TaggedMessage.S]

    def lastSlaveMsgOption: Option[TaggedMessage.S]
  }

  /** Represents a single merge diff: a messages that should be added, retained, merged (or skipped otherwise) */
  sealed trait MessagesMergeDiff extends MessagesMergeDecision

  object MessagesMergeDiff {
    /** Content is only present in master */
    case class Retain(
      firstMasterMsg: TaggedMessage.M,
      lastMasterMsg: TaggedMessage.M
    ) extends MessagesMergeDiff {
      override def firstMasterMsgOption: Option[TaggedMessage.M] = Some(firstMasterMsg)

      override def lastMasterMsgOption: Option[TaggedMessage.M] = Some(lastMasterMsg)

      override def firstSlaveMsgOption: Option[TaggedMessage.S] = None

      override def lastSlaveMsgOption: Option[TaggedMessage.S] = None
    }

    /** Content is only present in slave */
    case class Add(
      firstSlaveMsg: TaggedMessage.S,
      lastSlaveMsg: TaggedMessage.S
    ) extends MessagesMergeDiff {
      override def firstMasterMsgOption: Option[TaggedMessage.M] = None

      override def lastMasterMsgOption: Option[TaggedMessage.M] = None

      override def firstSlaveMsgOption: Option[TaggedMessage.S] = Some(firstSlaveMsg)

      override def lastSlaveMsgOption: Option[TaggedMessage.S] = Some(lastSlaveMsg)
    }

    /** Content is present in both */
    case class Replace(
      firstMasterMsg: TaggedMessage.M,
      lastMasterMsg: TaggedMessage.M,
      firstSlaveMsg: TaggedMessage.S,
      lastSlaveMsg: TaggedMessage.S
    ) extends MessagesMergeDiff {
      override def firstMasterMsgOption: Option[TaggedMessage.M] = Some(firstMasterMsg)

      override def lastMasterMsgOption: Option[TaggedMessage.M] = Some(lastMasterMsg)

      override def firstSlaveMsgOption: Option[TaggedMessage.S] = Some(firstSlaveMsg)

      override def lastSlaveMsgOption: Option[TaggedMessage.S] = Some(lastSlaveMsg)

      def asDontReplace: DontReplace = DontReplace(firstMasterMsg, lastMasterMsg, firstSlaveMsg, lastSlaveMsg)
    }

    case class DontReplace(
      firstMasterMsg: TaggedMessage.M,
      lastMasterMsg: TaggedMessage.M,
      firstSlaveMsg: TaggedMessage.S,
      lastSlaveMsg: TaggedMessage.S
    ) extends MessagesMergeDecision {
      override def firstMasterMsgOption: Option[TaggedMessage.M] = Some(firstMasterMsg)

      override def lastMasterMsgOption: Option[TaggedMessage.M] = Some(lastMasterMsg)

      override def firstSlaveMsgOption: Option[TaggedMessage.S] = Some(firstSlaveMsg)

      override def lastSlaveMsgOption: Option[TaggedMessage.S] = Some(lastSlaveMsg)
    }

    /** Master and slave has matching messages - but allows content on one/both sides to be missing */
    case class Match(
      firstMasterMsg: TaggedMessage.M,
      lastMasterMsg: TaggedMessage.M,
      firstSlaveMsg: TaggedMessage.S,
      lastSlaveMsg: TaggedMessage.S
    ) extends MessagesMergeDiff {
      override def firstMasterMsgOption: Option[TaggedMessage.M] = Some(firstMasterMsg)

      override def lastMasterMsgOption: Option[TaggedMessage.M] = Some(lastMasterMsg)

      override def firstSlaveMsgOption: Option[TaggedMessage.S] = Some(firstSlaveMsg)

      override def lastSlaveMsgOption: Option[TaggedMessage.S] = Some(lastSlaveMsg)
    }
  }
}
