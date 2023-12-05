package org.fs.chm.dao

import java.io.{File => JFile}

import com.github.nscala_time.time.Imports._
import org.fs.chm.dao.Entities._
import org.fs.chm.protobuf.Chat
import org.fs.chm.protobuf.Dataset
import org.fs.chm.protobuf.Message
import org.fs.chm.protobuf.PbUuid
import org.fs.chm.protobuf.User
import org.fs.chm.utility.LangUtils._
import org.fs.chm.utility.Logging
import org.fs.utility.StopWatch

/**
 * Everything except for messages should be pre-cached and readily available.
 * Should support equality.
 */
trait ChatHistoryDao extends AutoCloseable {
  sys.addShutdownHook(close())

  /** User-friendly name of a loaded data */
  def name: String

  /** Directory which stores eveything - including database itself at the root level. */
  def storagePath: JFile

  def datasets: Seq[Dataset]

  /** Directory which stores eveything in the dataset. All files are guaranteed to have this as a prefix */
  def datasetRoot(dsUuid: PbUuid): DatasetRoot

  /** List all files referenced by entities of this dataset. Some might not exist. */
  def datasetFiles(dsUuid: PbUuid): Set[JFile]

  def myself(dsUuid: PbUuid): User

  /** Contains myself as the first element. Order must be stable. Method is expected to be fast. */
  def users(dsUuid: PbUuid): Seq[User]

  def userOption(dsUuid: PbUuid, id: Long): Option[User] = {
    users(dsUuid).find(_.id == id)
  }

  def chats(dsUuid: PbUuid): Seq[ChatWithDetails]

  def chatOption(dsUuid: PbUuid, id: Long): Option[ChatWithDetails] = {
    chats(dsUuid).find(_.chat.id == id)
  }

  /** Return N messages after skipping first M of them. Trivial pagination in a nutshell. */
  def scrollMessages(chat: Chat, offset: Int, limit: Int): IndexedSeq[Message]

  def firstMessages(chat: Chat, limit: Int): IndexedSeq[Message] =
    scrollMessages(chat, 0, limit)

  def lastMessages(chat: Chat, limit: Int): IndexedSeq[Message]

  /**
   * Return N messages before the given one (inclusive).
   * Message must be present, so the result would contain at least one element.
   */
  final def messagesBefore(chat: Chat, msgId: MessageInternalId, limit: Int): IndexedSeq[Message] =
    messagesBeforeImpl(chat, msgId, limit).ensuring(seq =>
      seq.nonEmpty && seq.size <= limit && seq.last.internalId == msgId)

  protected def messagesBeforeImpl(chat: Chat, msgId: MessageInternalId, limit: Int): IndexedSeq[Message]

  /**
   * Return N messages after the given one (inclusive).
   * Message must be present, so the result would contain at least one element.
   */
  final def messagesAfter(chat: Chat, msgId: MessageInternalId, limit: Int): IndexedSeq[Message] =
    messagesAfterImpl(chat, msgId, limit).ensuring(seq =>
      seq.nonEmpty && seq.size <= limit && seq.head.internalId == msgId)

  protected def messagesAfterImpl(chat: Chat, msgId: MessageInternalId, limit: Int): IndexedSeq[Message]

  /**
   * Return N messages between the given ones (inclusive).
   * Messages must be present, so the result would contain at least one element (if both are the same message).
   */
  final def messagesSlice(chat: Chat, msgId1: MessageInternalId, msgId2: MessageInternalId): IndexedSeq[Message] =
    messagesSliceImpl(chat, msgId1, msgId2).ensuring(seq =>
      seq.nonEmpty && seq.head.internalId == msgId1 && seq.last.internalId == msgId2)

  protected def messagesSliceImpl(chat: Chat, msgId1: MessageInternalId, msgId2: MessageInternalId): IndexedSeq[Message]

  /**
   * Count messages between the given ones (inclusive).
   * Messages must be present.
   */
  def messagesSliceLength(chat: Chat, msgId1: MessageInternalId, msgId2: MessageInternalId): Int

  // /** Returns N messages before and N at-or-after the given date */
  // def messagesAroundDate(chat: Chat, date: DateTime, limit: Int): (IndexedSeq[Message], IndexedSeq[Message])

  def messageOption(chat: Chat, sourceId: MessageSourceId): Option[Message]

  def messageOptionByInternalId(chat: Chat, internalId: MessageInternalId): Option[Message]

  def isMutable: Boolean = this.isInstanceOf[MutableChatHistoryDao]

  override def close(): Unit = {}

  /** Whether given data path is the one loaded in this DAO */
  def isLoaded(storagePath: JFile): Boolean
}

object ChatHistoryDao extends Logging {
  private val BATCH_SIZE = 5_000

  def ensureDataSourcesAreEqual(srcDao: ChatHistoryDao, dstDao: ChatHistoryDao, dsUuid: PbUuid): Unit = {
    ensureDatasetsAreEqual(srcDao, srcDao, dsUuid, dsUuid)
  }

  def ensureDatasetsAreEqual(srcDao: ChatHistoryDao, dstDao: ChatHistoryDao, srcDsUuid: PbUuid, dstDsUuid: PbUuid): Unit = {
    StopWatch.measureAndCall {
      // Dataset
      val srcDsOption = srcDao.datasets.find(_.uuid == srcDsUuid)
      require(srcDsOption.isDefined, "Source dataset not found")
      val srcDs = srcDsOption.get;
      val dstDaoOption = dstDao.datasets.find(_.uuid == dstDsUuid)
      require(
        dstDaoOption.isDefined && dstDaoOption.isDefined,
        s"Dataset missing:\nWas    $srcDs\nBecame ${dstDaoOption getOrElse "<none>"}")
      val srcDsRoot = srcDao.datasetRoot(srcDsUuid)
      val dstDsRoot = dstDao.datasetRoot(dstDsUuid)

      // Users
      require(srcDao.myself(srcDsUuid) == dstDao.myself(dstDsUuid).copy(dsUuid = srcDsUuid),
        s"'myself' differs:\nWas    ${srcDao.myself(srcDsUuid)}\nBecame ${dstDao.myself(dstDsUuid)}")
      require(
        srcDao.users(srcDsUuid) == dstDao.users(dstDsUuid).map(_.copy(dsUuid = srcDsUuid)),
        s"Users differ:\nWas    ${srcDao.users(srcDsUuid)}\nBecame ${dstDao.users(dstDsUuid)}")
      val srcChats = srcDao.chats(srcDsUuid)
      val dstChats = dstDao.chats(dstDsUuid)
      require(srcChats.size == dstChats.size, s"Chat size differs:\nWas    ${srcChats.size}\nBecame ${dstChats.size}")
      for (((srcCwd, dstCwd), i) <- srcChats.zip(dstChats).zipWithIndex) {
        StopWatch.measureAndCall {
          log.info(s"Checking chat '${srcCwd.chat.nameOption.getOrElse("")}' with ${srcCwd.chat.msgCount} messages")
          require(srcCwd.chat == dstCwd.chat.copy(dsUuid = srcDsUuid),
            s"Chat #$i differs:\nWas    ${srcCwd.chat}\nBecame ${dstCwd.chat}")

          var offset = 0
          while (offset < srcCwd.chat.msgCount) {
            val srcMsgs = srcDao.scrollMessages(srcCwd.chat, offset, BATCH_SIZE)
            val dstMsgs = dstDao.scrollMessages(dstCwd.chat, offset, BATCH_SIZE)
            require(srcMsgs.nonEmpty && dstMsgs.nonEmpty,
              "Empty messages batch returned, either flawed batching logic or incorrect srcChat.msgCount")
            require(
              srcMsgs.size == dstMsgs.size,
              s"Messages size for chat ${srcCwd.chat.qualifiedName} (#$i) differs:\nWas    ${srcMsgs.size}\nBecame ${dstMsgs.size}")
            for (((m1, m2), j) <- srcMsgs.zip(dstMsgs).zipWithIndex) {
              require((m1, srcDsRoot, srcCwd) =~= (m2, dstDsRoot, dstCwd),
                s"Message #$j for chat ${srcCwd.chat.qualifiedName} (#$i) differs:\nWas    $m1\nBecame $m2")
            }
            offset += srcMsgs.length
          }
        }((_, t) => log.info(s"Chat checked in $t ms"))
      }
    }((_, t) => log.info(s"Dataset checked in $t ms"))
  }
}

trait MutableChatHistoryDao extends ChatHistoryDao {
  def renameDataset(dsUuid: PbUuid, newName: String): Dataset

  def deleteDataset(dsUuid: PbUuid): Unit

  /** Shift time of all timestamps in the dataset to accommodate timezone differences */
  def shiftDatasetTime(dsUuid: PbUuid, hrs: Int): Unit

  /** Sets the data (names and phone only) for a user with the given `id` and `dsUuid` to the given state */
  def updateUser(user: User): Unit

  def deleteChat(chat: Chat): Unit

  /** Create a backup, if enabled, otherwise do nothing */
  def backup(): Unit
}
