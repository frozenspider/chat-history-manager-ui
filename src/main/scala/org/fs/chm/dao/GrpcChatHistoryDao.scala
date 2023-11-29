package org.fs.chm.dao

import java.io.File

import org.fs.chm.dao.Entities._
import org.fs.chm.protobuf.HistoryDaoServiceGrpc._
import org.fs.chm.protobuf._
import org.fs.chm.utility.Logging
import org.fs.chm.loader.GrpcDataLoaderHolder.wrapRequest

/** Acts as a remote history DAO */
class GrpcChatHistoryDao(val key: String, override val name: String, rpcStub: HistoryDaoServiceBlockingStub)
  extends MutableChatHistoryDao with Logging {

  override lazy val storagePath: File = {
    new File(wrapRequest(StoragePathRequest(key))(rpcStub.storagePath).path)
  }

  override lazy val datasets: Seq[Dataset] = {
    wrapRequest(DatasetsRequest(key))(rpcStub.datasets).datasets
  }

  override def datasetRoot(dsUuid: PbUuid): DatasetRoot = {
    new File(wrapRequest(DatasetRootRequest(key, dsUuid))(rpcStub.datasetRoot).path).asInstanceOf[DatasetRoot]
  }

  override def datasetFiles(dsUuid: PbUuid): Set[File] = ???

  override def myself(dsUuid: PbUuid): User = {
    wrapRequest(MyselfRequest(key, dsUuid))(rpcStub.myself).myself
  }

  override def users(dsUuid: PbUuid): Seq[User] = {
    wrapRequest(UsersRequest(key, dsUuid))(rpcStub.users).users
  }

  override def chats(dsUuid: PbUuid): Seq[Entities.ChatWithDetails] = {
    val cwds = wrapRequest(ChatsRequest(key, dsUuid))(rpcStub.chats).cwds
    cwds.map(cwd => Entities.ChatWithDetails(cwd.chat, cwd.lastMsgOption, cwd.members))
  }

  override def scrollMessages(chat: Chat, offset: Int, limit: Int): IndexedSeq[Message] = {
    wrapRequest(ScrollMessagesRequest(key, chat, offset, limit))(rpcStub.scrollMessages).messages.toIndexedSeq
  }

  override def lastMessages(chat: Chat, limit: Int): IndexedSeq[Message] = {
    wrapRequest(LastMessagesRequest(key, chat, limit))(rpcStub.lastMessages).messages.toIndexedSeq
  }

  override protected def messagesBeforeImpl(chat: Chat, msgId: MessageInternalId, limit: Int): IndexedSeq[Message] = {
    // Due to different conventions, we query the message itself separately
    val msg = wrapRequest(MessageOptionByInternalIdRequest(key, chat, msgId))(rpcStub.messageOptionByInternalId).message.get
    val msgs = wrapRequest(MessagesBeforeRequest(key, chat, msgId, limit))(rpcStub.messagesBefore).messages
    (msgs :+ msg).toIndexedSeq.takeRight(limit)
  }

  override protected def messagesAfterImpl(chat: Chat, msgId: MessageInternalId, limit: Int): IndexedSeq[Message] = {
    // Due to different conventions, we query the message itself separately
    val msg = wrapRequest(MessageOptionByInternalIdRequest(key, chat, msgId))(rpcStub.messageOptionByInternalId).message.get
    val msgs = wrapRequest(MessagesAfterRequest(key, chat, msgId, limit))(rpcStub.messagesAfter).messages
    (msg +: msgs).toIndexedSeq.take(limit)
  }

  override protected def messagesSliceImpl(chat: Chat, msgId1: MessageInternalId, msgId2: MessageInternalId): IndexedSeq[Message] = {
    wrapRequest(MessagesSliceRequest(key, chat, msgId1, msgId2))(rpcStub.messagesSlice).messages.toIndexedSeq
  }

  override def messagesSliceLength(chat: Chat, msgId1: MessageInternalId, msgId2: MessageInternalId): Int = {
    wrapRequest(MessagesSliceRequest(key, chat, msgId1, msgId2))(rpcStub.messagesSliceLen).messagesCount
  }

  override def messageOption(chat: Chat, id: MessageSourceId): Option[Message] = {
    wrapRequest(MessageOptionRequest(key, chat, id))(rpcStub.messageOption).message
  }

  override def messageOptionByInternalId(chat: Chat, internalId: MessageInternalId): Option[Message] = {
    wrapRequest(MessageOptionRequest(key, chat, internalId))(rpcStub.messageOption).message
  }

  override def isLoaded(storagePath: File): Boolean = {
    wrapRequest(IsLoadedRequest(key, storagePath.getAbsolutePath))(rpcStub.isLoaded).isLoaded
  }

  override def close(): Unit = {
    if (!wrapRequest(CloseRequest(key))(rpcStub.close).success) {
      log.warn(s"Failed to close remote DAO '${name}'!")
    }
  }

  def saveAsRemote(file: File): GrpcChatHistoryDao = {
    val loaded = wrapRequest(SaveAsRequest(key, file.getName))(rpcStub.saveAs)
    new GrpcChatHistoryDao(loaded.key, loaded.name, rpcStub)
  }

  override def insertDataset(ds: Dataset): Unit = ???

  override def renameDataset(dsUuid: PbUuid, newName: String): Dataset = ???

  override def deleteDataset(dsUuid: PbUuid): Unit = ???

  /** Shift time of all timestamps in the dataset to accommodate timezone differences */
  override def shiftDatasetTime(dsUuid: PbUuid, hrs: Int): Unit = ???

  /** Insert a new user. It should not yet exist */
  override def insertUser(user: User, isMyself: Boolean): Unit = ???

  /** Sets the data (names and phone only) for a user with the given `id` and `dsUuid` to the given state */
  override def updateUser(user: User): Unit = ???

  /**
   * Merge absorbed user into base user, moving its personal chat messages into base user personal chat.
   */
  override def mergeUsers(baseUser: User, absorbedUser: User): Unit = ???

  /**
   * Insert a new chat.
   * It should have a proper DS UUID set, should not yet exist, and all users must already be inserted.
   * Content will be resolved based on the given dataset root and copied accordingly.
   */
  override def insertChat(srcDsRoot: DatasetRoot, chat: Chat): Unit = ???

  override def deleteChat(chat: Chat): Unit = ???

  /**
   * Insert a new message for the given chat.
   * Internal ID will be ignored.
   * Content will be resolved based on the given dataset root and copied accordingly.
   */
  override def insertMessages(srcDsRoot: DatasetRoot, chat: Chat, msgs: Seq[Message]): Unit = ???

  /** Don't do automatic backups on data changes until re-enabled */
  override def disableBackups(): Unit = ???

  /** Start doing backups automatically once again */
  override def enableBackups(): Unit = ???

  /** Create a backup, if enabled, otherwise do nothing */
  override def backup(): Unit = ???
}
