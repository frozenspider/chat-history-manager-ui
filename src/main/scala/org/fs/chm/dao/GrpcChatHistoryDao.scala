package org.fs.chm.dao

import java.io.File

import org.fs.chm.dao.Entities._
import org.fs.chm.protobuf.HistoryDaoServiceGrpc._
import org.fs.chm.protobuf.HistoryLoaderServiceGrpc.HistoryLoaderServiceBlockingStub
import org.fs.chm.protobuf._
import org.fs.chm.utility.Logging
import org.fs.chm.utility.RpcUtils._

/** Acts as a remote history DAO */
class GrpcChatHistoryDao(val key: String,
                         initial_name: String,
                         daoRpcStub: HistoryDaoServiceBlockingStub,
                         loaderRpcStub: HistoryLoaderServiceBlockingStub)
  extends MutableChatHistoryDao with Logging {

  private var backupsEnabled = true

  override val name: String = initial_name + " (remote)"

  override lazy val storagePath: File = {
    new File(sendRequest(StoragePathRequest(key))(daoRpcStub.storagePath).path)
  }

  override def datasets: Seq[Dataset] = {
    sendRequest(DatasetsRequest(key))(daoRpcStub.datasets).datasets
  }

  override def datasetRoot(dsUuid: PbUuid): DatasetRoot = {
    new File(sendRequest(DatasetRootRequest(key, dsUuid))(daoRpcStub.datasetRoot).path).asInstanceOf[DatasetRoot]
  }

  override def datasetFiles(dsUuid: PbUuid): Set[File] = {
    throw new UnsupportedOperationException("GrpcChatHistoryDao does not support datasetFiles!")
  }

  override def myself(dsUuid: PbUuid): User = {
    sendRequest(MyselfRequest(key, dsUuid))(daoRpcStub.myself).myself
  }

  override def users(dsUuid: PbUuid): Seq[User] = {
    sendRequest(UsersRequest(key, dsUuid))(daoRpcStub.users).users
  }

  override def chats(dsUuid: PbUuid): Seq[Entities.ChatWithDetails] = {
    val cwds = sendRequest(ChatsRequest(key, dsUuid))(daoRpcStub.chats).cwds
    cwds.map(cwd => Entities.ChatWithDetails(cwd.chat, cwd.lastMsgOption, cwd.members))
  }

  override def scrollMessages(chat: Chat, offset: Int, limit: Int): IndexedSeq[Message] = {
    sendRequest(ScrollMessagesRequest(key, chat, offset, limit))(daoRpcStub.scrollMessages).messages.toIndexedSeq
  }

  override def lastMessages(chat: Chat, limit: Int): IndexedSeq[Message] = {
    sendRequest(LastMessagesRequest(key, chat, limit))(daoRpcStub.lastMessages).messages.toIndexedSeq
  }

  override protected def messagesBeforeImpl(chat: Chat, msgId: MessageInternalId, limit: Int): IndexedSeq[Message] = {
    // Due to different conventions, we query the message itself separately
    val msg = sendRequest(MessageOptionByInternalIdRequest(key, chat, msgId))(daoRpcStub.messageOptionByInternalId).message.get
    val msgs = sendRequest(MessagesBeforeRequest(key, chat, msgId, limit))(daoRpcStub.messagesBefore).messages
    (msgs :+ msg).toIndexedSeq.takeRight(limit)
  }

  override protected def messagesAfterImpl(chat: Chat, msgId: MessageInternalId, limit: Int): IndexedSeq[Message] = {
    // Due to different conventions, we query the message itself separately
    val msg = sendRequest(MessageOptionByInternalIdRequest(key, chat, msgId))(daoRpcStub.messageOptionByInternalId).message.get
    val msgs = sendRequest(MessagesAfterRequest(key, chat, msgId, limit))(daoRpcStub.messagesAfter).messages
    (msg +: msgs).toIndexedSeq.take(limit)
  }

  override protected def messagesSliceImpl(chat: Chat, msgId1: MessageInternalId, msgId2: MessageInternalId): IndexedSeq[Message] = {
    sendRequest(MessagesSliceRequest(key, chat, msgId1, msgId2))(daoRpcStub.messagesSlice).messages.toIndexedSeq
  }

  override def messagesSliceLength(chat: Chat, msgId1: MessageInternalId, msgId2: MessageInternalId): Int = {
    sendRequest(MessagesSliceRequest(key, chat, msgId1, msgId2))(daoRpcStub.messagesSliceLen).messagesCount
  }

  override def messageOption(chat: Chat, id: MessageSourceId): Option[Message] = {
    sendRequest(MessageOptionRequest(key, chat, id))(daoRpcStub.messageOption).message
  }

  override def messageOptionByInternalId(chat: Chat, internalId: MessageInternalId): Option[Message] = {
    sendRequest(MessageOptionRequest(key, chat, internalId))(daoRpcStub.messageOption).message
  }

  override def isLoaded(storagePath: File): Boolean = {
    sendRequest(IsLoadedRequest(key, storagePath.getAbsolutePath))(daoRpcStub.isLoaded).isLoaded
  }

  override def close(): Unit = {
    if (!sendRequest(CloseRequest(key))(loaderRpcStub.close).success) {
      log.warn(s"Failed to close remote DAO '${name}'!")
    }
  }

  def saveAsRemote(newName: String): GrpcChatHistoryDao = {
    val loaded = sendRequest(SaveAsRequest(key, newName))(daoRpcStub.saveAs)
    new GrpcChatHistoryDao(loaded.key, loaded.name, daoRpcStub, loaderRpcStub)
  }

  override def renameDataset(dsUuid: PbUuid, newName: String): Dataset = {
    if (backupsEnabled) this.backup()
    sendRequest(UpdateDatasetRequest(key, Dataset(dsUuid, newName)))(daoRpcStub.updateDataset).dataset
  }

  override def deleteDataset(dsUuid: PbUuid): Unit = {
    if (backupsEnabled) this.backup()
    sendRequest(DeleteDatasetRequest(key, dsUuid))(daoRpcStub.deleteDataset)
  }

  /** Shift time of all timestamps in the dataset to accommodate timezone differences */
  override def shiftDatasetTime(dsUuid: PbUuid, hrs: Int): Unit = {
    if (backupsEnabled) this.backup()
    sendRequest(ShiftDatasetTimeRequest(key, dsUuid, hrs))(daoRpcStub.shiftDatasetTime)
  }

  override def updateUser(user: User): Unit = {
    if (backupsEnabled) this.backup()
    sendRequest(UpdateUserRequest(key, user))(daoRpcStub.updateUser)
  }

  override def deleteChat(chat: Chat): Unit = {
    if (backupsEnabled) this.backup()
    sendRequest(DeleteChatRequest(key, chat))(daoRpcStub.deleteChat)
  }

  /** Create a backup, if enabled, otherwise do nothing */
  override def backup(): Unit = {
    sendRequest(BackupRequest(key))(daoRpcStub.backup)
  }
}
