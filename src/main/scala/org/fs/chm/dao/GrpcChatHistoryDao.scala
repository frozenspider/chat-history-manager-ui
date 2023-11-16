package org.fs.chm.dao

import java.io.File

import org.fs.chm.dao.Entities._
import org.fs.chm.protobuf.ChatHistoryDaoServiceGrpc._
import org.fs.chm.protobuf._
import org.fs.chm.utility.Logging
import org.fs.chm.loader.GrpcDataLoaderHolder.wrapRequest

class GrpcChatHistoryDao(key: String, override val name: String, rpcStub: ChatHistoryDaoServiceBlockingStub)
  extends ChatHistoryDao with Logging {

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

  override def messageOption(chat: Chat, source_id: MessageSourceId): Option[Message] = {
    wrapRequest(MessageOptionRequest(key, chat, source_id))(rpcStub.messageOption).message
  }

  override def isLoaded(storagePath: File): Boolean = {
    wrapRequest(IsLoadedRequest(key, storagePath.getAbsolutePath))(rpcStub.isLoaded).isLoaded
  }

  override def close(): Unit = {
    if (!wrapRequest(CloseRequest(key))(rpcStub.close).success) {
      log.warn(s"Failed to close remote DAO '${name}'!")
    }
  }
}
