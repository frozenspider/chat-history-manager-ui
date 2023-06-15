package org.fs.chm.loader.telegram

import java.io.{File => JFile}

import scala.collection.immutable.ListMap

import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import org.fs.chm.dao.EagerChatHistoryDao
import org.fs.chm.dao.Entities._
import org.fs.chm.protobuf._
import org.fs.utility.StopWatch

class TelegramGRPCDataLoader(rpcPort: Int) extends TelegramDataLoader {
  private val channel: ManagedChannel = ManagedChannelBuilder
    .forAddress("127.0.0.1", rpcPort)
    .maxInboundMessageSize(Integer.MAX_VALUE)
    .usePlaintext()
    .build()

  override def doesLookRight(rootFile: JFile): Option[String] = {
    checkFormatLooksRight(rootFile, Seq()) // Any field works for us
  }

  override protected def loadDataInner(path: JFile, createNew: Boolean): EagerChatHistoryDao = {
    val request = ParseJsonFileRequest(path = path.getAbsolutePath)
    log.info(s"Sending gRPC parse request: ${request}")
    StopWatch.measureAndCall {
      val blockingStub = JsonLoaderGrpc.blockingStub(channel)
      val response: ParseJsonFileResponse = blockingStub.parseJsonFile(request)
      val root = new JFile(response.rootFile).getAbsoluteFile
      require(root.exists, s"Dataset root ${root} does not exist!")
      val chatsWithMessagesLM: ListMap[Chat, IndexedSeq[Message]] =
        ListMap.from(response.cwm.map(cwm => cwm.chat -> cwm.messages.map { m =>
          val textWithSs = m.text.map(rte => {
            rte.copy(searchableString = Some(makeSearchableString(rte)))
          })
          m.copy(
            searchableString = Some(makeSearchableString(textWithSs, m.typed)),
            text             = textWithSs
          )
        }.toIndexedSeq))
      new EagerChatHistoryDao(
        name               = "Telegram export data from " + root.getName,
        _dataRootFile      = root,
        dataset            = response.ds,
        myself1            = response.myself,
        users1             = response.users,
        _chatsWithMessages = chatsWithMessagesLM
      )
    }((_, ms) => log.info(s"Telegram history loaded in ${ms} ms (via gRPC)"))
  }
}
