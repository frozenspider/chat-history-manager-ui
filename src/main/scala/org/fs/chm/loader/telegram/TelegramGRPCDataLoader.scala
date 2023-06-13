package org.fs.chm.loader.telegram

import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import org.fs.chm.dao.EagerChatHistoryDao
import org.fs.chm.protobuf._
import org.fs.utility.StopWatch

import java.io.File

class TelegramGRPCDataLoader(rpcPort: Int) extends TelegramDataLoader {
  private val channel: ManagedChannel = ManagedChannelBuilder
    .forAddress("127.0.0.1", rpcPort)
    .maxInboundMessageSize(Integer.MAX_VALUE)
    .usePlaintext()
    .build()

  override def doesLookRight(rootFile: File): Option[String] = {
    checkFormatLooksRight(rootFile, Seq()) // Any field works for us
  }

  override protected def loadDataInner(path: File, createNew: Boolean): EagerChatHistoryDao = {
    val request = ParseJsonFileRequest(path = path.getAbsolutePath)
    log.info(s"Sending gRPC parse request: ${request}")
    StopWatch.measureAndCall {
      val blockingStub = JsonLoaderGrpc.blockingStub(channel)
      val response: ParseJsonFileResponse = blockingStub.parseJsonFile(request)
      ???
    }((_, t) => log.info(s"Request processed in $t ms"))
  }
}
