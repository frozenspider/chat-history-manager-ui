package org.fs.chm.loader

import java.io.{File => JFile}

import io.grpc.ManagedChannel
import org.fs.chm.dao.GrpcChatHistoryDao
import org.fs.chm.protobuf._
import org.fs.utility.StopWatch

class GrpcRemoteDataLoader(channel: ManagedChannel) extends DataLoader[GrpcChatHistoryDao] {
  override protected def loadDataInner(path: JFile, createNew: Boolean): GrpcChatHistoryDao = {
    val request = ParseRequest(path = path.getAbsolutePath)
    log.info(s"Sending gRPC parse request (remote): ${request}")
    StopWatch.measureAndCall {
      val response: ParseReturnHandleResponse = GrpcDataLoaderHolder.wrapRequestNoParams {
        val blockingStub = HistoryLoaderServiceGrpc.blockingStub(channel)
        blockingStub.parseReturnHandle(request)
      }
      val loaded = response.file.get
      val rpcStub = ChatHistoryDaoServiceGrpc.blockingStub(channel)
      new GrpcChatHistoryDao(loaded.key, loaded.name + " (remote)", rpcStub)
    }((_, ms) => log.info(s"Telegram history loaded in ${ms} ms (via gRPC, remote)"))
  }
}

object GrpcRemoteDataLoader extends App {
  val holder = new GrpcDataLoaderHolder(50051)
  holder.remoteLoader
  println("Press ENTER to terminate...")
  System.in.read();
}
