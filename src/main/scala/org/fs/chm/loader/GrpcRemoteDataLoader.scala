package org.fs.chm.loader

import java.io.{File => JFile}

import io.grpc.ManagedChannel
import org.fs.chm.dao.GrpcChatHistoryDao
import org.fs.chm.protobuf._
import org.fs.utility.StopWatch

class GrpcRemoteDataLoader(channel: ManagedChannel) extends DataLoader[GrpcChatHistoryDao] {
  override protected def loadDataInner(path: JFile, createNew: Boolean): GrpcChatHistoryDao = {
    val request = ParseLoadRequest(path = path.getAbsolutePath)
    log.info(s"Sending gRPC parse request (remote): ${request}")
    StopWatch.measureAndCall {
      val response: LoadResponse = GrpcDataLoaderHolder.wrapRequestNoParams {
        val blockingStub = HistoryLoaderServiceGrpc.blockingStub(channel)
        blockingStub.load(request)
      }
      val loaded = response.file.get
      val rpcStub = HistoryLoaderServiceGrpc.blockingStub(channel)
      new GrpcChatHistoryDao(loaded.key, loaded.name + " (remote)", rpcStub)
    }((_, ms) => log.info(s"Remote history loaded in ${ms} ms"))
  }
}

object GrpcRemoteDataLoader extends App {
  val holder = new GrpcDataLoaderHolder(50051, new GrpcDaoService(f => ???))
  holder.remoteLoader
  println("Press ENTER to terminate...")
  System.in.read();
}
