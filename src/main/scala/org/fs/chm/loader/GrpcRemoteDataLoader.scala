package org.fs.chm.loader

import java.io.{File => JFile}

import io.grpc.ManagedChannel
import org.fs.chm.dao.GrpcChatHistoryDao
import org.fs.chm.protobuf._
import org.fs.chm.utility.RpcUtils
import org.fs.utility.StopWatch

class GrpcRemoteDataLoader(channel: ManagedChannel) extends DataLoader[GrpcChatHistoryDao] {
  override protected def loadDataInner(path: JFile, createNew: Boolean): GrpcChatHistoryDao = {
    val key = path.getAbsolutePath
    val request = LoadRequest(key, path.getAbsolutePath)
    log.info(s"Sending gRPC parse request (remote): ${request}")
    val loaderRpcStub = HistoryLoaderServiceGrpc.blockingStub(channel)
    StopWatch.measureAndCall {
      val response: LoadResponse = RpcUtils.sendRequestNoParams {
        loaderRpcStub.load(request)
      }
      val daoRpcStub = HistoryDaoServiceGrpc.blockingStub(channel)
      new GrpcChatHistoryDao(key, response.name, daoRpcStub, loaderRpcStub)
    }((_, ms) => log.info(s"Remote history loaded in ${ms} ms"))
  }
}

object GrpcRemoteDataLoader extends App {
  val holder = new GrpcDataLoaderHolder(50051)
  holder.remoteLoader
  println("Press ENTER to terminate...")
  System.in.read();
}
