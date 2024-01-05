package org.fs.chm.loader

import java.io.{File => JFile}

import io.grpc.ManagedChannel
import org.fs.chm.dao.GrpcChatHistoryDao
import org.fs.chm.protobuf._
import org.fs.chm.utility.RpcUtils
import org.fs.utility.StopWatch

class GrpcRemoteDataLoader(channel: ManagedChannel) extends DataLoader[GrpcChatHistoryDao] {
  private val loaderRpcStub = HistoryLoaderServiceGrpc.blockingStub(channel)

  override protected def loadDataInner(path: JFile, createNew: Boolean): GrpcChatHistoryDao = {
    val key = path.getAbsolutePath
    val request = LoadRequest(key, path.getAbsolutePath)
    log.info(s"Sending gRPC parse request: ${request}")
    StopWatch.measureAndCall {
      val response: LoadResponse = RpcUtils.sendRequestNoParams {
        loaderRpcStub.load(request)
      }
      val daoRpcStub = HistoryDaoServiceGrpc.blockingStub(channel)
      new GrpcChatHistoryDao(key, response.name, daoRpcStub, loaderRpcStub)
    }((_, ms) => log.info(s"Remote history loaded in ${ms} ms"))
  }

  override def ensureSame(masterDaoKey: String, masterDsUuid: PbUuid, slaveDaoKey: String, slaveDsUuid: PbUuid): Seq[Difference] = {
    StopWatch.measureAndCall {
      RpcUtils.sendRequest(EnsureSameRequest(masterDaoKey, masterDsUuid, slaveDaoKey, slaveDsUuid)) { req =>
        loaderRpcStub.ensureSame(req)
      }.diffs
    }((_, ms) => log.info(s"Dataset equality checked in ${ms} ms"))
  }
}

object GrpcRemoteDataLoader extends App {
  val holder = new GrpcDataLoaderHolder(50051)
  holder.remoteLoader
  println("Press ENTER to terminate...")
  System.in.read();
}
