package org.fs.chm.loader

import java.io.{File => JFile}

import scala.collection.immutable.ListMap

import io.grpc.ManagedChannel
import org.fs.chm.dao.EagerChatHistoryDao
import org.fs.chm.protobuf._
import org.fs.chm.utility.RpcUtils
import org.fs.utility.StopWatch

class GrpcEagerDataLoader(channel: ManagedChannel) extends DataLoader[EagerChatHistoryDao] {
  override protected def loadDataInner(path: JFile, createNew: Boolean): EagerChatHistoryDao = {
    val request = ParseRequest(path = path.getAbsolutePath)
    log.info(s"Sending gRPC parse request (eager): ${request}")
    StopWatch.measureAndCall {
      val response: ParseResponse = RpcUtils.sendRequestNoParams {
        val blockingStub = HistoryParserServiceGrpc.blockingStub(channel)
        blockingStub.parse(request)
      }
      val root = new JFile(response.rootFile).getAbsoluteFile
      require(root.exists, s"Dataset root ${root} does not exist!")
      val chatsWithMessagesLM: ListMap[Chat, IndexedSeq[Message]] =
        ListMap.from(response.cwms.map(cwm => cwm.chat -> cwm.messages.toIndexedSeq))
      new EagerChatHistoryDao(
        name               = "Parsed (" + root.getName + ")",
        _dataRootFile      = root,
        dataset            = response.ds,
        myself1            = response.myself,
        users1             = response.users,
        _chatsWithMessages = chatsWithMessagesLM
      )
    }((_, ms) => log.info(s"History loaded in ${ms} ms (via gRPC, eager)"))
  }
}

object GrpcEagerDataLoader extends App {
  val holder = new GrpcDataLoaderHolder(50051, new GrpcDaoService(f => ???))
  holder.eagerLoader
  println("Press ENTER to terminate...")
  System.in.read();
}
