package org.fs.chm.loader.telegram

import java.io.{File => JFile}

import scala.collection.immutable.ListMap
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.grpc.Server
import io.grpc.ServerBuilder
import io.grpc.protobuf.services.ProtoReflectionService
import org.fs.chm.dao.EagerChatHistoryDao
import org.fs.chm.loader.DataLoader
import org.fs.chm.protobuf._
import org.fs.chm.utility.EntityUtils
import org.fs.utility.StopWatch

class GrpcDataLoader(rpcPort: Int) extends DataLoader[EagerChatHistoryDao] {
  private val channel: ManagedChannel = ManagedChannelBuilder
    .forAddress("127.0.0.1", rpcPort)
    .maxInboundMessageSize(Integer.MAX_VALUE)
    .usePlaintext()
    .build()

  private lazy val myselfChooserServer: Server = {
    val serverPort = rpcPort + 1
    log.info(s"Starting callback server at ${serverPort}")
    val server: Server = ServerBuilder.forPort(serverPort)
      .addService(ChooseMyselfServiceGrpc.bindService(new ChooseMyselfImpl, ExecutionContext.global))
      .addService(ProtoReflectionService.newInstance())
      .build.start
    sys.addShutdownHook {
      server.shutdown()
    }
    Thread.sleep(1000)
    server
  }

  override protected def loadDataInner(path: JFile, createNew: Boolean): EagerChatHistoryDao = {
    val request = ParseHistoryFileRequest(path = path.getAbsolutePath)
    log.info(s"Sending gRPC parse request: ${request}")
    myselfChooserServer
    StopWatch.measureAndCall {
      val response: ParseHistoryFileResponse = tryWrappingExceptions {
        val blockingStub = HistoryLoaderServiceGrpc.blockingStub(channel)
        blockingStub.parseHistoryFile(request)
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
    }((_, ms) => log.info(s"Telegram history loaded in ${ms} ms (via gRPC)"))
  }

  private class ChooseMyselfImpl extends ChooseMyselfServiceGrpc.ChooseMyselfService {
    override def chooseMyself(request: ChooseMyselfRequest): Future[ChooseMyselfResponse] = {
      try {
        val myselfIdx = EntityUtils.chooseMyself(request.users)
        val reply = ChooseMyselfResponse(pickedOption = myselfIdx)
        Future.successful(reply)
      } catch {
        case ex: Exception =>
          Future.failed(ex)
      }
    }
  }

  private def tryWrappingExceptions[T](block: => T): T = {
    try {
      block
    } catch {
      case ex: io.grpc.StatusRuntimeException if ex.getStatus.getCode == io.grpc.Status.Code.UNAVAILABLE =>
        throw new IllegalStateException(s"gRPC parsing server is not running at port $rpcPort")
    }
  }
}

object GrpcDataLoader extends App {
  val loader = new GrpcDataLoader(50051)
  loader.myselfChooserServer
  println("Press ENTER to terminate...")
  System.in.read();
}
