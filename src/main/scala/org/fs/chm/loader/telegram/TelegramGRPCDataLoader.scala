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
import org.fs.chm.dao.Entities._
import org.fs.chm.protobuf._
import org.fs.chm.utility.EntityUtils
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

  private lazy val myselfChooserServer: Server = {
    val serverPort = rpcPort + 1
    log.info(s"Starting callback server at ${serverPort}")
    val server: Server = ServerBuilder.forPort(serverPort)
      .addService(MyselfChooserGrpc.bindService(new MyselfChooserImpl, ExecutionContext.global))
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
      val blockingStub = HistoryLoaderGrpc.blockingStub(channel)
      val response: ParseHistoryFileResponse = blockingStub.parseHistoryFile(request)
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

  private class MyselfChooserImpl extends MyselfChooserGrpc.MyselfChooser {
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
}

object TelegramGRPCDataLoader extends App {
  val loader = new TelegramGRPCDataLoader(50051)
  loader.myselfChooserServer
  println("Press ENTER to terminate...")
  System.in.read();
}
