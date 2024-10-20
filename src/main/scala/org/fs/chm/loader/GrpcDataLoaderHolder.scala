package org.fs.chm.loader

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.grpc.Server
import io.grpc.ServerBuilder
import io.grpc.protobuf.services.ProtoReflectionService
import org.fs.chm.protobuf._
import org.fs.chm.utility.EntityUtils
import org.fs.chm.utility.Logging

class GrpcDataLoaderHolder(rpcPort: Int) extends Logging {
  val channel: ManagedChannel = ManagedChannelBuilder
    .forAddress("127.0.0.1", rpcPort)
    .maxInboundMessageSize(Integer.MAX_VALUE)
    .usePlaintext()
    .build()

  private lazy val server: Server = {
    val serverPort = rpcPort + 1
    log.info(s"Starting callback server at ${serverPort}")
    val server: Server = ServerBuilder.forPort(serverPort)
      .addService(UserInputServiceGrpc.UserInputService.bindService(new UserInputServiceImpl, ExecutionContext.global))
      .addService(ProtoReflectionService.newInstance())
      .build.start
    sys.addShutdownHook {
      server.shutdown()
    }
    server
  }

  server

  lazy val remoteLoader = new GrpcRemoteDataLoader(channel)

  private class UserInputServiceImpl extends UserInputServiceGrpc.UserInputService {
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

    override def askForText(request: TextInputRequest): Future[TextInputResponse] = {
      try {
        val userInput = EntityUtils.askForUserInput(request.prompt)
        val reply = TextInputResponse(userInput = userInput)
        Future.successful(reply)
      } catch {
        case ex: Exception =>
          Future.failed(ex)
      }
    }
  }
}
