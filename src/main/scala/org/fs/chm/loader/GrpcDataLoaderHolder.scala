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
    server
  }

  myselfChooserServer

  lazy val eagerLoader = new GrpcEagerDataLoader(channel)

  lazy val remoteLoader = new GrpcRemoteDataLoader(channel)

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
}

object GrpcDataLoaderHolder extends Logging {
  def wrapRequest[R, T](req: R)(doReq: R => T): T = {
    try {
      log.debug(s">>> Request:  ${req.toString.take(150)}")
      val res = doReq(req)
      log.debug(s"<<< Response: ${res.toString.linesIterator.next().take(150)}")
      res
    } catch {
      case ex: io.grpc.StatusRuntimeException if ex.getStatus.getCode == io.grpc.Status.Code.UNAVAILABLE =>
        throw new IllegalStateException(s"gRPC server is not running")
    }
  }

  def wrapRequestNoParams[T](doReq: => T): T = {
    wrapRequest(())(_ => doReq)
  }
}
