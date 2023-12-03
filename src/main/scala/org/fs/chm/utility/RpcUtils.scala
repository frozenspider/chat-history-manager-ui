package org.fs.chm.utility

import org.slf4s.Logger

object RpcUtils {
  /** Act as a client sending RPC request */
  def sendRequest[R, T](req: R)(doReq: R => T)(implicit log: Logger): T = {
    try {
      log.debug(s"<<< Request:  ${req.toString.take(150)}")
      val res = doReq(req)
      log.debug(s">>> Response: ${res.toString.linesIterator.next().take(150)}")
      res
    } catch {
      case ex: io.grpc.StatusRuntimeException if ex.getStatus.getCode == io.grpc.Status.Code.UNAVAILABLE =>
        throw new IllegalStateException(s"gRPC server is not running")
    }
  }

  def sendRequestNoParams[T](doReq: => T)(implicit log: Logger): T = {
    sendRequest(())(_ => doReq)
  }

  /** Act as a server processing RPC request */
  private def processRequest[T](req: Object)(logic: => T)(implicit log: Logger): T = {
    log.debug(s">>> Request:  ${req.toString.take(150)}")
    try {
      val res = logic
      log.debug(s"<<< Response: ${res.toString.linesIterator.next().take(150)}")
      res
    } catch {
      case th: Throwable =>
        log.debug(s"<<< Failure:  ${th.toString.take(150)}")
        throw th
    }
  }
}
