package org.fs.chm.utility

import org.fs.utility.StopWatch
import org.slf4s.Logger

object PerfUtils {
  def logPerformance[R](cb: => R)(msg: (R, Long) => String)(implicit log: Logger): R = {
    StopWatch.measureAndCall(cb)((res, ms) => if (ms > 50) log.debug(msg(res, ms)))
  }
}
