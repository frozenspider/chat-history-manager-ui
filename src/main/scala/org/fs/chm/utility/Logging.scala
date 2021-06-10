package org.fs.chm.utility

import org.slf4s.Logger
import org.slf4s.LoggerFactory

trait Logging {
  protected implicit val log: Logger = LoggerFactory.getLogger(this.getClass)
}
