package org.fs.chm.loader

import java.io.File
import java.io.FileNotFoundException

import org.fs.chm.dao.ChatHistoryDao
import org.fs.utility.StopWatch
import org.slf4s.Logging

trait DataLoader[D <: ChatHistoryDao] extends Logging {
  def loadData(path: File): D = {
    StopWatch.measureAndCall {
      if (!path.exists()) throw new FileNotFoundException(s"File ${path.getAbsolutePath} not found")
      loadDataInner(path)
    }((_, t) => log.info(s"File ${path.getAbsolutePath} loaded in $t ms"))
  }

  protected def loadDataInner(path: File): D
}
