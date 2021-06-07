package org.fs.chm.loader

import java.io.File
import java.io.FileNotFoundException

import org.fs.chm.dao.ChatHistoryDao
import org.fs.utility.StopWatch
import org.slf4s.Logging

trait DataLoader[D <: ChatHistoryDao] extends Logging {
  def create(path: File): D = {
    StopWatch.measureAndCall {
      if (!path.exists()) throw new FileNotFoundException(s"File ${path.getAbsolutePath} not found")
      loadDataInner(path, true)
    }((_, t) => log.info(s"File ${path.getAbsolutePath} created in $t ms"))
  }

  def loadData(path: File): D = {
    StopWatch.measureAndCall {
      if (!path.exists()) throw new FileNotFoundException(s"File ${path.getAbsolutePath} not found")
      loadDataInner(path, false)
    }((_, t) => log.info(s"File ${path.getAbsolutePath} loaded in $t ms"))
  }

  protected def loadDataInner(path: File, createNew: Boolean): D
}
