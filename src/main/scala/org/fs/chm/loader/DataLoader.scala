package org.fs.chm.loader

import java.io.File
import java.io.FileNotFoundException

import org.fs.chm.dao.ChatHistoryDao

trait DataLoader[D <: ChatHistoryDao] {
  def loadData(path: File): D = {
    if (!path.exists()) throw new FileNotFoundException(s"File ${path.getAbsolutePath} not found")
    loadDataInner(path)
  }

  protected def loadDataInner(path: File): D
}
