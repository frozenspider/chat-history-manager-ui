package org.fs.chm.loader

import java.io.File
import java.io.FileNotFoundException

import org.fs.chm.dao.ChatHistoryDao

trait DataLoader {
  def loadData(path: File): ChatHistoryDao = {
    if (!path.exists()) throw new FileNotFoundException(s"File ${path.getAbsolutePath} not found")
    loadDataInner(path)
  }

  def loadDataInner(path: File): ChatHistoryDao
}
