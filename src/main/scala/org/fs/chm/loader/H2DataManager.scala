package org.fs.chm.loader

import java.io.File
import java.io.FileNotFoundException

import scala.concurrent.ExecutionContext

import cats.effect.IO
import doobie.util.transactor.Transactor
import org.fs.chm.dao._

class H2DataManager extends DataLoader {
  private implicit val cs = IO.contextShift(ExecutionContext.global)

  private val defaultExt = ".mv.db"
  private val dataFileName = "data" + defaultExt

  def create(path: File): H2ChatHistoryDao = {
    val dataDbFile: File = new File(path, dataFileName)
    if (dataDbFile.exists()) throw new FileNotFoundException(s"$dataFileName already exists in " + path.getAbsolutePath)
    val dao = daoFromFile(dataDbFile)
    dao.createTables()
    dao
  }

  /** Path should point to the folder with `data.h2.db` and other stuff */
  override protected def loadDataInner(path: File): H2ChatHistoryDao = {
    val dataDbFile: File = new File(path, dataFileName)
    if (!dataDbFile.exists()) throw new FileNotFoundException(s"$dataFileName not found in " + path.getAbsolutePath)
    daoFromFile(dataDbFile)
  }


  private def daoFromFile(path: File): H2ChatHistoryDao = {
    val innerPath = path.getAbsolutePath.replaceAll(defaultExt.replace(".", "\\.") + "$", "").replace("\\", "/")
    val txctr = Transactor.fromDriverManager[IO](
      "org.h2.Driver",
      "jdbc:h2:" + innerPath + ";DATABASE_TO_UPPER=false;TRACE_LEVEL_SYSTEM_OUT=2",
      "sa",
      ""
    )
    new H2ChatHistoryDao(dataPathRoot = path, txctr = txctr)
  }
}
