package org.fs.chm.loader

import java.io.File
import java.io.FileNotFoundException

import scala.concurrent.ExecutionContext

import cats.effect.Blocker
import cats.effect.IO
import doobie.Transactor
import org.fs.chm.dao._
import org.h2.jdbcx.JdbcConnectionPool

class H2DataManager extends DataLoader {
  private implicit val cs = IO.contextShift(ExecutionContext.global)

  private val defaultExt = ".mv.db"
  private val dataFileName = "data" + defaultExt

  private val options = Seq(
    //"TRACE_LEVEL_FILE=2",
    //"TRACE_LEVEL_SYSTEM_OUT=2",
    "COMPRESS=TRUE",
    "DATABASE_TO_UPPER=false"
  )
  private val optionsString = options.mkString(";", ";", "")

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
    Class.forName("org.h2.Driver")
    val innerPath = path.getAbsolutePath.replaceAll(defaultExt.replace(".", "\\.") + "$", "").replace("\\", "/")
    val connPool = JdbcConnectionPool.create(
      "jdbc:h2:" + innerPath + optionsString,
      "sa",
      ""
    )
    val execCxt: ExecutionContext = ExecutionContext.global
    val blocker: Blocker = Blocker.liftExecutionContext(execCxt)
    val txctr = Transactor.fromDataSource[IO](connPool, execCxt, blocker)
    new H2ChatHistoryDao(dataPathRoot = path.getParentFile, txctr = txctr, () => txctr.kernel.dispose())
  }
}
