package org.fs.chm.loader

import java.io.File
import java.io.FileNotFoundException

import scala.concurrent.ExecutionContext

import cats.effect.Blocker
import cats.effect.IO
import doobie.Transactor
import org.flywaydb.core.Flyway
import org.fs.chm.dao._
import org.h2.jdbcx.JdbcConnectionPool

class H2DataManager extends DataLoader[H2ChatHistoryDao] {
  import org.fs.chm.loader.H2DataManager._

  private implicit val cs = IO.contextShift(ExecutionContext.global)

  private val dataFileName = "data" + DefaultExt

  private val options = Seq(
    //"TRACE_LEVEL_FILE=2",
    //"TRACE_LEVEL_SYSTEM_OUT=2",
    "COMPRESS=TRUE",
    "DATABASE_TO_UPPER=false"
  )
  private val optionsString = options.mkString(";", ";", "")

  def create(path: File): Unit = {
    val dataDbFile: File = new File(path, dataFileName)
    if (dataDbFile.exists()) throw new FileNotFoundException(s"$dataFileName already exists in " + path.getAbsolutePath)
    val dao = daoFromFile(dataDbFile)
    dao.createTables()
    dao.close()
    val flyway = Flyway.configure.dataSource(dbUrl(dataDbFile), "sa", "").load
    flyway.baseline()
  }

  /** Path should point to the folder with `data.h2.db` and other stuff */
  override protected def loadDataInner(path: File): H2ChatHistoryDao = {
    val dataDbFile: File = new File(path, dataFileName)
    if (!dataDbFile.exists()) throw new FileNotFoundException(s"$dataFileName not found in " + path.getAbsolutePath)
    val flyway = Flyway.configure.dataSource(dbUrl(dataDbFile), "sa", "").load
    flyway.migrate()
    daoFromFile(dataDbFile)
  }

  private def dbUrl(path: File): String = {
    val innerPath = path.getAbsolutePath.replaceAll(DefaultExt.replace(".", "\\.") + "$", "").replace("\\", "/")
    "jdbc:h2:" + innerPath + optionsString
  }

  private def daoFromFile(path: File): H2ChatHistoryDao = {
    Class.forName("org.h2.Driver")
    val connPool = JdbcConnectionPool.create(dbUrl(path), "sa", "")
    val execCxt: ExecutionContext = ExecutionContext.global
    val blocker: Blocker          = Blocker.liftExecutionContext(execCxt)
    val txctr = Transactor.fromDataSource[IO](connPool, execCxt, blocker)
    new H2ChatHistoryDao(dataPathRoot = path.getParentFile, txctr = txctr, () => txctr.kernel.dispose())
  }
}

object H2DataManager {
  val DefaultExt   = ".mv.db"
}
