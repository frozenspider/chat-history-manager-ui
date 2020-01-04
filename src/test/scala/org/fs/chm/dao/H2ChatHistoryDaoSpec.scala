package org.fs.chm.dao

import java.io.File
import java.nio.file.Files

import org.fs.chm.TestHelper
import org.fs.chm.loader.H2DataManager
import org.fs.chm.loader.TelegramDataLoader
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfter
import org.scalatest.FunSuite
import org.slf4s.Logging

@RunWith(classOf[org.scalatest.junit.JUnitRunner])
class H2ChatHistoryDaoSpec //
    extends FunSuite
    with TestHelper
    with Logging
    with BeforeAndAfter {

  private var h2dao: H2ChatHistoryDao = _
  private var dir:   File = _

  before {
    dir = Files.createTempDirectory(null).toFile
    log.info(s"Using temp dir $dir")

    val manager = new H2DataManager
    h2dao = manager.create(dir)
    h2dao.createTables()
  }

  after {
    def delete(f: File): Unit = {
      (Option(f.listFiles()) getOrElse Array.empty) foreach delete
      assert(f.delete(), s"Couldn't delete $f")
    }
    delete(dir)
  }

  test("copy existing Telegram DAO") {
    val loader = new TelegramDataLoader
    val eagerDao = loader.loadData(new File(resourcesFolder, "telegram"))
    h2dao.insertAll(eagerDao)
  }
}
