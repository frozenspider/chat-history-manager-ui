package org.fs.chm

import java.io.File
import java.nio.file.Files

import org.fs.chm.dao.H2ChatHistoryDao
import org.fs.chm.loader.H2DataManager
import org.scalatest.BeforeAndAfter
import org.scalatest.Suite
import org.slf4s.Logging

trait WithH2Dao extends BeforeAndAfter with Logging { this: Suite =>
  var h2dao: H2ChatHistoryDao = _
  var h2dir: File             = _

  protected def initH2Dao(): Unit = {
    val manager = new H2DataManager
    h2dir = Files.createTempDirectory("java_chm-h2_").toFile
    log.info(s"Using temp dir $h2dir for H2")
    h2dao = manager.create(h2dir)
  }

  protected def freeH2Dao(): Unit = {
    h2dao.close()
    def delete(f: File): Unit = {
      (Option(f.listFiles()) getOrElse Array.empty) foreach delete
      assert(f.delete(), s"Couldn't delete $f")
    }
    delete(h2dir)
  }
}
