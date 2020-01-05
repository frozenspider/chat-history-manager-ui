package org.fs.chm.dao

import java.io.File
import java.nio.charset.Charset
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
  private var dir:   File             = _

  before {
    dir = Files.createTempDirectory(null).toFile
    log.info(s"Using temp dir $dir")

    val manager = new H2DataManager
    manager.create(dir)
    h2dao = manager.loadData(dir)
  }

  after {
    h2dao.close()
    def delete(f: File): Unit = {
      (Option(f.listFiles()) getOrElse Array.empty) foreach delete
      assert(f.delete(), s"Couldn't delete $f")
    }
    delete(dir)
  }

  test("copy existing Telegram DAO") {
    val loader      = new TelegramDataLoader
    val telegramDir = new File(resourcesFolder, "telegram")
    val tgDao       = loader.loadData(telegramDir)
    val dsUuid      = tgDao.datasets.head.uuid

    // Most invariants are checked within copyAllFrom
    h2dao.copyAllFrom(tgDao)

    // Checking files copied
    val src         = new String(Files.readAllBytes(new File(telegramDir, "result.json").toPath), Charset.forName("UTF-8"))
    val pathRegex   = """(?<=")chats/[a-zA-Z0-9()\[\]./\\_ -]+(?=")""".r
    val srcDataPath = tgDao.dataPath(dsUuid)
    val dstDataPath = h2dao.dataPath(dsUuid)
    def bytesOf(f: File): Array[Byte] = Files.readAllBytes(f.toPath)

    for (path <- pathRegex.findAllIn(src).toList) {
      assert(new File(srcDataPath, path).exists(), s"File ${path} (source) isn't found! Bug in test?")
      assert(new File(dstDataPath, path).exists(), s"File ${path} wasn't copied!")
      val srcBytes = bytesOf(new File(srcDataPath, path))
      assert(!srcBytes.isEmpty, s"Source file ${path} was empty! Bug in test?")
      assert(srcBytes === bytesOf(new File(dstDataPath, path)), s"Copy of ${path} didn't match its source!")
    }

    val pathsNotToCopy = Seq(
      "dont_copy_me.txt",
      "chats/chat_01/dont_copy_me_either.txt"
    )
    for (path <- pathsNotToCopy) {
      assert(new File(srcDataPath, path).exists(), s"File ${path} (source) isn't found! Bug in test?")
      assert(!new File(dstDataPath, path).exists(), s"File ${path} was copied - but it shouldn't have been!")
      val srcBytes = bytesOf(new File(srcDataPath, path))
      assert(!srcBytes.isEmpty, s"Source file ${path} was empty! Bug in test?")
    }
  }
}
