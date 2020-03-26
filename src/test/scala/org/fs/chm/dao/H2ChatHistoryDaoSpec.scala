package org.fs.chm.dao

import java.io.File
import java.nio.charset.Charset
import java.nio.file.Files
import java.util.UUID

import org.fs.chm.TestHelper
import org.fs.chm.loader.H2DataManager
import org.fs.chm.loader.TelegramDataLoader
import org.fs.chm.utility.TestUtils
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfter
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite
import org.slf4s.Logging

@RunWith(classOf[org.scalatest.junit.JUnitRunner])
class H2ChatHistoryDaoSpec //
    extends FunSuite
    with TestHelper
    with Logging
    with BeforeAndAfter
    with BeforeAndAfterAll {

  val manager     = new H2DataManager
  val loader      = new TelegramDataLoader
  val telegramDir = new File(resourcesFolder, "telegram")

  var tgDao:  ChatHistoryDao   = _
  var dsUuid: UUID             = _
  var h2dao:  H2ChatHistoryDao = _
  var dir:    File             = _

  override def beforeAll(): Unit = {
    tgDao  = loader.loadData(telegramDir)
    dsUuid = tgDao.datasets.head.uuid
  }

  before {
    dir = Files.createTempDirectory(null).toFile
    log.info(s"Using temp dir $dir")
    manager.create(dir)
    h2dao = manager.loadData(dir)

    // Most invariants are checked within copyAllFrom
    h2dao.copyAllFrom(tgDao)
  }

  after {
    freeH2Dao()
  }

  test("relevant files are copied") {
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

  test("messages and chats are equal, retrieval methods work as needed") {
    val numMsgsToTake = 10
    assert(tgDao.chats(dsUuid).size === h2dao.chats(dsUuid).size)
    for ((tgChat, h2Chat) <- (tgDao.chats(dsUuid).sortBy(_.id) zip h2dao.chats(dsUuid).sortBy(_.id))) {
      assert(tgChat === h2Chat)

      val allTg = tgDao.lastMessages(tgChat, tgChat.msgCount)
      val allH2 = h2dao.lastMessages(h2Chat, tgChat.msgCount)
      assert(allH2.size == tgChat.msgCount)
      assert(allH2 === allTg)

      val scroll1 = h2dao.scrollMessages(h2Chat, 0, numMsgsToTake)
      assert(scroll1 === allH2.take(numMsgsToTake))
      assert(scroll1 === tgDao.scrollMessages(tgChat, 0, numMsgsToTake))

      val scroll2 = h2dao.scrollMessages(h2Chat, 1, numMsgsToTake)
      assert(scroll2 === allH2.tail.take(numMsgsToTake))
      assert(scroll2 === tgDao.scrollMessages(tgChat, 1, numMsgsToTake))

      val before1 = h2dao.messagesBefore(h2Chat, allH2.last.id, numMsgsToTake)
      assert(before1.last === allH2.last)
      assert(before1 === allH2.takeRight(numMsgsToTake))
      assert(before1 === tgDao.messagesBefore(tgChat, allTg.last.id, numMsgsToTake))

      val before2 = h2dao.messagesBefore(h2Chat, allH2.dropRight(1).last.id, numMsgsToTake)
      assert(before2.last === allH2.dropRight(1).last)
      assert(before2 === allH2.dropRight(1).takeRight(numMsgsToTake))
      assert(before2 === tgDao.messagesBefore(tgChat, allTg.dropRight(1).last.id, numMsgsToTake))

      val after1 = h2dao.messagesAfter(h2Chat, allH2.head.id, numMsgsToTake)
      assert(after1.head === allH2.head)
      assert(after1 === allH2.take(numMsgsToTake))
      assert(after1 === tgDao.messagesAfter(tgChat, allTg.head.id, numMsgsToTake))

      val after2 = h2dao.messagesAfter(h2Chat, allH2(1).id, numMsgsToTake)
      assert(after2.head === allH2(1))
      assert(after2 === allH2.tail.take(numMsgsToTake))
      assert(after2 === tgDao.messagesAfter(tgChat, allTg(1).id, numMsgsToTake))

      val between1 = h2dao.messagesBetween(h2Chat, allH2.head.id, allH2.last.id)
      assert(between1 === allH2)
      assert(between1 === tgDao.messagesBetween(tgChat, allTg.head.id, allTg.last.id))

      val between2 = h2dao.messagesBetween(h2Chat, allH2(1).id, allH2.last.id)
      assert(between2 === allH2.tail)
      assert(between2 === tgDao.messagesBetween(tgChat, allTg(1).id, allTg.last.id))

      val between3 = h2dao.messagesBetween(h2Chat, allH2.head.id, allH2.dropRight(1).last.id)
      assert(between3 === allH2.dropRight(1))
      assert(between3 === tgDao.messagesBetween(tgChat, allTg.head.id, allTg.dropRight(1).last.id))

      val countBetween1 = h2dao.countMessagesBetween(h2Chat, allH2.head.id, allH2.last.id)
      assert(countBetween1 === allH2.size - 2)
      assert(countBetween1 === tgDao.countMessagesBetween(tgChat, allTg.head.id, allTg.last.id))

      if (countBetween1 > 0) {
        val countBetween2 = h2dao.countMessagesBetween(h2Chat, allH2(1).id, allH2.last.id)
        assert(countBetween2 === allH2.size - 3)
        assert(countBetween2 === tgDao.countMessagesBetween(tgChat, allTg(1).id, allTg.last.id))

        val countBetween3 = h2dao.countMessagesBetween(h2Chat, allH2.head.id, allH2.dropRight(1).last.id)
        assert(countBetween3 === allH2.size - 3)
        assert(countBetween3 === tgDao.countMessagesBetween(tgChat, allTg.head.id, allTg.dropRight(1).last.id))
      }

      val last = h2dao.lastMessages(h2Chat, numMsgsToTake)
      assert(last === allH2.takeRight(numMsgsToTake))
      assert(last === tgDao.lastMessages(tgChat, numMsgsToTake))
    }
  }

  test("update user") {
    val myself = h2dao.myself(dsUuid)

    def personalChatWith(u: User): Option[Chat] =
      h2dao.chats(dsUuid) find { c =>
        c.tpe == ChatType.Personal &&
        h2dao.interlocutors(c).exists(_.id == u.id) &&
        h2dao.firstMessages(c, 99999).exists(_.fromId == myself.id)
      }

    val users = h2dao.users(dsUuid)
    val user1 = users.find(u => u != myself && personalChatWith(u).isDefined).get

    def doUpdate(u: User): Unit = {
      h2dao.updateUser(u)
      val usersA = h2dao.users(dsUuid)
      assert(usersA.find(_.id == user1.id).get === u)

      val chatA = personalChatWith(u) getOrElse fail("Chat not found after updating!")
      assert(chatA.nameOption === u.prettyNameOption)
    }

    doUpdate(
      user1.copy(
        firstNameOption   = Some("fn"),
        lastNameOption    = Some("ln"),
        usernameOption    = Some("un"),
        phoneNumberOption = Some("+123")
      )
    )

    doUpdate(
      user1.copy(
        firstNameOption   = None,
        lastNameOption    = None,
        usernameOption    = None,
        phoneNumberOption = None
      )
    )

    // Renaming self should not affect private chats
    {
      val chat1Before = personalChatWith(user1) getOrElse fail("Chat not found before updating!")
      h2dao.updateUser(
        myself.copy(
          firstNameOption = Some("My New"),
          lastNameOption  = Some("Name"),
        )
      )
      val chat1After = personalChatWith(user1) getOrElse fail("Chat not found after updating!")
      assert(chat1After.nameOption === chat1Before.nameOption)
    }
  }

  test("message fetching corner cases") {
    freeH2Dao()

    val msgs  = (3 to 7) map (TestUtils.createRegularMessage(_, 1))
    val tgDao = TestUtils.createSimpleDao("TG", msgs, 1)
    dir = Files.createTempDirectory(null).toFile

    manager.create(dir)
    h2dao = manager.loadData(dir)
    h2dao.copyAllFrom(tgDao)

    val dsUuid = tgDao.datasets.head.uuid
    for {
      dao  <- Seq(tgDao, h2dao)
      chat <- dao.chats(dsUuid).sortBy(_.id)
    } {
      assert(dao.messagesBefore(chat, 3, 10) === Seq(msgs.head))
      assert(dao.messagesBefore(chat, 2, 10) === Seq.empty)

      assert(dao.messagesAfter(chat, 7, 10) === Seq(msgs.last))
      assert(dao.messagesAfter(chat, 8, 10) === Seq.empty)

      assert(dao.messagesBetween(chat, 1, 3) === Seq(msgs.head))
      assert(dao.messagesBetween(chat, 1, 2) === Seq.empty)

      assert(dao.messagesBetween(chat, 7, 9) === Seq(msgs.last))
      assert(dao.messagesBetween(chat, 8, 9) === Seq.empty)

      assert(dao.countMessagesBetween(chat, 1, 4) === 1)
      assert(dao.countMessagesBetween(chat, 1, 3) === 0)

      assert(dao.countMessagesBetween(chat, 6, 9) === 1)
      assert(dao.countMessagesBetween(chat, 7, 9) === 0)
    }
  }

  test("delete chat") {
    val chats = h2dao.chats(dsUuid)
    val users = h2dao.users(dsUuid)

    {
      // User is not deleted because it participates in another chat
      val chatToDelete = chats.find(c => c.tpe == ChatType.Personal && c.id == 9777777777L).get
      h2dao.delete(chatToDelete)
      assert(h2dao.chats(dsUuid).size === chats.size - 1)
      assert(h2dao.users(dsUuid).size === users.size)
      assert(h2dao.firstMessages(chatToDelete, 10).isEmpty)
    }

    {
      // User is deleted
      val chatToDelete = chats.find(c => c.tpe == ChatType.Personal && c.id == 4321012345L).get
      h2dao.delete(chatToDelete)
      assert(h2dao.chats(dsUuid).size === chats.size - 2)
      assert(h2dao.users(dsUuid).size === users.size - 1)
      assert(h2dao.firstMessages(chatToDelete, 10).isEmpty)
    }
  }

  private def freeH2Dao(): Unit = {
    h2dao.close()
    def delete(f: File): Unit = {
      (Option(f.listFiles()) getOrElse Array.empty) foreach delete
      assert(f.delete(), s"Couldn't delete $f")
    }
    delete(dir)
  }
}
