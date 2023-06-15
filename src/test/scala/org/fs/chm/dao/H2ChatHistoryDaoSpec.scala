package org.fs.chm.dao

import java.io.File
import java.nio.charset.Charset
import java.nio.file.Files

import com.github.nscala_time.time.Imports._
import org.fs.chm.TestHelper
import org.fs.chm.WithH2Dao
import org.fs.chm.dao.Entities._
import org.fs.chm.loader.H2DataManager
import org.fs.chm.loader.telegram.TelegramFullDataLoader
import org.fs.chm.protobuf.Chat
import org.fs.chm.protobuf.ChatType
import org.fs.chm.protobuf.Message
import org.fs.chm.protobuf.PbUuid
import org.fs.chm.protobuf.User
import org.fs.chm.utility.IoUtils._
import org.fs.chm.utility.LangUtils._
import org.fs.chm.utility.TestUtils
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfter
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.slf4s.Logging

@RunWith(classOf[org.scalatestplus.junit.JUnitRunner])
class H2ChatHistoryDaoSpec //
    extends AnyFunSuite
    with TestHelper
    with WithH2Dao
    with Logging
    with BeforeAndAfter
    with BeforeAndAfterAll {

  val manager     = new H2DataManager
  val loader      = new TelegramFullDataLoader
  val telegramDir = new File(resourcesFolder, "telegram_2020-01")

  var tgDao:  ChatHistoryDao   = _
  var dsUuid: PbUuid           = _

  override def beforeAll(): Unit = {
    tgDao  = loader.loadData(telegramDir)
    dsUuid = tgDao.datasets.head.uuid
  }

  before {
    initH2Dao()
    // Most invariants are checked within copyAllFrom
    h2dao.copyAllFrom(tgDao)
  }

  after {
    freeH2Dao()
  }

  test("relevant files are copied") {
    val src         = new String(Files.readAllBytes(new File(telegramDir, "result.json").toPath), Charset.forName("UTF-8"))
    val pathRegex   = """(?<=")chats/[a-zA-Z0-9()\[\]./\\_ -]+(?=")""".r
    val srcDataPath = tgDao.datasetRoot(dsUuid)
    val dstDataPath = h2dao.datasetRoot(dsUuid)

    for (path <- pathRegex.findAllIn(src).toList) {
      assert(new File(srcDataPath, path).exists(), s"File ${path} (source) isn't found! Bug in test?")
      assert(new File(dstDataPath, path).exists(), s"File ${path} wasn't copied!")
      val srcBytes = new File(srcDataPath, path).bytes
      assert(!srcBytes.isEmpty, s"Source file ${path} was empty! Bug in test?")
      assert(srcBytes === new File(dstDataPath, path).bytes, s"Copy of ${path} didn't match its source!")
    }

    val pathsNotToCopy = Seq(
      "dont_copy_me.txt",
      "chats/chat_01/dont_copy_me_either.txt"
    )
    for (path <- pathsNotToCopy) {
      assert(new File(srcDataPath, path).exists(), s"File ${path} (source) isn't found! Bug in test?")
      assert(!new File(dstDataPath, path).exists(), s"File ${path} was copied - but it shouldn't have been!")
      val srcBytes = new File(srcDataPath, path).bytes
      assert(!srcBytes.isEmpty, s"Source file ${path} was empty! Bug in test?")
    }

    assert(tgDao.datasetFiles(dsUuid).toSeq.sortBy(_.getAbsolutePath)
      =~= h2dao.datasetFiles(dsUuid).toSeq.sortBy(_.getAbsolutePath))
  }

  test("messages and chats are equal, retrieval methods work as needed") {
    val numMsgsToTake = 10
    assert(tgDao.chats(dsUuid).size === h2dao.chats(dsUuid).size)
    for ((tgCwd, h2Cwd) <- (tgDao.chats(dsUuid).sortBy(_.chat.id) zip h2dao.chats(dsUuid).sortBy(_.chat.id))) {
      val tgChat = tgCwd.chat
      val h2Chat = h2Cwd.chat
      assert(tgChat === h2Chat)

      val tgRoot = tgDao.datasetRoot(tgChat.dsUuid)
      val h2Root = h2dao.datasetRoot(h2Chat.dsUuid)

      val allTg = tgDao.lastMessages(tgChat, tgChat.msgCount)
      val allH2 = h2dao.lastMessages(h2Chat, tgChat.msgCount)
      assert((allH2, h2Root) =~= (allTg, tgRoot))

      val scroll1 = h2dao.scrollMessages(h2Chat, 0, numMsgsToTake)
      assert(scroll1 === allH2.take(numMsgsToTake))
      assert((scroll1, h2Root) =~= (tgDao.scrollMessages(tgChat, 0, numMsgsToTake), tgRoot))

      val scroll2 = h2dao.scrollMessages(h2Chat, 1, numMsgsToTake)
      assert(scroll2 === allH2.tail.take(numMsgsToTake))
      assert((scroll2, h2Root) =~= (tgDao.scrollMessages(tgChat, 1, numMsgsToTake), tgRoot))

      val before1 = h2dao.messagesBefore(h2Chat, allH2.last, numMsgsToTake)
      assert(before1.last === allH2.last)
      assert(before1 === allH2.takeRight(numMsgsToTake))
      assert((before1, h2Root) =~= (tgDao.messagesBefore(tgChat, allTg.last, numMsgsToTake), tgRoot))

      val before2 = h2dao.messagesBefore(h2Chat, allH2.dropRight(1).last, numMsgsToTake)
      assert(before2.last === allH2.dropRight(1).last)
      assert(before2 === allH2.dropRight(1).takeRight(numMsgsToTake))
      assert((before2, h2Root) =~= (tgDao.messagesBefore(tgChat, allTg.dropRight(1).last, numMsgsToTake), tgRoot))

      val after1 = h2dao.messagesAfter(h2Chat, allH2.head, numMsgsToTake)
      assert(after1.head === allH2.head)
      assert(after1 === allH2.take(numMsgsToTake))
      assert((after1, h2Root) =~= (tgDao.messagesAfter(tgChat, allTg.head, numMsgsToTake), tgRoot))

      val after2 = h2dao.messagesAfter(h2Chat, allH2(1), numMsgsToTake)
      assert(after2.head === allH2(1))
      assert(after2 === allH2.tail.take(numMsgsToTake))
      assert((after2, h2Root) =~= (tgDao.messagesAfter(tgChat, allTg(1), numMsgsToTake), tgRoot))

      val between1 = h2dao.messagesBetween(h2Chat, allH2.head, allH2.last)
      assert(between1 === allH2)
      assert((between1, h2Root) =~= (tgDao.messagesBetween(tgChat, allTg.head, allTg.last), tgRoot))

      val between2 = h2dao.messagesBetween(h2Chat, allH2(1), allH2.last)
      assert(between2 === allH2.tail)
      assert((between2, h2Root) =~= (tgDao.messagesBetween(tgChat, allTg(1), allTg.last), tgRoot))

      val between3 = h2dao.messagesBetween(h2Chat, allH2.head, allH2.dropRight(1).last)
      assert(between3 === allH2.dropRight(1))
      assert((between3, h2Root) =~= (tgDao.messagesBetween(tgChat, allTg.head, allTg.dropRight(1).last), tgRoot))

      val countBetween1 = h2dao.countMessagesBetween(h2Chat, allH2.head, allH2.last)
      assert(countBetween1 === allH2.size - 2)
      assert(countBetween1 === tgDao.countMessagesBetween(tgChat, allTg.head, allTg.last))

      if (countBetween1 > 0) {
        val countBetween2 = h2dao.countMessagesBetween(h2Chat, allH2(1), allH2.last)
        assert(countBetween2 === allH2.size - 3)
        assert(countBetween2 === tgDao.countMessagesBetween(tgChat, allTg(1), allTg.last))

        val countBetween3 = h2dao.countMessagesBetween(h2Chat, allH2.head, allH2.dropRight(1).last)
        assert(countBetween3 === allH2.size - 3)
        assert(countBetween3 === tgDao.countMessagesBetween(tgChat, allTg.head, allTg.dropRight(1).last))
      }

      val last = h2dao.lastMessages(h2Chat, numMsgsToTake)
      assert(last === allH2.takeRight(numMsgsToTake))
      assert((last, h2Root) =~= (tgDao.lastMessages(tgChat, numMsgsToTake), tgRoot))
      assert(last.lastOption === h2Cwd.lastMsgOption)
      (last.lastOption, tgCwd.lastMsgOption) match {
        case (Some(last1), Some(last2)) => assert((last1, h2Root) =~= (last2, tgRoot))
        case (None, None)               => // NOOP
        case _                          => fail("Mismatch! " + (last.lastOption, tgCwd.lastMsgOption))
      }
    }
  }

  test("update user") {
    val myself = h2dao.myself(dsUuid)

    def personalChatWith(u: User): Option[Chat] =
      h2dao.chats(dsUuid) map (_.chat) find { c =>
        c.tpe == ChatType.Personal &&
        c.memberIds.contains(u.id) &&
        h2dao.firstMessages(c, 99999).exists(_.fromId == myself.id)
      }

    val users = h2dao.users(dsUuid)
    val user1 = users.find(u => u != myself && personalChatWith(u).isDefined).get

    def doUpdate(u: User): Unit = {
      h2dao.updateUser(u)
      val usersA = h2dao.users(dsUuid)
      assert(usersA.find(_.id == user1.id).get === u)

      val chatA = personalChatWith(u) getOrElse fail("Chat not found after updating!")
      assert(chatA.name === u.prettyNameOption)
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
      assert(chat1After.name === chat1Before.name)
    }
  }

  test("message fetching corner cases") {
    freeH2Dao()

    val localTgDao = TestUtils.createSimpleDao("TG", {
      (3 to 7) map (TestUtils.createRegularMessage(_, 1))
    }, 2)
    val localDsUuid = localTgDao.datasets.head.uuid
    val chat        = localTgDao.chats(localDsUuid).head.chat
    val msgs        = localTgDao.firstMessages(chat, 999999)
    dir = Files.createTempDirectory(null).toFile

    h2dao = manager.create(dir)
    h2dao.copyAllFrom(localTgDao)

    val dsUuid = localTgDao.datasets.head.uuid
    for {
      dao  <- Seq(localTgDao, h2dao)
      chat <- dao.chats(localDsUuid).map(_.chat).sortBy(_.id)
    } {
      val clue                        = s"Dao is ${dao.getClass.getSimpleName}"
      implicit def toMessage(i: Long) = msgs.find(_.sourceId contains i).get

      assert(dao.messagesBefore(chat, 3L, 10) === Seq(msgs.head), clue)

      assert(dao.messagesAfter(chat, 7L, 10) === Seq(msgs.last), clue)

      assert(dao.messagesBetween(chat, 3L, 3L) === Seq(msgs.head), clue)
      assert(dao.messagesBetween(chat, 7L, 7L) === Seq(msgs.last), clue)

      assert(dao.countMessagesBetween(chat, 3L, 3L) === 0, clue)
      assert(dao.countMessagesBetween(chat, 3L, 4L) === 0, clue)
      assert(dao.countMessagesBetween(chat, 3L, 5L) === 1, clue)

      assert(dao.countMessagesBetween(chat, 7L, 7L) === 0, clue)
      assert(dao.countMessagesBetween(chat, 6L, 7L) === 0, clue)
      assert(dao.countMessagesBetween(chat, 5L, 7L) === 1, clue)
    }
  }

  test("delete chat") {
    val chats = h2dao.chats(dsUuid).map(_.chat)
    val users = h2dao.users(dsUuid)

    {
      // User is not deleted because it participates in another chat
      val chatToDelete = chats.find(c => c.tpe == ChatType.Personal && c.id == 9777777777L).get
      h2dao.deleteChat(chatToDelete)
      assert(h2dao.chats(dsUuid).size === chats.size - 1)
      assert(h2dao.users(dsUuid).size === users.size)
      assert(h2dao.firstMessages(chatToDelete, 10).isEmpty)
    }

    {
      // User is deleted
      val chatToDelete = chats.find(c => c.tpe == ChatType.Personal && c.id == 4321012345L).get
      h2dao.deleteChat(chatToDelete)
      assert(h2dao.chats(dsUuid).size === chats.size - 2)
      assert(h2dao.users(dsUuid).size === users.size - 1)
      assert(h2dao.firstMessages(chatToDelete, 10).isEmpty)
    }
  }

  test("merge (absorb) user") {
    def fetchPersonalChat(u: User): Chat = {
      h2dao.chats(dsUuid).map(_.chat).find(c => c.tpe == ChatType.Personal && c.memberIds.contains(u.id)) getOrElse {
        fail(s"Chat for user $u not found!")
      }
    }

    val usersBefore = h2dao.users(dsUuid)
    val chatsBefore = h2dao.chats(dsUuid)

    val baseUser     = usersBefore.find(_.id == 777777777L).get
    val absorbedUser = usersBefore.find(_.id == 32507588L).get

    val baseUserPc     = fetchPersonalChat(baseUser)
    val absorbedUserPc = fetchPersonalChat(absorbedUser)

    val baseUserPcMsgs     = h2dao.firstMessages(baseUserPc, 99999)
    val absorbedUserPcMsgs = h2dao.firstMessages(absorbedUserPc, 99999)

    h2dao.mergeUsers(baseUser, absorbedUser.copy(firstNameOption = Some("new-name")))

    val chatsAfter = h2dao.chats(dsUuid)
    val usersAfter = h2dao.users(dsUuid)

    // Verify users
    assert(usersAfter.size === usersBefore.size - 1)
    val expectedUser = absorbedUser.copy(
      id              = baseUser.id,
      firstNameOption = Some("new-name")
    )
    assert(usersAfter.find(_.id == baseUser.id) === Some(expectedUser))
    assert(!usersAfter.exists(_.id == absorbedUser.id))

    // Verify chats
    assert(chatsAfter.size === chatsBefore.size - 1)
    val expectedChat = baseUserPc.copy(
      name       = expectedUser.firstNameOption,
      msgCount   = baseUserPcMsgs.size + absorbedUserPcMsgs.size
    )
    assert(chatsAfter.find(_.chat.id == baseUserPc.id).map(_.chat) === Some(expectedChat))
    assert(!chatsAfter.exists(_.chat.id == absorbedUserPc.id))

    // Verify messages
    val expectedMessages =
      (baseUserPcMsgs ++ absorbedUserPcMsgs.map { m =>
        m.copy(
          sourceId = None,
          fromId   = baseUser.id,
        )
      }).sortBy(_.timestamp)
    assert(h2dao.firstMessages(chatsAfter.find(_.chat.id == baseUserPc.id).get.chat, 99999) === expectedMessages)
  }

  test("delete dataset") {
    h2dao.deleteDataset(dsUuid)
    assert(h2dao.datasets.isEmpty)

    // Dataset files has been moved to a backup dir
    assert(!h2dao.datasetRoot(dsUuid).exists())
    assert(new File(h2dao.getBackupPath(), dsUuid.value).exists())
  }

  test("shift dataset time") {
    val chat = h2dao.chats(dsUuid).head.chat
    def getMsg() = {
      h2dao.firstMessages(chat, 1).head
    }
    val msg0 = getMsg()

    {
      // +8
      h2dao.shiftDatasetTime(dsUuid, 8)
      val msg1 = getMsg()
      assert(msg1.internalId == msg0.internalId)
      assert(msg1.time == msg0.time.plusHours(8))
    }

    {
      // +8 -5
      h2dao.shiftDatasetTime(dsUuid, -5)
      val msg1 = getMsg()
      assert(msg1.internalId == msg0.internalId)
      assert(msg1.time == msg0.time.plusHours(3))
    }
  }
}
