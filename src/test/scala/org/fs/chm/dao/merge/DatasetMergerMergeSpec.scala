package org.fs.chm.dao.merge

import java.io.File
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets
import java.nio.file.Files

import scala.collection.immutable.ListMap

import org.fs.chm.TestHelper
import org.fs.chm.WithH2Dao
import org.fs.chm.dao._
import org.fs.chm.dao.merge.DatasetMerger._
import org.fs.chm.utility.IoUtils._
import org.fs.chm.utility.TestUtils._
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfter
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite
import org.slf4s.Logging

@RunWith(classOf[org.scalatest.junit.JUnitRunner])
class DatasetMergerMergeSpec //
    extends FunSuite
    with TestHelper
    with WithH2Dao
    with Logging
    with BeforeAndAfter
    with BeforeAndAfterAll {
  import DatasetMergerHelper._

  before {
    initH2Dao()
  }

  after {
    freeH2Dao()
  }

  test("merge users - all options") {
    val users      = (1 to 5) map (i => createUser(noUuid, i))
    val usersA     = users filter (Seq(1, 3, 4, 5) contains _.id)
    val usersB     = changedUsers(users filter (Seq(2, 3, 4, 5) contains _.id), _ => true)
    val helper     = H2MergerHelper.fromUsers(usersA, usersB)

    val newDs = helper.merger.merge(
      Seq(
        UserMergeOption.Keep(helper.d1users.byId(1)),
        UserMergeOption.Add(helper.d2users.byId(2)),
        UserMergeOption.Replace(helper.d1users.byId(3), helper.d2users.byId(3))
      ), Seq.empty
    )
    assert(helper.dao1.chats(newDs.uuid).isEmpty)
    val newUsers = helper.dao1.users(newDs.uuid)
    assert(newUsers.size === 3)
    assert(newUsers.byId(1) === helper.d1users.byId(1).copy(dsUuid = newDs.uuid))
    assert(newUsers.byId(2) === helper.d2users.byId(2).copy(dsUuid = newDs.uuid))
    assert(newUsers.byId(3) === helper.d2users.byId(3).copy(dsUuid = newDs.uuid))
  }

  test("merge chats - keep single message, basic checks") {
    val msgs   = Seq(createRegularMessage(1, 1))
    val helper = H2MergerHelper.fromMessages(msgs, msgs)
    val chatMerges = Seq[ChatMergeOption](
      ChatMergeOption.Keep(helper.d1chat)
    )
    assert(helper.dao1.datasets.size === 1)
    val newDs = helper.merger.merge(keepSignleUser(helper), chatMerges)
    assert(helper.dao1.datasets.size === 2)
    val newChats = helper.dao1.chats(newDs.uuid)
    assert(helper.dao1.chats(newDs.uuid).size === 1)
    val newMessages = helper.dao1.firstMessages(newChats.head, Int.MaxValue)
    assert(newMessages.size === 1)
    assert(newMessages.head =~= helper.d1msgs.head)
    assertFiles(helper.dao1, newDs, msgsPaths(helper.dao1, helper.d1ds, helper.d1msgs))
  }

  test("merge chats - keep two messages") {
    val msgs   = for (i <- 1 to 6) yield createRegularMessage(i, 1)
    val msgsA  = msgs.filter(Seq(3, 4) contains _.sourceIdOption.get)
    val msgsB  = changedMessages(msgs, _ => true)
    val helper = H2MergerHelper.fromMessages(msgsA, msgsB)
    val chatMerges = Seq[ChatMergeOption](
      ChatMergeOption.Keep(helper.d1chat)
    )
    val newDs = helper.merger.merge(keepSignleUser(helper), chatMerges)
    val newMessages = {
      val newChats = helper.dao1.chats(newDs.uuid)
      helper.dao1.firstMessages(newChats.head, Int.MaxValue)
    }
    assert(newMessages.size === helper.d1msgs.size)
    for ((nm, om) <- (newMessages zip helper.d1msgs)) {
      assert(nm =~= om)
    }

    assertFiles(helper.dao1, newDs, msgsPaths(helper.dao1, helper.d1ds, newMessages))
  }

  test("merge chats - add two messages") {
    val msgs   = for (i <- 1 to 5) yield createRegularMessage(i, 1)
    val msgsA  = msgs
    val msgsB  = changedMessages(msgs.filter(Seq(3, 4) contains _.sourceIdOption.get), _ => true)
    val helper = H2MergerHelper.fromMessages(msgsA, msgsB)
    val chatMerges = Seq[ChatMergeOption](
      ChatMergeOption.Add(helper.d2chat)
    )

    val newDs = helper.merger.merge(keepSignleUser(helper), chatMerges)
    val newMessages = {
      val newChats = helper.dao1.chats(newDs.uuid)
      helper.dao1.firstMessages(newChats.head, Int.MaxValue)
    }
    assert(newMessages.size === 2)
    assert(newMessages(0) =~= helper.d2msgs.bySrcId(3))
    assert(newMessages(1) =~= helper.d2msgs.bySrcId(4))

    assertFiles(helper.dao1, newDs, msgsPaths(helper.dao2, helper.d2ds, newMessages))
  }

  /**
   * {{{
   * Master messages - 1  2  3  4  5  6
   * Slave messages  - 1* 2* 3* 4* 5* 6*
   * }}}
   * R(3, 4)
   */
  test("merge chats - replace two messages, dicard the rest") {
    val msgs   = for (i <- 1 to 5) yield createRegularMessage(i, 1)
    val msgsA  = msgs
    val msgsB  = changedMessages(msgs, _ => true)
    val helper = H2MergerHelper.fromMessages(msgsA, msgsB)
    val chatMerges = Seq[ChatMergeOption](
      ChatMergeOption.Combine(
        helper.d1chat,
        helper.d2chat,
        IndexedSeq(
          MessagesMergeOption.Replace(
            firstMasterMsg = helper.d1msgs.bySrcId(3),
            lastMasterMsg  = helper.d1msgs.bySrcId(4),
            firstSlaveMsg  = helper.d2msgs.bySrcId(3),
            lastSlaveMsg   = helper.d2msgs.bySrcId(4)
          )
        )
      )
    )
    val newDs = helper.merger.merge(keepSignleUser(helper), chatMerges)
    val newMessages = {
      val newChats = helper.dao1.chats(newDs.uuid)
      helper.dao1.firstMessages(newChats.head, Int.MaxValue)
    }
    assert(newMessages.size === 2)
    assert(newMessages(0) =~= helper.d2msgs.bySrcId(3))
    assert(newMessages(1) =~= helper.d2msgs.bySrcId(4))

    assertFiles(helper.dao1, newDs, msgsPaths(helper.dao2, helper.d2ds, newMessages))
  }

  /**
   * {{{
   * Master messages - 1  2  ... max
   * Slave messages  - 1' 2' ... max'
   * }}}
   * A(2, max/3), KA(max/3 + 1, max/2), KR(max/2 + 1, max*2/3), R(max*2/3 + 1, max - 1)
   */
  test("merge chats - combine, each") {
    val msgs            = for (i <- 1 to maxId) yield createRegularMessage(i, 1)
    val msgsA           = msgs
    val msgsB           = changedMessages(msgs, _ => true)
    val helper          = H2MergerHelper.fromMessages(msgsA, msgsB)
    val (bp1, bp2, bp3) = (maxId / 3, maxId / 2, maxId * 2 / 3)
    val chatMerges = Seq[ChatMergeOption](
      ChatMergeOption.Combine(
        helper.d1chat,
        helper.d2chat,
        IndexedSeq(
          MessagesMergeOption.Add(
            firstSlaveMsg  = helper.d2msgs.bySrcId(2),
            lastSlaveMsg   = helper.d2msgs.bySrcId(bp1)
          ),
          // Keep, with added messages specified - should act as replace
          MessagesMergeOption.Keep(
            firstMasterMsg      = helper.d1msgs.bySrcId(bp1 + 1),
            lastMasterMsg       = helper.d1msgs.bySrcId(bp2),
            firstSlaveMsgOption = Some(helper.d2msgs.bySrcId(bp1 + 1)),
            lastSlaveMsgOption  = Some(helper.d2msgs.bySrcId(bp2))
          ),
          MessagesMergeOption.Keep(
            firstMasterMsg      = helper.d1msgs.bySrcId(bp2 + 1),
            lastMasterMsg       = helper.d1msgs.bySrcId(bp3),
            firstSlaveMsgOption = None,
            lastSlaveMsgOption  = None
          ),
          MessagesMergeOption.Replace(
            firstMasterMsg = helper.d1msgs.bySrcId(bp3 + 1),
            lastMasterMsg  = helper.d1msgs.bySrcId(maxId - 1),
            firstSlaveMsg  = helper.d2msgs.bySrcId(bp3 + 1),
            lastSlaveMsg   = helper.d2msgs.bySrcId(maxId - 1)
          )
        )
      )
    )
    val newDs = helper.merger.merge(keepSignleUser(helper), chatMerges)
    val newMessages = {
      val newChats = helper.dao1.chats(newDs.uuid)
      helper.dao1.firstMessages(newChats.head, Int.MaxValue)
    }
    assert(newMessages.size === msgs.size - 2)
    assert(newMessages(0) =~= helper.d2msgs.bySrcId(2))
    val (expectedMessages, expectedFiles) = {
      val msgs1 = helper.d2msgs.slice(1, bp1)
      val msgs2 = helper.d2msgs.slice(bp1, bp2)
      val msgs3 = helper.d1msgs.slice(bp2, bp3)
      val msgs4 = helper.d2msgs.slice(bp3, maxId - 1)
      (
        msgs1 ++ msgs2 ++ msgs3 ++ msgs4,
        (msgsPaths(helper.dao2, helper.d2ds, msgs1)
          ++ msgsPaths(helper.dao2, helper.d2ds, msgs2)
          ++ msgsPaths(helper.dao1, helper.d1ds, msgs3)
          ++ msgsPaths(helper.dao2, helper.d2ds, msgs4))
      )
    }
    assert(expectedMessages.size === newMessages.size)
    for ((m1, m2) <- (expectedMessages zip newMessages).par) {
      assert(m1 =~= m2, (m1, m2))
    }

    assertFiles(helper.dao1, newDs, expectedFiles)
  }

  //
  // Helpers
  //

  def keepSignleUser(helper: H2MergerHelper): Seq[UserMergeOption] = {
    Seq(UserMergeOption.Keep(helper.d1users.head))
  }

  def msgsPaths(dao: ChatHistoryDao, ds: Dataset, msgs: Seq[Message]): Set[File] = {
    msgs.flatMap(_.files).toSet
  }

  def assertFiles(dao: ChatHistoryDao, ds: Dataset, expectedFiles: Set[File]) = {
    val files = dao.datasetFiles(ds.uuid)
    assert(files.size === expectedFiles.size)
    // Given that expected files might come from different DS's, we rely on random names being unique and sort by name
    val sortedFiles         = files.toSeq.sortBy(_.getName)
    val sortedExpectedFiles = expectedFiles.toSeq.sortBy(_.getName)
    for ((af, ef) <- (sortedFiles zip sortedExpectedFiles).par) {
      assert(af.getName === ef.getName, (af, ef))
      assert(af.exists === ef.exists, (af, ef))
      assert(af =~= ef, ((af, ef), (af.bytes, ef.bytes)))
    }
  }

  class H2MergerHelper(
      users1: Seq[User],
      chatsWithMsgs1: ListMap[Chat, Seq[Message]],
      users2: Seq[User],
      chatsWithMsgs2: ListMap[Chat, Seq[Message]]
  ) {
    val (dao1, d1ds, d1users, d1chat, d1msgs) = {
      h2dao.copyAllFrom(createDao("One", users1, chatsWithMsgs1, amendMessageWithContent))
      val (ds, users, chat, msgs) = getSimpleDaoEntities(h2dao)
      (h2dao, ds, users, chat, msgs)
    }
    val (dao2, d2ds, d2users, d2chat, d2msgs) = {
      val dao                     = createDao("Two", users2, chatsWithMsgs2, amendMessageWithContent)
      val (ds, users, chat, msgs) = getSimpleDaoEntities(dao)
      (dao, ds, users, chat, msgs)
    }

    def merger: DatasetMerger =
      new DatasetMerger(dao1, d1ds, dao2, d2ds)

    private def amendMessageWithContent(path: File, msg: Message): Message = msg match {
      case msg: Message.Regular =>
        val file1 = new File(path, rnd.alphanumeric.take(30).mkString("", "", ".bin"))
        val file2 = new File(path, rnd.alphanumeric.take(31).mkString("", "", ".bin"))
        Files.write(file1.toPath, rnd.alphanumeric.take(256).mkString.getBytes)
        Files.write(file2.toPath, rnd.alphanumeric.take(256).mkString.getBytes)
        val content = Content.File(
          pathOption          = Some(file1),
          thumbnailPathOption = Some(file2),
          mimeTypeOption      = Some("mt"),
          titleOption         = Some("t"),
          performerOption     = Some("p"),
          durationSecOption   = Some(1),
          widthOption         = Some(2),
          heightOption        = Some(3),
        )
        msg.copy(contentOption = Some(content))
      case _ =>
        throw new MatchError("Unexpected message type for " + msg)
    }
  }

  object H2MergerHelper {
    private val noChatMsgs1 = ListMap(createChat(noUuid, 1, "One", 0) -> Seq.empty[Message])
    private val noChatMsgs2 = ListMap(createChat(noUuid, 2, "Two", 0) -> Seq.empty[Message])

    def fromMessages(msgs1: Seq[Message], msgs2: Seq[Message]): H2MergerHelper = {
      val users1    = (1 to msgs1.map(_.fromId).max.toInt).map(i => createUser(noUuid, i))
      val users2    = (1 to msgs2.map(_.fromId).max.toInt).map(i => createUser(noUuid, i))
      val chatMsgs1 = ListMap(createChat(noUuid, 1, "One", msgs1.size) -> msgs1)
      val chatMsgs2 = ListMap(createChat(noUuid, 2, "Two", msgs2.size) -> msgs2)
      new H2MergerHelper(users1, chatMsgs1, users2, chatMsgs2)
    }

    def fromUsers(users1: Seq[User], users2: Seq[User]): H2MergerHelper = {
      new H2MergerHelper(users1, noChatMsgs1, users2, noChatMsgs2)
    }
  }
}
