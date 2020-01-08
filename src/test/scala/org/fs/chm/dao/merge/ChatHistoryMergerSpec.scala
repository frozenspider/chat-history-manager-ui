package org.fs.chm.dao.merge

import java.util.UUID

import scala.collection.immutable.ListMap
import scala.util.Random

import org.fs.chm.TestHelper
import org.fs.chm.dao._
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfter
import org.scalatest.FunSuite
import org.slf4s.Logging
import com.github.nscala_time.time.Imports._
import org.fs.chm.dao.merge.ChatHistoryMerger._

@RunWith(classOf[org.scalatest.junit.JUnitRunner])
class ChatHistoryMergerSpec //
    extends FunSuite
    with TestHelper
    with Logging
    with BeforeAndAfter {

  val date      = DateTime.parse("2019-01-02T11:15:21")
  val rnd       = new Random()
  val maxId     = (ChatHistoryMerger.BatchSize * 3)
  val maxUserId = 3
  def rndUserId = 1 + rnd.nextInt(maxUserId)

  test("merge chats - simplest single message") {
    val msgs     = Seq(createMessage(1, 1))
    val helper   = new MergerHelper(msgs, msgs)
    val analysis = helper.merger.analyzeCombine(helper.d1chat, helper.d2chat)
    assert(analysis.isEmpty)
  }

  test("merge chats - same multiple messages") {
    val msgs: Seq[Message] = (1 to maxId) map { i => createMessage(i, rndUserId) }
    val helper   = new MergerHelper(msgs, msgs)
    val analysis = helper.merger.analyzeCombine(helper.d1chat, helper.d2chat)
    assert(analysis.isEmpty)
  }

  test("merge chats - added one message in the middle") {
    val msgs123  = for (i <- 1 to 3) yield createMessage(i, rndUserId)
    val msgs13   = msgs123.filter(_.id != 2)
    val helper   = new MergerHelper(msgs13, msgs123)
    val analysis = helper.merger.analyzeCombine(helper.d1chat, helper.d2chat)
    assert(analysis === Seq(Mismatch.Addition(1, (2, 2))))
  }

  test("merge chats - changed one message in the middle") {
    val msgsA    = for (i <- 1 to 3) yield createMessage(i, rndUserId)
    val msgsB    = changedMessages(msgsA, (_ == 2))
    val helper   = new MergerHelper(msgsA, msgsB)
    val analysis = helper.merger.analyzeCombine(helper.d1chat, helper.d2chat)
    assert(analysis === Seq(Mismatch.Conflict((2, 2), (2, 2))))
  }

  /**
   * {{{
   * Master messages - 1 2     5
   * Slave messages  - 1   3 4 5
   * }}}
   */
  test("merge chats - changes one message to multiple") {
    val msgs = for (i <- 1 to 5) yield createMessage(i, rndUserId)
    val msgsA    = msgs.filter(Seq(1, 2, 5) contains _.id)
    val msgsB    = msgs.filter(Seq(1, 3, 4, 5) contains _.id)
    val helper   = new MergerHelper(msgsA, msgsB)
    val analysis = helper.merger.analyzeCombine(helper.d1chat, helper.d2chat)
    assert(analysis === Seq(Mismatch.Conflict((2, 2), (3, 4))))
  }

  /**
   * {{{
   * Master messages -         N
   * Slave messages  - 1 2 ... N
   * }}}
   */
  test("merge chats - added multiple message in the beginning") {
    val msgs     = for (i <- 1 to maxId) yield createMessage(i, rndUserId)
    val msgsL    = Seq(msgs.last)
    val helper   = new MergerHelper(msgsL, msgs)
    val analysis = helper.merger.analyzeCombine(helper.d1chat, helper.d2chat)
    assert(analysis === Seq(Mismatch.Addition(-1, (1, maxId - 1))))
  }

  /**
   * {{{
   * Master messages - 1  2  ...  N
   * Slave messages  - 1* 2* ...* N
   * }}}
   */
  test("merge chats - changed multiple message in the beginning") {
    val msgsA    = for (i <- 1 to maxId) yield createMessage(i, rndUserId)
    val msgsB    = changedMessages(msgsA, (_ < maxId))
    val helper   = new MergerHelper(msgsA, msgsB)
    val analysis = helper.merger.analyzeCombine(helper.d1chat, helper.d2chat)
    assert(analysis === Seq(Mismatch.Conflict((1, maxId - 1), (1, maxId - 1))))
  }

  /**
   * {{{
   * Master messages - 1       N
   * Slave messages  - 1 2 ... N
   * }}}
   */
  test("merge chats - added multiple message in the middle") {
    val msgs     = for (i <- 1 to maxId) yield createMessage(i, rndUserId)
    val msgsFL   = Seq(msgs.head, msgs.last)
    val helper   = new MergerHelper(msgsFL, msgs)
    val analysis = helper.merger.analyzeCombine(helper.d1chat, helper.d2chat)
    assert(analysis === Seq(Mismatch.Addition(1, (2, maxId - 1))))
  }

  /**
   * {{{
   * Master messages - 1  2  ...  N
   * Slave messages  - 1  2* ...* N
   * }}}
   */
  test("merge chats - changed multiple message in the middle") {
    val msgsA    = for (i <- 1 to maxId) yield createMessage(i, rndUserId)
    val msgsB    = changedMessages(msgsA, (id => id > 1 && id < maxId))
    val helper   = new MergerHelper(msgsA, msgsB)
    val analysis = helper.merger.analyzeCombine(helper.d1chat, helper.d2chat)
    assert(analysis === Seq(Mismatch.Conflict((2, maxId - 1), (2, maxId - 1))))
  }

  /**
   * {{{
   * Master messages - 1
   * Slave messages  - 1 2 ... N
   * }}}
   */
  test("merge chats - added multiple message in the end") {
    val msgs     = for (i <- 1 to maxId) yield createMessage(i, rndUserId)
    val msgsF    = Seq(msgs.head)
    val helper   = new MergerHelper(msgsF, msgs)
    val analysis = helper.merger.analyzeCombine(helper.d1chat, helper.d2chat)
    assert(analysis === Seq(Mismatch.Addition(1, (2, maxId))))
  }

  /**
   * {{{
   * Master messages - 1  2  ...  N
   * Slave messages  - 1  2* ...* N*
   * }}}
   */
  test("merge chats - changed multiple message in the end") {
    val msgsA    = for (i <- 1 to maxId) yield createMessage(i, rndUserId)
    val msgsB    = changedMessages(msgsA, (_ > 1))
    val helper   = new MergerHelper(msgsA, msgsB)
    val analysis = helper.merger.analyzeCombine(helper.d1chat, helper.d2chat)
    assert(analysis === Seq(Mismatch.Conflict((2, maxId), (2, maxId))))
  }

  //
  // Helpers
  //

  def createMessage(idx: Int, userId: Int): Message = {
    Message.Regular(
      id                     = idx,
      time                   = date.plusMinutes(idx),
      editTimeOption         = Some(date.plusMinutes(idx).plusSeconds(5)),
      fromNameOption         = Some("u" + userId),
      fromId                 = userId,
      forwardFromNameOption  = Some("u" + userId),
      replyToMessageIdOption = Some(rnd.nextInt(idx)), // Any previous message
      textOption             = Some(RichText(Seq(RichText.Plain(s"Hello there, ${idx}!")))),
      contentOption          = Some(Content.Poll(s"Hey, ${idx}!"))
    )
  }

  def changedMessages(msgs: Seq[Message], idCondition: Long => Boolean): Seq[Message] = {
    msgs.collect {
      case m: Message.Regular if idCondition(m.id) =>
        m.copy(textOption = Some(RichText(Seq(RichText.Plain("Different message")))))
      case m =>
        m
    }
  }

  class MergerHelper(msgs1: Seq[Message], msgs2: Seq[Message]) {

    val (dao1, d1ds, d1chat) = createDao("One", msgs1)
    val (dao2, d2ds, d2chat) = createDao("Two", msgs2)

    def merger: ChatHistoryMerger =
      new ChatHistoryMerger(dao1, d1ds, dao2, d2ds, Seq(MergeOption.Combine(d1chat, d2chat)))

    private def createDao(nameSuffix: String, msgs: Seq[Message]) = {
      val ds = Dataset(
        uuid       = UUID.randomUUID(),
        alias      = "Dataset " + nameSuffix,
        sourceType = "test source"
      )
      val chat  = createChat(ds, 1, "One", msgs.size)
      val users = (1 to maxUserId).map(i => createUser(ds, i))
      val dao = new EagerChatHistoryDao(
        name              = "Dao " + nameSuffix,
        dataPathRoot      = null,
        dataset           = ds,
        myself1           = users.head,
        rawUsers          = users,
        chatsWithMessages = ListMap(chat -> msgs.toIndexedSeq)
      ) with MutableChatHistoryDao

      (dao, ds, chat)
    }

    private def createChat(ds: Dataset, idx: Int, nameSuffix: String, messagesSize: Int): Chat = Chat(
      dsUuid        = ds.uuid,
      id            = idx,
      nameOption    = Some("Chat " + nameSuffix),
      tpe           = ChatType.Personal,
      imgPathOption = None,
      msgCount      = messagesSize
    )

    private def createUser(ds: Dataset, idx: Int): User = User(
      dsUuid             = ds.uuid,
      id                 = idx,
      firstNameOption    = Some("User"),
      lastNameOption     = Some(idx.toString),
      usernameOption     = Some("user" + idx),
      phoneNumberOption  = Some("xxx xx xx".replaceAll("x", idx.toString)),
      lastSeenTimeOption = Some(DateTime.now())
    )
  }

}
