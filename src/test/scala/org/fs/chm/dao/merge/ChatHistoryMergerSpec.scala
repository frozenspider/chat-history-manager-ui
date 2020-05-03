package org.fs.chm.dao.merge

import java.util.UUID

import org.fs.chm.TestHelper
import org.fs.chm.dao._
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfter
import org.scalatest.FunSuite
import org.slf4s.Logging
import com.github.nscala_time.time.Imports._
import org.fs.chm.dao.merge.ChatHistoryMerger._
import org.fs.chm.utility.TestUtils._

@RunWith(classOf[org.scalatest.junit.JUnitRunner])
class ChatHistoryMergerSpec //
    extends FunSuite
    with TestHelper
    with Logging
    with BeforeAndAfter {

  val maxId = (ChatHistoryMerger.BatchSize * 3)
  val maxUserId = 3
  def rndUserId = 1 + rnd.nextInt(maxUserId)

  test("merge chats - same single message") {
    val msgs = Seq(createRegularMessage(1, 1))
    val helper = new MergerHelper(msgs, msgs)
    val analysis = helper.merger.analyzeMergingChats(helper.d1chat, helper.d2chat)
    assert(analysis.isEmpty)
  }

  test("merge chats - same multiple messages") {
    val msgs: Seq[Message] = (1 to maxId) map { i =>
      createRegularMessage(i, rndUserId)
    }
    val helper = new MergerHelper(msgs, msgs)
    val analysis = helper.merger.analyzeMergingChats(helper.d1chat, helper.d2chat)
    assert(analysis.isEmpty)
  }

  test("merge chats - added one message in the middle") {
    val msgs = for (i <- 1 to 3) yield createRegularMessage(i, rndUserId)
    val msgs123 = msgs
    val msgs13 = msgs123.filter(_.sourceIdOption.get != 2)
    val helper = new MergerHelper(msgs13, msgs123)
    val analysis = helper.merger.analyzeMergingChats(helper.d1chat, helper.d2chat)
    assert(
      analysis === Seq(
        Mismatch.Addition(
          Some(helper.d1msgs.bySrcId(1)),
          Some(helper.d1msgs.bySrcId(3)),
          (helper.d2msgs.bySrcId(2), helper.d2msgs.bySrcId(2))
        )
      )
    )
  }

  test("merge chats - changed one message in the middle") {
    val msgs = for (i <- 1 to 3) yield createRegularMessage(i, rndUserId)
    val msgsA = msgs
    val msgsB = changedMessages(msgsA, (_ == 2))
    val helper = new MergerHelper(msgsA, msgsB)
    val analysis = helper.merger.analyzeMergingChats(helper.d1chat, helper.d2chat)
    assert(
      analysis === Seq(
        Mismatch.Conflict(
          (helper.d1msgs.bySrcId(2), helper.d1msgs.bySrcId(2)),
          (helper.d2msgs.bySrcId(2), helper.d2msgs.bySrcId(2))
        )
      )
    )
  }

  /**
   * {{{
   * Master messages -         N
   * Slave messages  - 1 2 ... N
   * }}}
   */
  test("merge chats - added multiple message in the beginning") {
    val msgs = for (i <- 1 to maxId) yield createRegularMessage(i, rndUserId)
    val msgsL = Seq(msgs.last)
    val helper = new MergerHelper(msgsL, msgs)
    val analysis = helper.merger.analyzeMergingChats(helper.d1chat, helper.d2chat)
    assert(
      analysis === Seq(
        Mismatch.Addition(
          None,
          Some(helper.d1msgs.bySrcId(maxId)),
          (helper.d2msgs.bySrcId(1), helper.d2msgs.bySrcId(maxId - 1))
        )
      )
    )
  }

  /**
   * {{{
   * Master messages - 1  2  ...  N
   * Slave messages  - 1* 2* ...* N
   * }}}
   */
  test("merge chats - changed multiple message in the beginning") {
    val msgs = for (i <- 1 to maxId) yield createRegularMessage(i, rndUserId)
    val msgsA = msgs
    val msgsB = changedMessages(msgsA, (_ < maxId))
    val helper = new MergerHelper(msgsA, msgsB)
    val analysis = helper.merger.analyzeMergingChats(helper.d1chat, helper.d2chat)
    assert(
      analysis === Seq(
        Mismatch.Conflict(
          (helper.d1msgs.bySrcId(1), helper.d1msgs.bySrcId(maxId - 1)),
          (helper.d2msgs.bySrcId(1), helper.d2msgs.bySrcId(maxId - 1))
        )
      )
    )
  }

  /**
   * {{{
   * Master messages - 1       N
   * Slave messages  - 1 2 ... N
   * }}}
   */
  test("merge chats - added multiple message in the middle") {
    val msgs = for (i <- 1 to maxId) yield createRegularMessage(i, rndUserId)
    val msgsFL = Seq(msgs.head, msgs.last)
    val helper = new MergerHelper(msgsFL, msgs)
    val analysis = helper.merger.analyzeMergingChats(helper.d1chat, helper.d2chat)
    assert(
      analysis === Seq(
        Mismatch.Addition(
          Some(helper.d1msgs.bySrcId(1)),
          Some(helper.d1msgs.bySrcId(maxId)),
          (helper.d2msgs.bySrcId(2), helper.d2msgs.bySrcId(maxId - 1))
        )
      )
    )
  }

  /**
   * {{{
   * Master messages - 1  2  ...  N
   * Slave messages  - 1  2* ...* N
   * }}}
   */
  test("merge chats - changed multiple message in the middle") {
    val msgs = for (i <- 1 to maxId) yield createRegularMessage(i, rndUserId)
    val msgsA = msgs
    val msgsB = changedMessages(msgsA, (id => id > 1 && id < maxId))
    val helper = new MergerHelper(msgsA, msgsB)
    val analysis = helper.merger.analyzeMergingChats(helper.d1chat, helper.d2chat)
    assert(
      analysis === Seq(
        Mismatch.Conflict(
          (helper.d1msgs.bySrcId(2), helper.d1msgs.bySrcId(maxId - 1)),
          (helper.d2msgs.bySrcId(2), helper.d2msgs.bySrcId(maxId - 1))
        )
      )
    )
  }

  /**
   * {{{
   * Master messages - 1
   * Slave messages  - 1 2 ... N
   * }}}
   */
  test("merge chats - added multiple message in the end") {
    val msgs = for (i <- 1 to maxId) yield createRegularMessage(i, rndUserId)
    val msgsF = Seq(msgs.head)
    val helper = new MergerHelper(msgsF, msgs)
    val analysis = helper.merger.analyzeMergingChats(helper.d1chat, helper.d2chat)
    assert(
      analysis === Seq(
        Mismatch.Addition(
          Some(helper.d1msgs.bySrcId(1)),
          None,
          (helper.d2msgs.bySrcId(2), helper.d2msgs.bySrcId(maxId))
        )
      )
    )
  }

  /**
   * {{{
   * Master messages - 1  2  ...  N
   * Slave messages  - 1  2* ...* N*
   * }}}
   */
  test("merge chats - changed multiple message in the end") {
    val msgs = for (i <- 1 to maxId) yield createRegularMessage(i, rndUserId)
    val msgsA = msgs
    val msgsB = changedMessages(msgsA, (_ > 1))
    val helper = new MergerHelper(msgsA, msgsB)
    val analysis = helper.merger.analyzeMergingChats(helper.d1chat, helper.d2chat)
    assert(
      analysis === Seq(
        Mismatch.Conflict(
          (helper.d1msgs.bySrcId(2), helper.d1msgs.bySrcId(maxId)),
          (helper.d2msgs.bySrcId(2), helper.d2msgs.bySrcId(maxId))
        )
      )
    )
  }

  /**
   * {{{
   * Master messages - 1 2 3 4 5
   * Slave messages  -   2   4
   * }}}
   */
  test("merge chats - master has messages not present in slave") {
    val msgs = for (i <- 1 to 5) yield createRegularMessage(i, rndUserId)
    val msgsA = msgs
    val msgsB = msgs.filter(Seq(2, 4) contains _.sourceIdOption.get)
    val helper = new MergerHelper(msgsA, msgsB)
    val analysis = helper.merger.analyzeMergingChats(helper.d1chat, helper.d2chat)
    // We don't treat this as a conflict
    assert(analysis.isEmpty)
  }

  /**
   * {{{
   * Master messages - 1 2     5  6  7 8 9  10
   * Slave messages  -     3 4 5* 6* 7 8 9* 10* 11 12
   * }}}
   */
  test("merge chats - everything") {
    val msgs = for (i <- 1 to 12) yield createRegularMessage(i, rndUserId)
    val msgsA = msgs.filter(Seq(1, 2, 5, 6, 7, 8, 9, 10) contains _.sourceIdOption.get)
    val msgsB = changedMessages(
      msgs.filter((3 to 12) contains _.sourceIdOption.get),
      (id => Seq(5, 6, 9, 10) contains id)
    )
    val helper   = new MergerHelper(msgsA, msgsB)
    val analysis = helper.merger.analyzeMergingChats(helper.d1chat, helper.d2chat)
    assert(
      analysis === Seq(
        Mismatch.Addition(
          Some(helper.d1msgs.bySrcId(2)),
          Some(helper.d1msgs.bySrcId(5)),
          (helper.d2msgs.bySrcId(3), helper.d2msgs.bySrcId(4))
        ),
        Mismatch.Conflict(
          (helper.d1msgs.bySrcId(5), helper.d1msgs.bySrcId(6)),
          (helper.d2msgs.bySrcId(5), helper.d2msgs.bySrcId(6))
        ),
        Mismatch.Conflict(
          (helper.d1msgs.bySrcId(9), helper.d1msgs.bySrcId(10)),
          (helper.d2msgs.bySrcId(9), helper.d2msgs.bySrcId(10))
        ),
        Mismatch.Addition(
          Some(helper.d1msgs.bySrcId(10)),
          None,
          (helper.d2msgs.bySrcId(11), helper.d2msgs.bySrcId(12))
        )
      )
    )
  }

  /**
   * {{{
   * Master messages -     3 4 5* 6* 7 8 9* 10* 11 12
   * Slave messages  - 1 2     5  6  7 8 9  10
   * }}}
   */
  test("merge chats - everything, roles inverted") {
    val msgs  = for (i <- 1 to 12) yield createRegularMessage(i, rndUserId)
    val msgsA = msgs.filter((3 to 12) contains _.sourceIdOption.get)
    val msgsB = changedMessages(
      msgs.filter(Seq(1, 2, 5, 6, 7, 8, 9, 10) contains _.sourceIdOption.get),
      (id => Seq(5, 6, 9, 10) contains id)
    )
    val helper   = new MergerHelper(msgsA, msgsB)
    val analysis = helper.merger.analyzeMergingChats(helper.d1chat, helper.d2chat)
    assert(
      analysis === Seq(
        Mismatch.Addition(
          None,
          Some(helper.d1msgs.bySrcId(3)),
          (helper.d2msgs.bySrcId(1), helper.d2msgs.bySrcId(2))
        ),
        Mismatch.Conflict(
          (helper.d1msgs.bySrcId(5), helper.d1msgs.bySrcId(6)),
          (helper.d2msgs.bySrcId(5), helper.d2msgs.bySrcId(6))
        ),
        Mismatch.Conflict(
          (helper.d1msgs.bySrcId(9), helper.d1msgs.bySrcId(10)),
          (helper.d2msgs.bySrcId(9), helper.d2msgs.bySrcId(10))
        )
      ))
  }

  //
  // Helpers
  //

  def changedMessages(msgs: Seq[Message], idCondition: Long => Boolean): Seq[Message] = {
    msgs.collect {
      case m: Message.Regular if idCondition(m.sourceIdOption.get) =>
        m.copy(textOption = Some(RichText(Seq(RichText.Plain("Different message")))))
      case m =>
        m
    }
  }

  class MergerHelper(msgs1: Seq[Message], msgs2: Seq[Message]) {
    val (dao1, d1ds, d1users, d1chat, d1msgs) = createDaoAndEntities("One", msgs1, maxUserId)
    val (dao2, d2ds, d2users, d2chat, d2msgs) = createDaoAndEntities("Two", msgs2, maxUserId)

    def merger: ChatHistoryMerger =
      new ChatHistoryMerger(dao1, d1ds, dao2, d2ds)

    private def createDaoAndEntities(nameSuffix: String, srcMsgs: Seq[Message], numUsers: Int) = {
      val dao                     = createSimpleDao(nameSuffix, srcMsgs, numUsers)
      val (ds, users, chat, msgs) = getSimpleDaoEntities(dao)
      (dao, ds, users, chat, msgs)
    }
  }
}
