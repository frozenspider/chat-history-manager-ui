package org.fs.chm.dao.merge

import org.fs.chm.TestHelper
import org.fs.chm.dao._
import org.fs.chm.dao.merge.DatasetMerger._
import org.fs.chm.dao.merge.DatasetMerger.{ ChatMergeOption => CMO }
import org.fs.chm.utility.TestUtils._
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfter
import org.scalatest.FunSuite
import org.slf4s.Logging

@RunWith(classOf[org.scalatest.junit.JUnitRunner])
class DatasetMergerAnalyzeSpec //
    extends FunSuite
    with TestHelper
    with Logging
    with BeforeAndAfter {
  import DatasetMergerHelper._

  test("messages stream") {
    def messagesForChat1(helper: MergerHelper, fromOption: Option[Message]) =
      helper.merger.messagesStream(helper.dao1, helper.d1chat, fromOption.asInstanceOf[Option[TaggedMessage.M]])

    val msgs = for (i <- 1 to maxId) yield createRegularMessage(i, rndUserId)

    { // No messages
      val helper = new MergerHelper(Seq.empty, Seq.empty)
      assert(messagesForChat1(helper, None) === Seq.empty)
    }

    { // One message
      val helper = new MergerHelper(msgs.take(1), Seq.empty)
      assert(messagesForChat1(helper, None) === helper.d1msgs)
      assert(messagesForChat1(helper, helper.d1msgs.headOption) === Seq.empty)
    }

    { // All messages
      val helper = new MergerHelper(msgs, Seq.empty)
      assert(messagesForChat1(helper, None) === helper.d1msgs)
      assert(messagesForChat1(helper, helper.d1msgs.headOption) === helper.d1msgs.tail)
      val since4 = helper.d1msgs.drop(3)
      assert(since4.head.sourceIdOption === Some(4))
      assert(messagesForChat1(helper, Some(helper.d1msgs.bySrcId(3))) === since4)
    }

    { // All messages but last
      val helper = new MergerHelper(msgs.dropRight(1), Seq.empty)
      assert(messagesForChat1(helper, None) === helper.d1msgs)
      assert(messagesForChat1(helper, helper.d1msgs.headOption) === helper.d1msgs.tail)
    }
  }

  test("keep - no messages") {
    val helper   = new MergerHelper(Seq.empty, Seq.empty)
    val keep     = CMO.Keep(helper.d1chat)
    val analyzed = helper.merger.analyzeChatHistoryMerge(keep)
    assert(analyzed === keep)
  }

  test("add - no messages") {
    val helper   = new MergerHelper(Seq.empty, Seq.empty)
    val add      = CMO.Add(helper.d2chat)
    val analysis = helper.merger.analyzeChatHistoryMerge(add)
    assert(analysis === add)
  }

  test("combine - same single message") {
    val msgs     = Seq(createRegularMessage(1, 1))
    val helper   = new MergerHelper(msgs, msgs)
    val combine  = CMO.Combine(helper.d1chat, helper.d2chat, IndexedSeq.empty)
    val analysis = helper.merger.analyzeChatHistoryMerge(combine).messageMergeOptions
    assert(
      analysis === Seq(
        MessagesMergeOption.Keep(
          firstMasterMsg      = helper.d1msgs.bySrcId(1),
          lastMasterMsg       = helper.d1msgs.bySrcId(1),
          firstSlaveMsgOption = Some(helper.d2msgs.bySrcId(1)),
          lastSlaveMsgOption  = Some(helper.d2msgs.bySrcId(1))
        )
      )
    )
  }

  test("combine - same multiple messages") {
    val msgs     = for (i <- 1 to maxId) yield createRegularMessage(i, rndUserId)
    val helper   = new MergerHelper(msgs, msgs)
    val combine  = CMO.Combine(helper.d1chat, helper.d2chat, IndexedSeq.empty)
    val analysis = helper.merger.analyzeChatHistoryMerge(combine).messageMergeOptions
    assert(
      analysis === Seq(
        MessagesMergeOption.Keep(
          firstMasterMsg      = helper.d1msgs.bySrcId(1),
          lastMasterMsg       = helper.d1msgs.bySrcId(maxId),
          firstSlaveMsgOption = Some(helper.d2msgs.bySrcId(1)),
          lastSlaveMsgOption  = Some(helper.d2msgs.bySrcId(maxId))
        )
      )
    )
  }

  test("combine - no slave messages") {
    val msgs     = for (i <- 1 to maxId) yield createRegularMessage(i, rndUserId)
    val helper   = new MergerHelper(msgs, IndexedSeq.empty)
    val combine  = CMO.Combine(helper.d1chat, helper.d2chat, IndexedSeq.empty)
    val analysis = helper.merger.analyzeChatHistoryMerge(combine).messageMergeOptions
    assert(
      analysis === Seq(
        MessagesMergeOption.Keep(
          firstMasterMsg      = helper.d1msgs.bySrcId(1),
          lastMasterMsg       = helper.d1msgs.bySrcId(maxId),
          firstSlaveMsgOption = None,
          lastSlaveMsgOption  = None
        )
      )
    )
  }

  test("combine - no new slave messages, matching sequence in the middle") {
    val msgs     = for (i <- 1 to maxId) yield createRegularMessage(i, rndUserId)
    val msgs2    = msgs.filter(m => (5 to 10) contains m.sourceIdOption.get)
    val helper   = new MergerHelper(msgs, msgs2)
    val combine  = CMO.Combine(helper.d1chat, helper.d2chat, IndexedSeq.empty)
    val analysis = helper.merger.analyzeChatHistoryMerge(combine).messageMergeOptions
    assert(
      analysis === Seq(
        MessagesMergeOption.Keep(
          firstMasterMsg      = helper.d1msgs.bySrcId(1),
          lastMasterMsg       = helper.d1msgs.bySrcId(4),
          firstSlaveMsgOption = None,
          lastSlaveMsgOption  = None
        ),
        MessagesMergeOption.Keep(
          firstMasterMsg      = helper.d1msgs.bySrcId(5),
          lastMasterMsg       = helper.d1msgs.bySrcId(10),
          firstSlaveMsgOption = Some(helper.d2msgs.bySrcId(5)),
          lastSlaveMsgOption  = Some(helper.d2msgs.bySrcId(10))
        ),
        MessagesMergeOption.Keep(
          firstMasterMsg      = helper.d1msgs.bySrcId(11),
          lastMasterMsg       = helper.d1msgs.bySrcId(maxId),
          firstSlaveMsgOption = None,
          lastSlaveMsgOption  = None
        )
      )
    )
  }

  test("combine - added one message in the middle") {
    val msgs     = for (i <- 1 to 3) yield createRegularMessage(i, rndUserId)
    val msgs123  = msgs
    val msgs13   = msgs123.filter(_.sourceIdOption.get != 2)
    val helper   = new MergerHelper(msgs13, msgs123)
    val combine  = CMO.Combine(helper.d1chat, helper.d2chat, IndexedSeq.empty)
    val analysis = helper.merger.analyzeChatHistoryMerge(combine).messageMergeOptions
    assert(
      analysis === Seq(
        MessagesMergeOption.Keep(
          firstMasterMsg      = helper.d1msgs.bySrcId(1),
          lastMasterMsg       = helper.d1msgs.bySrcId(1),
          firstSlaveMsgOption = Some(helper.d2msgs.bySrcId(1)),
          lastSlaveMsgOption  = Some(helper.d2msgs.bySrcId(1))
        ),
        MessagesMergeOption.Add(
          firstSlaveMsg = helper.d2msgs.bySrcId(2),
          lastSlaveMsg  = helper.d2msgs.bySrcId(2)
        ),
        MessagesMergeOption.Keep(
          firstMasterMsg      = helper.d1msgs.bySrcId(3),
          lastMasterMsg       = helper.d1msgs.bySrcId(3),
          firstSlaveMsgOption = Some(helper.d2msgs.bySrcId(3)),
          lastSlaveMsgOption  = Some(helper.d2msgs.bySrcId(3))
        )
      )
    )
  }


  test("combine - changed one message in the middle") {
    val msgs     = for (i <- 1 to 3) yield createRegularMessage(i, rndUserId)
    val msgsA    = msgs
    val msgsB    = changedMessages(msgsA, (_ == 2))
    val helper   = new MergerHelper(msgsA, msgsB)
    val combine  = CMO.Combine(helper.d1chat, helper.d2chat, IndexedSeq.empty)
    val analysis = helper.merger.analyzeChatHistoryMerge(combine).messageMergeOptions
    assert(
      analysis === Seq(
        MessagesMergeOption.Keep(
          firstMasterMsg      = helper.d1msgs.bySrcId(1),
          lastMasterMsg       = helper.d1msgs.bySrcId(1),
          firstSlaveMsgOption = Some(helper.d2msgs.bySrcId(1)),
          lastSlaveMsgOption  = Some(helper.d2msgs.bySrcId(1))
        ),
        MessagesMergeOption.Replace(
          firstMasterMsg = helper.d1msgs.bySrcId(2),
          lastMasterMsg  = helper.d1msgs.bySrcId(2),
          firstSlaveMsg  = helper.d2msgs.bySrcId(2),
          lastSlaveMsg   = helper.d2msgs.bySrcId(2)
        ),
        MessagesMergeOption.Keep(
          firstMasterMsg      = helper.d1msgs.bySrcId(3),
          lastMasterMsg       = helper.d1msgs.bySrcId(3),
          firstSlaveMsgOption = Some(helper.d2msgs.bySrcId(3)),
          lastSlaveMsgOption  = Some(helper.d2msgs.bySrcId(3))
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
  test("combine - added multiple message in the beginning") {
    val msgs     = for (i <- 1 to maxId) yield createRegularMessage(i, rndUserId)
    val msgsL    = Seq(msgs.last)
    val helper   = new MergerHelper(msgsL, msgs)
    val combine  = CMO.Combine(helper.d1chat, helper.d2chat, IndexedSeq.empty)
    val analysis = helper.merger.analyzeChatHistoryMerge(combine).messageMergeOptions
    assert(
      analysis === Seq(
        MessagesMergeOption.Add(
          firstSlaveMsg  = helper.d2msgs.bySrcId(1),
          lastSlaveMsg   = helper.d2msgs.bySrcId(maxId - 1)
        ),
        MessagesMergeOption.Keep(
          firstMasterMsg      = helper.d1msgs.bySrcId(maxId),
          lastMasterMsg       = helper.d1msgs.bySrcId(maxId),
          firstSlaveMsgOption = Some(helper.d2msgs.bySrcId(maxId)),
          lastSlaveMsgOption  = Some(helper.d2msgs.bySrcId(maxId))
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
  test("combine - changed multiple message in the beginning") {
    val msgs     = for (i <- 1 to maxId) yield createRegularMessage(i, rndUserId)
    val msgsA    = msgs
    val msgsB    = changedMessages(msgsA, (_ < maxId))
    val helper   = new MergerHelper(msgsA, msgsB)
    val combine  = CMO.Combine(helper.d1chat, helper.d2chat, IndexedSeq.empty)
    val analysis = helper.merger.analyzeChatHistoryMerge(combine).messageMergeOptions
    assert(
      analysis === Seq(
        MessagesMergeOption.Replace(
          firstMasterMsg = helper.d1msgs.bySrcId(1),
          lastMasterMsg  = helper.d1msgs.bySrcId(maxId - 1),
          firstSlaveMsg  = helper.d2msgs.bySrcId(1),
          lastSlaveMsg   = helper.d2msgs.bySrcId(maxId - 1)
        ),
        MessagesMergeOption.Keep(
          firstMasterMsg      = helper.d1msgs.bySrcId(maxId),
          lastMasterMsg       = helper.d1msgs.bySrcId(maxId),
          firstSlaveMsgOption = Some(helper.d2msgs.bySrcId(maxId)),
          lastSlaveMsgOption  = Some(helper.d2msgs.bySrcId(maxId))
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
  test("combine - added multiple message in the middle") {
    val msgs     = for (i <- 1 to maxId) yield createRegularMessage(i, rndUserId)
    val msgsFL   = Seq(msgs.head, msgs.last)
    val helper   = new MergerHelper(msgsFL, msgs)
    val combine  = CMO.Combine(helper.d1chat, helper.d2chat, IndexedSeq.empty)
    val analysis = helper.merger.analyzeChatHistoryMerge(combine).messageMergeOptions
    assert(
      analysis === Seq(
        MessagesMergeOption.Keep(
          firstMasterMsg      = helper.d1msgs.bySrcId(1),
          lastMasterMsg       = helper.d1msgs.bySrcId(1),
          firstSlaveMsgOption = Some(helper.d2msgs.bySrcId(1)),
          lastSlaveMsgOption  = Some(helper.d2msgs.bySrcId(1))
        ),
        MessagesMergeOption.Add(
          firstSlaveMsg  = helper.d2msgs.bySrcId(2),
          lastSlaveMsg   = helper.d2msgs.bySrcId(maxId - 1)
        ),
        MessagesMergeOption.Keep(
          firstMasterMsg      = helper.d1msgs.bySrcId(maxId),
          lastMasterMsg       = helper.d1msgs.bySrcId(maxId),
          firstSlaveMsgOption = Some(helper.d2msgs.bySrcId(maxId)),
          lastSlaveMsgOption  = Some(helper.d2msgs.bySrcId(maxId))
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
  test("combine - changed multiple message in the middle") {
    val msgs     = for (i <- 1 to maxId) yield createRegularMessage(i, rndUserId)
    val msgsA    = msgs
    val msgsB    = changedMessages(msgsA, (id => id > 1 && id < maxId))
    val helper   = new MergerHelper(msgsA, msgsB)
    val combine  = CMO.Combine(helper.d1chat, helper.d2chat, IndexedSeq.empty)
    val analysis = helper.merger.analyzeChatHistoryMerge(combine).messageMergeOptions
    assert(
      analysis === Seq(
        MessagesMergeOption.Keep(
          firstMasterMsg      = helper.d1msgs.bySrcId(1),
          lastMasterMsg       = helper.d1msgs.bySrcId(1),
          firstSlaveMsgOption = Some(helper.d2msgs.bySrcId(1)),
          lastSlaveMsgOption  = Some(helper.d2msgs.bySrcId(1))
        ),
        MessagesMergeOption.Replace(
          firstMasterMsg = helper.d1msgs.bySrcId(2),
          lastMasterMsg  = helper.d1msgs.bySrcId(maxId - 1),
          firstSlaveMsg  = helper.d2msgs.bySrcId(2),
          lastSlaveMsg   = helper.d2msgs.bySrcId(maxId - 1)
        ),
        MessagesMergeOption.Keep(
          firstMasterMsg      = helper.d1msgs.bySrcId(maxId),
          lastMasterMsg       = helper.d1msgs.bySrcId(maxId),
          firstSlaveMsgOption = Some(helper.d2msgs.bySrcId(maxId)),
          lastSlaveMsgOption  = Some(helper.d2msgs.bySrcId(maxId))
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
  test("combine - added multiple message in the end") {
    val msgs     = for (i <- 1 to maxId) yield createRegularMessage(i, rndUserId)
    val msgsF    = Seq(msgs.head)
    val helper   = new MergerHelper(msgsF, msgs)
    val combine  = CMO.Combine(helper.d1chat, helper.d2chat, IndexedSeq.empty)
    val analysis = helper.merger.analyzeChatHistoryMerge(combine).messageMergeOptions
    assert(
      analysis === Seq(
        MessagesMergeOption.Keep(
          firstMasterMsg      = helper.d1msgs.bySrcId(1),
          lastMasterMsg       = helper.d1msgs.bySrcId(1),
          firstSlaveMsgOption = Some(helper.d2msgs.bySrcId(1)),
          lastSlaveMsgOption  = Some(helper.d2msgs.bySrcId(1))
        ),
        MessagesMergeOption.Add(
          firstSlaveMsg  = helper.d2msgs.bySrcId(2),
          lastSlaveMsg   = helper.d2msgs.bySrcId(maxId)
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
  test("combine - changed multiple message in the end") {
    val msgs     = for (i <- 1 to maxId) yield createRegularMessage(i, rndUserId)
    val msgsA    = msgs
    val msgsB    = changedMessages(msgsA, (_ > 1))
    val helper   = new MergerHelper(msgsA, msgsB)
    val combine  = CMO.Combine(helper.d1chat, helper.d2chat, IndexedSeq.empty)
    val analysis = helper.merger.analyzeChatHistoryMerge(combine).messageMergeOptions
    assert(
      analysis === Seq(
        MessagesMergeOption.Keep(
          firstMasterMsg      = helper.d1msgs.bySrcId(1),
          lastMasterMsg       = helper.d1msgs.bySrcId(1),
          firstSlaveMsgOption = Some(helper.d2msgs.bySrcId(1)),
          lastSlaveMsgOption  = Some(helper.d2msgs.bySrcId(1))
        ),
        MessagesMergeOption.Replace(
          firstMasterMsg = helper.d1msgs.bySrcId(2),
          lastMasterMsg  = helper.d1msgs.bySrcId(maxId),
          firstSlaveMsg  = helper.d2msgs.bySrcId(2),
          lastSlaveMsg   = helper.d2msgs.bySrcId(maxId)
        )
      )
    )
  }

  /**
   * {{{
   * Master messages - 1  2  ...  N
   * Slave messages  - 1* 2* ...* N*
   * }}}
   */
  test("combine - changed all messages") {
    val msgs     = for (i <- 1 to maxId) yield createRegularMessage(i, rndUserId)
    val msgsA    = msgs
    val msgsB    = changedMessages(msgsA, (_ => true))
    val helper   = new MergerHelper(msgsA, msgsB)
    val combine  = CMO.Combine(helper.d1chat, helper.d2chat, IndexedSeq.empty)
    val analysis = helper.merger.analyzeChatHistoryMerge(combine).messageMergeOptions
    assert(
      analysis === Seq(
        MessagesMergeOption.Replace(
          firstMasterMsg = helper.d1msgs.bySrcId(1),
          lastMasterMsg  = helper.d1msgs.bySrcId(maxId),
          firstSlaveMsg  = helper.d2msgs.bySrcId(1),
          lastSlaveMsg   = helper.d2msgs.bySrcId(maxId)
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
  test("combine - master has messages not present in slave") {
    val msgs     = for (i <- 1 to 5) yield createRegularMessage(i, rndUserId)
    val msgsA    = msgs
    val msgsB    = msgs.filter(Seq(2, 4) contains _.sourceIdOption.get)
    val helper   = new MergerHelper(msgsA, msgsB)
    val combine  = CMO.Combine(helper.d1chat, helper.d2chat, IndexedSeq.empty)
    val analysis = helper.merger.analyzeChatHistoryMerge(combine).messageMergeOptions
    assert(
      analysis === Seq(
        MessagesMergeOption.Keep(
          firstMasterMsg      = helper.d1msgs.bySrcId(1),
          lastMasterMsg       = helper.d1msgs.bySrcId(1),
          firstSlaveMsgOption = None,
          lastSlaveMsgOption  = None
        ),
        MessagesMergeOption.Keep(
          firstMasterMsg      = helper.d1msgs.bySrcId(2),
          lastMasterMsg       = helper.d1msgs.bySrcId(2),
          firstSlaveMsgOption = Some(helper.d2msgs.bySrcId(2)),
          lastSlaveMsgOption  = Some(helper.d2msgs.bySrcId(2))
        ),
        MessagesMergeOption.Keep(
          firstMasterMsg      = helper.d1msgs.bySrcId(3),
          lastMasterMsg       = helper.d1msgs.bySrcId(3),
          firstSlaveMsgOption = None,
          lastSlaveMsgOption  = None
        ),
        MessagesMergeOption.Keep(
          firstMasterMsg      = helper.d1msgs.bySrcId(4),
          lastMasterMsg       = helper.d1msgs.bySrcId(4),
          firstSlaveMsgOption = Some(helper.d2msgs.bySrcId(4)),
          lastSlaveMsgOption  = Some(helper.d2msgs.bySrcId(4))
        ),
        MessagesMergeOption.Keep(
          firstMasterMsg      = helper.d1msgs.bySrcId(5),
          lastMasterMsg       = helper.d1msgs.bySrcId(5),
          firstSlaveMsgOption = None,
          lastSlaveMsgOption  = None
        )
      )
    )
  }

  /**
   * {{{
   * Master messages - 1 2     5  6  7 8 9  10
   * Slave messages  -     3 4 5* 6* 7 8 9* 10* 11 12
   * }}}
   */
  test("combine - everything") {
    val msgs  = for (i <- 1 to 12) yield createRegularMessage(i, rndUserId)
    val msgsA = msgs.filter(Seq(1, 2, 5, 6, 7, 8, 9, 10) contains _.sourceIdOption.get)
    val msgsB = changedMessages(
      msgs.filter((3 to 12) contains _.sourceIdOption.get),
      (id => Seq(5, 6, 9, 10) contains id)
    )
    val helper   = new MergerHelper(msgsA, msgsB)
    val combine  = CMO.Combine(helper.d1chat, helper.d2chat, IndexedSeq.empty)
    val analysis = helper.merger.analyzeChatHistoryMerge(combine).messageMergeOptions
    assert(
      analysis === Seq(
        MessagesMergeOption.Keep(
          firstMasterMsg      = helper.d1msgs.bySrcId(1),
          lastMasterMsg       = helper.d1msgs.bySrcId(2),
          firstSlaveMsgOption = None,
          lastSlaveMsgOption  = None
        ),
        MessagesMergeOption.Add(
          firstSlaveMsg  = helper.d2msgs.bySrcId(3),
          lastSlaveMsg   = helper.d2msgs.bySrcId(4)
        ),
        MessagesMergeOption.Replace(
          firstMasterMsg = helper.d1msgs.bySrcId(5),
          lastMasterMsg  = helper.d1msgs.bySrcId(6),
          firstSlaveMsg  = helper.d2msgs.bySrcId(5),
          lastSlaveMsg   = helper.d2msgs.bySrcId(6)
        ),
        MessagesMergeOption.Keep(
          firstMasterMsg      = helper.d1msgs.bySrcId(7),
          lastMasterMsg       = helper.d1msgs.bySrcId(8),
          firstSlaveMsgOption = Some(helper.d2msgs.bySrcId(7)),
          lastSlaveMsgOption  = Some(helper.d2msgs.bySrcId(8))
        ),
        MessagesMergeOption.Replace(
          firstMasterMsg = helper.d1msgs.bySrcId(9),
          lastMasterMsg  = helper.d1msgs.bySrcId(10),
          firstSlaveMsg  = helper.d2msgs.bySrcId(9),
          lastSlaveMsg   = helper.d2msgs.bySrcId(10)
        ),
        MessagesMergeOption.Add(
          firstSlaveMsg  = helper.d2msgs.bySrcId(11),
          lastSlaveMsg   = helper.d2msgs.bySrcId(12)
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
  test("combine - everything, roles inverted") {
    val msgs  = for (i <- 1 to 12) yield createRegularMessage(i, rndUserId)
    val msgsA = msgs.filter((3 to 12) contains _.sourceIdOption.get)
    val msgsB = changedMessages(
      msgs.filter(Seq(1, 2, 5, 6, 7, 8, 9, 10) contains _.sourceIdOption.get),
      (id => Seq(5, 6, 9, 10) contains id)
    )
    val helper   = new MergerHelper(msgsA, msgsB)
    val combine  = CMO.Combine(helper.d1chat, helper.d2chat, IndexedSeq.empty)
    val analysis = helper.merger.analyzeChatHistoryMerge(combine).messageMergeOptions
    assert(
      analysis === Seq(
        MessagesMergeOption.Add(
          firstSlaveMsg  = helper.d2msgs.bySrcId(1),
          lastSlaveMsg   = helper.d2msgs.bySrcId(2)
        ),
        MessagesMergeOption.Keep(
          firstMasterMsg      = helper.d1msgs.bySrcId(3),
          lastMasterMsg       = helper.d1msgs.bySrcId(4),
          firstSlaveMsgOption = None,
          lastSlaveMsgOption  = None
        ),
        MessagesMergeOption.Replace(
          firstMasterMsg = helper.d1msgs.bySrcId(5),
          lastMasterMsg  = helper.d1msgs.bySrcId(6),
          firstSlaveMsg  = helper.d2msgs.bySrcId(5),
          lastSlaveMsg   = helper.d2msgs.bySrcId(6)
        ),
        MessagesMergeOption.Keep(
          firstMasterMsg      = helper.d1msgs.bySrcId(7),
          lastMasterMsg       = helper.d1msgs.bySrcId(8),
          firstSlaveMsgOption = Some(helper.d2msgs.bySrcId(7)),
          lastSlaveMsgOption  = Some(helper.d2msgs.bySrcId(8))
        ),
        MessagesMergeOption.Replace(
          firstMasterMsg = helper.d1msgs.bySrcId(9),
          lastMasterMsg  = helper.d1msgs.bySrcId(10),
          firstSlaveMsg  = helper.d2msgs.bySrcId(9),
          lastSlaveMsg   = helper.d2msgs.bySrcId(10)
        ),
        MessagesMergeOption.Keep(
          firstMasterMsg      = helper.d1msgs.bySrcId(11),
          lastMasterMsg       = helper.d1msgs.bySrcId(12),
          firstSlaveMsgOption = None,
          lastSlaveMsgOption  = None
        )
      ))
  }

  //
  // Helpers
  //

  class MergerHelper(msgs1: Seq[Message], msgs2: Seq[Message]) {
    val (dao1, d1ds, d1users, d1chat, d1msgs) = createDaoAndEntities("One", msgs1, maxUserId)
    val (dao2, d2ds, d2users, d2chat, d2msgs) = createDaoAndEntities("Two", msgs2, maxUserId)

    def merger: DatasetMerger =
      new DatasetMerger(dao1, d1ds, dao2, d2ds)

    private def createDaoAndEntities(nameSuffix: String, srcMsgs: Seq[Message], numUsers: Int) = {
      val dao                     = createSimpleDao(nameSuffix, srcMsgs, numUsers)
      val (ds, users, chat, msgs) = getSimpleDaoEntities(dao)
      (dao, ds, users, chat, msgs)
    }
  }
}
