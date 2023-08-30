package org.fs.chm.dao.merge

import java.io.File
import java.nio.file.Files

import org.fs.chm.TestHelper
import org.fs.chm.dao.Entities._
import org.fs.chm.dao.merge.DatasetMerger._
import org.fs.chm.dao.merge.DatasetMerger.{ChatMergeOption => CMO}
import org.fs.chm.protobuf._
import org.fs.chm.utility.LangUtils._
import org.fs.chm.utility.TestUtils._
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.slf4s.Logging

@RunWith(classOf[org.scalatestplus.junit.JUnitRunner])
class DatasetMergerAnalyzeSpec //
    extends AnyFunSuite
    with TestHelper
    with Logging
    with BeforeAndAfter {
  import DatasetMergerHelper._

  test("messages stream") {
    def messagesForChat1(helper: MergerHelper, fromOption: Option[Message]) =
      helper.merger.messagesStream(helper.dao1, helper.d1cwd.chat, fromOption.asInstanceOf[Option[TaggedMessage.M]])

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
    val keep     = CMO.Keep(helper.d1cwd)
    val analyzed = helper.merger.analyzeChatHistoryMerge(keep)
    assert(analyzed === keep)
  }

  test("add - no messages") {
    val helper   = new MergerHelper(Seq.empty, Seq.empty)
    val add      = CMO.Add(helper.d2cwd)
    val analysis = helper.merger.analyzeChatHistoryMerge(add)
    assert(analysis === add)
  }

  test("match - same single message") {
    val msgs     = Seq(createRegularMessage(1, 1))
    val helper   = new MergerHelper(msgs, msgs)
    val combine  = CMO.Combine(helper.d1cwd, helper.d2cwd, IndexedSeq.empty)
    val analysis = helper.merger.analyzeChatHistoryMerge(combine).messageMergeOptions
    assert(
      analysis === Seq(
        MessagesMergeDiff.Match(
          firstMasterMsg = helper.d1msgs.bySrcId(1),
          lastMasterMsg  = helper.d1msgs.bySrcId(1),
          firstSlaveMsg  = helper.d2msgs.bySrcId(1),
          lastSlaveMsg   = helper.d2msgs.bySrcId(1)
        )
      )
    )
  }

  test("match - same multiple messages") {
    val msgs     = for (i <- 1 to maxId) yield createRegularMessage(i, rndUserId)
    val helper   = new MergerHelper(msgs, msgs)
    val combine  = CMO.Combine(helper.d1cwd, helper.d2cwd, IndexedSeq.empty)
    val analysis = helper.merger.analyzeChatHistoryMerge(combine).messageMergeOptions
    assert(
      analysis === Seq(
        MessagesMergeDiff.Match(
          firstMasterMsg = helper.d1msgs.bySrcId(1),
          lastMasterMsg  = helper.d1msgs.bySrcId(maxId),
          firstSlaveMsg  = helper.d2msgs.bySrcId(1),
          lastSlaveMsg   = helper.d2msgs.bySrcId(maxId)
        )
      )
    )
  }

  test("retain - no slave messages") {
    val msgs     = for (i <- 1 to maxId) yield createRegularMessage(i, rndUserId)
    val helper   = new MergerHelper(msgs, IndexedSeq.empty)
    val combine  = CMO.Combine(helper.d1cwd, helper.d2cwd, IndexedSeq.empty)
    val analysis = helper.merger.analyzeChatHistoryMerge(combine).messageMergeOptions
    assert(
      analysis === Seq(
        MessagesMergeDiff.Retain(
          firstMasterMsg = helper.d1msgs.bySrcId(1),
          lastMasterMsg  = helper.d1msgs.bySrcId(maxId),
        )
      )
    )
  }

  test("retain - no new slave messages, matching sequence in the middle") {
    val msgs     = for (i <- 1 to maxId) yield createRegularMessage(i, rndUserId)
    val msgs2    = msgs.filter(m => (5 to 10) contains m.sourceIdOption.get)
    val helper   = new MergerHelper(msgs, msgs2)
    val combine  = CMO.Combine(helper.d1cwd, helper.d2cwd, IndexedSeq.empty)
    val analysis = helper.merger.analyzeChatHistoryMerge(combine).messageMergeOptions
    assert(
      analysis === Seq(
        MessagesMergeDiff.Retain(
          firstMasterMsg = helper.d1msgs.bySrcId(1),
          lastMasterMsg  = helper.d1msgs.bySrcId(4),
        ),
        MessagesMergeDiff.Match(
          firstMasterMsg = helper.d1msgs.bySrcId(5),
          lastMasterMsg  = helper.d1msgs.bySrcId(10),
          firstSlaveMsg  = helper.d2msgs.bySrcId(5),
          lastSlaveMsg   = helper.d2msgs.bySrcId(10)
        ),
        MessagesMergeDiff.Retain(
          firstMasterMsg = helper.d1msgs.bySrcId(11),
          lastMasterMsg  = helper.d1msgs.bySrcId(maxId),
        )
      )
    )
  }

  test("combine - added one new message in the middle") {
    val msgs     = for (i <- 1 to 3) yield createRegularMessage(i, rndUserId)
    val msgs123  = msgs
    val msgs13   = msgs123.filter(_.sourceIdOption.get != 2)
    val helper   = new MergerHelper(msgs13, msgs123)
    val combine  = CMO.Combine(helper.d1cwd, helper.d2cwd, IndexedSeq.empty)
    val analysis = helper.merger.analyzeChatHistoryMerge(combine).messageMergeOptions
    assert(
      analysis === Seq(
        MessagesMergeDiff.Match(
          firstMasterMsg = helper.d1msgs.bySrcId(1),
          lastMasterMsg  = helper.d1msgs.bySrcId(1),
          firstSlaveMsg  = helper.d2msgs.bySrcId(1),
          lastSlaveMsg   = helper.d2msgs.bySrcId(1)
        ),
        MessagesMergeDiff.Add(
          firstSlaveMsg = helper.d2msgs.bySrcId(2),
          lastSlaveMsg  = helper.d2msgs.bySrcId(2)
        ),
        MessagesMergeDiff.Match(
          firstMasterMsg = helper.d1msgs.bySrcId(3),
          lastMasterMsg  = helper.d1msgs.bySrcId(3),
          firstSlaveMsg  = helper.d2msgs.bySrcId(3),
          lastSlaveMsg   = helper.d2msgs.bySrcId(3)
        )
      )
    )
  }

  test("combine - changed one message in the middle") {
    val msgs     = for (i <- 1 to 3) yield createRegularMessage(i, rndUserId)
    val msgsA    = msgs
    val msgsB    = changedMessages(msgsA, (_ == 2))
    val helper   = new MergerHelper(msgsA, msgsB)
    val combine  = CMO.Combine(helper.d1cwd, helper.d2cwd, IndexedSeq.empty)
    val analysis = helper.merger.analyzeChatHistoryMerge(combine).messageMergeOptions
    assert(
      analysis === Seq(
        MessagesMergeDiff.Match(
          firstMasterMsg = helper.d1msgs.bySrcId(1),
          lastMasterMsg  = helper.d1msgs.bySrcId(1),
          firstSlaveMsg  = helper.d2msgs.bySrcId(1),
          lastSlaveMsg   = helper.d2msgs.bySrcId(1)
        ),
        MessagesMergeDiff.Replace(
          firstMasterMsg = helper.d1msgs.bySrcId(2),
          lastMasterMsg  = helper.d1msgs.bySrcId(2),
          firstSlaveMsg  = helper.d2msgs.bySrcId(2),
          lastSlaveMsg   = helper.d2msgs.bySrcId(2)
        ),
        MessagesMergeDiff.Match(
          firstMasterMsg = helper.d1msgs.bySrcId(3),
          lastMasterMsg  = helper.d1msgs.bySrcId(3),
          firstSlaveMsg  = helper.d2msgs.bySrcId(3),
          lastSlaveMsg   = helper.d2msgs.bySrcId(3)
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
    val combine  = CMO.Combine(helper.d1cwd, helper.d2cwd, IndexedSeq.empty)
    val analysis = helper.merger.analyzeChatHistoryMerge(combine).messageMergeOptions
    assert(
      analysis === Seq(
        MessagesMergeDiff.Add(
          firstSlaveMsg  = helper.d2msgs.bySrcId(1),
          lastSlaveMsg   = helper.d2msgs.bySrcId(maxId - 1)
        ),
        MessagesMergeDiff.Match(
          firstMasterMsg = helper.d1msgs.bySrcId(maxId),
          lastMasterMsg  = helper.d1msgs.bySrcId(maxId),
          firstSlaveMsg  = helper.d2msgs.bySrcId(maxId),
          lastSlaveMsg   = helper.d2msgs.bySrcId(maxId)
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
    val combine  = CMO.Combine(helper.d1cwd, helper.d2cwd, IndexedSeq.empty)
    val analysis = helper.merger.analyzeChatHistoryMerge(combine).messageMergeOptions
    assert(
      analysis === Seq(
        MessagesMergeDiff.Replace(
          firstMasterMsg = helper.d1msgs.bySrcId(1),
          lastMasterMsg  = helper.d1msgs.bySrcId(maxId - 1),
          firstSlaveMsg  = helper.d2msgs.bySrcId(1),
          lastSlaveMsg   = helper.d2msgs.bySrcId(maxId - 1)
        ),
        MessagesMergeDiff.Match(
          firstMasterMsg = helper.d1msgs.bySrcId(maxId),
          lastMasterMsg  = helper.d1msgs.bySrcId(maxId),
          firstSlaveMsg  = helper.d2msgs.bySrcId(maxId),
          lastSlaveMsg   = helper.d2msgs.bySrcId(maxId)
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
    val combine  = CMO.Combine(helper.d1cwd, helper.d2cwd, IndexedSeq.empty)
    val analysis = helper.merger.analyzeChatHistoryMerge(combine).messageMergeOptions
    assert(
      analysis === Seq(
        MessagesMergeDiff.Match(
          firstMasterMsg = helper.d1msgs.bySrcId(1),
          lastMasterMsg  = helper.d1msgs.bySrcId(1),
          firstSlaveMsg  = helper.d2msgs.bySrcId(1),
          lastSlaveMsg   = helper.d2msgs.bySrcId(1)
        ),
        MessagesMergeDiff.Add(
          firstSlaveMsg  = helper.d2msgs.bySrcId(2),
          lastSlaveMsg   = helper.d2msgs.bySrcId(maxId - 1)
        ),
        MessagesMergeDiff.Match(
          firstMasterMsg = helper.d1msgs.bySrcId(maxId),
          lastMasterMsg  = helper.d1msgs.bySrcId(maxId),
          firstSlaveMsg  = helper.d2msgs.bySrcId(maxId),
          lastSlaveMsg   = helper.d2msgs.bySrcId(maxId)
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
    val combine  = CMO.Combine(helper.d1cwd, helper.d2cwd, IndexedSeq.empty)
    val analysis = helper.merger.analyzeChatHistoryMerge(combine).messageMergeOptions
    assert(
      analysis === Seq(
        MessagesMergeDiff.Match(
          firstMasterMsg = helper.d1msgs.bySrcId(1),
          lastMasterMsg  = helper.d1msgs.bySrcId(1),
          firstSlaveMsg  = helper.d2msgs.bySrcId(1),
          lastSlaveMsg   = helper.d2msgs.bySrcId(1)
        ),
        MessagesMergeDiff.Replace(
          firstMasterMsg = helper.d1msgs.bySrcId(2),
          lastMasterMsg  = helper.d1msgs.bySrcId(maxId - 1),
          firstSlaveMsg  = helper.d2msgs.bySrcId(2),
          lastSlaveMsg   = helper.d2msgs.bySrcId(maxId - 1)
        ),
        MessagesMergeDiff.Match(
          firstMasterMsg = helper.d1msgs.bySrcId(maxId),
          lastMasterMsg  = helper.d1msgs.bySrcId(maxId),
          firstSlaveMsg  = helper.d2msgs.bySrcId(maxId),
          lastSlaveMsg   = helper.d2msgs.bySrcId(maxId)
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
    val combine  = CMO.Combine(helper.d1cwd, helper.d2cwd, IndexedSeq.empty)
    val analysis = helper.merger.analyzeChatHistoryMerge(combine).messageMergeOptions
    assert(
      analysis === Seq(
        MessagesMergeDiff.Match(
          firstMasterMsg = helper.d1msgs.bySrcId(1),
          lastMasterMsg  = helper.d1msgs.bySrcId(1),
          firstSlaveMsg  = helper.d2msgs.bySrcId(1),
          lastSlaveMsg   = helper.d2msgs.bySrcId(1)
        ),
        MessagesMergeDiff.Add(
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
    val combine  = CMO.Combine(helper.d1cwd, helper.d2cwd, IndexedSeq.empty)
    val analysis = helper.merger.analyzeChatHistoryMerge(combine).messageMergeOptions
    assert(
      analysis === Seq(
        MessagesMergeDiff.Match(
          firstMasterMsg = helper.d1msgs.bySrcId(1),
          lastMasterMsg  = helper.d1msgs.bySrcId(1),
          firstSlaveMsg  = helper.d2msgs.bySrcId(1),
          lastSlaveMsg   = helper.d2msgs.bySrcId(1)
        ),
        MessagesMergeDiff.Replace(
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
    val combine  = CMO.Combine(helper.d1cwd, helper.d2cwd, IndexedSeq.empty)
    val analysis = helper.merger.analyzeChatHistoryMerge(combine).messageMergeOptions
    assert(
      analysis === Seq(
        MessagesMergeDiff.Replace(
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
    val combine  = CMO.Combine(helper.d1cwd, helper.d2cwd, IndexedSeq.empty)
    val analysis = helper.merger.analyzeChatHistoryMerge(combine).messageMergeOptions
    assert(
      analysis === Seq(
        MessagesMergeDiff.Retain(
          firstMasterMsg = helper.d1msgs.bySrcId(1),
          lastMasterMsg  = helper.d1msgs.bySrcId(1),
        ),
        MessagesMergeDiff.Match(
          firstMasterMsg = helper.d1msgs.bySrcId(2),
          lastMasterMsg  = helper.d1msgs.bySrcId(2),
          firstSlaveMsg  = helper.d2msgs.bySrcId(2),
          lastSlaveMsg   = helper.d2msgs.bySrcId(2)
        ),
        MessagesMergeDiff.Retain(
          firstMasterMsg = helper.d1msgs.bySrcId(3),
          lastMasterMsg  = helper.d1msgs.bySrcId(3),
        ),
        MessagesMergeDiff.Match(
          firstMasterMsg = helper.d1msgs.bySrcId(4),
          lastMasterMsg  = helper.d1msgs.bySrcId(4),
          firstSlaveMsg  = helper.d2msgs.bySrcId(4),
          lastSlaveMsg   = helper.d2msgs.bySrcId(4)
        ),
        MessagesMergeDiff.Retain(
          firstMasterMsg = helper.d1msgs.bySrcId(5),
          lastMasterMsg  = helper.d1msgs.bySrcId(5),
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
    val combine  = CMO.Combine(helper.d1cwd, helper.d2cwd, IndexedSeq.empty)
    val analysis = helper.merger.analyzeChatHistoryMerge(combine).messageMergeOptions
    assert(
      analysis === Seq(
        MessagesMergeDiff.Retain(
          firstMasterMsg = helper.d1msgs.bySrcId(1),
          lastMasterMsg  = helper.d1msgs.bySrcId(2),
        ),
        MessagesMergeDiff.Add(
          firstSlaveMsg  = helper.d2msgs.bySrcId(3),
          lastSlaveMsg   = helper.d2msgs.bySrcId(4)
        ),
        MessagesMergeDiff.Replace(
          firstMasterMsg = helper.d1msgs.bySrcId(5),
          lastMasterMsg  = helper.d1msgs.bySrcId(6),
          firstSlaveMsg  = helper.d2msgs.bySrcId(5),
          lastSlaveMsg   = helper.d2msgs.bySrcId(6)
        ),
        MessagesMergeDiff.Match(
          firstMasterMsg = helper.d1msgs.bySrcId(7),
          lastMasterMsg  = helper.d1msgs.bySrcId(8),
          firstSlaveMsg  = helper.d2msgs.bySrcId(7),
          lastSlaveMsg   = helper.d2msgs.bySrcId(8)
        ),
        MessagesMergeDiff.Replace(
          firstMasterMsg = helper.d1msgs.bySrcId(9),
          lastMasterMsg  = helper.d1msgs.bySrcId(10),
          firstSlaveMsg  = helper.d2msgs.bySrcId(9),
          lastSlaveMsg   = helper.d2msgs.bySrcId(10)
        ),
        MessagesMergeDiff.Add(
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
    val combine  = CMO.Combine(helper.d1cwd, helper.d2cwd, IndexedSeq.empty)
    val analysis = helper.merger.analyzeChatHistoryMerge(combine).messageMergeOptions
    assert(
      analysis === Seq(
        MessagesMergeDiff.Add(
          firstSlaveMsg  = helper.d2msgs.bySrcId(1),
          lastSlaveMsg   = helper.d2msgs.bySrcId(2)
        ),
        MessagesMergeDiff.Retain(
          firstMasterMsg = helper.d1msgs.bySrcId(3),
          lastMasterMsg  = helper.d1msgs.bySrcId(4),
        ),
        MessagesMergeDiff.Replace(
          firstMasterMsg = helper.d1msgs.bySrcId(5),
          lastMasterMsg  = helper.d1msgs.bySrcId(6),
          firstSlaveMsg  = helper.d2msgs.bySrcId(5),
          lastSlaveMsg   = helper.d2msgs.bySrcId(6)
        ),
        MessagesMergeDiff.Match(
          firstMasterMsg = helper.d1msgs.bySrcId(7),
          lastMasterMsg  = helper.d1msgs.bySrcId(8),
          firstSlaveMsg  = helper.d2msgs.bySrcId(7),
          lastSlaveMsg   = helper.d2msgs.bySrcId(8)
        ),
        MessagesMergeDiff.Replace(
          firstMasterMsg = helper.d1msgs.bySrcId(9),
          lastMasterMsg  = helper.d1msgs.bySrcId(10),
          firstSlaveMsg  = helper.d2msgs.bySrcId(9),
          lastSlaveMsg   = helper.d2msgs.bySrcId(10)
        ),
        MessagesMergeDiff.Retain(
          firstMasterMsg = helper.d1msgs.bySrcId(11),
          lastMasterMsg  = helper.d1msgs.bySrcId(12),
        )
      )
    )
  }

  // "not found" should NOT conflict with "not downloaded" and vice versa
  test("combine - content preset vs absent vs not downloaded") {
    val userIds = Seq(rndUserId, rndUserId, rndUserId, rndUserId)

    val notFound = ContentPhoto(
      pathOption = Some("non/existent/path.jpg"),
      width      = 100500,
      height     = 100600
    )

    val notDownloaded = notFound.copy(pathOption = None)

    val placeholder1 = ContentPhoto(
      pathOption = Some("placeholder-1"),
      width      = -1,
      height     = -1
    )

    val placeholder2 = placeholder1.copy(pathOption = Some("placeholder-2"))

    val tmpDir = makeTempDir()
    val placeholder1File1 = createRandomTempFile(tmpDir)
    val placeholder1File2 = createRandomTempFile(tmpDir)

    def makeRegularMsgPhoto(idx: Int, regular: Boolean, photo: ContentPhoto) = {
      val typed: Message.Typed = if (regular) {
        Message.Typed.Regular(MessageRegular(
          editTimestampOption    = Some(baseDate.plusMinutes(10 + idx).unixTimestamp),
          replyToMessageIdOption = None,
          forwardFromNameOption  = Some("some user"),
          contentOption          = Some(photo)
        ))
      } else {
        Message.Typed.Service(Some(MessageServiceGroupEditPhoto(photo)))
      }
      val text = Seq(RichText.makePlain(s"Message for a photo $idx"))
      Message(
        internalId       = NoInternalId,
        sourceIdOption   = Some((100L + idx).asInstanceOf[MessageSourceId]),
        timestamp        = baseDate.unixTimestamp,
        fromId           = userIds.head,
        searchableString = Some(makeSearchableString(text, typed)),
        text             = text,
        typed            = typed
      )
    }

    val msgsA = Seq(
      makeRegularMsgPhoto(1,  regular = true,  notFound),
      makeRegularMsgPhoto(2,  regular = true,  notDownloaded),
      makeRegularMsgPhoto(3,  regular = false, notFound),
      makeRegularMsgPhoto(4,  regular = false, notDownloaded),

      makeRegularMsgPhoto(4,  regular = true,  placeholder1),
      makeRegularMsgPhoto(5,  regular = true,  placeholder1),
      makeRegularMsgPhoto(6,  regular = true,  placeholder1),
      makeRegularMsgPhoto(7,  regular = true,  placeholder1),
      makeRegularMsgPhoto(8,  regular = true,  notDownloaded),
      makeRegularMsgPhoto(9,  regular = true,  notFound),

      makeRegularMsgPhoto(10, regular = false, placeholder1),
      makeRegularMsgPhoto(11, regular = false, placeholder1),
      makeRegularMsgPhoto(12, regular = false, placeholder1),
      makeRegularMsgPhoto(13, regular = false, placeholder1),
      makeRegularMsgPhoto(14, regular = false, notDownloaded),
      makeRegularMsgPhoto(15, regular = false, notFound),
    )
    val msgsB = Seq(
      makeRegularMsgPhoto(1,  regular = true,  notDownloaded),
      makeRegularMsgPhoto(2,  regular = true,  notFound),
      makeRegularMsgPhoto(3,  regular = false, notDownloaded),
      makeRegularMsgPhoto(4,  regular = false, notFound),

      makeRegularMsgPhoto(4,  regular = true,  placeholder1),
      makeRegularMsgPhoto(5,  regular = true,  notDownloaded),
      makeRegularMsgPhoto(6,  regular = true,  notFound),
      makeRegularMsgPhoto(7,  regular = true,  placeholder2),
      makeRegularMsgPhoto(8,  regular = true,  placeholder1),
      makeRegularMsgPhoto(9,  regular = true,  placeholder1),

      makeRegularMsgPhoto(10, regular = false, placeholder1),
      makeRegularMsgPhoto(11, regular = false, notDownloaded),
      makeRegularMsgPhoto(12, regular = false, notFound),
      makeRegularMsgPhoto(13, regular = false, placeholder2),
      makeRegularMsgPhoto(14, regular = false, placeholder1),
      makeRegularMsgPhoto(15, regular = false, placeholder1),
    )
    val helper = new MergerHelper(msgsA, msgsB, ((isMaster, path, msg) => {
      def transformContent(photo: ContentPhoto): ContentPhoto = photo match {
        case `notFound` | `notDownloaded` => photo
        case `placeholder1` =>
          val file = new File(path, placeholder1File1.getName)
          if (!file.exists) {
            file.deleteOnExit()
            Files.copy(placeholder1File1.toPath, file.toPath)
          }
          photo.copy(pathOption = Some(file.toRelativePath(path)))
        case `placeholder2` =>
          val file = new File(path, placeholder1File2.getName)
          if (!file.exists) {
            file.deleteOnExit()
            Files.copy(placeholder1File2.toPath, file.toPath)
          }
          photo.copy(pathOption = Some(file.toRelativePath(path)))
      }

      msg.copy(typed = msg.typed match {
        case Message.Typed.Regular(msg) =>
          Message.Typed.Regular(msg.copy(contentOption = msg.contentOption map (c => transformContent(c.asInstanceOf[ContentPhoto]))))
        case Message.Typed.Service(Some(MessageServiceGroupEditPhoto(photo, _))) =>
          Message.Typed.Service(Some(MessageServiceGroupEditPhoto(transformContent(photo))))
      })
    }))

    val combine = CMO.Combine(helper.d1cwd, helper.d2cwd, IndexedSeq.empty)
    val analysis = helper.merger.analyzeChatHistoryMerge(combine).messageMergeOptions
    assert(
      analysis === Seq(
        MessagesMergeDiff.Match(
          firstMasterMsg = helper.d1msgs.bySrcId(101L),
          lastMasterMsg  = helper.d1msgs.bySrcId(106L),
          firstSlaveMsg  = helper.d2msgs.bySrcId(101L),
          lastSlaveMsg   = helper.d2msgs.bySrcId(106L)
        ),
        MessagesMergeDiff.Replace(
          firstMasterMsg = helper.d1msgs.bySrcId(107L),
          lastMasterMsg  = helper.d1msgs.bySrcId(107L),
          firstSlaveMsg  = helper.d2msgs.bySrcId(107L),
          lastSlaveMsg   = helper.d2msgs.bySrcId(107L)
        ),
        MessagesMergeDiff.Match(
          firstMasterMsg = helper.d1msgs.bySrcId(108L),
          lastMasterMsg  = helper.d1msgs.bySrcId(112L),
          firstSlaveMsg  = helper.d2msgs.bySrcId(108L),
          lastSlaveMsg   = helper.d2msgs.bySrcId(112L)
        ),
        MessagesMergeDiff.Replace(
          firstMasterMsg = helper.d1msgs.bySrcId(113L),
          lastMasterMsg  = helper.d1msgs.bySrcId(113L),
          firstSlaveMsg  = helper.d2msgs.bySrcId(113L),
          lastSlaveMsg   = helper.d2msgs.bySrcId(113L)
        ),
        MessagesMergeDiff.Match(
          firstMasterMsg = helper.d1msgs.bySrcId(114L),
          lastMasterMsg  = helper.d1msgs.bySrcId(115L),
          firstSlaveMsg  = helper.d2msgs.bySrcId(114L),
          lastSlaveMsg   = helper.d2msgs.bySrcId(115L)
        ),
      )
    )
  }

  //
  // Helpers
  //

  class MergerHelper(msgs1: Seq[Message],
                     msgs2: Seq[Message],
                     amendMessage: ((Boolean, DatasetRoot, Message) => Message) = ((_, _, m) => m)) {
    val (dao1, d1ds, d1root, d1users, d1cwd, d1msgs) = createDaoAndEntities(isMaster = true, "One", msgs1, maxUserId)
    val (dao2, d2ds, d2root, d2users, d2cwd, d2msgs) = createDaoAndEntities(isMaster = false, "Two", msgs2, maxUserId)

    def merger: DatasetMerger =
      new DatasetMerger(dao1, d1ds, dao2, d2ds)

    private def createDaoAndEntities(isMaster: Boolean,
                                     nameSuffix: String,
                                     srcMsgs: Seq[Message],
                                     numUsers: Int) = {
      val dao                          = createSimpleDao(isMaster, nameSuffix, srcMsgs, numUsers, amendMessage)
      val (ds, root, users, cwd, msgs) = getSimpleDaoEntities(dao)
      (dao, ds, root, users, cwd, msgs)
    }
  }
}
