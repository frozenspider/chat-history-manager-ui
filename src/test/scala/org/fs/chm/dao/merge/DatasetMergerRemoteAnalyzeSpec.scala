package org.fs.chm.dao.merge

import java.io.File
import java.nio.file.Files

import scala.concurrent.ExecutionContext

import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.grpc.Server
import io.grpc.ServerBuilder
import org.fs.chm.TestHelper
import org.fs.chm.dao.Entities._
import org.fs.chm.dao.GrpcChatHistoryDao
import org.fs.chm.dao.merge.DatasetMerger._
import org.fs.chm.loader.GrpcDaoService
import org.fs.chm.protobuf._
import org.fs.chm.utility.LangUtils._
import org.fs.chm.utility.Logging
import org.fs.chm.utility.TestUtils._
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

/** This spec requires Rust counterpart to be up and running */
@RunWith(classOf[org.scalatestplus.junit.JUnitRunner])
class DatasetMergerRemoteAnalyzeSpec //
    extends AnyFunSuite
    with TestHelper
    with Logging
    with BeforeAndAfter {
  import DatasetMergerHelper._

  test("match - same single message") {
    val msgs = Seq(createRegularMessage(1, 1))
    tryWith(new MergerHelper(msgs, msgs)) { helper =>
      val analysis = helper.merger.analyze(helper.d1cwd, helper.d2cwd, "")
      assert(
        analysis === Seq(
          MessagesMergeDiff.Match(
            firstMasterMsgId = helper.d1msgs.internalIdBySrcId(1),
            lastMasterMsgId  = helper.d1msgs.internalIdBySrcId(1),
            firstSlaveMsgId  = helper.d2msgs.internalIdBySrcId(1),
            lastSlaveMsgId   = helper.d2msgs.internalIdBySrcId(1)
          )
        )
      )
    }
  }

  test("match - same multiple messages") {
    val msgs = for (i <- 1 to maxId) yield createRegularMessage(i, rndUserId)
    tryWith(new MergerHelper(msgs, msgs)) { helper =>
      val analysis = helper.merger.analyze(helper.d1cwd, helper.d2cwd, "")
      assert(
        analysis === Seq(
          MessagesMergeDiff.Match(
            firstMasterMsgId = helper.d1msgs.internalIdBySrcId(1),
            lastMasterMsgId  = helper.d1msgs.internalIdBySrcId(maxId),
            firstSlaveMsgId  = helper.d2msgs.internalIdBySrcId(1),
            lastSlaveMsgId   = helper.d2msgs.internalIdBySrcId(maxId)
          )
        )
      )
    }
  }

  test("retain - no slave messages") {
    val msgs = for (i <- 1 to maxId) yield createRegularMessage(i, rndUserId)
    tryWith(new MergerHelper(msgs, IndexedSeq.empty)) { helper =>
      val analysis = helper.merger.analyze(helper.d1cwd, helper.d2cwd, "")
      assert(
        analysis === Seq(
          MessagesMergeDiff.Retain(
            firstMasterMsgId = helper.d1msgs.internalIdBySrcId(1),
            lastMasterMsgId  = helper.d1msgs.internalIdBySrcId(maxId),
          )
        )
      )
    }
  }

  test("retain - no new slave messages, matching sequence in the middle") {
    val msgs  = for (i <- 1 to maxId) yield createRegularMessage(i, rndUserId)
    val msgs2 = msgs.filter(m => (5 to 10) contains m.sourceIdOption.get)
    tryWith(new MergerHelper(msgs, msgs2)) { helper =>
      val analysis = helper.merger.analyze(helper.d1cwd, helper.d2cwd, "")
      assert(
        analysis === Seq(
          MessagesMergeDiff.Retain(
            firstMasterMsgId = helper.d1msgs.internalIdBySrcId(1),
            lastMasterMsgId  = helper.d1msgs.internalIdBySrcId(4),
          ),
          MessagesMergeDiff.Match(
            firstMasterMsgId = helper.d1msgs.internalIdBySrcId(5),
            lastMasterMsgId  = helper.d1msgs.internalIdBySrcId(10),
            firstSlaveMsgId  = helper.d2msgs.internalIdBySrcId(5),
            lastSlaveMsgId   = helper.d2msgs.internalIdBySrcId(10)
          ),
          MessagesMergeDiff.Retain(
            firstMasterMsgId = helper.d1msgs.internalIdBySrcId(11),
            lastMasterMsgId  = helper.d1msgs.internalIdBySrcId(maxId),
          )
        )
      )
    }
  }

  test("combine - added one new message in the middle") {
    val msgs    = for (i <- 1 to 3) yield createRegularMessage(i, rndUserId)
    val msgs123 = msgs
    val msgs13  = msgs123.filter(_.sourceIdOption.get != 2)
    tryWith(new MergerHelper(msgs13, msgs123)) { helper =>
      val analysis = helper.merger.analyze(helper.d1cwd, helper.d2cwd, "")
      assert(
        analysis === Seq(
          MessagesMergeDiff.Match(
            firstMasterMsgId = helper.d1msgs.internalIdBySrcId(1),
            lastMasterMsgId  = helper.d1msgs.internalIdBySrcId(1),
            firstSlaveMsgId  = helper.d2msgs.internalIdBySrcId(1),
            lastSlaveMsgId   = helper.d2msgs.internalIdBySrcId(1)
          ),
          MessagesMergeDiff.Add(
            firstSlaveMsgId = helper.d2msgs.internalIdBySrcId(2),
            lastSlaveMsgId  = helper.d2msgs.internalIdBySrcId(2)
          ),
          MessagesMergeDiff.Match(
            firstMasterMsgId = helper.d1msgs.internalIdBySrcId(3),
            lastMasterMsgId  = helper.d1msgs.internalIdBySrcId(3),
            firstSlaveMsgId  = helper.d2msgs.internalIdBySrcId(3),
            lastSlaveMsgId   = helper.d2msgs.internalIdBySrcId(3)
          )
        )
      )
    }
  }

  test("combine - changed one message in the middle") {
    val msgs  = for (i <- 1 to 3) yield createRegularMessage(i, rndUserId)
    val msgsA = msgs
    val msgsB = changedMessages(msgsA, (_ == 2))
    tryWith(new MergerHelper(msgsA, msgsB)) { helper =>
      val analysis = helper.merger.analyze(helper.d1cwd, helper.d2cwd, "")
      assert(
        analysis === Seq(
          MessagesMergeDiff.Match(
            firstMasterMsgId = helper.d1msgs.internalIdBySrcId(1),
            lastMasterMsgId  = helper.d1msgs.internalIdBySrcId(1),
            firstSlaveMsgId  = helper.d2msgs.internalIdBySrcId(1),
            lastSlaveMsgId   = helper.d2msgs.internalIdBySrcId(1)
          ),
          MessagesMergeDiff.Replace(
            firstMasterMsgId = helper.d1msgs.internalIdBySrcId(2),
            lastMasterMsgId  = helper.d1msgs.internalIdBySrcId(2),
            firstSlaveMsgId  = helper.d2msgs.internalIdBySrcId(2),
            lastSlaveMsgId   = helper.d2msgs.internalIdBySrcId(2)
          ),
          MessagesMergeDiff.Match(
            firstMasterMsgId = helper.d1msgs.internalIdBySrcId(3),
            lastMasterMsgId  = helper.d1msgs.internalIdBySrcId(3),
            firstSlaveMsgId  = helper.d2msgs.internalIdBySrcId(3),
            lastSlaveMsgId   = helper.d2msgs.internalIdBySrcId(3)
          )
        )
      )
    }
  }

  /**
   * {{{
   * Master messages -         N
   * Slave messages  - 1 2 ... N
   * }}}
   */
  test("combine - added multiple message in the beginning") {
    val msgs  = for (i <- 1 to maxId) yield createRegularMessage(i, rndUserId)
    val msgsL = Seq(msgs.last)
      tryWith(new MergerHelper(msgsL, msgs)) { helper =>
      val analysis = helper.merger.analyze(helper.d1cwd, helper.d2cwd, "")
      assert(
        analysis === Seq(
          MessagesMergeDiff.Add(
            firstSlaveMsgId  = helper.d2msgs.internalIdBySrcId(1),
            lastSlaveMsgId   = helper.d2msgs.internalIdBySrcId(maxId - 1)
          ),
          MessagesMergeDiff.Match(
            firstMasterMsgId = helper.d1msgs.internalIdBySrcId(maxId),
            lastMasterMsgId  = helper.d1msgs.internalIdBySrcId(maxId),
            firstSlaveMsgId  = helper.d2msgs.internalIdBySrcId(maxId),
            lastSlaveMsgId   = helper.d2msgs.internalIdBySrcId(maxId)
          )
        )
      )
    }
  }

  /**
   * {{{
   * Master messages - 1  2  ...  N
   * Slave messages  - 1* 2* ...* N
   * }}}
   */
  test("combine - changed multiple message in the beginning") {
    val msgs  = for (i <- 1 to maxId) yield createRegularMessage(i, rndUserId)
    val msgsA = msgs
    val msgsB = changedMessages(msgsA, (_ < maxId))
    tryWith(new MergerHelper(msgsA, msgsB)) { helper =>
      val analysis = helper.merger.analyze(helper.d1cwd, helper.d2cwd, "")
      assert(
        analysis === Seq(
          MessagesMergeDiff.Replace(
            firstMasterMsgId = helper.d1msgs.internalIdBySrcId(1),
            lastMasterMsgId  = helper.d1msgs.internalIdBySrcId(maxId - 1),
            firstSlaveMsgId  = helper.d2msgs.internalIdBySrcId(1),
            lastSlaveMsgId   = helper.d2msgs.internalIdBySrcId(maxId - 1)
          ),
          MessagesMergeDiff.Match(
            firstMasterMsgId = helper.d1msgs.internalIdBySrcId(maxId),
            lastMasterMsgId  = helper.d1msgs.internalIdBySrcId(maxId),
            firstSlaveMsgId  = helper.d2msgs.internalIdBySrcId(maxId),
            lastSlaveMsgId   = helper.d2msgs.internalIdBySrcId(maxId)
          )
        )
      )
    }
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
    tryWith(new MergerHelper(msgsFL, msgs)) { helper =>
      val analysis = helper.merger.analyze(helper.d1cwd, helper.d2cwd, "")
      assert(
        analysis === Seq(
          MessagesMergeDiff.Match(
            firstMasterMsgId = helper.d1msgs.internalIdBySrcId(1),
            lastMasterMsgId  = helper.d1msgs.internalIdBySrcId(1),
            firstSlaveMsgId  = helper.d2msgs.internalIdBySrcId(1),
            lastSlaveMsgId   = helper.d2msgs.internalIdBySrcId(1)
          ),
          MessagesMergeDiff.Add(
            firstSlaveMsgId  = helper.d2msgs.internalIdBySrcId(2),
            lastSlaveMsgId   = helper.d2msgs.internalIdBySrcId(maxId - 1)
          ),
          MessagesMergeDiff.Match(
            firstMasterMsgId = helper.d1msgs.internalIdBySrcId(maxId),
            lastMasterMsgId  = helper.d1msgs.internalIdBySrcId(maxId),
            firstSlaveMsgId  = helper.d2msgs.internalIdBySrcId(maxId),
            lastSlaveMsgId   = helper.d2msgs.internalIdBySrcId(maxId)
          )
        )
      )
    }
  }

  /**
   * {{{
   * Master messages - 1  2  ...  N
   * Slave messages  - 1  2* ...* N
   * }}}
   */
  test("combine - changed multiple message in the middle") {
    val msgs  = for (i <- 1 to maxId) yield createRegularMessage(i, rndUserId)
    val msgsA = msgs
    val msgsB = changedMessages(msgsA, (id => id > 1 && id < maxId))
    tryWith(new MergerHelper(msgsA, msgsB)) { helper =>
      val analysis = helper.merger.analyze(helper.d1cwd, helper.d2cwd, "")
      assert(
        analysis === Seq(
          MessagesMergeDiff.Match(
            firstMasterMsgId = helper.d1msgs.internalIdBySrcId(1),
            lastMasterMsgId  = helper.d1msgs.internalIdBySrcId(1),
            firstSlaveMsgId  = helper.d2msgs.internalIdBySrcId(1),
            lastSlaveMsgId   = helper.d2msgs.internalIdBySrcId(1)
          ),
          MessagesMergeDiff.Replace(
            firstMasterMsgId = helper.d1msgs.internalIdBySrcId(2),
            lastMasterMsgId  = helper.d1msgs.internalIdBySrcId(maxId - 1),
            firstSlaveMsgId  = helper.d2msgs.internalIdBySrcId(2),
            lastSlaveMsgId   = helper.d2msgs.internalIdBySrcId(maxId - 1)
          ),
          MessagesMergeDiff.Match(
            firstMasterMsgId = helper.d1msgs.internalIdBySrcId(maxId),
            lastMasterMsgId  = helper.d1msgs.internalIdBySrcId(maxId),
            firstSlaveMsgId  = helper.d2msgs.internalIdBySrcId(maxId),
            lastSlaveMsgId   = helper.d2msgs.internalIdBySrcId(maxId)
          )
        )
      )
    }
  }

  /**
   * {{{
   * Master messages - 1
   * Slave messages  - 1 2 ... N
   * }}}
   */
  test("combine - added multiple message in the end") {
    val msgs  = for (i <- 1 to maxId) yield createRegularMessage(i, rndUserId)
    val msgsF = Seq(msgs.head)
    tryWith(new MergerHelper(msgsF, msgs)) { helper =>
      val analysis = helper.merger.analyze(helper.d1cwd, helper.d2cwd, "")
      assert(
        analysis === Seq(
          MessagesMergeDiff.Match(
            firstMasterMsgId = helper.d1msgs.internalIdBySrcId(1),
            lastMasterMsgId  = helper.d1msgs.internalIdBySrcId(1),
            firstSlaveMsgId  = helper.d2msgs.internalIdBySrcId(1),
            lastSlaveMsgId   = helper.d2msgs.internalIdBySrcId(1)
          ),
          MessagesMergeDiff.Add(
            firstSlaveMsgId  = helper.d2msgs.internalIdBySrcId(2),
            lastSlaveMsgId   = helper.d2msgs.internalIdBySrcId(maxId)
          )
        )
      )
    }
  }

  /**
   * {{{
   * Master messages - 1  2  ...  N
   * Slave messages  - 1  2* ...* N*
   * }}}
   */
  test("combine - changed multiple message in the end") {
    val msgs  = for (i <- 1 to maxId) yield createRegularMessage(i, rndUserId)
    val msgsA = msgs
    val msgsB = changedMessages(msgsA, (_ > 1))
    tryWith(new MergerHelper(msgsA, msgsB)) { helper =>
      val analysis = helper.merger.analyze(helper.d1cwd, helper.d2cwd, "")
      assert(
        analysis === Seq(
          MessagesMergeDiff.Match(
            firstMasterMsgId = helper.d1msgs.internalIdBySrcId(1),
            lastMasterMsgId  = helper.d1msgs.internalIdBySrcId(1),
            firstSlaveMsgId  = helper.d2msgs.internalIdBySrcId(1),
            lastSlaveMsgId   = helper.d2msgs.internalIdBySrcId(1)
          ),
          MessagesMergeDiff.Replace(
            firstMasterMsgId = helper.d1msgs.internalIdBySrcId(2),
            lastMasterMsgId  = helper.d1msgs.internalIdBySrcId(maxId),
            firstSlaveMsgId  = helper.d2msgs.internalIdBySrcId(2),
            lastSlaveMsgId   = helper.d2msgs.internalIdBySrcId(maxId)
          )
        )
      )
    }
  }

  /**
   * {{{
   * Master messages - 1  2  ...  N
   * Slave messages  - 1* 2* ...* N*
   * }}}
   */
  test("combine - changed all messages") {
    val msgs  = for (i <- 1 to maxId) yield createRegularMessage(i, rndUserId)
    val msgsA = msgs
    val msgsB = changedMessages(msgsA, (_ => true))
    tryWith(new MergerHelper(msgsA, msgsB)) { helper =>
      val analysis = helper.merger.analyze(helper.d1cwd, helper.d2cwd, "")
      assert(
        analysis === Seq(
          MessagesMergeDiff.Replace(
            firstMasterMsgId = helper.d1msgs.internalIdBySrcId(1),
            lastMasterMsgId  = helper.d1msgs.internalIdBySrcId(maxId),
            firstSlaveMsgId  = helper.d2msgs.internalIdBySrcId(1),
            lastSlaveMsgId   = helper.d2msgs.internalIdBySrcId(maxId)
          )
        )
      )
    }
  }

  /**
   * {{{
   * Master messages - 1 2 3 4 5
   * Slave messages  -   2   4
   * }}}
   */
  test("combine - master has messages not present in slave") {
    val msgs  = for (i <- 1 to 5) yield createRegularMessage(i, rndUserId)
    val msgsA = msgs
    val msgsB = msgs.filter(Seq(2, 4) contains _.sourceIdOption.get)
    tryWith(new MergerHelper(msgsA, msgsB)) { helper =>
      val analysis = helper.merger.analyze(helper.d1cwd, helper.d2cwd, "")
      assert(
        analysis === Seq(
          MessagesMergeDiff.Retain(
            firstMasterMsgId = helper.d1msgs.internalIdBySrcId(1),
            lastMasterMsgId  = helper.d1msgs.internalIdBySrcId(1),
          ),
          MessagesMergeDiff.Match(
            firstMasterMsgId = helper.d1msgs.internalIdBySrcId(2),
            lastMasterMsgId  = helper.d1msgs.internalIdBySrcId(2),
            firstSlaveMsgId  = helper.d2msgs.internalIdBySrcId(2),
            lastSlaveMsgId   = helper.d2msgs.internalIdBySrcId(2)
          ),
          MessagesMergeDiff.Retain(
            firstMasterMsgId = helper.d1msgs.internalIdBySrcId(3),
            lastMasterMsgId  = helper.d1msgs.internalIdBySrcId(3),
          ),
          MessagesMergeDiff.Match(
            firstMasterMsgId = helper.d1msgs.internalIdBySrcId(4),
            lastMasterMsgId  = helper.d1msgs.internalIdBySrcId(4),
            firstSlaveMsgId  = helper.d2msgs.internalIdBySrcId(4),
            lastSlaveMsgId   = helper.d2msgs.internalIdBySrcId(4)
          ),
          MessagesMergeDiff.Retain(
            firstMasterMsgId = helper.d1msgs.internalIdBySrcId(5),
            lastMasterMsgId  = helper.d1msgs.internalIdBySrcId(5),
          )
        )
      )
    }
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
    tryWith(new MergerHelper(msgsA, msgsB)) { helper =>
      val analysis = helper.merger.analyze(helper.d1cwd, helper.d2cwd, "")
      assert(
        analysis === Seq(
          MessagesMergeDiff.Retain(
            firstMasterMsgId = helper.d1msgs.internalIdBySrcId(1),
            lastMasterMsgId  = helper.d1msgs.internalIdBySrcId(2),
          ),
          MessagesMergeDiff.Add(
            firstSlaveMsgId  = helper.d2msgs.internalIdBySrcId(3),
            lastSlaveMsgId   = helper.d2msgs.internalIdBySrcId(4)
          ),
          MessagesMergeDiff.Replace(
            firstMasterMsgId = helper.d1msgs.internalIdBySrcId(5),
            lastMasterMsgId  = helper.d1msgs.internalIdBySrcId(6),
            firstSlaveMsgId  = helper.d2msgs.internalIdBySrcId(5),
            lastSlaveMsgId   = helper.d2msgs.internalIdBySrcId(6)
          ),
          MessagesMergeDiff.Match(
            firstMasterMsgId = helper.d1msgs.internalIdBySrcId(7),
            lastMasterMsgId  = helper.d1msgs.internalIdBySrcId(8),
            firstSlaveMsgId  = helper.d2msgs.internalIdBySrcId(7),
            lastSlaveMsgId   = helper.d2msgs.internalIdBySrcId(8)
          ),
          MessagesMergeDiff.Replace(
            firstMasterMsgId = helper.d1msgs.internalIdBySrcId(9),
            lastMasterMsgId  = helper.d1msgs.internalIdBySrcId(10),
            firstSlaveMsgId  = helper.d2msgs.internalIdBySrcId(9),
            lastSlaveMsgId   = helper.d2msgs.internalIdBySrcId(10)
          ),
          MessagesMergeDiff.Add(
            firstSlaveMsgId  = helper.d2msgs.internalIdBySrcId(11),
            lastSlaveMsgId   = helper.d2msgs.internalIdBySrcId(12)
          )
        )
      )
    }
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
    tryWith(new MergerHelper(msgsA, msgsB)) { helper =>
      val analysis = helper.merger.analyze(helper.d1cwd, helper.d2cwd, "")
      assert(
        analysis === Seq(
          MessagesMergeDiff.Add(
            firstSlaveMsgId  = helper.d2msgs.internalIdBySrcId(1),
            lastSlaveMsgId   = helper.d2msgs.internalIdBySrcId(2)
          ),
          MessagesMergeDiff.Retain(
            firstMasterMsgId = helper.d1msgs.internalIdBySrcId(3),
            lastMasterMsgId  = helper.d1msgs.internalIdBySrcId(4),
          ),
          MessagesMergeDiff.Replace(
            firstMasterMsgId = helper.d1msgs.internalIdBySrcId(5),
            lastMasterMsgId  = helper.d1msgs.internalIdBySrcId(6),
            firstSlaveMsgId  = helper.d2msgs.internalIdBySrcId(5),
            lastSlaveMsgId   = helper.d2msgs.internalIdBySrcId(6)
          ),
          MessagesMergeDiff.Match(
            firstMasterMsgId = helper.d1msgs.internalIdBySrcId(7),
            lastMasterMsgId  = helper.d1msgs.internalIdBySrcId(8),
            firstSlaveMsgId  = helper.d2msgs.internalIdBySrcId(7),
            lastSlaveMsgId   = helper.d2msgs.internalIdBySrcId(8)
          ),
          MessagesMergeDiff.Replace(
            firstMasterMsgId = helper.d1msgs.internalIdBySrcId(9),
            lastMasterMsgId  = helper.d1msgs.internalIdBySrcId(10),
            firstSlaveMsgId  = helper.d2msgs.internalIdBySrcId(9),
            lastSlaveMsgId   = helper.d2msgs.internalIdBySrcId(10)
          ),
          MessagesMergeDiff.Retain(
            firstMasterMsgId = helper.d1msgs.internalIdBySrcId(11),
            lastMasterMsgId  = helper.d1msgs.internalIdBySrcId(12),
          )
        )
      )
    }
  }

  // "not found" should NOT conflict with "not downloaded" and vice versa
  test("combine - content preset vs absent vs not downloaded") {
    val userIds = Seq(rndUserId, rndUserId, rndUserId, rndUserId)

    val notFound = ContentPhoto(
      pathOption = Some("non/existent/path.jpg"),
      width      = 100500,
      height     = 100600,
      isOneTime  = false
    )

    val notDownloaded = notFound.copy(pathOption = None)

    val placeholder1 = ContentPhoto(
      pathOption = Some("placeholder-1"),
      width      = -1,
      height     = -1,
      isOneTime  = false
    )

    val placeholder2 = placeholder1.copy(pathOption = Some("placeholder-2"))

    val tmpDir = makeTempDir()
    val placeholder1File1 = createRandomTempFile(tmpDir)
    val placeholder1File2 = createRandomTempFile(tmpDir)

    def makeRegularMsgPhoto(idx: Int, regular: Boolean, photo: ContentPhoto) = {
      val typed: Message.Typed = if (regular) {
        Message.Typed.Regular(MessageRegular(
          editTimestampOption    = Some(baseDate.plusMinutes(10 + idx).unixTimestamp),
          isDeleted              = false,
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
        searchableString = makeSearchableString(text, typed),
        text             = text,
        typed            = typed
      )
    }

    val msgsA = Seq(
      makeRegularMsgPhoto(1,  regular = true,  notFound),
      makeRegularMsgPhoto(2,  regular = true,  notDownloaded),
      makeRegularMsgPhoto(3,  regular = false, notFound),
      makeRegularMsgPhoto(4,  regular = false, notDownloaded),

      makeRegularMsgPhoto(5,  regular = true,  placeholder1),
      makeRegularMsgPhoto(6,  regular = true,  placeholder1),
      makeRegularMsgPhoto(7,  regular = true,  placeholder1),
      makeRegularMsgPhoto(8,  regular = true,  placeholder1),
      makeRegularMsgPhoto(9,  regular = true,  notDownloaded),
      makeRegularMsgPhoto(10, regular = true,  notFound),

      makeRegularMsgPhoto(11, regular = false, placeholder1),
      makeRegularMsgPhoto(12, regular = false, placeholder1),
      makeRegularMsgPhoto(13, regular = false, placeholder1),
      makeRegularMsgPhoto(14, regular = false, placeholder1),
      makeRegularMsgPhoto(15, regular = false, notDownloaded),
      makeRegularMsgPhoto(16, regular = false, notFound),
    )
    val msgsB = Seq(
      makeRegularMsgPhoto(1,  regular = true,  notDownloaded),
      makeRegularMsgPhoto(2,  regular = true,  notFound),
      makeRegularMsgPhoto(3,  regular = false, notDownloaded),
      makeRegularMsgPhoto(4,  regular = false, notFound),

      makeRegularMsgPhoto(5,  regular = true,  placeholder1),
      makeRegularMsgPhoto(6,  regular = true,  notDownloaded),
      makeRegularMsgPhoto(7,  regular = true,  notFound),
      makeRegularMsgPhoto(8,  regular = true,  placeholder2),
      makeRegularMsgPhoto(9,  regular = true,  placeholder1),
      makeRegularMsgPhoto(10, regular = true,  placeholder1),

      makeRegularMsgPhoto(11, regular = false, placeholder1),
      makeRegularMsgPhoto(12, regular = false, notDownloaded),
      makeRegularMsgPhoto(13, regular = false, notFound),
      makeRegularMsgPhoto(14, regular = false, placeholder2),
      makeRegularMsgPhoto(15, regular = false, placeholder1),
      makeRegularMsgPhoto(16, regular = false, placeholder1),
    )

    def amendMessage(isMaster: Boolean, path: DatasetRoot, msg: Message): Message = {
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
    }

    tryWith(new MergerHelper(msgsA, msgsB, amendMessage)) { helper =>
      val analysis = helper.merger.analyze(helper.d1cwd, helper.d2cwd, "")
      assert(
        analysis === Seq(
          MessagesMergeDiff.Match(
            firstMasterMsgId = helper.d1msgs.internalIdBySrcId(101L),
            lastMasterMsgId  = helper.d1msgs.internalIdBySrcId(107L),
            firstSlaveMsgId  = helper.d2msgs.internalIdBySrcId(101L),
            lastSlaveMsgId   = helper.d2msgs.internalIdBySrcId(107L)
          ),
          MessagesMergeDiff.Replace(
            firstMasterMsgId = helper.d1msgs.internalIdBySrcId(108L),
            lastMasterMsgId  = helper.d1msgs.internalIdBySrcId(108L),
            firstSlaveMsgId  = helper.d2msgs.internalIdBySrcId(108L),
            lastSlaveMsgId   = helper.d2msgs.internalIdBySrcId(108L)
          ),
          MessagesMergeDiff.Match(
            firstMasterMsgId = helper.d1msgs.internalIdBySrcId(109L),
            lastMasterMsgId  = helper.d1msgs.internalIdBySrcId(113L),
            firstSlaveMsgId  = helper.d2msgs.internalIdBySrcId(109L),
            lastSlaveMsgId   = helper.d2msgs.internalIdBySrcId(113L)
          ),
          MessagesMergeDiff.Replace(
            firstMasterMsgId = helper.d1msgs.internalIdBySrcId(114L),
            lastMasterMsgId  = helper.d1msgs.internalIdBySrcId(114L),
            firstSlaveMsgId  = helper.d2msgs.internalIdBySrcId(114L),
            lastSlaveMsgId   = helper.d2msgs.internalIdBySrcId(114L)
          ),
          MessagesMergeDiff.Match(
            firstMasterMsgId = helper.d1msgs.internalIdBySrcId(115L),
            lastMasterMsgId  = helper.d1msgs.internalIdBySrcId(116L),
            firstSlaveMsgId  = helper.d2msgs.internalIdBySrcId(115L),
            lastSlaveMsgId   = helper.d2msgs.internalIdBySrcId(116L)
          ),
        )
      )
    }
  }

  //
  // Helpers
  //

  class MergerHelper(msgs1: Seq[Message],
                     msgs2: Seq[Message],
                     amendMessage: ((Boolean, DatasetRoot, Message) => Message) = ((_, _, m) => m)) extends AutoCloseable {
    val (dao1, d1ds, d1root, d1users, d1cwd, d1msgs) = createDaoAndEntities(isMaster = true, "One", msgs1, maxUserId)
    val (dao2, d2ds, d2root, d2users, d2cwd, d2msgs) = createDaoAndEntities(isMaster = false, "Two", msgs2, maxUserId)

    import DatasetMergerRemoteAnalyzeSpec._

    private val rdao1 = new GrpcChatHistoryDao(TestDaoKey1, dao1.name, RDaoStub, RLoaderStub)
    private val rdao2 = new GrpcChatHistoryDao(TestDaoKey2, dao2.name, RDaoStub, RLoaderStub)

    RDaoService.addDao(TestDaoKey1, dao1)
    RDaoService.addDao(TestDaoKey2, dao2)

    RLoaderStub.load(LoadRequest(TestDaoKey1, new File(dao1.storagePath, tmpFileName).getAbsolutePath))
    RLoaderStub.load(LoadRequest(TestDaoKey2, new File(dao2.storagePath, tmpFileName).getAbsolutePath))

    val merger: DatasetMergerRemote =
      new DatasetMergerRemote(ClientChannel, rdao1, d1ds, rdao2, d2ds)

    private def createDaoAndEntities(isMaster: Boolean,
                                     nameSuffix: String,
                                     srcMsgs: Seq[Message],
                                     numUsers: Int) = {
      val dao                          = createSimpleDao(isMaster, nameSuffix, srcMsgs, numUsers, amendMessage)
      val (ds, root, users, cwd, msgs) = getSimpleDaoEntities(dao)
      (dao, ds, root, users, cwd, msgs)
    }

    override def close(): Unit = {
      rdao1.close()
      rdao2.close()
    }
  }
}

object DatasetMergerRemoteAnalyzeSpec extends Logging {
  private val TestDaoKey1 = "test://dao1"
  private val TestDaoKey2 = "test://dao2"

  private val RpcPort = 50051

  private val ClientChannel: ManagedChannel = ManagedChannelBuilder
    .forAddress("127.0.0.1", RpcPort)
    .maxInboundMessageSize(Integer.MAX_VALUE)
    .usePlaintext()
    .build()

  private val RLoaderStub = HistoryLoaderServiceGrpc.blockingStub(ClientChannel)
  private val RDaoStub = HistoryDaoServiceGrpc.blockingStub(ClientChannel)

  private val RDaoService = new GrpcDaoService(_ => throw new UnsupportedOperationException("Asked to load a DAO!"))

  private val Server = {
    val serverPort = RpcPort + 1
    log.info(s"Starting callback server at ${serverPort}")
    val server: Server = ServerBuilder.forPort(serverPort)
      .addService(HistoryDaoServiceGrpc.bindService(RDaoService, ExecutionContext.global))
      .addService(HistoryLoaderServiceGrpc.bindService(RDaoService.loader, ExecutionContext.global))
      .build.start

    sys.addShutdownHook {
      server.shutdown()
    }
    server
  }
}
