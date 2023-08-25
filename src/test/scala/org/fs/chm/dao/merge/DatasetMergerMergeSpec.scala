package org.fs.chm.dao.merge

import java.io.File
import java.nio.file.Files

import scala.collection.immutable.ListMap
import scala.collection.parallel.CollectionConverters._

import org.fs.chm.TestHelper
import org.fs.chm.WithH2Dao
import org.fs.chm.dao.ChatHistoryDao
import org.fs.chm.dao.Entities._
import org.fs.chm.dao.merge.DatasetMerger._
import org.fs.chm.protobuf._
import org.fs.chm.utility.LangUtils._
import org.fs.chm.utility.TestUtils._
import org.fs.utility.Imports._
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfter
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.slf4s.Logging

@RunWith(classOf[org.scalatestplus.junit.JUnitRunner])
class DatasetMergerMergeSpec //
    extends AnyFunSuite
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
    assert(newUsers.size === 5)
    assert(newUsers.byId(1) === helper.d1users.byId(1).copy(dsUuid = newDs.uuid))
    assert(newUsers.byId(2) === helper.d2users.byId(2).copy(dsUuid = newDs.uuid))
    assert(newUsers.byId(3) === helper.d2users.byId(3).copy(dsUuid = newDs.uuid))
    assert(newUsers.byId(4) === helper.d1users.byId(4).copy(dsUuid = newDs.uuid))
    assert(newUsers.byId(5) === helper.d1users.byId(5).copy(dsUuid = newDs.uuid))
  }

  test("merge chats - keep single message, basic checks") {
    val msgs   = Seq(createRegularMessage(1, 1))
    val helper = H2MergerHelper.fromMessages(msgs, msgs)
    val chatMerges = Seq[ChatMergeOption](
      ChatMergeOption.Keep(helper.d1cwd)
    )
    assert(helper.dao1.datasets.size === 1)
    val newDs = helper.merger.merge(keepBothUsers(helper), chatMerges)
    assert(helper.dao1.datasets.size === 2)
    val newChats = helper.dao1.chats(newDs.uuid)
    assert(helper.dao1.chats(newDs.uuid).size === 1)
    val newMessages = helper.dao1.firstMessages(newChats.head.chat, Int.MaxValue)
    assert(newMessages.size === 1)
    assert((newMessages.head, helper.d1root, newChats.head) =~= (helper.d1msgs.head, helper.d1root, helper.d1cwd))
    assertFiles(helper.dao1, newDs, msgsPaths(helper.dao1, helper.d1ds, helper.d1msgs))
  }

  test("merge chats - keep single animation, content preserved") {
    mergeFilesHelper(amendSlaveMessage = true, helper => Seq(
      ChatMergeOption.Keep(helper.d1cwd)
    ))

    mergeFilesHelper(amendSlaveMessage = false, helper => Seq(
      ChatMergeOption.Keep(helper.d1cwd)
    ))

    mergeFilesHelper(amendSlaveMessage = true, helper => Seq(
      ChatMergeOption.Combine(
        helper.d1cwd, helper.d2cwd,
        IndexedSeq(
          MessagesMergeOption.Keep(
            firstMasterMsg      = tag(helper.d1msgs.head),
            lastMasterMsg       = tag(helper.d1msgs.last),
            firstSlaveMsgOption = Some(tag(helper.d2msgs.head)),
            lastSlaveMsgOption  = Some(tag(helper.d2msgs.last))
          )
        )
      )
    ))

    mergeFilesHelper(amendSlaveMessage = false, helper => Seq(
      ChatMergeOption.Combine(
        helper.d1cwd, helper.d2cwd,
        IndexedSeq(
          MessagesMergeOption.Keep(
            firstMasterMsg      = tag(helper.d1msgs.head),
            lastMasterMsg       = tag(helper.d1msgs.last),
            firstSlaveMsgOption = Some(tag(helper.d2msgs.head)),
            lastSlaveMsgOption  = Some(tag(helper.d2msgs.last))
          )
        )
      )
    ))

    // With Replace command, content that was previously there will disappear.
  }

  def mergeFilesHelper(amendSlaveMessage: Boolean,
                       makeChatMerges: H2MergerHelper => Seq[ChatMergeOption]): Unit = {
    def makeAnimationContent(path: File): Content = {
      val file1 = createRandomFile(path)
      val file2 = createRandomFile(path)
      ContentAnimation(
        pathOption          = Some(file1.toRelativePath(path)),
        width               = 111,
        height              = 222,
        mimeType            = "mt",
        durationSecOption   = Some(10),
        thumbnailPathOption = Some(file2.toRelativePath(path))
      )
    }

    def addAnimationToMasterMessage(isMaster: Boolean, path: File, msg: Message): Message = {
      val regular = msg.typed.regular.get
      msg.copy(typed = Message.Typed.Regular(regular.copy(
        contentOption = if (isMaster) Some(makeAnimationContent(path)) else None
      )))
    }

    def addAnimationToAnyMessage(unused: Boolean, path: File, msg: Message): Message =
      addAnimationToMasterMessage(isMaster = true, path, msg)

    val msg = createRegularMessage(1, 1)
    val helper = H2MergerHelper.fromMessages(
      Seq(msg), Seq(msg),
      if (amendSlaveMessage) addAnimationToAnyMessage else addAnimationToMasterMessage
    )
    assert(helper.dao1.datasets.size === 1)
    val chatMerges = makeChatMerges(helper)
    val newDs = helper.merger.merge(keepBothUsers(helper), chatMerges)
    assert(helper.dao1.datasets.size === 2)
    val newChats = helper.dao1.chats(newDs.uuid)
    assert(helper.dao1.chats(newDs.uuid).size === 1)
    val newMessages = helper.dao1.firstMessages(newChats.head.chat, Int.MaxValue)
    assert(newMessages.size === 1)
    val paths1 = msgsPaths(helper.dao1, helper.d1ds, helper.d1msgs)
    val paths2 = msgsPaths(helper.dao1, helper.d2ds, helper.d2msgs)
    assert(paths1.size === 2)
    assert(paths2.size === (if (amendSlaveMessage) 2 else 0))
    assertFiles(helper.dao1, newDs, paths1)
    assert((newMessages.head, helper.d1root, newChats.head) =~= (helper.d1msgs.head, helper.d1root, helper.d1cwd))

    // As a final step, reset DAO to initial state
    freeH2Dao()
    initH2Dao()
  }

  test("merge chats - replace single message, basic checks") {
    val usersA    = (1 to 2) map (i => createUser(noUuid, i))
    val usersB    = changedUsers(usersA, _ => true);
    val msgsA     = Seq(createRegularMessage(1, 1))
    val msgsB     = changedMessages(msgsA, _ => true)
    val chatMsgsA = ListMap(createPersonalChat(noUuid, 1, usersA.head, usersA.map(_.id), msgsA.size) -> msgsA)
    val chatMsgsB = ListMap(createPersonalChat(noUuid, 2, usersB.head, usersB.map(_.id), msgsB.size) -> msgsB)

    val helper = new H2MergerHelper(usersA, chatMsgsA, usersB, chatMsgsB)

    val chatMerges = Seq[ChatMergeOption](
      ChatMergeOption.Combine(
        helper.d1cwd,
        helper.d2cwd,
        IndexedSeq(
          MessagesMergeOption.Replace(
            firstMasterMsg = helper.d1msgs.bySrcId(1),
            lastMasterMsg  = helper.d1msgs.bySrcId(1),
            firstSlaveMsg  = helper.d2msgs.bySrcId(1),
            lastSlaveMsg   = helper.d2msgs.bySrcId(1)
          )
        )
      )
    )

    assert(helper.dao1.datasets.size === 1)
    val newDs = helper.merger.merge(helper.d1users.map(UserMergeOption.Keep), chatMerges)
    assert(helper.dao1.datasets.size === 2)
    val newChats = helper.dao1.chats(newDs.uuid)
    assert(helper.dao1.chats(newDs.uuid).size === 1)
    assert(helper.dao1.chats(newDs.uuid).head.chat.nameOption === usersA(1).prettyNameOption)
    val newMessages = helper.dao1.firstMessages(newChats.head.chat, Int.MaxValue)
    assert(newMessages.size === 1)
    assert((newMessages.head, helper.d1root, helper.d1cwd) =~= (helper.d2msgs.head, helper.d2root, helper.d2cwd))
    assertFiles(helper.dao1, newDs, msgsPaths(helper.dao2, helper.d2ds, helper.d2msgs))
  }

  test("merge chats - keep two messages") {
    val msgs   = for (i <- 1 to 6) yield createRegularMessage(i, 1)
    val msgsA  = msgs.filter(Seq(3, 4) contains _.sourceIdOption.get)
    val msgsB  = changedMessages(msgs, _ => true)
    val helper = H2MergerHelper.fromMessages(msgsA, msgsB)
    val chatMerges = Seq[ChatMergeOption](
      ChatMergeOption.Keep(helper.d1cwd)
    )
    val newDs = helper.merger.merge(keepBothUsers(helper), chatMerges)
    val newMessages = {
      val newChats = helper.dao1.chats(newDs.uuid)
      helper.dao1.firstMessages(newChats.head.chat, Int.MaxValue)
    }
    assert(newMessages.size === helper.d1msgs.size)
    for ((nm, om) <- (newMessages zip helper.d1msgs)) {
      assert((nm, helper.d1root, helper.d1cwd) =~= (om, helper.d1root, helper.d1cwd))
    }

    assertFiles(helper.dao1, newDs, msgsPaths(helper.dao1, helper.d1ds, newMessages))
  }

  test("merge chats - add two messages") {
    val msgs   = for (i <- 1 to 5) yield createRegularMessage(i, 1)
    val msgsA  = msgs
    val msgsB  = changedMessages(msgs.filter(Seq(3, 4) contains _.sourceIdOption.get), _ => true)
    val helper = H2MergerHelper.fromMessages(msgsA, msgsB)
    val chatMerges = Seq[ChatMergeOption](
      ChatMergeOption.Add(helper.d2cwd)
    )

    val newDs = helper.merger.merge(keepBothUsers(helper), chatMerges)
    val newMessages = {
      val newChats = helper.dao1.chats(newDs.uuid)
      helper.dao1.firstMessages(newChats.head.chat, Int.MaxValue)
    }
    assert(newMessages.size === 2)
    assert((newMessages(0), helper.d1root, helper.d1cwd) =~= (helper.d2msgs.bySrcId(3), helper.d2root, helper.d2cwd))
    assert((newMessages(1), helper.d1root, helper.d1cwd) =~= (helper.d2msgs.bySrcId(4), helper.d2root, helper.d2cwd))

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
        helper.d1cwd,
        helper.d2cwd,
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
    val newDs = helper.merger.merge(keepBothUsers(helper), chatMerges)
    val newMessages = {
      val newChats = helper.dao1.chats(newDs.uuid)
      helper.dao1.firstMessages(newChats.head.chat, Int.MaxValue)
    }
    assert(newMessages.size === 2)
    assert((newMessages(0), helper.d1root, helper.d1cwd) =~= (helper.d2msgs.bySrcId(3), helper.d2root, helper.d2cwd))
    assert((newMessages(1), helper.d1root, helper.d1cwd) =~= (helper.d2msgs.bySrcId(4), helper.d2root, helper.d2cwd))

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
        helper.d1cwd,
        helper.d2cwd,
        IndexedSeq(
          MessagesMergeOption.Add(
            firstSlaveMsg  = helper.d2msgs.bySrcId(2),
            lastSlaveMsg   = helper.d2msgs.bySrcId(bp1)
          ),
          // Keep, with added messages specified - should act as normal keep
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
    val newDs = helper.merger.merge(keepBothUsers(helper), chatMerges)
    val newMessages = {
      val newChats = helper.dao1.chats(newDs.uuid)
      helper.dao1.firstMessages(newChats.head.chat, Int.MaxValue)
    }
    assert(newMessages.size === msgs.size - 2)
    assert((newMessages(0), helper.d1root, helper.d1cwd) =~= (helper.d2msgs.bySrcId(2), helper.d2root, helper.d2cwd))
    val (expectedMessagesWithRoots, expectedFiles) = {
      val msgs1 = helper.d2msgs.slice(1, bp1).map(m => (m, helper.d2root, helper.d2cwd))
      val msgs2 = helper.d1msgs.slice(bp1, bp2).map(m => (m, helper.d2root, helper.d2cwd))
      val msgs3 = helper.d1msgs.slice(bp2, bp3).map(m => (m, helper.d1root, helper.d1cwd))
      val msgs4 = helper.d2msgs.slice(bp3, maxId - 1).map(m => (m, helper.d2root, helper.d2cwd))
      (
        msgs1 ++ msgs2 ++ msgs3 ++ msgs4,
        (msgsPaths(helper.dao2, helper.d2ds, msgs1.map(_._1))
          ++ msgsPaths(helper.dao1, helper.d1ds, msgs2.map(_._1))
          ++ msgsPaths(helper.dao1, helper.d1ds, msgs3.map(_._1))
          ++ msgsPaths(helper.dao2, helper.d2ds, msgs4.map(_._1)))
      )
    }
    assert(expectedMessagesWithRoots.size === newMessages.size)
    for ((m1withRooot, m2) <- (expectedMessagesWithRoots zip newMessages).par) {
      assert(m1withRooot =~= (m2, helper.d1root, helper.d1cwd), (m1withRooot._1, m2))
    }

    assertFiles(helper.dao1, newDs, expectedFiles)
  }

  test("merge chats - group messages with 'members' field should adapt to user renames applied/skipped") {
    membersTestHelper(
      "Scenario 1: Messages are replaced",
      populateOldMessages = true,
      populateNewMessages = true,
      makeChatMerges = helper => Seq(
        ChatMergeOption.Combine(
          helper.d1cwd, helper.d2cwd,
          IndexedSeq(
            MessagesMergeOption.Replace(
              firstMasterMsg = tag(helper.d1msgs.head),
              lastMasterMsg  = tag(helper.d1msgs.last),
              firstSlaveMsg  = tag(helper.d2msgs.head),
              lastSlaveMsg   = tag(helper.d2msgs.last)
            )
          )
        )
      )
    )

    membersTestHelper(
      "Scenario 2: Messages are kept",
      populateOldMessages = true,
      populateNewMessages = true,
      makeChatMerges = helper => Seq(
        ChatMergeOption.Combine(
          helper.d1cwd, helper.d2cwd,
          IndexedSeq(
            MessagesMergeOption.Keep(
              firstMasterMsg      = tag(helper.d1msgs.head),
              lastMasterMsg       = tag(helper.d1msgs.last),
              firstSlaveMsgOption = Some(tag(helper.d2msgs.head)),
              lastSlaveMsgOption  = Some(tag(helper.d2msgs.last))
            )
          )
        )
      )
    )

    membersTestHelper(
      "Scenario 3: New messages are added",
      populateOldMessages = false,
      populateNewMessages = true,
      makeChatMerges = helper => Seq(
        ChatMergeOption.Combine(
          helper.d1cwd, helper.d2cwd,
          IndexedSeq(
            MessagesMergeOption.Add(
              firstSlaveMsg = tag(helper.d2msgs.head),
              lastSlaveMsg  = tag(helper.d2msgs.last)
            )
          )
        )
      )
    )

    membersTestHelper(
      "Scenario 4: Old messages are kept unrelated to new ones",
      populateOldMessages = true,
      populateNewMessages = false,
      makeChatMerges = helper => Seq(
        ChatMergeOption.Combine(
          helper.d1cwd, helper.d2cwd,
          IndexedSeq(
            MessagesMergeOption.Keep(
              firstMasterMsg      = tag(helper.d1msgs.head),
              lastMasterMsg       = tag(helper.d1msgs.last),
              firstSlaveMsgOption = None,
              lastSlaveMsgOption  = None
            )
          )
        )
      )
    )

    membersTestHelper(
      "Scenario 5: Entire chat is added",
      populateOldMessages = false,
      populateNewMessages = true,
      makeChatMerges = helper => Seq(
        ChatMergeOption.Add(helper.d2cwd)
      )
    )

    membersTestHelper(
      "Scenario 6: Entire chat is kept",
      populateOldMessages = true,
      populateNewMessages = false,
      makeChatMerges = helper => Seq(
        ChatMergeOption.Keep(helper.d1cwd)
      )
    )
  }

  /**
   * Creates 4 users, users 3 and 4 are renamed. Creates one message of each type that has members.
   * <p>
   * In all scenarios, outcome should be the same - group messages should be half-baked.
   */
  def membersTestHelper(clue: String,
                        populateOldMessages: Boolean,
                        populateNewMessages: Boolean,
                        makeChatMerges: H2MergerHelper => Seq[ChatMergeOption]): Unit = {
    def makeMessages(users: Seq[User], groupChatTitle: String) = {
      val members = users.map(_.prettyName)
      val typeds = Seq(
        Message.Typed.Service(Some(MessageServiceGroupCreate(
          title   = groupChatTitle,
          members = members
        ))),
        Message.Typed.Service(Some(MessageServiceGroupInviteMembers(
          members = members
        ))),
        Message.Typed.Service(Some(MessageServiceGroupRemoveMembers(
          members = members
        ))),
        Message.Typed.Service(Some(MessageServiceGroupCall(
          members = members
        )))
      )
      typeds.mapWithIndex((typed, idx) => {
        val text = Seq(RichText.makePlain(s"Message for a group service message ${idx + 1}"))
        Message(
          internalId       = NoInternalId,
          sourceIdOption   = Some((100L + idx).asInstanceOf[MessageSourceId]),
          timestamp        = baseDate.unixTimestamp,
          fromId           = users(idx).id,
          searchableString = Some(makeSearchableString(text, typed)),
          text             = text,
          typed            = typed
        )
      })
    }
    require(populateOldMessages || populateNewMessages, "No messages to populate, that's not how it's supposed to be used")

    val masterUsers = (1 to 4) map (createUser(noUuid, _))
    val slaveUsers  = masterUsers map (u => u.copy(lastNameOption = Some(u.lastNameOption.get + " (new name)")))

    val helper = {
      val groupChatStub = createGroupChat(noUuid, 1, "GC", masterUsers.map(_.id), 9999)

      val masterMsgs = if (populateOldMessages) makeMessages(masterUsers, groupChatStub.nameOrUnnamed) else Seq.empty
      val slaveMsgs  = if (populateNewMessages) makeMessages(slaveUsers, groupChatStub.nameOrUnnamed) else Seq.empty

      val masterGroupChat = groupChatStub.copy(msgCount = masterMsgs.size)
      val slaveGroupChat  = groupChatStub.copy(msgCount = slaveMsgs.size)

      new H2MergerHelper(
        masterUsers, ListMap(masterGroupChat -> masterMsgs),
        slaveUsers,  ListMap(slaveGroupChat  -> slaveMsgs),
        amendMessageWithContent = (_, _, m) => m
      )
    }

    // Users 1/2 are kept, users 3/4 are replaced.
    val usersResolution = Seq(
      UserMergeOption.Keep(helper.d1users.find(_.id == 1).get),
      UserMergeOption.Keep(helper.d1users.find(_.id == 2).get),
      UserMergeOption.Replace(helper.d1users.find(_.id == 3).get, helper.d2users.find(_.id == 3).get),
      UserMergeOption.Replace(helper.d1users.find(_.id == 4).get, helper.d2users.find(_.id == 4).get),
    )

    val expectedMembers = Seq(masterUsers(0), masterUsers(1), slaveUsers(2), slaveUsers(3)).map(_.prettyName)
    assert(expectedMembers !== masterUsers.map(_.prettyName), clue)
    assert(expectedMembers !== slaveUsers.map(_.prettyName), clue)

    val chatMerges = makeChatMerges(helper)
    val newDs = helper.merger.merge(usersResolution, chatMerges)
    val newMessages = {
      val newChats = helper.dao1.chats(newDs.uuid)
      helper.dao1.firstMessages(newChats.head.chat, Int.MaxValue)
    }
    // New messages will be 4 messages no matter what
    def serviceValue(m: Message) =
      m.typed.service.get.get.asMessage.sealedValueOptional
    assert(newMessages.size === (if (populateOldMessages) helper.d1msgs.size else helper.d2msgs.size), clue)
    assert(serviceValue(newMessages(0)).groupCreate.get.members        === expectedMembers, clue)
    assert(serviceValue(newMessages(1)).groupInviteMembers.get.members === expectedMembers, clue)
    assert(serviceValue(newMessages(2)).groupRemoveMembers.get.members === expectedMembers, clue)
    assert(serviceValue(newMessages(3)).groupCall.get.members          === expectedMembers, clue)

    // As a final step, reset DAO to initial state
    freeH2Dao()
    initH2Dao()
  }

  test("merge chats - content should not be discarded") {
    val helper = {
      val msgs       = for (i <- 1 to 4) yield createRegularMessage(i, 1)
      val tmpDir     = makeTempDir().asInstanceOf[DatasetRoot]
      val addContent = addRandomFileContent(tmpDir)(_)

      // New messages: one has no content, the other has content missing
      val masterMsgs = msgs.map(addContent)
      val slaveMsgs  = Seq(msgs(0), addContent(msgs(1)), msgs(2), addContent(msgs(3)))
      slaveMsgs.foreach(_.files(tmpDir).foreach(_.delete()))
      assert(masterMsgs.size === slaveMsgs.size)

      H2MergerHelper.fromMessages(masterMsgs, slaveMsgs, (_, _, m) => m)
    }

    val chatMerges = Seq[ChatMergeOption](
      ChatMergeOption.Combine(
        helper.d1cwd,
        helper.d2cwd,
        IndexedSeq(
          MessagesMergeOption.Keep(
            firstMasterMsg      = tag(helper.d1msgs(0)),
            lastMasterMsg       = tag(helper.d1msgs(3)),
            firstSlaveMsgOption = Some(tag(helper.d2msgs(0))),
            lastSlaveMsgOption  = Some(tag(helper.d2msgs(3)))
          ),
          /* MessagesMergeOption.Replace(
            firstMasterMsg = tag(helper.d1msgs(2)),
            lastMasterMsg  = tag(helper.d1msgs(3)),
            firstSlaveMsg  = tag(helper.d2msgs(2)),
            lastSlaveMsg   = tag(helper.d2msgs(3))
          ) */
        )
      )
    )

    val newDs = helper.merger.merge(keepBothUsers(helper), chatMerges)
    val newMsgs = {
      val newChats = helper.dao1.chats(newDs.uuid)
      helper.dao1.firstMessages(newChats.head.chat, Int.MaxValue)
    }

    def getContentOption(m: Message) = m.typed.regular.get.contentOption
    assert(newMsgs.size === 4)
    assert(getContentOption(newMsgs(0)) === getContentOption(helper.d1msgs(0)))
    assert(getContentOption(newMsgs(1)) === getContentOption(helper.d1msgs(1)))
    assert(getContentOption(newMsgs(2)) === getContentOption(helper.d1msgs(2)))
    assert(getContentOption(newMsgs(3)) === getContentOption(helper.d1msgs(3)))
  }

  //
  // Helpers
  //

  def keepBothUsers(helper: H2MergerHelper): Seq[UserMergeOption] = {
    Seq(UserMergeOption.Keep(helper.d1users.head), UserMergeOption.Keep(helper.d1users(1)))
  }

  def msgsPaths(dao: ChatHistoryDao, ds: Dataset, msgs: Seq[Message]): Set[File] = {
    msgs.flatMap(_.files(dao.datasetRoot(ds.uuid))).toSet
  }

  def assertFiles(dao: ChatHistoryDao, ds: Dataset, expectedFiles: Set[File]): Unit = {
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
    val dsRoot = dao.datasetRoot(ds.uuid)
    for (c <- dao.chats(ds.uuid)) {
      val files = dao.firstMessages(c.chat, c.chat.msgCount).flatMap(_.files(dsRoot))
      for (file <- files) {
        assert(sortedFiles contains file)
      }
    }
  }

  def addRandomFileContent(path: File)(msg: Message): Message =
      msg.copy(typed = msg.typed match {
        case Message.Typed.Regular(msg) =>
          val file1 = createRandomFile(path)
          val file2 = createRandomFile(path)
          val content = ContentFile(
            pathOption          = Some(file1.toRelativePath(path)),
            thumbnailPathOption = Some(file2.toRelativePath(path)),
            mimeTypeOption      = Some("mt"),
            title               = "t",
            performerOption     = Some("p"),
            durationSecOption   = Some(1),
            widthOption         = Some(2),
            heightOption        = Some(3),
          )
          Message.Typed.Regular(msg.copy(contentOption = Some(content)))
        case _ =>
          throw new MatchError("Unexpected message type for " + msg)
      })

  class H2MergerHelper(
      users1: Seq[User],
      chatsWithMsgs1: ListMap[Chat, Seq[Message]],
      users2: Seq[User],
      chatsWithMsgs2: ListMap[Chat, Seq[Message]],
      amendMessageWithContent: ((Boolean, File, Message) => Message) = H2MergerHelper.amendMessageWithContent
  ) {
    val (dao1, d1ds, d1root, d1users, d1cwd, d1msgs) = {
      h2dao.copyAllFrom(createDao(isMaster = true, "One", users1, chatsWithMsgs1, amendMessageWithContent))
      val (ds, root, users, chat, msgs) = getSimpleDaoEntities(h2dao)
      (h2dao, ds, root, users, chat, msgs)
    }
    val (dao2, d2ds, d2root, d2users, d2cwd, d2msgs) = {
      val dao                           = createDao(isMaster = false, "Two", users2, chatsWithMsgs2, amendMessageWithContent)
      val (ds, root, users, chat, msgs) = getSimpleDaoEntities(dao)
      (dao, ds, root, users, chat, msgs)
    }

    def merger: DatasetMerger =
      new DatasetMerger(dao1, d1ds, dao2, d2ds)
  }

  object H2MergerHelper {
    def fromUsers(users1: Seq[User], users2: Seq[User]): H2MergerHelper = {
      new H2MergerHelper(
        users1,
        ListMap(createGroupChat(noUuid, 1, "One", users1.map(_.id), 0) -> Seq.empty[Message]),
        users2,
        ListMap(createGroupChat(noUuid, 2, "Two", users2.map(_.id), 0) -> Seq.empty[Message])
      )
    }

    def fromMessages(
      msgs1: Seq[Message],
      msgs2: Seq[Message],
      amendMessagesWithContent: ((Boolean, File, Message) => Message) = amendMessageWithContent
    ): H2MergerHelper = {
      val users1 = (1 to math.max(msgs1.map(_.fromId).max.toInt, 2)).map(i => createUser(noUuid, i))
      val users2 = (1 to math.max(msgs2.map(_.fromId).max.toInt, 2)).map(i => createUser(noUuid, i))
      val chatMsgs1 = ListMap(createGroupChat(noUuid, 1, "One", users1.map(_.id), msgs1.size) -> msgs1)
      val chatMsgs2 = ListMap(createGroupChat(noUuid, 2, "Two", users1.map(_.id), msgs2.size) -> msgs2)
      new H2MergerHelper(users1, chatMsgs1, users2, chatMsgs2, amendMessagesWithContent)
    }

    private def amendMessageWithContent(isLeft: Boolean, path: File, msg: Message): Message =
      addRandomFileContent(path)(msg)
  }
}
