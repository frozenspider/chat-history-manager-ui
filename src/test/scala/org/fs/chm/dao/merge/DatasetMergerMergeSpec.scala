package org.fs.chm.dao.merge

import scala.collection.immutable.ListMap

import org.fs.chm.TestHelper
import org.fs.chm.WithH2Dao
import org.fs.chm.dao._
import org.fs.chm.dao.merge.DatasetMerger._
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
        UserMergeOption.Retain(helper.d1users.byId(1)),
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

  test("merge chats - retain single message, basic checks") {
    val msgs   = Seq(createRegularMessage(1, 1))
    val helper = H2MergerHelper.fromMessages(msgs, msgs)
    val chatMerges = Seq[ChatMergeOption](
      ChatMergeOption.Retain(helper.d1chat)
    )
    assert(helper.dao1.datasets.size === 1)
    val newDs = helper.merger.merge(retainSignleUser(helper), chatMerges)
    assert(helper.dao1.datasets.size === 2)
    val newChats = helper.dao1.chats(newDs.uuid)
    assert(helper.dao1.chats(newDs.uuid).size === 1)
    val newMessages = helper.dao1.firstMessages(newChats.head, Int.MaxValue)
    assert(newMessages.size === 1)
    assert(newMessages.head =~= helper.d1msgs.head)
  }

  test("merge chats - retain two messages") {
    val msgs   = for (i <- 1 to 6) yield createRegularMessage(i, 1)
    val msgsA  = msgs.filter(Seq(3, 4) contains _.sourceIdOption.get)
    val msgsB  = changedMessages(msgs, _ => true)
    val helper = H2MergerHelper.fromMessages(msgsA, msgsB)
    val chatMerges = Seq[ChatMergeOption](
      ChatMergeOption.Retain(helper.d1chat)
    )
    val newMessages = {
      val newDs    = helper.merger.merge(retainSignleUser(helper), chatMerges)
      val newChats = helper.dao1.chats(newDs.uuid)
      helper.dao1.firstMessages(newChats.head, Int.MaxValue)
    }
    assert(newMessages.size === helper.d1msgs.size)
    for ((nm, om) <- (newMessages zip helper.d1msgs)) {
      assert(nm =~= om)
    }
  }

  test("merge chats - add two messages") {
    val msgs   = for (i <- 1 to 5) yield createRegularMessage(i, 1)
    val msgsA  = msgs
    val msgsB  = changedMessages(msgs.filter(Seq(3, 4) contains _.sourceIdOption.get), _ => true)
    val helper = H2MergerHelper.fromMessages(msgsA, msgsB)
    val chatMerges = Seq[ChatMergeOption](
      ChatMergeOption.Add(helper.d2chat)
    )
    val newMessages = {
      val newDs    = helper.merger.merge(retainSignleUser(helper), chatMerges)
      val newChats = helper.dao1.chats(newDs.uuid)
      helper.dao1.firstMessages(newChats.head, Int.MaxValue)
    }
    assert(newMessages.size === 2)
    assert(newMessages(0) =~= helper.d2msgs.bySrcId(3))
    assert(newMessages(1) =~= helper.d2msgs.bySrcId(4))
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
    val newMessages = {
      val newDs    = helper.merger.merge(retainSignleUser(helper), chatMerges)
      val newChats = helper.dao1.chats(newDs.uuid)
      helper.dao1.firstMessages(newChats.head, Int.MaxValue)
    }
    assert(newMessages.size === 2)
    assert(newMessages(0) =~= helper.d2msgs.bySrcId(3))
    assert(newMessages(1) =~= helper.d2msgs.bySrcId(4))
  }

  /**
   * {{{
   * Master messages - 1  2  ... max
   * Slave messages  - 1* 2* ... max*
   * }}}
   * A(2, max/3), K(max/3 + 1, max*2/3), R(max*2/3 + 1, max - 1)
   */
  test("merge chats - combine, each") {
    val msgs       = for (i <- 1 to maxId) yield createRegularMessage(i, 1)
    val msgsA      = msgs
    val msgsB      = changedMessages(msgs, _ => true)
    val helper     = H2MergerHelper.fromMessages(msgsA, msgsB)
    val (bp1, bp2) = (maxId / 3, maxId * 2 / 3)
    val chatMerges = Seq[ChatMergeOption](
      ChatMergeOption.Combine(
        helper.d1chat,
        helper.d2chat,
        IndexedSeq(
          MessagesMergeOption.Add(
            firstSlaveMsg  = helper.d2msgs.bySrcId(2),
            lastSlaveMsg   = helper.d2msgs.bySrcId(bp1)
          ),
          MessagesMergeOption.Retain(
            firstMasterMsg      = helper.d1msgs.bySrcId(bp1 + 1),
            lastMasterMsg       = helper.d1msgs.bySrcId(bp2),
            firstSlaveMsgOption = Some(helper.d2msgs.bySrcId(bp1 + 1)),
            lastSlaveMsgOption  = Some(helper.d2msgs.bySrcId(bp2))
          ),
          MessagesMergeOption.Replace(
            firstMasterMsg = helper.d1msgs.bySrcId(bp2 + 1),
            lastMasterMsg  = helper.d1msgs.bySrcId(maxId - 1),
            firstSlaveMsg  = helper.d2msgs.bySrcId(bp2 + 1),
            lastSlaveMsg   = helper.d2msgs.bySrcId(maxId - 1)
          )
        )
      )
    )
    val newMessages = {
      val newDs    = helper.merger.merge(retainSignleUser(helper), chatMerges)
      val newChats = helper.dao1.chats(newDs.uuid)
      helper.dao1.firstMessages(newChats.head, Int.MaxValue)
    }
    assert(newMessages.size === msgs.size - 2)
    assert(newMessages(0) =~= helper.d2msgs.bySrcId(2))
    val expected = helper.d2msgs.slice(1, bp1) ++ helper.d1msgs.slice(bp1, bp2) ++ helper.d2msgs.slice(bp2, maxId - 1)
    assert(expected.size === newMessages.size)
    for ((m1, m2) <- (expected zip newMessages)) {
      assert(m1 =~= m2, (m1, m2))
    }
  }

  //
  // Helpers
  //

  def retainSignleUser(helper: H2MergerHelper): Seq[UserMergeOption] = {
    Seq(UserMergeOption.Retain(helper.d1users.head))
  }

  class H2MergerHelper(
      users1: Seq[User],
      chatsWithMsgs1: ListMap[Chat, Seq[Message]],
      users2: Seq[User],
      chatsWithMsgs2: ListMap[Chat, Seq[Message]]
  ) {
    val (dao1, d1ds, d1users, d1chat, d1msgs) = {
      val dao = createDao("One", users1, chatsWithMsgs1)
      h2dao.copyAllFrom(dao)
      val (ds, users, chat, msgs) = getSimpleDaoEntities(dao)
      (h2dao, ds, users, chat, msgs)
    }
    val (dao2, d2ds, d2users, d2chat, d2msgs) = {
      val dao                     = createDao("Two", users2, chatsWithMsgs2)
      val (ds, users, chat, msgs) = getSimpleDaoEntities(dao)
      (dao, ds, users, chat, msgs)
    }

    def merger: DatasetMerger =
      new DatasetMerger(dao1, d1ds, dao2, d2ds)
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
