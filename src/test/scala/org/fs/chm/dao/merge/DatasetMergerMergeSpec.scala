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
    val chatMerges = ListMap.empty[ChatMergeOption, Seq[MessagesMergeOption]]

    val newDs = helper.merger.merge(
      Seq(
        UserMergeOption.Retain(helper.d1users.byId(1)),
        UserMergeOption.Add(helper.d2users.byId(2)),
        UserMergeOption.Replace(helper.d1users.byId(3), helper.d2users.byId(3))
      ), chatMerges
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
    val chatMerges = ListMap[ChatMergeOption, Seq[MessagesMergeOption]](
      ChatMergeOption.Retain(helper.d1chat) -> Seq(
        MessagesMergeOption.Retain(
          firstMasterMsg      = helper.d1msgs.bySrcId(1),
          lastMasterMsg       = helper.d1msgs.bySrcId(1),
          firstSlaveMsgOption = None,
          lastSlaveMsgOption  = None
        )
      )
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

  /**
   * {{{
   * Master messages -       3  4
   * Slave messages  - 1* 2* 3* 4* 5*
   * }}}
   * Retain 3, 4
   */
  test("merge chats - retain two message, dicard the rest") {
    val msgs   = for (i <- 1 to 5) yield createRegularMessage(i, 1)
    val msgsA  = msgs.filter(Seq(3, 4) contains _.sourceIdOption.get)
    val msgsB  = changedMessages(msgs, _ => true)
    val helper = H2MergerHelper.fromMessages(msgsA, msgsB)
    val chatMerges = ListMap[ChatMergeOption, Seq[MessagesMergeOption]](
      ChatMergeOption.Retain(helper.d1chat) -> Seq(
        MessagesMergeOption.Retain(
          firstMasterMsg      = helper.d1msgs.bySrcId(3),
          lastMasterMsg       = helper.d1msgs.bySrcId(4),
          firstSlaveMsgOption = Some(helper.d2msgs.bySrcId(3)),
          lastSlaveMsgOption  = Some(helper.d2msgs.bySrcId(4))
        )
      )
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
