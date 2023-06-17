package org.fs.chm.loader.telegram

import java.io.File

import org.fs.chm.TestHelper
import org.fs.chm.dao.Entities._
import org.fs.chm.protobuf._
import org.fs.chm.utility.LangUtils._
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.slf4s.Logging

@RunWith(classOf[org.scalatestplus.junit.JUnitRunner])
class TelegramFullDataLoaderSpec //
    extends AnyFunSuite
    with TestHelper
    with Logging {

  val loader = new TelegramFullDataLoader

  test("loading @ 2020-01") {
    val dao = loader.loadData(new File(resourcesFolder, "telegram_2020-01"))
    assert(dao.datasets.size === 1)
    val ds = dao.datasets.head
    val self = dao.myself(ds.uuid)
    assert(self === expectedSelf(ds.uuid))
    assert(dao.chats(ds.uuid).size === 4)

    // "Ordered" chat
    {
      val cwm = dao.chats(ds.uuid).find(_.chat.id == 4321012345L).get
      assert(cwm.chat.tpe === ChatType.Personal)
      assert(cwm.members.size === 2)
      val member = User(
        dsUuid            = ds.uuid,
        id                = 32507588L,
        firstNameOption   = None,
        lastNameOption    = None,
        usernameOption    = None,
        phoneNumberOption = None
      )
      assert(cwm.members.head === self)
      assert(cwm.members.last === member)
      val msgs = dao.lastMessages(cwm.chat, 100500)
      assert(msgs.size === 5)
      assert(msgs.forall(_.typed.isRegular))
      assert(msgs.map(_.fromId).distinct === Seq(member.id))
      assert(
        msgs.flatMap(_.text) === Seq(
          "Message from null-names contact",
          "These messages...",
          "...have the same timestamp...",
          "...but different IDs...",
          "...and we expect order to be preserved."
        ).map(s => RichText.makePlain(s))
      )
    }
  }

  test("loading @ 2021-05") {
    val dao = loader.loadData(new File(resourcesFolder, "telegram_2021-05"))
    assert(dao.datasets.size === 1)
    val ds = dao.datasets.head
    val self = dao.myself(ds.uuid)
    assert(self === expectedSelf(ds.uuid))
    assert(dao.chats(ds.uuid).size === 1)

    // Group chat
    {
      val cwm = dao.chats(ds.uuid).find(_.chat.id == 8713057715L).get // Chat ID is shifted by 2^33
      assert(cwm.chat.nameOption === Some("My Group"))
      assert(cwm.chat.tpe === ChatType.PrivateGroup)
      // We only know of myself + two users (other's IDs aren't known), as well as service "member".
      assert(cwm.members.size === 4)
      val serviceMember = User(
        dsUuid            = ds.uuid,
        id                = 8112233L,
        firstNameOption   = Some("My Old Group"),
        lastNameOption    = None,
        usernameOption    = None,
        phoneNumberOption = None
      )
      val member1 = User(
        dsUuid            = ds.uuid,
        id                = 22222222L,
        firstNameOption   = Some("Wwwwww"),
        lastNameOption    = Some("Www"),
        usernameOption    = None,
        phoneNumberOption = Some("+998 90 9998877") // Taken from contacts list
      )
      val member2 = User(
        dsUuid            = ds.uuid,
        id                = 44444444L,
        firstNameOption   = Some("Eeeee"),
        lastNameOption    = Some("Eeeeeeeeee"),
        usernameOption    = None,
        phoneNumberOption = Some("+7 916 337 53 10") // Taken from contacts list
      )
      assert(cwm.members.head === self)
      assert(cwm.members(1) === serviceMember)
      assert(cwm.members(2) === member1)
      assert(cwm.members(3) === member2)
      val msgs = dao.lastMessages(cwm.chat, 100500)
      assert(msgs.size === 3)
      val typed = msgs.map(_.typed)
      assert(typed(0).service.get.`val`.isGroupCreate)
      assert(typed(1).service.get.`val`.isGroupMigrateFrom)
      assert(typed(2).isRegular)
    }
  }

test("loading @ 2021-06 (supergroup and added user)") {
  val dao = loader.loadData(new File(resourcesFolder, "telegram_2021-06_supergroup"))
  assert(dao.datasets.size === 1)
  val ds = dao.datasets.head
  val self = dao.myself(ds.uuid)
  assert(self === expectedSelf(ds.uuid))

  val u222222222 = User(
    dsUuid            = ds.uuid,
    id                = 222222222L,
    firstNameOption   = Some("Sssss Pppppp"),
    lastNameOption    = None,
    usernameOption    = None,
    phoneNumberOption = None
  )
  val u333333333 = User(
    dsUuid            = ds.uuid,
    id                = 333333333L,
    firstNameOption   = Some("Tttttt Yyyyyyy"),
    lastNameOption    = None,
    usernameOption    = None,
    phoneNumberOption = None
  )
  val u444444444 = User(
    dsUuid            = ds.uuid,
    id                = 444444444L,
    firstNameOption   = Some("Vvvvvvvv Bbbbbbb"),
    lastNameOption    = None,
    usernameOption    = None,
    phoneNumberOption = None
  )

  {
    val sortedUsers = dao.users(ds.uuid).sortBy(_.id)
    assert(sortedUsers.head === self)
    assert(sortedUsers(1) === u222222222)
    assert(sortedUsers(2) === u333333333)
    assert(sortedUsers(3) === u444444444)
  }

  assert(dao.chats(ds.uuid).size === 1)

  // Group chat
  {
    val cwm = dao.chats(ds.uuid).find(_.chat.id == 1234567890L + 8589934592L).get // Chat ID is shifted by 2^33
    assert(cwm.chat.nameOption === Some("My Supergroup"))
    assert(cwm.chat.tpe === ChatType.PrivateGroup)

    {
      // All users are taken from chat itself
      val sortedMember = cwm.members.sortBy(_.id)
      assert(sortedMember.size === 4)
      assert(sortedMember.head === self)
      assert(sortedMember(1) === u222222222)
      assert(sortedMember(2) === u333333333)
      assert(sortedMember(3) === u444444444)
    }

    val msgs = dao.lastMessages(cwm.chat, 100500)
    assert(msgs.size === 3)
    assert(msgs.head === Message(
      internalId       = 1,
      sourceIdOption   = Some(-999681092),
      timestamp        = dt("2020-12-22 23:11:21").unixTimestamp,
      fromId           = u222222222.id,
      text             = Seq.empty,
      searchableString = Some(u444444444.firstNameOption.get),
      typed            = Message.Typed.Service(MessageService(
        MessageService.Val.GroupInviteMembers(MessageServiceGroupInviteMembers(
          members = Seq(u444444444.firstNameOption.get)
        ))
      ))
    ))
    assert(msgs(1) === Message(
      internalId       = 2,
      sourceIdOption   = Some(-999681090),
      timestamp        = dt("2020-12-22 23:12:09").unixTimestamp,
      fromId           = u333333333.id,
      text             = Seq(RichText.makePlain("Message text with emoji 🙂")),
      searchableString = Some("Message text with emoji 🙂"),
      typed            = Message.Typed.Regular(MessageRegular(
        editTimestampOption    = None,
        forwardFromNameOption  = None,
        replyToMessageIdOption = None,
        contentOption          = None
      ))
    ))
    assert(msgs(2) === Message(
      internalId       = 3,
      sourceIdOption   = Some(-999681087),
      timestamp        = dt("2020-12-22 23:12:51").unixTimestamp,
      fromId           = u444444444.id,
      text             = Seq(RichText.makePlain("Message from an added user")),
      searchableString = Some("Message from an added user"),
      typed            = Message.Typed.Regular(MessageRegular(
        editTimestampOption    = None,
        forwardFromNameOption  = None,
        replyToMessageIdOption = None,
        contentOption          = None
      ))
    ))
  }
}

  test("loading @ 2021-07 (group calls)") {
    val dao = loader.loadData(new File(resourcesFolder, "telegram_2021-07"))
    assert(dao.datasets.size === 1)
    val ds = dao.datasets.head
    val self = dao.myself(ds.uuid)
    assert(self === expectedSelf(ds.uuid))
    assert(dao.chats(ds.uuid).size === 1)

    // Group chat
    {
      val cwm = dao.chats(ds.uuid).find(_.chat.id == 8713057715L).get // Chat ID is shifted by 2^33
      assert(cwm.chat.nameOption === Some("My Group"))
      assert(cwm.chat.tpe === ChatType.PrivateGroup)
      // We only know of myself + one users (ID of one more isn't known).
      assert(cwm.members.size === 2)
      val member = User(
        dsUuid            = ds.uuid,
        id                = 44444444L,
        firstNameOption   = Some("Eeeee"),
        lastNameOption    = Some("Eeeeeeeeee"),
        usernameOption    = None,
        phoneNumberOption = Some("+7 916 337 53 10") // Taken from contacts list
      )
      assert(cwm.members.head === self)
      assert(cwm.members(1) === member)
      val msgs = dao.lastMessages(cwm.chat, 100500)
      assert(msgs.size === 2)
      assert(msgs(0).typed.service.flatMap(_.`val`.groupCall).isDefined)
      assert(msgs(1).typed.service.flatMap(_.`val`.groupCall).isDefined)
      assert(
        msgs.map(_.typed.service.get.`val`.groupCall.get.members) === Seq(
          Seq("Www Wwwwww"),
          Seq("Myself")
        )
      )
    }
  }

  ignore("loading @ 2021-12 (named locations)") {
    val dao = loader.loadData(new File(resourcesFolder, "telegram_2021-12"))
    assert(dao.datasets.size === 1)
    val ds = dao.datasets.head
    val self = dao.myself(ds.uuid)
    assert(self === expectedSelf(ds.uuid))
    assert(dao.chats(ds.uuid).size === 1)

    // Group chat
    {
      val cwm = dao.chats(ds.uuid).find(_.chat.id == 8713057715L).get // Chat ID is shifted by 2^33
      assert(cwm.chat.nameOption === Some("My Group"))
      assert(cwm.chat.tpe === ChatType.PrivateGroup)
      // We only know of myself + one users (ID of one more isn't known).
      assert(cwm.members.size === 2)
      val member = User(
        dsUuid = ds.uuid,
        id = 44444444L,
        firstNameOption = Some("Bbbbbbbb Bbbbbbb"),
        lastNameOption = None, // No contact, could not distinguish first name from last.
        usernameOption = None,
        phoneNumberOption = None
      )
      assert(cwm.members.head === self)
      assert(cwm.members(1) === member)
      val msgs = dao.lastMessages(cwm.chat, 100500)
      assert(msgs.size === 1)
      assert(msgs.head.typed.isRegular)
      val regularMsg = msgs.head.typed.regular.get
      assert(regularMsg.contentOption === Some(Content(Content.Val.Location(ContentLocation(
        titleOption       = Some("Çıralı Plajı"),
        addressOption     = Some("Olympos"),
        latStr            = "36.417978",
        lonStr            = "30.482614",
        durationSecOption = None
      )))))
    }
  }

  //
  // Helpers
  //

  private def expectedSelf(dsUuid: PbUuid) = User(
    dsUuid            = dsUuid,
    id                = 11111111L,
    firstNameOption   = Some("Aaaaa"),
    lastNameOption    = Some("Aaaaaaaaaaa"),
    usernameOption    = Some("@frozenspider"),
    phoneNumberOption = Some("+998 91 1234567")
  )
}
