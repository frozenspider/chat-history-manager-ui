package org.fs.chm.loader.telegram

import java.io.File
import java.util.UUID

import org.fs.chm.TestHelper
import org.fs.chm.dao.Entities._
import org.fs.chm.protobuf.Content
import org.fs.chm.protobuf.ContentLocation
import org.fs.chm.protobuf.Message
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
      assert(regularMsg.content === Some(Content(Content.Val.Location(ContentLocation(
        title       = Some("Çıralı Plajı"),
        address     = Some("Olympos"),
        latStr      = "36.417978",
        lonStr      = "30.482614",
        durationSec = None
      )))))
    }
  }

  //
  // Helpers
  //

  private def expectedSelf(dsUuid: UUID) = User(
    dsUuid            = dsUuid,
    id                = 11111111L,
    firstNameOption   = Some("Aaaaa"),
    lastNameOption    = Some("Aaaaaaaaaaa"),
    usernameOption    = Some("@frozenspider"),
    phoneNumberOption = Some("+998 91 1234567")
  )
}
