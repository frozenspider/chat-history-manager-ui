package org.fs.chm.loader

import java.io.File

import org.fs.chm.TestHelper
import org.fs.chm.dao._
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfter
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.slf4s.Logging

@RunWith(classOf[org.scalatestplus.junit.JUnitRunner])
class GTS5610DataLoaderSpec //
    extends AnyFunSuite
    with TestHelper
    with Logging
    with BeforeAndAfter
    with BeforeAndAfterAll {

  val loader = new GTS5610DataLoader
  val dir    = new File(resourcesFolder, "gts5610")

  test("loading works") {
    val dao = loader.loadData(dir)

    assert(dao.datasets.size === 1)
    val ds = dao.datasets.head

    val users = dao.users(ds.uuid)
    assert(users.size === 4)
    assert(dao.myself(ds.uuid).firstNameOption === Some("Me"))

    val chats = dao.chats(ds.uuid).map(_.chat)
    assert(chats.size === 3)

    // Someguy
    {
      val user     = users.find(_.firstNameOption contains "someguy") getOrElse fail("User not found!")
      val chat     = chats.find(_.nameOption == user.firstNameOption) getOrElse fail("Chat not found!")
      val messages = dao.firstMessages(chat, 99999)
      assert(messages.size === 2)

      assertMsg(messages(0))(user.id, "2014-10-28 18:08:03", "Hi there, when will you be online?")
      assertMsg(messages(1))(user.id, "2015-03-06 18:16:08", "Talking to you here!11")
    }

    // +12345
    {
      val user     = users.find(_.phoneNumberOption contains "+12345") getOrElse fail("User not found!")
      val chat     = chats.find(_.nameOption == user.firstNameOption) getOrElse fail("Chat not found!")
      val messages = dao.firstMessages(chat, 99999)
      assert(messages.size === 1)

      assertMsg(messages(0))(user.id, "2016-12-31 22:23:12", "С Новым Годом!\nС праздником зимнего покрова!")
    }

    // Орк
    // (Cyrillic names are fucked up in the vcards)
    {
      val user     = users.find(_.firstNameOption contains "Орк") getOrElse fail("User not found!")
      val chat     = chats.find(_.nameOption == user.firstNameOption) getOrElse fail("Chat not found!")
      val messages = dao.firstMessages(chat, 99999)
      assert(user.phoneNumberOption === Some("911"))
      assert(messages.size === 1)

      assertMsg(messages(0))(user.id, "2016-01-01 00:30:47", "Lok'tar ogar!")
    }
  }

  def assertMsg(m: Message)(id: Long, dtStr: String, text: String) = {
    assert(m.fromId === id)
    assert(m.time === dt(dtStr))
    assert(txt(m) === text)
  }

  def txt(m: Message): String =
    m.textOption.map { t =>
      assert(t.components.size === 1)
      assert(t.components.head.isInstanceOf[RichText.Plain])
      t.components.head.text
    } getOrElse fail(s"No text for message $m!")
}
