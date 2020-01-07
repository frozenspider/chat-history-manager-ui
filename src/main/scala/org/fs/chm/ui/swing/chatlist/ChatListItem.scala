package org.fs.chm.ui.swing.chatlist

import java.awt.Color
import java.awt.{ Container => AwtContainer }

import scala.swing.BorderPanel.Position._
import scala.swing._
import scala.swing.event._

import javax.swing.SwingUtilities
import javax.swing.border.EmptyBorder
import javax.swing.border.LineBorder
import org.apache.commons.lang3.StringEscapeUtils
import org.fs.chm.dao.ChatType._
import org.fs.chm.dao.Content
import org.fs.chm.dao.Message
import org.fs.chm.ui.swing.general.ChatWithDao
import org.fs.chm.ui.swing.general.SwingUtils._

class ChatListItem(
    cc: ChatWithDao,
    callbacks: ChatListSelectionCallbacks,
) extends BorderPanel {
  val labelPreferredWidth = 200 // TODO: Remove
  val labelBorderWidth    = 3

  val interlocutors = cc.dao.interlocutors(cc.chat)

  val popupMenu = new PopupMenu {
    contents += menuItem("Details")(showDetailsPopup())
  }

  {
    val emptyBorder = new EmptyBorder(labelBorderWidth, labelBorderWidth, labelBorderWidth, labelBorderWidth)

    layout(new BorderPanel {
      // Name
      val nameString = cc.chat.nameOption getOrElse "<Unnamed>"
      val nameLabel = new Label(
        s"""<html><p style="text-align: left; width: ${labelPreferredWidth}px;">"""
          + StringEscapeUtils.escapeHtml4(nameString)
          + "</p></html>")
      nameLabel.border = emptyBorder
      layout(nameLabel) = North

      // Last message
      val lastMsgString = cc.dao.lastMessages(cc.chat, 1) match {
        case x if x.isEmpty => "<No messages>"
        case msg +: _       => simpleRenderMsg(msg)
      }
      val msgLabel = new Label(lastMsgString)
      msgLabel.horizontalAlignment = Alignment.Left
      msgLabel.foreground = new Color(0, 0, 0, 100)
      msgLabel.preferredWidth = labelPreferredWidth
      msgLabel.border = emptyBorder
      layout(msgLabel) = Center

      opaque = false
    }) = Center

    // Type
    val tpeString = cc.chat.tpe match {
      case Personal     => ""
      case PrivateGroup => "(" + interlocutors.size + ")"
    }
    val tpeLabel = new Label(tpeString)
    tpeLabel.preferredWidth = 30
    tpeLabel.verticalAlignment = Alignment.Center
    layout(tpeLabel) = East

    // Reactions
    listenTo(this, this.mouse.clicks)
    reactions += {
      case e @ MouseReleased(_, __, _, _, _) if SwingUtilities.isLeftMouseButton(e.peer) && enabled =>
        select()
      case e @ MouseReleased(_, pt, _, _, _) if SwingUtilities.isRightMouseButton(e.peer) && enabled =>
        popupMenu.show(this, pt.x, pt.y)
    }

    maximumSize = new Dimension(Int.MaxValue, preferredSize.height)
    markDeselected()
  }

  private def select(): Unit = {
    ChatListItem.Lock.synchronized {
      ChatListItem.SelectedOption foreach (_.markDeselected())
      ChatListItem.SelectedOption = Some(this)
      markSelected()
    }
    callbacks.chatSelected(cc)
  }

  override def enabled_=(b: Boolean): Unit = {
    super.enabled_=(b)
    def changeClickableRecursive(c: AwtContainer): Unit = {
      c.setEnabled(enabled)
      c.getComponents foreach {
        case c: AwtContainer => changeClickableRecursive(c)
        case _               => //NOOP
      }
    }
    changeClickableRecursive(peer)
  }

  private def simpleRenderMsg(msg: Message): String = {
    val prefix =
      if (interlocutors.size == 2 && msg.fromId == interlocutors(1).id) ""
      else (msg.fromNameOption.getOrElse("<Unnamed>") + ": ")
    val text = msg match {
      case msg: Message.Regular =>
        (msg.textOption, msg.contentOption) match {
          case (None, Some(s: Content.Sticker))       => s.emojiOption.map(_ + " ").getOrElse("") + "(sticker)"
          case (None, Some(_: Content.Photo))         => "(photo)"
          case (None, Some(_: Content.VoiceMsg))      => "(voice)"
          case (None, Some(_: Content.VideoMsg))      => "(video)"
          case (None, Some(_: Content.Animation))     => "(animation)"
          case (None, Some(_: Content.File))          => "(file)"
          case (None, Some(_: Content.Location))      => "(location)"
          case (None, Some(_: Content.Poll))          => "(poll)"
          case (None, Some(_: Content.SharedContact)) => "(contact)"
          case (Some(_), _)                           => msg.plainSearchableString
          case (None, None)                           => "(???)" // We don't really expect this
        }
      case _: Message.Service.PhoneCall           => "(phone call)"
      case _: Message.Service.PinMessage          => "(message pinned)"
      case _: Message.Service.ClearHistory        => "(history cleared)"
      case _: Message.Service.EditPhoto           => "(photo changed)"
      case _: Message.Service.Group.Create        => "(group created)"
      case _: Message.Service.Group.InviteMembers => "(invited members)"
      case _: Message.Service.Group.RemoveMembers => "(removed members)"
    }
    prefix + text.take(50)
  }

  private def markSelected(): Unit = {
    border = new LineBorder(Color.BLACK, 1)
    background = Color.LIGHT_GRAY
  }

  private def markDeselected(): Unit = {
    border = new LineBorder(Color.GRAY, 1)
    background = Color.WHITE
  }

  private def showDetailsPopup(): Unit = {
    println("There will be popup!")
  }
}

private object ChatListItem {
  private val Lock = new Object
  private var SelectedOption: Option[ChatListItem] = None
}
