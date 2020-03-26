package org.fs.chm.ui.swing.list.chat

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
import org.fs.chm.ui.swing.list.DaoItem
import org.fs.chm.ui.swing.general.ChatWithDao
import org.fs.chm.ui.swing.general.SwingUtils._
import org.fs.chm.utility.EntityUtils

class ChatListItem(
    cc: ChatWithDao,
    selectionGroupOption: Option[ChatListItemSelectionGroup],
    callbacksOption: Option[ChatListSelectionCallbacks]
) extends BorderPanel { self =>
  private val labelPreferredWidth = DaoItem.PanelWidth - 100 // TODO: Remove

  val chat = cc.chat

  private val labelBorderWidth = 3

  private val interlocutors = cc.dao.interlocutors(cc.chat)

  private val popupMenu = new PopupMenu {
    contents += menuItem("Details")(showDetailsPopup())
    contents += new Separator()
    contents += menuItem("Delete", enabled = callbacksOption.nonEmpty && cc.dao.isMutable)(showDeletePopup())
  }

  private var _activeColor:   Color = Color.LIGHT_GRAY
  private var _inactiveColor: Color = Color.WHITE

  {
    val emptyBorder = new EmptyBorder(labelBorderWidth, labelBorderWidth, labelBorderWidth, labelBorderWidth)

    layout(new BorderPanel {
      // Name
      val nameString = EntityUtils.getOrUnnamed(cc.chat.nameOption)
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
      msgLabel.foreground          = new Color(0, 0, 0, 100)
      msgLabel.preferredWidth      = labelPreferredWidth
      msgLabel.border              = emptyBorder
      layout(msgLabel) = Center

      opaque = false
    }) = Center

    // Type
    val tpeString = cc.chat.tpe match {
      case Personal     => ""
      case PrivateGroup => "(" + interlocutors.size + ")"
    }
    val tpeLabel = new Label(tpeString)
    tpeLabel.preferredWidth    = 30
    tpeLabel.verticalAlignment = Alignment.Center
    layout(tpeLabel) = East

    // Reactions
    listenTo(this, this.mouse.clicks)
    reactions += {
      case e @ MouseReleased(_, __, _, _, _) if SwingUtilities.isLeftMouseButton(e.peer) && enabled =>
        select()
      case e @ MouseReleased(src, pt, _, _, _) if SwingUtilities.isRightMouseButton(e.peer) && enabled =>
        popupMenu.show(src, pt.x, pt.y)
    }

    maximumSize = new Dimension(Int.MaxValue, preferredSize.height)
    markDeselected()
    selectionGroupOption foreach (_.add(this))
  }

  def activeColor:               Color = _activeColor
  def activeColor_=(c: Color):   Unit  = { _activeColor = c; }
  def inactiveColor:             Color = _inactiveColor
  def inactiveColor_=(c: Color): Unit  = _inactiveColor = c

  def select(): Unit = {
    markSelected()
    selectionGroupOption foreach (_.deselectOthers(this))
    callbacksOption foreach (_.chatSelected(cc))
  }

  def markSelected(): Unit = {
    border     = new LineBorder(Color.BLACK, 1)
    background = _activeColor
  }

  def markDeselected(): Unit = {
    border     = new LineBorder(Color.GRAY, 1)
    background = _inactiveColor
  }

  private def showDetailsPopup(): Unit = {
    Dialog.showMessage(
      title       = "Chat Details",
      message     = new ChatDetailsPane(cc).peer,
      messageType = Dialog.Message.Plain
    )
  }

  private def showDeletePopup(): Unit = {
    Dialog.showConfirmation(
      title   = "Deleting Chat",
      message = s"Are you sure you want to delete a chat '${EntityUtils.getOrUnnamed(cc.chat.nameOption)}'?"
    ) match {
      case Dialog.Result.Yes => callbacksOption.get.deleteChat(cc)
      case _                 => // NOOP
    }
  }

  override def enabled_=(b: Boolean): Unit = {
    super.enabled_=(b)
    def changeClickableRecursive(c: AwtContainer): Unit = {
      c.setEnabled(enabled)
      c.getComponents foreach {
        case c: AwtContainer => changeClickableRecursive(c)
        case _               => // NOOP
      }
    }
    changeClickableRecursive(peer)
  }

  private def simpleRenderMsg(msg: Message): String = {
    val prefix =
      if (interlocutors.size == 2 && msg.fromId == interlocutors(1).id) ""
      else {
        // Avoid querying DB if possible
        val fromNameOption =
          (interlocutors find (_.id == msg.fromId))
            .orElse(cc.dao.userOption(cc.dsUuid, msg.fromId))
            .flatMap(_.prettyNameOption)
        (EntityUtils.getOrUnnamed(fromNameOption) + ": ")
      }
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
}

class ChatListItemSelectionGroup {
  private val lock:           AnyRef               = new AnyRef
  private var selectedOption: Option[ChatListItem] = None
  private var items:          Seq[ChatListItem]    = Seq.empty

  def add(item: ChatListItem): Unit = {
    items = items :+ item
  }

  def deselectOthers(item: ChatListItem): Unit =
    lock.synchronized {
      selectedOption = Some(item)
      for (item2 <- items if item2 != item) {
        item2.markDeselected()
      }
    }

  def deselectAll(): Unit =
    lock.synchronized {
      selectedOption = None
      items map (_.markDeselected())
    }
}
