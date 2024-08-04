package org.fs.chm.ui.swing.list.chat

import java.awt.Color
import java.awt.{Container => AwtContainer}
import java.io.{File => JFile}

import javax.swing.ImageIcon

import scala.swing.BorderPanel.Position._
import scala.swing._
import scala.swing.event._
import javax.swing.SwingUtilities
import javax.swing.border.EmptyBorder
import javax.swing.border.LineBorder

import org.apache.commons.lang3.StringEscapeUtils
import org.fs.chm.dao.ChatHistoryDao
import org.fs.chm.dao.Entities._
import org.fs.chm.protobuf._
import org.fs.chm.ui.swing.Callbacks
import org.fs.chm.ui.swing.list.DaoItem
import org.fs.chm.ui.swing.general.SwingUtils._
import org.fs.chm.utility.LangUtils._
import org.fs.utility.Imports._

class ChatListItem(
    dao: ChatHistoryDao,
    cc: CombinedChat,
    myself: User,
    selectionGroupOption: Option[ChatListItemSelectionGroup],
    callbacksOption: Option[Callbacks.ChatCb]
) extends BorderPanel { self =>
  private val labelPreferredWidth = DaoItem.PanelWidth - 100 // TODO: Remove

  val mainChat: Chat = cc.mainCwd.chat

  private val labelBorderWidth = 3

  private val popupMenu = new PopupMenu {
    contents += menuItem("Details")(showDetailsPopup())
    contents += menuItem("Combine Into", enabled = callbacksOption.nonEmpty && mainChat.tpe == ChatType.Personal)(showCombinePopup())
    contents += menuItem("Compare With", enabled = callbacksOption.nonEmpty)(showComparePopup())
    contents += menuItem("Export as HTML", enabled = callbacksOption.nonEmpty)(showExprotAsHtmlPopup())
    contents += menuItem("Delete", enabled = callbacksOption.nonEmpty && dao.isMutable)(showDeletePopup())
  }

  private var _activeColor:   Color = Color.LIGHT_GRAY
  private var _inactiveColor: Color = Color.WHITE

  {
    val emptyBorder = new EmptyBorder(labelBorderWidth, labelBorderWidth, labelBorderWidth, labelBorderWidth)

    layout({
      // Chat image
      val label = new Label
      label.preferredHeight = 48
      label.preferredWidth = 48
      val image = resolveImage(mainChat.imgPathOption, dao.datasetRoot(mainChat.dsUuid)) match {
        case None if mainChat.tpe == ChatType.Personal =>
          cc.mainCwd.members.find(_ != myself) match {
            case Some(user) =>
              user.profilePictures
                .map(pic => resolveImage(Some(pic.path), dao.datasetRoot(mainChat.dsUuid)))
                .yieldDefined
                .headOption
            case _ =>
              // No other members are known - might happen e.g. when interlocutor didn't write anything
              None
          }
        case x => x
      }
      for (image <- image) {
        val scaled = image.getScaledInstance(label.preferredWidth, label.preferredHeight, java.awt.Image.SCALE_SMOOTH)
        label.icon = new ImageIcon(scaled)
      }
      label
    }) = West

    layout(new BorderPanel {
      layout(new BorderPanel {
        // Name
        val nameString = mainChat.nameOrUnnamed
        val nameLabel = new Label(
          s"""<html><p style="text-align: left; width: ${labelPreferredWidth - 40}px;">"""
            + StringEscapeUtils.escapeHtml4(nameString)
            + "</p></html>")
        nameLabel.border = emptyBorder
        layout(nameLabel) = West


        // Source
        val sourceLabel = new Label(mainChat.sourceType.prettyString)
        sourceLabel.background = new Color(200, 200, 0, 100)
        sourceLabel.foreground = new Color(0, 0, 0, 100)
        layout(sourceLabel) = East
      }) = North

      // Last message
      val lastMsgOption = cc.cwds.map(_.lastMsgOption).yieldDefined.maxByOption(m => (m.timestamp, m.internalId))
      val lastMsgString = lastMsgOption match {
        case None      => "<No messages>"
        case Some(msg) => simpleRenderMsg(msg)
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
    val tpeString = mainChat.tpe match {
      case ChatType.Personal     => ""
      case ChatType.PrivateGroup => "(" + cc.members.size + ")"
      case _                     => unexpectedCase(mainChat.tpe)
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
    callbacksOption foreach (_.selectChat(dao, cc))
  }

  private def setBgColorRecursively(c: Component, color: Color): Unit = {
    c.background = color
    c match {
      case c: Container =>
        for (c <- c.contents) {
          setBgColorRecursively(c, color)
        }
      case _ => // NOOP
    }
  }

  def markSelected(): Unit = {
    border = new LineBorder(Color.BLACK, 1)
    setBgColorRecursively(this, _activeColor)
  }

  def markDeselected(): Unit = {
    border = new LineBorder(Color.GRAY, 1)
    setBgColorRecursively(this, _inactiveColor)
  }

  private def showDetailsPopup(): Unit = {
    Dialog.showMessage(
      title       = "Chat Details",
      message     = new ChatDetailsPane(dao, cc, myself, full = true, callbacksOption).peer,
      messageType = Dialog.Message.Plain
    )
  }

  private def showCombinePopup(): Unit = {
    val slaveChat = cc.mainCwd.chat
    val dialog = new SelectChatDialog(
      dao,
      slaveChat,
      myself,
      s"Select chat to be combined with ${slaveChat.qualifiedName}",
      "Previously selected chat will be combined with the given one and will no longer \n" +
        "be shown separately in the main list.\n" +
        "For the history merge purposes, chats will remain separate so it will continue to work.",
      cwd => cwd.chat.tpe == ChatType.Personal
    )
    dialog.visible = true
    dialog.selection foreach { masterChat =>
      callbacksOption.get.combineChats(dao, masterChat, slaveChat)
    }
  }

  private def showComparePopup(): Unit = {
    val baseChat = cc.mainCwd.chat
    val dialog = new SelectChatDialog(
      dao,
      baseChat,
      myself,
      s"Select chat to be compared with ${baseChat.qualifiedName}",
      "",
      _ => true
    )
    dialog.visible = true
    dialog.selection foreach { secondaryChat =>
      callbacksOption.get.compareChats(dao, baseChat, secondaryChat)
    }
  }

  private def showExprotAsHtmlPopup(): Unit = {
    val baseChat = cc.mainCwd.chat
    val fc = new FileChooser(dao.storagePath) {
      title        = "Export chat as HTML"
      selectedFile = new JFile(dao.storagePath, s"${baseChat.nameOrUnnamed}.html")
    }
    fc.showSaveDialog(null) match {
      case FileChooser.Result.Approve => callbacksOption.get.exportChatAsHtml(dao, cc, fc.selectedFile)
      case _                          => // NOOP
    }
  }

  private def showDeletePopup(): Unit = {
    Dialog.showConfirmation(
      title   = "Deleting Chat",
      message = s"Are you sure you want to delete a chat '${mainChat.nameOrUnnamed}'?"
    ) match {
      case Dialog.Result.Yes => callbacksOption.get.deleteChat(dao, cc)
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
      if (mainChat.tpe == ChatType.Personal && msg.fromId != cc.members.head.id) ""
      else {
        // Avoid querying DB if possible
        val fromNameOption =
          (cc.members find (_.id == msg.fromId))
            .orElse(dao.userOption(mainChat.dsUuid, msg.fromId))
            .flatMap(_.prettyNameOption)
        (fromNameOption.getOrElse(Unnamed) + ": ")
      }
    val text: String = msg.typed match {
      case Message.Typed.Regular(msgRegular) =>
        msgRegular.contentOption match {
          case _ if msgRegular.isDeleted     => "(message deleted)"
          case None                          => msg.searchableString
          case Some(s: ContentSticker)       => s.emojiOption.map(_ + " ").getOrElse("") + "(sticker)"
          case Some(_: ContentPhoto)         => "(photo)"
          case Some(_: ContentVoiceMsg)      => "(voice message)"
          case Some(_: ContentAudio)         => "(audio)"
          case Some(_: ContentVideoMsg)      => "(video message)"
          case Some(_: ContentVideo)         => "(video)"
          case Some(_: ContentFile)          => "(file)"
          case Some(_: ContentLocation)      => "(location)"
          case Some(_: ContentPoll)          => "(poll)"
          case Some(_: ContentSharedContact) => "(contact)"
        }
      case Message.Typed.Service(Some(service)) =>
        service match {
          case _: MessageServicePhoneCall           => "(call)"
          case _: MessageServiceSuggestProfilePhoto => "(suggested photo)"
          case _: MessageServicePinMessage          => "(message pinned)"
          case _: MessageServiceClearHistory        => "(history cleared)"
          case s: MessageServiceBlockUser           => s"(user ${if (s.isBlocked) "" else "un"}blocked)"
          case _: MessageServiceStatusTextChanged   => "(status) " +  msg.searchableString
          case _: MessageServiceNotice              => "(notice) " +  msg.searchableString
          case _: MessageServiceGroupCreate         => "(group created)"
          case _: MessageServiceGroupEditTitle      => "(title changed)"
          case _: MessageServiceGroupEditPhoto      => "(photo changed)"
          case _: MessageServiceGroupDeletePhoto    => "(photo deleted)"
          case _: MessageServiceGroupInviteMembers  => "(invited members)"
          case _: MessageServiceGroupRemoveMembers  => "(removed members)"
          case _: MessageServiceGroupMigrateFrom    => "(migrated from group)"
          case _: MessageServiceGroupMigrateTo      => "(migrated to group)"
        }
      case Message.Typed.Empty | Message.Typed.Service(None) =>
        unexpectedCase(msg)
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
