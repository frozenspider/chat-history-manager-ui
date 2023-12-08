package org.fs.chm.ui.swing.list.chat

import scala.swing.GridBagPanel.Anchor
import scala.swing.GridBagPanel.Fill
import scala.swing._

import org.fs.chm.dao.ChatHistoryDao
import org.fs.chm.dao.Entities._
import org.fs.chm.protobuf.Chat
import org.fs.chm.protobuf.ChatType
import org.fs.chm.ui.swing.general.CustomDialog
import org.fs.chm.ui.swing.general.SwingUtils._

class SelectCombineChatsDialog(dao: ChatHistoryDao, slaveChat: Chat) extends CustomDialog[Chat](takeFullHeight = true) {
  {
    require(slaveChat.tpe == ChatType.Personal, "Non-personal chat cannot be combined")
    title = s"Select chat to be combined with ${slaveChat.qualifiedName}"
  }

  override protected def headerText: String =
    "Previously selected chat will be combined with the given one and will no longer \n" +
      "be shown separately in the main list.\n" +
      "For the history merge purposes, chats will remain separate so it will continue to work."

  private lazy val elementsSeq: Seq[(RadioButton, ChatDetailsPane, Chat)] = {
    val group = new ButtonGroup()
    val allChats = dao.chats(slaveChat.dsUuid)
    allChats
      .filter(cwd => cwd.chat.id != slaveChat.id && cwd.chat.tpe == ChatType.Personal && cwd.chat.mainChatId.isEmpty)
      .zipWithIndex
      .collect {
        case (cwd, idx) =>
          val radio = new RadioButton()
          group.buttons += radio

          val pane = new ChatDetailsPane(dao, CombinedChat(cwd, Seq.empty), full = false)
          pane.stylizeName(Colors.forIdx(idx))
          (radio, pane, cwd.chat)
      }
  }

  override protected lazy val dialogComponent: Component = {
    val containerPanel = new GridBagPanel {
      val c = new Constraints
      c.ipady = 3

      // Radio button
      c.insets = new Insets(0, 10, 0, 10)
      c.gridx = 0
      c.anchor = Anchor.East
      c.fill = Fill.Vertical
      for ((rb, _, _) <- elementsSeq) {
        layout(rb) = c
      }

      // Data column
      c.insets = new Insets(0, 0, 0, 0)
      c.gridx = 1
      c.anchor = Anchor.NorthWest
      c.weightx = 1
      c.fill = Fill.Horizontal
      for ((_, pane, _) <- elementsSeq) {
        layout(pane) = c
      }
    }
    containerPanel.wrapInScrollpaneAndAdjustWidth()
  }

  override protected def validateChoices(): Option[Chat] = {
    elementsSeq collectFirst {
      case (radio, _, chat) if radio.selected => chat
    }
  }
}
