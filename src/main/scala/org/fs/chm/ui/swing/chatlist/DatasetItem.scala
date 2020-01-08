package org.fs.chm.ui.swing.chatlist

import scala.swing.Alignment
import scala.swing.Dialog
import scala.swing.GridBagPanel
import scala.swing.GridBagPanel._
import scala.swing.Label
import scala.swing.PopupMenu
import scala.swing.event.MouseReleased

import javax.swing.SwingUtilities
import org.fs.chm.dao.ChatHistoryDao
import org.fs.chm.dao.Dataset
import org.fs.chm.ui.swing.general.ChatWithDao
import org.fs.chm.ui.swing.general.SwingUtils._
import org.fs.utility.Imports._

class DatasetItem(
    ds: Dataset,
    itemSelectionGroup: ChatListItemSelectionGroup,
    callbacks: ChatListSelectionCallbacks,
    dao: ChatHistoryDao
) extends GridBagPanel {

  val header: Label = new Label {
    text                = ds.alias
    horizontalAlignment = Alignment.Center
    tooltip             = ds.alias
    this.fontSize       = this.fontSize + 2

    // Reactions
    listenTo(this, mouse.clicks)
    reactions += {
      case e @ MouseReleased(src, pt, _, _, _) if SwingUtilities.isRightMouseButton(e.peer) && enabled =>
        headerPopupMenu.show(this, pt.x, pt.y)
    }
  }

  val headerPopupMenu = new PopupMenu {
    contents += menuItem("Rename", enabled = dao.isMutable)(rename())
  }

  val items: Seq[ChatListItem] =
    dao.chats(ds.uuid) map (c => new ChatListItem(ChatWithDao(c, dao), Some(itemSelectionGroup), Some(callbacks)))

  {
    val c = new Constraints
    c.fill  = Fill.Horizontal
    c.gridx = 0
    c.gridy = 0

    add(header, c)

    items.foreachWithIndex { (item, idx) =>
      c.gridy = idx + 1
      add(item, c)
    }
  }

  private def rename(): Unit = {
    // This element will be removed so we don't care about stale data
    Dialog.showInput(
      message = "Choose a new name",
      title   = "Rename dataset",
      initial = ds.alias
    ) foreach (newAlias => callbacks.renameDataset(ds.uuid, newAlias, dao))
  }

  override def enabled_=(b: Boolean): Unit = {
    super.enabled_=(b)
    header.enabled = b
    items foreach (i => i.enabled = b)
  }
}
