package org.fs.chm.ui.swing.chatlist

import scala.swing.Alignment
import scala.swing.GridBagPanel
import scala.swing.GridBagPanel._
import scala.swing.Label

import org.fs.chm.dao.ChatHistoryDao
import org.fs.chm.dao.Dataset
import org.fs.chm.ui.swing.general.ChatWithDao
import org.fs.chm.ui.swing.general.SwingUtils._
import org.fs.utility.Imports._

class DatasetItem(
    ds: Dataset,
    callbacks: ChatListSelectionCallbacks,
    dao: ChatHistoryDao
) extends GridBagPanel {

  val header: Label = new Label {
    text = ds.alias
    horizontalAlignment = Alignment.Center
    tooltip = ds.alias
    this.fontSize = this.fontSize + 2
  }

  val items: Seq[ChatListItem] =
    dao.chats(ds.uuid) map (c => new ChatListItem(ChatWithDao(c, dao), callbacks))

  {
    val c = new Constraints
    c.fill = Fill.Horizontal
    c.gridx = 0
    c.gridy = 0

    add(header, c)

    items.foreachWithIndex { (item, idx) =>
      c.gridy = idx + 1
      add(item, c)
    }
  }

  override def enabled_=(b: Boolean): Unit = {
    super.enabled_=(b)
    items foreach (i => i.enabled = b)
  }
}
