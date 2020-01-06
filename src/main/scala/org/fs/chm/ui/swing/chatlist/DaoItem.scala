package org.fs.chm.ui.swing.chatlist

import scala.swing.Alignment
import scala.swing.GridBagPanel
import scala.swing.GridBagPanel._
import scala.swing.Label

import org.fs.chm.dao.ChatHistoryDao
import org.fs.chm.ui.swing.general.SwingUtils._
import org.fs.utility.Imports._

class DaoItem(
    callbacks: ChatListSelectionCallbacks,
    dao: ChatHistoryDao
) extends GridBagPanel {

  val header: Label = new Label {
    text = dao.name
    horizontalAlignment = Alignment.Center
    tooltip = dao.name
    this.fontSize = this.fontSize + 5
  }

  val items: Seq[DatasetItem] =
    dao.datasets map (ds => new DatasetItem(ds, callbacks, dao))

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
