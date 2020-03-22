package org.fs.chm.ui.swing.list

import java.awt.Color

import scala.swing.Alignment
import scala.swing.GridBagPanel
import scala.swing.GridBagPanel._
import scala.swing.Label
import scala.swing.Panel

import javax.swing.border.MatteBorder
import org.fs.chm.dao.ChatHistoryDao
import org.fs.chm.dao.Dataset
import org.fs.chm.ui.swing.general.SwingUtils._
import org.fs.utility.Imports._

class DaoItem(
    dao: ChatHistoryDao,
    callbacksOption: Option[DaoDatasetSelectionCallbacks],
    getInnerItems: Dataset => Seq[Panel]
) extends GridBagPanel {

  val header: Label = new Label {
    this.text                = dao.name
    this.horizontalAlignment = Alignment.Center
    this.tooltip             = dao.name
    this.fontSize            = this.fontSize + 5
    this.preferredWidth      = DaoItem.PanelWidth
    this.border              = new MatteBorder(0, 0, 1, 0, Color.GRAY)
  }

  val items: Seq[DatasetItem] =
    dao.datasets map (ds => new DatasetItem(ds, dao, callbacksOption, getInnerItems))

  {
    val c = new Constraints
    c.fill  = Fill.Horizontal
    c.gridx = 0
    c.gridy = 0

    add(header, c)

    items.foreachWithIndex { (item, idx) =>
      c.gridy  = idx + 1
      add(item, c)
    }
  }

  override def enabled_=(b: Boolean): Unit = {
    super.enabled_=(b)
    items foreach (i => i.enabled = b)
  }
}

object DaoItem {

  /**
   * This shouldn't really be necesasry, but currenyly it is. It serves the following purposes:
   *
   *  - Defines a preferred for an empty chat panel
   *  - Defines a maximum width of a Dao header label
   *  - Defines a maximum width of a dataset header label
   *  - Used to calculate preferred width of ChatListItem component (also a hack)
   */
  val PanelWidth = 300
}
