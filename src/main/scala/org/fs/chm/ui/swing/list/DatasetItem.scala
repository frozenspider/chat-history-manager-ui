package org.fs.chm.ui.swing.list

import java.awt.Color

import scala.swing.GridBagPanel
import scala.swing._
import scala.swing.event.MouseReleased

import javax.swing.SwingUtilities
import javax.swing.border.MatteBorder
import org.fs.chm.dao.ChatHistoryDao
import org.fs.chm.dao.Dataset
import org.fs.chm.ui.swing.general.SwingUtils._

class DatasetItem[I <: Panel](
    ds: Dataset,
    dao: ChatHistoryDao,
    callbacksOption: Option[DaoDatasetSelectionCallbacks],
    getInnerItems: Dataset => Seq[I]
) extends GridBagPanel {

  val headerPopupMenu = new PopupMenu {
    callbacksOption foreach { _ =>
      contents += menuItem("Rename", enabled = dao.isMutable)(rename())
    }
  }

  val header: Label = new Label {
    this.text                = ds.alias
    this.horizontalAlignment = Alignment.Center
    this.tooltip             = ds.alias
    this.fontSize            = this.fontSize + 2
    this.preferredWidth      = DaoItem.PanelWidth
    this.border              = new MatteBorder(0, 0, 1, 0, Color.GRAY)

    // Reactions
    listenTo(this, mouse.clicks)
    reactions += {
      case e @ MouseReleased(src, pt, _, _, _) if SwingUtilities.isRightMouseButton(e.peer) && enabled =>
        headerPopupMenu.show(this, pt.x, pt.y)
    }
  }

  val items: Seq[I] = getInnerItems(ds)

  {
    val c = verticalListConstraint(this)
    layout(header) = c
    items foreach { item =>
      layout(item) = c
    }
  }

  private def rename(): Unit = {
    // This element will be removed so we don't care about stale data
    Dialog.showInput(
      message = "Choose a new name",
      title   = "Rename dataset",
      initial = ds.alias
    ) foreach (newAlias => callbacksOption.get.renameDataset(ds.uuid, newAlias, dao))
  }

  override def enabled_=(b: Boolean): Unit = {
    super.enabled_=(b)
    header.enabled = b
    items foreach (i => i.enabled = b)
  }
}
