package org.fs.chm.ui.swing.merge

import scala.annotation.tailrec
import scala.swing.GridBagPanel._
import scala.swing._

import javax.swing.DefaultListSelectionModel
import javax.swing.ListSelectionModel
import javax.swing.border.LineBorder
import javax.swing.table.AbstractTableModel
import javax.swing.table.DefaultTableCellRenderer
import javax.swing.table.TableModel
import org.fs.chm.dao.ChatHistoryDao
import org.fs.chm.dao.Entities.Dataset
import org.fs.chm.dao.MutableChatHistoryDao
import org.fs.chm.ui.swing.general.CustomDialog
import org.fs.chm.ui.swing.general.SwingUtils._

class SelectMergeDatasetDialog(
    daos: Seq[ChatHistoryDao]
) extends CustomDialog[((MutableChatHistoryDao, Dataset), (ChatHistoryDao, Dataset))](takeFullHeight = false) {

  // All values are lazy to be accessible from parent's constructor

  private lazy val TableWidth = 500

  private lazy val daosWithDatasets        = daos map (dao => (dao, dao.datasets))
  private lazy val mutableDaosWithDatasets = daosWithDatasets filter (_._1.isMutable)

  private lazy val masterTable = {
    checkEdt()
    createTable("Base dataset", mutableDaosWithDatasets)
  }
  private lazy val slaveTable  = {
    checkEdt()
    createTable("Dataset to be added to it", daosWithDatasets)
  }

  private def createTable(title: String, data: Seq[(ChatHistoryDao, Seq[Dataset])]): Table = {
    val models = new MergeDatasetModels(title, data)
    new Table(models.tableModel) {
      peer.setDefaultRenderer(classOf[Any], new MergeDatasetCellRenderer())
      peer.setSelectionModel(models.selectionModel)
      val col = peer.getColumnModel.getColumn(0)
      col.setMinWidth(TableWidth)
      col.setPreferredWidth(TableWidth)
      peer.setFillsViewportHeight(true)
      rowHeight = 20
      border = LineBorder.createGrayLineBorder()
      val header = peer.getTableHeader
      header.setReorderingAllowed(false)
      header.setResizingAllowed(false)
    }
  }

  override lazy val dialogComponent: Panel =
    new GridBagPanel {
      val c = new Constraints
      c.fill  = Fill.Both
      c.gridy = 0
      c.gridx = 0
      peer.add(masterTable.peer.getTableHeader, c.peer)
      c.gridx = 1
      peer.add(slaveTable.peer.getTableHeader, c.peer)

      c.gridy = 1
      c.gridx = 0
      add(masterTable, c)
      c.gridx = 1
      add(slaveTable, c)
    }

  override def validateChoices(): Option[((MutableChatHistoryDao, Dataset), (ChatHistoryDao, Dataset))] = {
    /** Find the DAO for the given dataset row by abusing the table structure */
    @tailrec
    def findDao(tm: TableModel, idx: Int): ChatHistoryDao = tm.getValueAt(idx, 0) match {
      case dao: ChatHistoryDao => dao
      case _                   => findDao(tm, idx - 1)
    }

    val masterRowOption = masterTable.selection.rows.toIndexedSeq.headOption
    val slaveRowOption  = slaveTable.selection.rows.toIndexedSeq.headOption
    (masterRowOption, slaveRowOption) match {
      case (Some(masterRow), Some(slaveRow)) =>
        val masterDs  = masterTable.model.getValueAt(masterRow, 0).asInstanceOf[Dataset]
        val slaveDs   = slaveTable.model.getValueAt(slaveRow, 0).asInstanceOf[Dataset]
        val masterDao = findDao(masterTable.model, masterRow).asInstanceOf[MutableChatHistoryDao]
        val slaveDao  = findDao(slaveTable.model, slaveRow)
        if (masterDao == slaveDao && masterDs == slaveDs) {
          showWarning("Can't merge dataset with itself.")
          None
        } else {
          Some((masterDao, masterDs), (slaveDao, slaveDs))
        }
      case _ =>
        showWarning("Select both base and added datasets.")
        None
    }
  }
}

private class MergeDatasetModels(title: String, values: Seq[(ChatHistoryDao, Seq[Dataset])]) {
  private val elements: IndexedSeq[AnyRef] =
    (for {
      (dao, daoDatasets) <- values
    } yield {
      dao +: daoDatasets
    }).flatten.toIndexedSeq

  val tableModel = new AbstractTableModel {
    override val getRowCount:    Int = elements.size
    override val getColumnCount: Int = 1

    override def getValueAt(rowIndex: Int, columnIndex: Int): AnyRef = elements(rowIndex)

    override def getColumnName(column: Int): String = title
  }

  val selectionModel = new DefaultListSelectionModel {
    setSelectionMode(ListSelectionModel.SINGLE_SELECTION)

    override def setSelectionInterval(unused: Int, idx: Int): Unit = {
      if (idx >= 0 && idx < elements.size && elements(idx).isInstanceOf[ChatHistoryDao]) {
        // Ignore
      } else {
        super.setSelectionInterval(unused, idx)
      }
    }
  }
}

private class MergeDatasetCellRenderer extends DefaultTableCellRenderer {
  override def setValue(v: AnyRef): Unit = v match {
    case dao: ChatHistoryDao =>
      setText(dao.name)
      setFont {
        val oldFont = getFont
        new Font(oldFont.getName, Font.Bold.id, oldFont.getSize + 2)
      }
      setFocusable(false)
      setEnabled(false)
    case ds: Dataset =>
      setText("    " + ds.alias + " (" + ds.uuid.toString.toLowerCase + ")")
      setFocusable(true)
      setEnabled(true)
  }
}
