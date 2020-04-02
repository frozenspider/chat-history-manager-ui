package org.fs.chm.ui.swing.merge

import java.awt.{ Component => AwtComponent }

import scala.swing._

import javax.swing.DefaultListSelectionModel
import javax.swing.JTable
import javax.swing.ListSelectionModel
import javax.swing.table._
import org.fs.chm.ui.swing.general.SwingUtils._
import org.fs.utility.Imports._

class SelectMergesTable[V, R](models: SelectMergesTable.MergeModels[V, R]) //
    extends Table(models.tableModel) { thisTable =>

  {
    peer.setDefaultRenderer(classOf[SelectMergesTable.ChatRenderable[V]], models.renderer)
    peer.setSelectionModel(models.selectionModel)
    peer.setColumnModel(models.columnModel)
    peer.setFillsViewportHeight(true)
    val header = peer.getTableHeader
    header.setReorderingAllowed(false)
    header.setResizingAllowed(false)
    rowHeight = models.maxItemHeight
  }

  def selected: Seq[R] = models.selected
}

object SelectMergesTable {

  trait MergeModels[V, R] {

    def allElems: Seq[RowData[V]]

    def renderer: ListItemRenderer[V, _]

    protected def rowDataToResultOption(rd: RowData[V], isSelected: Boolean): Option[R]

    protected def isInBothSelectable(mv: V, sv: V): Boolean
    protected def isInSlaveSelectable(sv: V): Boolean
    protected def isInMasterSelectable(mv: V): Boolean

    lazy val (maxItemHeight: Int, maxItemWidth: Int) = {
      val uiItems = for {
        r  <- 0 until tableModel.getRowCount
        c  <- 0 until tableModel.getColumnCount
        cr = tableModel.getValueAt(r, c) if cr.isInstanceOf[ChatRenderable[V]]
      } yield {
        renderer.getTableCellRendererComponent(null, cr, false, false, r, c)
      }
      val sizes     = uiItems.map(_.getPreferredSize)
      val maxHeight = sizes.maxBy(_.height).height
      val maxWidth  = sizes.maxBy(_.width).width
      (maxHeight, maxWidth)
    }

    val selectionModel = new DefaultListSelectionModel {
      setSelectionMode(ListSelectionModel.SINGLE_SELECTION)

      override def setSelectionInterval(idx1: Int, idx2: Int): Unit = () // Suppress selection
    }

    lazy val columnModel = new DefaultTableColumnModel {
      addColumn(new TableColumn(0) {
        setMinWidth(maxItemWidth)
        getCellRenderer
      })
      addColumn(new TableColumn(1) {
        // To make sure header fits
        val w = new Label(tableModel.getColumnName(1)).preferredWidth + 2
        setMinWidth(w)
        setPreferredWidth(w)
        setMaxWidth(w)
      })
      addColumn(new TableColumn(2) {
        setMinWidth(maxItemWidth)
      })
      import scala.collection.JavaConverters._
      getColumns.asScala.toSeq foreachWithIndex ((tc, i) => {
        tc.setResizable(false)
        tc.setHeaderValue(tableModel.getColumnName(i))
      })
    }

    lazy val tableModel: TableModel = new DefaultTableModel(Array[AnyRef]("Base", "Apply?", "Added"), 0) {
      allElems.foreachWithIndex { (merge, i) =>
        def checkboxOrEmpty(isSelectable: Boolean) = if (isSelectable) (true: java.lang.Boolean) else ""
        val row: Array[AnyRef] = merge match {
          case RowData.InBoth(mv, sv) =>
            val isSelectable = isInBothSelectable(mv, sv)
            Array(
              ChatRenderable[V](mv, isSelectable, isCombine = true),
              checkboxOrEmpty(isSelectable),
              ChatRenderable[V](sv, isSelectable,isCombine = true)
            )
          case RowData.InSlaveOnly(sv) =>
            val isSelectable = isInSlaveSelectable(sv)
            Array(
              "",
              checkboxOrEmpty(isSelectable),
              ChatRenderable[V](sv, isSelectable, isAdd = true)
            )
          case RowData.InMasterOnly(mv) =>
            val isSelectable = isInMasterSelectable(mv)
            Array(
              ChatRenderable[V](mv, isSelectable),
              checkboxOrEmpty(isSelectable),
              ""
            )
        }
        addRow(row)
      }

      override def isCellEditable(rowIndex: Int, columnIndex: Int): Boolean =
        columnIndex == 1

      override def getColumnClass(i: Int): Class[_] = i match {
        case 0 => classOf[ChatRenderable[V]]
        case 1 => classOf[java.lang.Boolean]
        case 2 => classOf[ChatRenderable[V]]
      }
    }

    def selected: Seq[R] = {
      def isSelected(rowIdx: Int): Boolean = {
        tableModel.getValueAt(rowIdx, 1) match {
          case b: java.lang.Boolean => b.booleanValue
          case _                    => false
        }
      }
      allElems.zipWithIndex.map {
        case (rd, rowIdx) => rowDataToResultOption(rd, isSelected(rowIdx))
      }.yieldDefined
    }
  }

  trait ListItemRenderer[V, C <: Component] extends TableCellRenderer {
    private var componentsCache: Map[ChatRenderable[V], C] = Map.empty

    override def getTableCellRendererComponent(
        table: JTable,
        value: Any,
        isSelected: Boolean,
        hasFocus: Boolean,
        row: Int,
        column: Int
    ): AwtComponent = {
      val renderable = value.asInstanceOf[ChatRenderable[V]]
      val renderer = if (componentsCache.contains(renderable)) {
        componentsCache(renderable)
      } else {
        val r = setUpComponent(renderable)
        componentsCache = componentsCache.updated(renderable, r)
        r
      }
      renderer.peer
    }

    def setUpComponent(renderable: ChatRenderable[V]): C
  }

  sealed trait RowData[V]
  object RowData {
    sealed case class InBoth[V](masterValue: V, slaveValue: V) extends RowData[V]
    sealed case class InMasterOnly[V](masterValue: V)          extends RowData[V]
    sealed case class InSlaveOnly[V](slaveValue: V)            extends RowData[V]
  }

  case class ChatRenderable[V](v: V, isSelectable: Boolean, isCombine: Boolean = false, isAdd: Boolean = false)
}
