package org.fs.chm.ui.swing.merge

import java.awt.BorderLayout
import java.awt.event.ActionListener
import java.awt.event.MouseAdapter
import java.awt.event.MouseEvent
import java.awt.{ Component => AwtComponent }
import java.util.EventObject

import scala.swing._

import javax.swing.DefaultCellEditor
import javax.swing.DefaultListSelectionModel
import javax.swing.JButton
import javax.swing.JCheckBox
import javax.swing.JComponent
import javax.swing.JPanel
import javax.swing.JTable
import javax.swing.ListSelectionModel
import javax.swing.SwingConstants
import javax.swing.UIManager
import javax.swing.event.CellEditorListener
import javax.swing.table._
import org.fs.chm.ui.swing.general.SwingUtils._
import org.fs.utility.Imports._
import org.slf4s.Logging

class SelectMergesTable[V, R](
    models: SelectMergesTable.MergeModels[V, R]
) extends Table(models.tableModel) { thisTable =>

  private val header = peer.getTableHeader

  {
    val renderer = models.renderer
    peer.setDefaultRenderer(
      classOf[SelectMergesTable.ListItemRenderable[V]],
      renderer
    )
    peer.setDefaultEditor(
      classOf[SelectMergesTable.ListItemRenderable[V]],
      new SelectMergesTable.ListItemEditor(renderer)
    )
    val checkboxComponent = new CheckboxComponent(peer.getDefaultRenderer(classOf[java.lang.Boolean]))
    peer.setDefaultRenderer(
      classOf[SelectMergesTable.CheckboxBoolean],
      checkboxComponent
    )
    peer.setDefaultEditor(
      classOf[SelectMergesTable.CheckboxBoolean],
      checkboxComponent
    )
    peer.setSelectionModel(models.selectionModel)
    peer.setColumnModel(models.columnModel)
    peer.setFillsViewportHeight(true)

    header.setReorderingAllowed(false)
    header.setResizingAllowed(false)

    val headerColumnModel = header.getColumnModel
    headerColumnModel
      .getColumn(0)
      .setHeaderRenderer(new CheckboxHeaderRenderer(false, _ => {
        models.changeSelected(false)
        updateSelectedCount()
      }))
    headerColumnModel
      .getColumn(2)
      .setHeaderRenderer(new CheckboxHeaderRenderer(true, _ => {
        models.changeSelected(true)
        updateSelectedCount()
      }))

    rowHeight = models.maxItemHeight
    updateSelectedCount()
  }

  def selected: Seq[R] = models.selected

  private def updateSelectedCount(): Unit = {
    header.getColumnModel
      .getColumn(1)
      .setHeaderValue(s"${models.currentlySelectedCount} of ${models.totalSelectableCount}")
    header.repaint()
  }

  private class CheckboxComponent(defaultRenderer: TableCellRenderer)
      extends DefaultCellEditor(new JCheckBox)
      with TableCellRenderer {
    private val checkboxRn = defaultRenderer.asInstanceOf[JCheckBox with TableCellRenderer]
    private val checkboxEd = editorComponent.asInstanceOf[JCheckBox]

    {
      checkboxEd.setHorizontalAlignment(SwingConstants.CENTER);
      checkboxEd.removeActionListener(delegate)
      delegate = new EditorDelegate() {
        var typedValue: SelectMergesTable.CheckboxBoolean = _

        override def setValue(value: Any): Unit = {
          typedValue = value.asInstanceOf[SelectMergesTable.CheckboxBoolean]
          checkboxEd.setSelected(typedValue.v)
        }

        override def getCellEditorValue: AnyRef = {
          typedValue.copy(v = checkboxEd.isSelected)
        }
      }
      checkboxEd.addActionListener(delegate)
      checkboxEd.setRequestFocusEnabled(false)

      checkboxRn.addItemListener(e => updateSelectedCount())
    }

    override def getTableCellRendererComponent(
        table: JTable,
        value: Any,
        isSelected: Boolean,
        hasFocus: Boolean,
        row: Int,
        column: Int
    ): AwtComponent = {
      val typedValue = value.asInstanceOf[SelectMergesTable.CheckboxBoolean]
      val jlbValue   = java.lang.Boolean.valueOf(typedValue.v)
      val res        = checkboxRn.getTableCellRendererComponent(table, jlbValue, isSelected, hasFocus, row, column)
      if (typedValue.v) {
        if (typedValue.isAdd) {
          res.setBackground(Colors.AdditionBg)
        } else if (typedValue.isCombine) {
          res.setBackground(Colors.CombineBg)
        } else {
          res.setBackground(Colors.NoBg)
        }
      } else {
        res.setBackground(Colors.ConflictBg)
      }
      res
    }
  }

  private class CheckboxHeaderRenderer(isAll: Boolean, listener: ActionListener) extends JPanel with TableCellRenderer {
    private val defaultRenderer = header.getDefaultRenderer
    private val colIdx          = if (isAll) 2 else 0

    {
      val button = new JButton(if (isAll) "All" else "None")
      button.setPreferredSize(new Dimension(button.getPreferredSize.width, button.getPreferredSize.height * 2 / 3))
      button.addActionListener(listener)

      this.setLayout(new BorderLayout())
      this.add(button, if (isAll) BorderLayout.WEST else BorderLayout.EAST)

      header.addMouseListener(new MouseAdapter {
        val colModel  = peer.getColumnModel
        var isPressed = false

        /** Should be dynamic*/
        def widthOfPrevCols: Int = {
          ((0 until colIdx) map (colModel.getColumn) map (_.getWidth)).sum
        }

        def isButtonLeftClicked(e: MouseEvent): Boolean = {
          e.getButton == MouseEvent.BUTTON1 && colModel.getColumnIndexAtX(e.getX) == colIdx && {
            val xLocal = e.getX - widthOfPrevCols
            xLocal >= button.getX && xLocal <= (button.getX + button.getWidth)
          }
        }

        override def mousePressed(e: MouseEvent): Unit = {
          if (isButtonLeftClicked(e)) {
            isPressed = true
            button.getModel.setPressed(true)
            header.repaint()
          }
        }

        override def mouseReleased(e: MouseEvent): Unit = {
          if (e.getClickCount == 1 && isButtonLeftClicked(e) && isPressed) {
            button.doClick()
          }
          button.getModel.setPressed(false)
          header.repaint()
          isPressed = false
        }
      })
    }

    override def getTableCellRendererComponent(
        table: JTable,
        value: Any,
        isSelected: Boolean,
        hasFocus: Boolean,
        row: Int,
        column: Int
    ): AwtComponent = {
      val border = UIManager.getBorder(if (hasFocus) "TableHeader.focusCellBorder" else "TableHeader.cellBorder")
      val rc = defaultRenderer
        .getTableCellRendererComponent(table, value, isSelected, hasFocus, row, column)
        .asInstanceOf[JComponent]
      rc.setBorder(null)
      this.add(rc, BorderLayout.CENTER)
      this.setBorder(border)
      this
    }
  }
}

object SelectMergesTable extends Logging {

  abstract class MergeModels[V, R] {

    def allElems: Seq[RowData[V]]

    def cellsAreInteractive: Boolean

    def renderer: ListItemRenderer[V, _]

    private var _totalSelectableCount = 0

    protected def rowDataToResultOption(rd: RowData[V], isSelected: Boolean): Option[R]

    protected def isInBothSelectable(mv: V, sv: V): Boolean
    protected def isInSlaveSelectable(sv: V): Boolean
    protected def isInMasterSelectable(mv: V): Boolean

    lazy val (maxItemHeight: Int, maxItemWidth: Int) = {
      val uiItems = for {
        r  <- 0 until tableModel.getRowCount
        c  <- 0 until tableModel.getColumnCount
        cr = tableModel.getValueAt(r, c) if cr.isInstanceOf[ListItemRenderable[_]]
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

    lazy val tableModel: TableModel = new DefaultTableModel(Array[AnyRef]("Base", "999 of 999", "Added"), 0) {
      allElems.foreachWithIndex { (merge, i) =>
        def checkboxOrEmpty(isSelectable: Boolean, isCombine: Boolean = false, isAdd: Boolean = false) = {
          if (isSelectable) {
            _totalSelectableCount += 1
            CheckboxBoolean(v = isSelectable, isCombine = isCombine, isAdd = isAdd)
          } else ""
        }
        val row: Array[AnyRef] = merge match {
          case RowData.InBoth(mv, sv) =>
            val isSelectable = isInBothSelectable(mv, sv)
            Array(
              ListItemRenderable[V](mv, isSelectable, isCombine = true),
              checkboxOrEmpty(isSelectable, isCombine = true),
              ListItemRenderable[V](sv, isSelectable, isCombine = true)
            )
          case RowData.InSlaveOnly(sv) =>
            val isSelectable = isInSlaveSelectable(sv)
            Array(
              "",
              checkboxOrEmpty(isSelectable, isAdd = true),
              ListItemRenderable[V](sv, isSelectable, isAdd = true)
            )
          case RowData.InMasterOnly(mv) =>
            val isSelectable = isInMasterSelectable(mv)
            Array(
              ListItemRenderable[V](mv, isSelectable),
              checkboxOrEmpty(isSelectable),
              ""
            )
        }
        addRow(row)
      }

      override def isCellEditable(rowIndex: Int, columnIndex: Int): Boolean =
        cellsAreInteractive || columnIndex == 1

      override def getColumnClass(i: Int): Class[_] = i match {
        case 0 => classOf[ListItemRenderable[V]]
        case 1 => classOf[CheckboxBoolean]
        case 2 => classOf[ListItemRenderable[V]]
      }
    }

    protected def isSelected(rowIdx: Int): Boolean = {
      tableModel.getValueAt(rowIdx, 1) match {
        case b: CheckboxBoolean => b.v
        case _                  => false
      }
    }

    def changeSelected(newValue: Boolean): Unit = {
      (0 until tableModel.getRowCount) foreach { rowIdx =>
        tableModel.getValueAt(rowIdx, 1) match {
          case b: CheckboxBoolean => tableModel.setValueAt(b.copy(v = newValue), rowIdx, 1)
          case _                  => // NOOP
        }
      }
    }

    def selected: Seq[R] = {
      allElems.zipWithIndex.map {
        case (rd, rowIdx) => rowDataToResultOption(rd, isSelected(rowIdx))
      }.yieldDefined
    }

    def totalSelectableCount: Int = _totalSelectableCount

    def currentlySelectedCount: Int = {
      (0 until tableModel.getRowCount).count(isSelected)
    }
  }

  trait ListItemRenderer[V, C <: Component] extends TableCellRenderer {
    private var componentsCache: Map[ListItemRenderable[V], C] = Map.empty

    override def getTableCellRendererComponent(
        table: JTable,
        value: Any,
        isSelected: Boolean,
        hasFocus: Boolean,
        row: Int,
        column: Int
    ): AwtComponent = {
      val renderable = value.asInstanceOf[ListItemRenderable[V]]
      val renderer = if (componentsCache.contains(renderable)) {
        componentsCache(renderable)
      } else {
        val r = setUpComponent(renderable)
        componentsCache = componentsCache.updated(renderable, r)
        r
      }
      renderer.peer
    }

    def setUpComponent(renderable: ListItemRenderable[V]): C
  }

  /** A simple wrapper around ListItemRenderer to allow selecting text */
  private class ListItemEditor(renderer: ListItemRenderer[_, _]) extends TableCellEditor {
    override def getTableCellEditorComponent(
        table: JTable,
        value: Any,
        isSelected: Boolean,
        row: Int,
        column: Int
    ): AwtComponent = {
      renderer.getTableCellRendererComponent(table, value, isSelected, true, row, column)
    }

    override def getCellEditorValue:                              AnyRef  = null
    override def isCellEditable(anEvent: EventObject):            Boolean = true
    override def shouldSelectCell(anEvent: EventObject):          Boolean = true
    override def stopCellEditing():                               Boolean = true
    override def cancelCellEditing():                             Unit    = {}
    override def addCellEditorListener(l: CellEditorListener):    Unit    = {}
    override def removeCellEditorListener(l: CellEditorListener): Unit    = {}
  }

  sealed trait RowData[V]
  object RowData {
    sealed case class InBoth[V](masterValue: V, slaveValue: V) extends RowData[V]
    sealed case class InMasterOnly[V](masterValue: V)          extends RowData[V]
    sealed case class InSlaveOnly[V](slaveValue: V)            extends RowData[V]
  }

  case class ListItemRenderable[V](v: V, isSelectable: Boolean, isCombine: Boolean = false, isAdd: Boolean = false)

  case class CheckboxBoolean(v: Boolean, isCombine: Boolean = false, isAdd: Boolean = false)
}
