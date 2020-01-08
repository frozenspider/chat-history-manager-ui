package org.fs.chm.ui.swing.merge

import java.awt.Color
import java.awt.{ Component => AwtComponent }

import scala.swing._

import javax.swing.DefaultListSelectionModel
import javax.swing.JTable
import javax.swing.ListSelectionModel
import javax.swing.table.DefaultTableColumnModel
import javax.swing.table.DefaultTableModel
import javax.swing.table.TableCellRenderer
import javax.swing.table.TableColumn
import org.fs.chm.dao.Chat
import org.fs.chm.dao.ChatHistoryDao
import org.fs.chm.dao.Dataset
import org.fs.chm.dao.H2ChatHistoryDao
import org.fs.chm.dao.merge.ChatHistoryMerger._
import org.fs.chm.ui.swing.chatlist.ChatListItem
import org.fs.chm.ui.swing.general.ChatWithDao
import org.fs.chm.ui.swing.general.CustomDialog
import org.fs.chm.ui.swing.general.SwingUtils._
import org.fs.chm.utility.EntityUtils._
import org.fs.utility.Imports._

class SelectMergeChatsDialog(
    masterDao: H2ChatHistoryDao,
    masterDs: Dataset,
    slaveDao: ChatHistoryDao,
    slaveDs: Dataset,
) extends CustomDialog[Seq[ChangedMergeOption]] {
  import org.fs.chm.ui.swing.merge.SelectMergeChatsDialog._
  {
    title = "Select chats to merge"
  }

  private lazy val models = new MergeChatsModels(masterDao.chats(masterDs.uuid), slaveDao.chats(slaveDs.uuid))

  override protected def dialogComponent(): Component = {
    val table = new Table(models.tableModel) {
      peer.setDefaultRenderer(classOf[ChatRenderable], models.renderer)
      peer.setSelectionModel(models.selectionModel)
      peer.setColumnModel(models.columnModel)
      peer.setFillsViewportHeight(true)
      val header = peer.getTableHeader
      header.setReorderingAllowed(false)
      header.setResizingAllowed(false)
      rowHeight = models.maxItemHeight
    }
    new ScrollPane(table) {
      verticalScrollBar.unitIncrement = comfortableScrollSpeed
      verticalScrollBarPolicy         = ScrollPane.BarPolicy.Always
      horizontalScrollBarPolicy       = ScrollPane.BarPolicy.Never
      this.preferredWidth             = table.preferredWidth + verticalScrollBar.preferredWidth
    }
  }

  override protected def validateChoices(): Option[Seq[ChangedMergeOption]] = {
    Some(models.selected)
  }

  private class MergeChatsModels(masterChats: Seq[Chat], slaveChats: Seq[Chat]) {
    val allMerges: Seq[MergeOption] = {
      val masterChatsMap = groupChatsById(masterChats)

      val merges: Seq[MergeOption] =
        for (sc <- slaveChats) yield {
          masterChatsMap.get(sc.id) match {
            case None     => MergeOption.Add(sc)
            case Some(mc) => MergeOption.Combine(mc, sc)
          }
        }

      var mergesAcc: Seq[MergeOption] = Seq.empty

      // 1) Combined and unchanged chats
      val combinesMasterToSlaveMap: Map[Chat, Chat] =
        merges.collect { case MergeOption.Combine(mc, sc) => (mc, sc) }.toMap
      for (mc <- masterChats) {
        combinesMasterToSlaveMap.get(mc) match {
          case Some(sc) => mergesAcc = mergesAcc :+ MergeOption.Combine(masterChat   = mc, slaveChat = sc)
          case None     => mergesAcc = mergesAcc :+ MergeOption.Unchanged(masterChat = mc)
        }
      }

      // 2) Added chats
      val additionsSet: Set[Chat] = merges.collect { case add: MergeOption.Add => add.slaveChat }.toSet
      for (sc <- slaveChats if additionsSet.contains(sc)) {
        mergesAcc = mergesAcc :+ MergeOption.Add(sc)
      }

      mergesAcc
    }

    val renderer = new ChatListItemRenderer()

    val tableModel = new DefaultTableModel(Array[AnyRef]("Base", "Apply?", "Added"), 0) {

      allMerges.foreachWithIndex { (merge, i) =>
        val row: Array[AnyRef] = merge match {
          case MergeOption.Combine(mc, sc) =>
            Array(
              ChatRenderable(ChatWithDao(mc, masterDao), isCombine = true),
              true: java.lang.Boolean,
              ChatRenderable(ChatWithDao(sc, slaveDao), isCombine = true)
            )
          case MergeOption.Add(sc) =>
            Array(
              "",
              true: java.lang.Boolean,
              ChatRenderable(ChatWithDao(sc, slaveDao), isAdd = true)
            )
          case MergeOption.Unchanged(mc) =>
            Array(
              ChatRenderable(ChatWithDao(mc, masterDao)),
              "",
              ""
            )
        }
        addRow(row)
      }

      override def isCellEditable(rowIndex: Int, columnIndex: Int): Boolean =
        columnIndex == 1

      override def getColumnClass(i: Int): Class[_] = i match {
        case 0 => classOf[ChatRenderable]
        case 1 => classOf[java.lang.Boolean]
        case 2 => classOf[ChatRenderable]
      }
    }

    val (maxItemHeight: Int, maxItemWidth: Int) = {
      val uiItems = for {
        r  <- 0 until tableModel.getRowCount
        c  <- 0 until tableModel.getColumnCount
        cr = tableModel.getValueAt(r, c) if cr.isInstanceOf[ChatRenderable]
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

    val columnModel = new DefaultTableColumnModel {
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

    def selected: Seq[ChangedMergeOption] = {
      def isSelected(rowIdx: Int): Boolean = {
        tableModel.getValueAt(rowIdx, 1).asInstanceOf[java.lang.Boolean].booleanValue
      }
      allMerges.zipWithIndex.collect {
        case (m: ChangedMergeOption, rowIdx) if isSelected(rowIdx) => m
      }
    }
  }

  private class ChatListItemRenderer extends TableCellRenderer {
    private var componentsCache: Map[ChatRenderable, ChatListItem] = Map.empty

    override def getTableCellRendererComponent(
        table: JTable,
        value: Any,
        isSelected: Boolean,
        hasFocus: Boolean,
        row: Int,
        column: Int
    ): AwtComponent = {
      val v = value.asInstanceOf[ChatRenderable]
      val renderer = if (componentsCache.contains(v)) {
        componentsCache(v)
      } else {
        val r = new ChatListItem(v.cwd, None, None)
        if (v.isCombine) {
          r.inactiveColor = Color.YELLOW
        } else if (v.isAdd) {
          r.inactiveColor = Color.GREEN
        }
        r.markDeselected()
        componentsCache = componentsCache.updated(v, r)
        r
      }
      renderer.peer
    }
  }
}

object SelectMergeChatsDialog {
  case class ChatRenderable(cwd: ChatWithDao, isCombine: Boolean = false, isAdd: Boolean = false)
}
