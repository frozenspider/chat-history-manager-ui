package org.fs.chm.ui.swing.list

import scala.swing.BufferWrapper
import scala.swing.Component
import scala.swing.GridBagPanel
import scala.swing.Panel

import org.fs.chm.dao.ChatHistoryDao
import org.fs.chm.ui.swing.general.SwingUtils._

class DaoList[I <: Panel, Dao <: ChatHistoryDao](createItem: Dao => DaoItem[I]) {
  // GridBagPanel is a much better fit than BoxPanel for a vertical list that enforces children's width
  val panel = new GridBagPanel()

  private val contents = panel.contents.asInstanceOf[BufferWrapper[Component]]

  val constraints = verticalListConstraint(panel)

  def clear(): Unit = {
    checkEdt()
    contents.clear()
  }

  def append(dao: Dao): Unit = {
    checkEdt()
    if (panel.contents.nonEmpty)
      panel.layout(new FillerComponent(false, 20)) = constraints
    panel.layout(createItem(dao)) = constraints
  }

  def replaceWith(daos: Seq[Dao]): Unit = {
    checkEdt()
    clear()
    daos foreach append
  }

  def innerItems: Seq[I] = {
    (for {
      daoItem   <- panel.contents if daoItem.isInstanceOf[DaoItem[_]]
      dsItem    <- daoItem.asInstanceOf[DaoItem[_]].items
      innerItem <- dsItem.items
    } yield innerItem.asInstanceOf[I]).toSeq
  }
}
