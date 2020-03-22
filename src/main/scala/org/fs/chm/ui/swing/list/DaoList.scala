package org.fs.chm.ui.swing.list

import scala.swing.BufferWrapper
import scala.swing.Component
import scala.swing.GridBagPanel

import org.fs.chm.dao.ChatHistoryDao
import org.fs.chm.ui.swing.general.SwingUtils._

class DaoList(createItem: ChatHistoryDao => DaoItem) {
  // GridBagPanel is a much better fit than BoxPanel for a vertical list that enforces children's width
  val panel = new GridBagPanel()

  private val contents = panel.contents.asInstanceOf[BufferWrapper[Component]]

  val constraints = verticalListConstraint(panel)

  def clear(): Unit = {
    checkEdt()
    contents.clear()
  }

  def append(dao: ChatHistoryDao): Unit = {
    checkEdt()
    if (panel.contents.nonEmpty)
      panel.layout(new FillerComponent(false, 20)) = constraints
    panel.layout(createItem(dao)) = constraints
  }

  def replaceWith(daos: Seq[ChatHistoryDao]): Unit = {
    checkEdt()
    clear()
    daos foreach append
  }
}
