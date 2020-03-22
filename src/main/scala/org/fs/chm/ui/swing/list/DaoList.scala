package org.fs.chm.ui.swing.list

import scala.swing.BoxPanel
import scala.swing.Orientation

import org.fs.chm.dao.ChatHistoryDao
import org.fs.chm.ui.swing.general.SwingUtils._

class DaoList(createItem: ChatHistoryDao => DaoItem) {
  val panel = new BoxPanel(Orientation.Vertical)

  def clear(): Unit = {
    checkEdt()
    panel.contents.clear()
  }

  def append(dao: ChatHistoryDao): Unit = {
    checkEdt()
    if (panel.contents.nonEmpty)
      panel.contents += new FillerComponent(false, 20)
    panel.contents += createItem(dao)
  }

  def replaceWith(daos: Seq[ChatHistoryDao]): Unit = {
    checkEdt()
    clear()
    daos foreach append
  }
}
