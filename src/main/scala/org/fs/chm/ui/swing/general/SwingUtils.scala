package org.fs.chm.ui.swing.general

import java.awt.Font
import java.io.File

import scala.swing.Action
import scala.swing.Dimension
import scala.swing.MenuItem
import scala.swing.UIElement

import javax.swing.filechooser.FileFilter

object SwingUtils {
  implicit class RichUIElement(el: UIElement) {
    def preferredWidth = el.preferredSize.width

    def preferredWidth_=(w: Int) = el.preferredSize = new Dimension(w, el.preferredSize.height)

    def preferredHeight = el.preferredSize.height

    def preferredHeight_=(h: Int) = el.preferredSize = new Dimension(el.preferredSize.width, h)

    def fontSize = el.font.getSize

    def fontSize_=(s: Int) = el.font = new Font(el.font.getName, el.font.getStyle, s)
  }

  def menuItem(title: String, action: () => _): MenuItem =
    new MenuItem(new Action(title) { override def apply(): Unit = action() })

  def easyFileFilter(desc: String)(filter: File => Boolean): FileFilter =
    new FileFilter() {
      override def accept(f: File): Boolean = f.isDirectory || filter(f)
      override def getDescription:  String  = desc
    }
}
