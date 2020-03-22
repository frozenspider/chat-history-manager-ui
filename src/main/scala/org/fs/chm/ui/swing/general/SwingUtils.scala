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
    def width            = el.size.width
    def width_=(w: Int)  = el.peer.setSize(w, el.size.height)
    def height           = el.size.height
    def height_=(h: Int) = el.peer.setSize(el.size.width, h)

    def preferredWidth            = el.preferredSize.width
    def preferredWidth_=(w: Int)  = el.preferredSize = new Dimension(w, el.preferredSize.height)
    def preferredHeight           = el.preferredSize.height
    def preferredHeight_=(h: Int) = el.preferredSize = new Dimension(el.preferredSize.width, h)

    def minimumWidth            = el.minimumSize.width
    def minimumWidth_=(w: Int)  = el.minimumSize = new Dimension(w, el.minimumSize.height)
    def minimumHeight           = el.minimumSize.height
    def minimumHeight_=(h: Int) = el.minimumSize = new Dimension(el.minimumSize.width, h)

    def maximumWidth            = el.maximumSize.width
    def maximumWidth_=(w: Int)  = el.maximumSize = new Dimension(w, el.maximumSize.height)
    def maximumHeight           = el.maximumSize.height
    def maximumHeight_=(h: Int) = el.maximumSize = new Dimension(el.maximumSize.width, h)

    def fontSize = el.font.getSize

    def fontSize_=(s: Int) = el.font = new Font(el.font.getName, el.font.getStyle, s)
  }

  def menuItem(title: String, enabled: Boolean = true)(action: => Any): MenuItem = {
    val e = enabled // To avoid name shadowing
    new MenuItem(new Action(title) { override def apply(): Unit = action }) {
      enabled = e
    }
  }

  def easyFileFilter(desc: String)(filter: File => Boolean): FileFilter =
    new FileFilter() {
      override def accept(f: File): Boolean = f.isDirectory || filter(f)
      override def getDescription:  String  = desc
    }
}
