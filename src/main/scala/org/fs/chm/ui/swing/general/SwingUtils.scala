package org.fs.chm.ui.swing.general

import java.awt.Font

import scala.swing.Dimension
import scala.swing.UIElement

object SwingUtils {
  implicit class RichUIElement(el: UIElement) {
    def preferredWidth = el.preferredSize.width

    def preferredWidth_=(w: Int) = el.preferredSize = new Dimension(w, el.preferredSize.height)

    def preferredHeight = el.preferredSize.height

    def preferredHeight_=(h: Int) = el.preferredSize = new Dimension(el.preferredSize.width, h)

    def fontSize = el.font.getSize

    def fontSize_=(s: Int) = el.font = new Font(el.font.getName, el.font.getStyle, s)
  }
}
