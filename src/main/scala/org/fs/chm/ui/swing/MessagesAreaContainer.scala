package org.fs.chm.ui.swing

import scala.swing._

import javax.swing.text.DefaultCaret
import javax.swing.text.StyledDocument
import javax.swing.text.html.HTMLEditorKit

class MessagesAreaContainer(htmlKit: HTMLEditorKit) {

  val textPane: TextPane = {
    val ta = new TextPane()
    ta.peer.setEditorKit(htmlKit)
    ta.peer.setEditable(false)
    ta.peer.setSize(new Dimension(10, 10))
    ta
  }

  val scrollPane: ScrollPane = {
    new ScrollPane(textPane)
  }

  private val caret = textPane.peer.getCaret.asInstanceOf[DefaultCaret]

  private val viewport = scrollPane.peer.getViewport

  val panel: BorderPanel = new BorderPanel {
    import scala.swing.BorderPanel.Position._
    layout(scrollPane) = Center
  }

  def document = {
    textPane.peer.getStyledDocument
  }

  def document_=(sd: StyledDocument): Unit = {
    textPane.peer.setStyledDocument(sd)
  }

  def caretUpdatesEnabled = {
    caret.getUpdatePolicy != DefaultCaret.NEVER_UPDATE
  }

  /** Allows disabling message caret updates while messages are loading to avoid scrolling */
  def caretUpdatesEnabled_=(enabled: Boolean): Unit = {
    caret.setUpdatePolicy(if (enabled) DefaultCaret.UPDATE_WHEN_ON_EDT else DefaultCaret.NEVER_UPDATE)
  }

  object scroll {
    def toEnd(): Unit = {
      scrollPane.peer.getViewport.setViewPosition(new Point(0, textPane.preferredSize.height))
    }
  }

  object view {
    def posAndSize: (Point, Dimension) = {
      scrollPane.validate()
      (viewport.getViewPosition, viewport.getViewSize)
    }

    def show(x: Int, y: Int): Unit = {
      viewport.setViewPosition(new Point(x, y))
    }
  }
}
