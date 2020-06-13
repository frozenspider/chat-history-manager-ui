package org.fs.chm.ui.swing.messages.impl

import scala.swing._
import scala.swing.event._

import javax.swing.JLayeredPane
import javax.swing.text.html.HTMLEditorKit
import org.fs.chm.ui.swing.general.SwingUtils._

class MessagesAreaOverlaidContainer(htmlKit: HTMLEditorKit) extends MessagesAreaContainer(htmlKit) {
  lazy val calendarBtn = new Button("Calendar") {
    visible = false
  }

  lazy val parentCmp = super.component

  override lazy val component: Component = {
    val layeredCmp = new Component with Container.Wrapper {
      override lazy val peer = new JLayeredPane with SuperMixin {
        override def isOptimizedDrawingEnabled: Boolean = false
        override def isValidateRoot: Boolean = true
      }
    }

    layeredCmp.peer.add(calendarBtn.peer, new Integer(1))
    layeredCmp.peer.add(parentCmp.peer, new Integer(0))
    calendarBtn.peer.setBounds(0, 0, calendarBtn.preferredWidth, calendarBtn.preferredHeight)

    // Listening to itself seems silly but Scala Swing is funny like that
    layeredCmp.listenTo(layeredCmp)
    layeredCmp.reactions += {
      case UIElementResized(_) => reposition()
    }

    layeredCmp
  }

  override protected def onDocumentChange(): Unit = {
    calendarBtn.visible = true
    reposition()
  }

  private def reposition(): Unit = {
    val sz      = component.size
    val spWidth = if (scrollPane.verticalScrollBar.visible) scrollPane.verticalScrollBar.preferredWidth else 0
    val newX    = sz.width - calendarBtn.width - spWidth
    parentCmp.peer.setBounds(0, 0, sz.width, sz.height)
    calendarBtn.peer.setLocation(newX, 0)
    component.peer.revalidate()
  }
}
