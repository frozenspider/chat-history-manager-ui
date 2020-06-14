package org.fs.chm.ui.swing.messages.impl

import scala.annotation.tailrec
import scala.swing._
import scala.swing.event._

import javax.swing.JLayeredPane
import javax.swing.text.Element
import javax.swing.text.html.HTML
import javax.swing.text.html.HTMLEditorKit
import org.fs.chm.ui.swing.general.SwingUtils._
import org.fs.utility.Imports._

class MessagesAreaOverlaidContainer(htmlKit: HTMLEditorKit) extends MessagesAreaContainer(htmlKit) {
  lazy val dateBtn = new Button("<Date>") {
    visible = false
  }

  lazy val parentCmp = super.component

  override lazy val component: Component = {
    val layeredCmp = new Component with Container.Wrapper {
      override lazy val peer = new JLayeredPane with SuperMixin {
        override def isOptimizedDrawingEnabled: Boolean = false
        override def isValidateRoot:            Boolean = true
      }
    }

    layeredCmp.peer.add(parentCmp.peer, new Integer(0))
    layeredCmp.peer.add(dateBtn.peer, new Integer(1))

    // Listening to itself seems silly but Scala Swing is funny like that
    layeredCmp.listenTo(layeredCmp)
    layeredCmp.reactions += {
      case UIElementResized(_) => reposition()
    }

    // Listen to scrolls
    viewport.addChangeListener(x => updateDateButton())

    layeredCmp
  }

  override protected def onDocumentChange(): Unit = {
    dateBtn.visible = true
    reposition()
  }

  private def reposition(): Unit = {
    val sz          = component.size
    val scrollWidth = if (scrollPane.verticalScrollBar.visible) scrollPane.verticalScrollBar.preferredWidth else 0
    val newX        = sz.width - dateBtn.preferredWidth - scrollWidth
    // JLayeredPane operates using bounds, not sizes
    parentCmp.peer.setBounds(0, 0, sz.width, sz.height)
    dateBtn.peer.setBounds(newX, 0, dateBtn.preferredWidth, dateBtn.preferredHeight)
    component.peer.revalidate()
  }

  private def updateDateButton(): Unit = {
    val startPoint  = viewport.getViewPosition
    val startOffset = textPane.peer.viewToModel(startPoint)
    val msgOption   = messageAtOffsetOption(startOffset)
    msgOption.foreach { el =>
      dateBtn.text = el.getAttributes.getAttribute("date").toString
      reposition()
    }
  }

  /** Find message element closest to the given offest (see [[Element#getElementIndex]]) */
  private def messageAtOffsetOption(offset: Int): Option[Element] = {
    @tailrec
    def recurseFindMessagesBlock(el: Element): Element = {
      val idx = el.getElementIndex(offset)
      val el2 = el.getElement(idx)
      if (el2.getAttributes.getAttribute(HTML.Attribute.ID) == "messages") el2 else recurseFindMessagesBlock(el2)
    }
    documentOption flatMap { md =>
      val messagesEl = recurseFindMessagesBlock(md.doc.getDefaultRootElement)

      @tailrec
      def findMessageParent(el: Element): Option[Element] = {
        if (el == null || el == messagesEl) {
          None
        } else if (el.getAttributes.getAttribute(HTML.Attribute.CLASS) == "message") {
          Some(el)
        } else {
          findMessageParent(el.getParentElement)
        }
      }

      {
        // Try to move up first, if it fails - try to move down
        val idx0 = messagesEl.getElementIndex(offset)
        (idx0 to 0 by -1).toStream #::: ((idx0 + 1) until messagesEl.getElementCount).toStream
      }.map(idx => findMessageParent(messagesEl.getElement(idx))).yieldDefined.headOption
    }
  }

}
