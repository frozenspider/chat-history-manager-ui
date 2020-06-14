package org.fs.chm.ui.swing.messages.impl

import scala.swing._
import scala.swing.BorderPanel.Position._
import scala.swing.event._

import javax.swing.text.html.HTMLEditorKit
import org.fs.chm.ui.swing.general.SwingUtils._
import org.fs.chm.ui.swing.messages.MessageNavigationCallbacks
import org.fs.utility.Imports._

class MessagesAreaEnhancedContainer(
    htmlKit: HTMLEditorKit,
    callbacks: MessageNavigationCallbacks
) extends MessagesAreaContainer(htmlKit) {
  lazy val goToBeginningBtn = new Button("Beginning")
  lazy val goToDateBtn      = new Button("Date")
  lazy val goToEndBtn       = new Button("End")

  lazy val navPanel = new GridBagPanel {
    val dateBtns        = Seq(goToBeginningBtn, goToDateBtn, goToEndBtn)
    val maxDateBtnWidth = dateBtns.map(_.preferredWidth).max
    dateBtns.foreach(_.preferredWidth = maxDateBtnWidth)

    val constraints = verticalListConstraint(this)
    val datePanel   = new FlowPanel(goToBeginningBtn, goToDateBtn, goToEndBtn)
    layout(datePanel) = constraints

    dateBtns.foreach(b => listenTo(b))

    reactions += {
      case ButtonClicked(`goToBeginningBtn`) => callbacks.navigateToBeginning()
      case ButtonClicked(`goToDateBtn`)      => pickDateAndNavigate()
      case ButtonClicked(`goToEndBtn`)       => callbacks.navigateToEnd()
    }
  }

  override lazy val component: Component = {
    val superPanel = super.component
    new BorderPanel {
      layout(navPanel)   = North
      layout(superPanel) = Center
    }
  }

  def pickDateAndNavigate(): Int = {
    ???
  }
}
