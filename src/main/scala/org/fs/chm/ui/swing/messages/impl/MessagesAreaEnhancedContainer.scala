package org.fs.chm.ui.swing.messages.impl

import java.util.Calendar
import java.util.Locale

import scala.annotation.tailrec
import scala.swing.BorderPanel.Position._
import scala.swing._
import scala.swing.event._

import com.github.nscala_time.time.Imports._
import javax.swing.text.Element
import javax.swing.text.html.HTML
import javax.swing.text.html.HTMLEditorKit

import org.fs.chm.ui.swing.Callbacks
import org.fs.chm.ui.swing.general.SwingUtils._
import org.fs.utility.Imports._
import org.jdatepicker._
import org.jdatepicker.impl._

class MessagesAreaEnhancedContainer(
    htmlKit: HTMLEditorKit,
    showSeconds: Boolean,
    callbacks: Callbacks.MessageHistoryCb
) extends MessagesAreaContainer(htmlKit, showSeconds) {
  private lazy val goToBeginBtn = new Button("Beginning")
  private lazy val goToDateBtn  = new Button("Date (NYI)")
  private lazy val goToEndBtn   = new Button("End")

  private val dtf = DateTimeFormat.forPattern("yyyy-MM-dd")

  /** Date used as current when showing a date picker, tries to keep in sync with the top-most visible message */
  private var shownDate: DateTime = DateTime.now()

  lazy val navPanel = new GridBagPanel {
    goToDateBtn.enabled = false // FIXME: Change after this is fixed
    val dateBtns        = Seq(goToBeginBtn, goToDateBtn, goToEndBtn)
    val maxDateBtnWidth = dateBtns.map(_.preferredWidth).max
    dateBtns.foreach(_.preferredWidth = maxDateBtnWidth)

    val constraints = verticalListConstraint(this)
    val datePanel   = new FlowPanel(goToBeginBtn, goToDateBtn, goToEndBtn)
    layout(datePanel) = constraints

    dateBtns.foreach(b => listenTo(b))

    reactions += {
      case ButtonClicked(`goToBeginBtn`) => callbacks.navigateToBeginning()
      case ButtonClicked(`goToDateBtn`)  => pickDateAndNavigate()
      case ButtonClicked(`goToEndBtn`)   => callbacks.navigateToEnd()
    }

    // Listen to scrolls
    viewport.addChangeListener(x => updateShownDate())
  }

  override lazy val component: Component = {
    val superPanel = super.component
    new BorderPanel {
      layout(navPanel)   = North
      layout(superPanel) = Center
    }
  }

  def pickDateAndNavigate(): Unit = {
    val dateModel = new JodaDateModel(shownDate.withTimeAtStartOfDay())
    val datePanel = {
      // JDateComponentFactory is a crap which can ONLY construct the model it knows, so it's useless per se
      val factory = new JDateComponentFactory {
        // Can't access private field, or a superclass protected method from outside
        val i18nStrings = super.getI18nStrings(Locale.ENGLISH)
      }
      new JDatePanelImpl(dateModel, factory.i18nStrings)
    }
    Dialog.showConfirmation(
      title       = "Pick a date",
      message     = datePanel,
      messageType = Dialog.Message.Plain,
      optionType  = Dialog.Options.OkCancel
    ) match {
      case Dialog.Result.Ok => callbacks.navigateToDate(dateModel.getValue)
      case _                => // NOOP
    }
  }

  private def updateShownDate(): Unit = {
    val startPoint  = viewport.getViewPosition
    val startOffset = textPane.peer.viewToModel(startPoint)
    val msgOption   = messageAtOffsetOption(startOffset)
    shownDate = msgOption map { el =>
      dtf.parseDateTime(el.getAttributes.getAttribute("date").toString)
    } getOrElse {
      DateTime.now()
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

  class JodaDateModel(startDate: DateTime) extends AbstractDateModel[DateTime] {
    setValue(startDate.withTimeAtStartOfDay())

    override def fromCalendar(from: Calendar): DateTime = {
      new DateTime(from.getTimeInMillis)
    }

    override def toCalendar(from: DateTime): Calendar = {
      val to = Calendar.getInstance
      to.setTimeInMillis(from.getMillis)
      to
    }
  }
}
