package org.fs.chm.ui.swing.messages.impl

import scala.swing._

import javax.swing.text.DefaultCaret
import javax.swing.text.html.HTMLEditorKit
import org.fs.chm.dao.Message
import org.fs.chm.ui.swing.general.ChatWithDao
import org.fs.chm.ui.swing.messages.MessagesRenderingComponent
import org.fs.chm.ui.swing.messages.impl.MessagesService._

class MessagesAreaContainer(htmlKit: HTMLEditorKit) extends MessagesRenderingComponent[MessageDocument] {
  // TODO: This should really be private, but we're hacking into it for SelectMergeMessagesDialog
  val msgService = new MessagesService(htmlKit)

  //
  // Fields
  //

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

  protected val viewport = scrollPane.peer.getViewport

  private val caret = textPane.peer.getCaret.asInstanceOf[DefaultCaret]

  private var viewPosSizeOption: Option[(Point, Dimension)] = None

  private var _documentOption: Option[MessageDocument] = None

  // Workaround for https://github.com/scala/bug/issues/1938: Can't call super.x if x is a lazy val
  private lazy val _component: Component = new BorderPanel {
    import scala.swing.BorderPanel.Position._
    layout(scrollPane) = Center
  }

  override def component: Component = _component

  //
  // Methods
  //

  override def renderPleaseWait(): Unit = {
    document = msgService.pleaseWaitDoc
  }

  override def render(cwd: ChatWithDao, msgs: IndexedSeq[Message], beginReached: Boolean): MessageDocument = {
    val md = msgService.createStubDoc
    val sb = new StringBuilder
    if (beginReached) {
      sb.append(msgService.nothingNewerHtml)
    }
    for (m <- msgs) {
      sb.append(msgService.renderMessageHtml(cwd, m))
    }
    md.insert(sb.toString, MessageInsertPosition.Leading)
    document = md
    document
  }

  override def render(md: MessageDocument): Unit = {
    document = md
    scrollToEnd()
  }

  override def prependLoading(): MessageDocument = {
    document.insert(msgService.loadingHtml, MessageInsertPosition.Leading)
    document
  }

  override def prepend(cwd: ChatWithDao, msgs: IndexedSeq[Message], beginReached: Boolean): MessageDocument = {
    // TODO: Prevent flickering
    // TODO: Preserve selection
    val sb = new StringBuilder
    if (beginReached) {
      sb.append(msgService.nothingNewerHtml)
    }
    for (m <- msgs.reverse) {
      sb.append(msgService.renderMessageHtml(cwd, m))
    }
    document.removeFirst() // Remove "Loading"
    document.insert(sb.toString, MessageInsertPosition.Leading)
    document
  }

  override def updateStarted(): Unit = {
    scrollPane.validate()
    viewPosSizeOption = Some(currentViewPosSize)
    // Allows disabling message caret updates while messages are loading to avoid scrolling
    caret.setUpdatePolicy(DefaultCaret.NEVER_UPDATE)
  }

  override def updateFinished(): Unit = {
    require(viewPosSizeOption.isDefined, "updateStarted() wasn't called?")
    val Some((pos1, size1)) = viewPosSizeOption
    val (_, size2)          = currentViewPosSize
    val heightDiff          = size2.height - size1.height
    show(pos1.x, pos1.y + heightDiff)
    viewPosSizeOption = None
    caret.setUpdatePolicy(DefaultCaret.UPDATE_WHEN_ON_EDT)
  }

  //
  // Helpers
  //

  protected def onDocumentChange(): Unit = {}

  protected def documentOption = _documentOption

  protected def document = _documentOption.get

  private def document_=(md: MessageDocument): Unit = {
    _documentOption = Some(md)
    textPane.peer.setStyledDocument(md.doc)
    onDocumentChange()
  }

  private def currentViewPosSize = {
    (viewport.getViewPosition, viewport.getViewSize)
  }

  private def scrollToEnd(): Unit = {
    // FIXME: Doesn't always work!
    scrollPane.peer.getViewport.setViewPosition(new Point(0, textPane.preferredSize.height))
  }

  private def show(x: Int, y: Int): Unit = {
    viewport.setViewPosition(new Point(x, y))
  }
}

object MessagesAreaContainer {
  type MessageDocument = MessagesService.MessageDocument
}
