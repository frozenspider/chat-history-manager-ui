package org.fs.chm.ui.swing

import java.awt.event.AdjustmentEvent
import java.awt.{ Container => AwtContainer }
import java.io.StringReader
import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.swing.Dimension
import scala.swing._
import scala.swing.event.ButtonClicked

import javax.swing.text.DefaultCaret
import javax.swing.text.Element
import javax.swing.text.html.HTMLDocument
import javax.swing.text.html.HTMLEditorKit
import org.fs.chm.dao._
import org.fs.utility.Imports._

class MainFrameApp(dao: ChatHistoryDao) extends SimpleSwingApplication {
  val Lock             = new Object
  val MsgBatchLoadSize = 100

  var documentsCache:  Map[Chat, MessageDocument] = Map.empty
  var loadStatusCache: Map[Chat, LoadStatus]      = Map.empty

  var currentChatOption:      Option[Chat]  = None
  var loadMessagesInProgress: AtomicBoolean = new AtomicBoolean(false)

  // TODO:
  // word-wrap and narrower width
  // search
  // content (stickers, voices)
  // emoji and fonts

  override def top = new MainFrame {
//    import BuildInfo._
//    title    = s"$name ${version}b${buildInfoBuildNumber}"
    title    = "Chat History Manager"
    contents = ui
    size     = new Dimension(1000, 700)
    peer.setLocationRelativeTo(null)
  }

  val ui = new BorderPanel {
    import scala.swing.BorderPanel.Position._

//    val browseBtn = new Button("Browse")
//    listenTo(startBtn, browseBtn)
    layout(chatsPane)    = West
    layout(messagesPane) = Center
//    reactions += {
//      case ButtonClicked(`startBtn`)  => eventStartClick
//      case ButtonClicked(`browseBtn`) => eventBrowseClick
//    }
  }

  lazy val chatsPane = new BorderPanel {
    import scala.swing.BorderPanel.Position._

    layout(new ScrollPane(new BoxPanel(Orientation.Vertical) {
      for (c <- dao.chats) {
        val el = new Button(c.nameOption getOrElse "<Unnamed>")
        contents += el
        listenTo(el)
        reactions += {
          case ButtonClicked(`el`) => chatSelected(c)
        }
      }
    })) = Center
  }

  lazy val htmlKit = new HTMLEditorKit

  lazy val (messagesArea: TextPane, messagesScrollPane: ScrollPane, messagesPane) = {
    val ta = new TextPane()
    ta.peer.setEditorKit(htmlKit)
    ta.peer.setEditable(false)
    ta.peer.setSize(new Dimension(10, 10))

    val sp = new ScrollPane(ta)
    sp.verticalScrollBar.peer.addAdjustmentListener((e: AdjustmentEvent) => {
      if (e.getValue < 500 && !e.getValueIsAdjusting) {
        tryLoadPreviousMessages()
      }
    })
    val mp = new BorderPanel {
      import scala.swing.BorderPanel.Position._
      layout(sp) = Center
    }
    (ta, sp, mp)
  }

  lazy val pleaseWaitDoc: HTMLDocument = {
    val md = createStubDoc
    md.insert("<div><h1>Please wait...</h1></div>", MessageInsertPosition.Leading)
    md.doc
  }

  /** Allows disabling message caret updates while messages are loading to avoid scrolling */
  def changeMessagesCaretUpdatesEnabled(enabled: Boolean): Unit = {
    val caret = messagesArea.peer.getCaret.asInstanceOf[DefaultCaret]
    caret.setUpdatePolicy(if (enabled) DefaultCaret.UPDATE_WHEN_ON_EDT else DefaultCaret.NEVER_UPDATE)
  }

  def changeChatsClickable(enabled: Boolean): Unit = {
    chatsPane.enabled = enabled
    def changeClickableRecursive(c: AwtContainer): Unit = {
      c.setEnabled(enabled)
      c.getComponents foreach {
        case c: AwtContainer => changeClickableRecursive(c)
        case _               => //NOOP
      }
    }
    changeClickableRecursive(chatsPane.peer)
  }

  //
  // Events
  //

  def chatSelected(c: Chat): Unit = {
    Lock.synchronized {
      currentChatOption = None
      // If the chat has been already rendered, restore previous document as-is
      messagesArea.peer.setStyledDocument(pleaseWaitDoc)
      changeChatsClickable(false)
    }
    val f = Future {
      if (!documentsCache.contains(c)) {
        val md       = createStubDoc
        val messages = dao.lastMessages(c, MsgBatchLoadSize)
        for (m <- messages) {
          renderMessage(md, c, m, MessageInsertPosition.Trailing)
        }
        val loadStatus = LoadStatus(
          firstId      = messages.headOption map (_.id) getOrElse (-1),
          lastId       = messages.lastOption map (_.id) getOrElse (-1),
          beginReached = messages.size < MsgBatchLoadSize,
          endReached   = true
        )
        Lock.synchronized {
          loadStatusCache = loadStatusCache + ((c, loadStatus))
          documentsCache  = documentsCache + ((c, md))
        }
      }
    }
    f foreach { _ =>
      Swing.onEDTWait(Lock.synchronized {
        currentChatOption = Some(c)
        val doc = documentsCache(c).doc
        messagesArea.peer.setStyledDocument(doc)
        // Scroll to the end
        messagesScrollPane.peer.getViewport.setViewPosition(new Point(0, messagesArea.preferredSize.height))
        changeChatsClickable(true)
      })
    }
  }

  def tryLoadPreviousMessages(): Unit =
    currentChatOption match {
      case _ if loadMessagesInProgress.get => // NOOP
      case None                            => // NOOP
      case Some(c) =>
        Lock.synchronized {
          changeMessagesCaretUpdatesEnabled(false)
          val loadStatus = loadStatusCache(c)
          if (!loadStatus.beginReached) {
            changeChatsClickable(false)
            loadMessagesInProgress set true
            val md = documentsCache(c)
            val f = Future {
              Lock.synchronized {
                val viewport                        = messagesScrollPane.peer.getViewport
                val (viewPosBefore, viewSizeBefore) = (viewport.getViewPosition, viewport.getViewSize)
                md.insert("<div id=\"loading\"><hr><p> Loading... </p><hr></div>", MessageInsertPosition.Leading)
                val addedMessages = dao.messagesBefore(c, loadStatus.firstId, MsgBatchLoadSize)
                loadStatusCache = loadStatusCache.updated(
                  c,
                  loadStatusCache(c).copy(
                    firstId      = addedMessages.headOption map (_.id) getOrElse (-1),
                    beginReached = addedMessages.size < MsgBatchLoadSize
                  )
                )
                // We're still holding the lock
                Swing.onEDTWait {
                  // TODO: Prevent flickering
                  // TODO: Preserve selection
                  md.removeFirst()
                  for (m <- addedMessages.reverse) {
                    renderMessage(md, c, m, MessageInsertPosition.Leading)
                  }
                  messagesScrollPane.validate()
                  val heightDiff = viewport.getViewSize.height - viewSizeBefore.height
                  viewport.setViewPosition({
                    val p = new Point(viewPosBefore)
                    p.translate(0, heightDiff)
                    p
                  })
                }
              }
            }
            f.onComplete(_ =>
              Swing.onEDTWait(Lock.synchronized {
                loadMessagesInProgress set false
                changeChatsClickable(true)
                changeMessagesCaretUpdatesEnabled(true)
              }))
          }
        }
    }

  //
  // Renderers and helpers
  //

  def createStubDoc: MessageDocument = {
    val doc     = htmlKit.createDefaultDocument().asInstanceOf[HTMLDocument]
    val content = """
                    |<html>
                    | <head>
                    |   <style type="text/css">
                    |     .message-title-name { font-weight: bold; }
                    |   </style>
                    | </head>
                    | <body>
                    |   <div id="messages"></div>
                    | </body>
                    |</html>
            """.stripMargin
    htmlKit.read(new StringReader(content), doc, 0)
    val msgEl = doc.getElement("messages")
    MessageDocument(doc, msgEl)
  }

  def renderMessage(md: MessageDocument, c: Chat, m: Message, pos: MessageInsertPosition): Unit = {
    val msgHtmlString: String = m match {
      case m: Message.Regular =>
        val textHtmlOption = m.textOption map { rt =>
          s"""<div class="message-text">${RichTextRenderer.renderHtml(rt)}</div>"""
        }
        val contentHtmlOption = m.contentOption map { ct =>
          s"""<div class="message-content">${ContentRenderer.renderHtml(ct)}</div>"""
        }
        Seq(textHtmlOption, contentHtmlOption).yieldDefined.mkString
      case _ => s"[Unsupported - ${m.getClass.getSimpleName}]" // NOOP, FIXME later on
    }
    val titleStyleCss = {
      val idx = {
        val intl = dao.interlocutors(c)
        val idx1 = intl indexWhere (_.id == m.fromId)
        val idx2 = intl indexWhere (_.prettyName == m.fromName)
        if (idx1 != -1) idx1 else idx2
      }
      val color = if (idx >= 0 && idx < NameColors.length) NameColors(idx) else "#000000"
      s"color: $color;"
    }
    val titleHtmlString =
      s"""<span class="message-title-name" style="$titleStyleCss">${m.fromName}</span> """ +
        s"(${m.date.toString("yyyy-MM-dd HH:mm")})"
    val htmlToInsert = s"""
                          |<div class="message" message_id="${m.id}">
                          |   <div class="message-title">${titleHtmlString}</div>
                          |   <div class="message-body">${msgHtmlString}</div>
                          |</div>
                          |<p>
    """.stripMargin
    md.insert(htmlToInsert, pos)
  }

  object RichTextRenderer {
    def renderHtml(rt: RichText): String = {
      val components = for (rtel <- rt.components) yield {
        renderComponent(rtel)
      }
      val hiddenLinkOption = rt.components collectFirst { case l: RichText.Link if l.hidden => l }
      val link = hiddenLinkOption map { l =>
        "<p> &gt; Link: " + renderComponent(l.copy(textOption = Some(l.href), hidden = false))
      } getOrElse ""
      components.mkString + link
    }

    def renderComponent(rtel: RichText.Element): String = {
      rtel match {
        case rtel: RichText.Plain =>
          rtel.text replace ("\n", "<br>")
        case rtel: RichText.Bold =>
          s"<b>${rtel.text replace ("\n", "<br>")}</b>"
        case rtel: RichText.Italic =>
          s"<i>${rtel.text replace ("\n", "<br>")}</i>"
        case rtel: RichText.Link if rtel.hidden =>
          rtel.plainSearchableString
        case rtel: RichText.Link =>
          val text = rtel.textOption getOrElse rtel.href
          s"""<a href="${rtel.href}">${text}</a> """ // Space in the end is needed if link is followed by text
        case rtel: RichText.PrefmtBlock =>
          s"""<pre>${rtel.text}</pre>"""
        case rtel: RichText.PrefmtInline =>
          s"""<code>${rtel.text}</code>"""
      }
    }
  }

  object ContentRenderer {
    def renderHtml(ct: Content): String = {
      s"[Unsupported content - ${ct.getClass.getSimpleName}]" // TODO
    }
  }

  case class MessageDocument(
      doc: HTMLDocument,
      contentParent: Element
  ) {

    // We need to account for "p-implied" parasitic element...
    private lazy val pImplied = contentParent.getElement(0)

    def removeFirst(): Unit = {
      doc.removeElement(contentParent.getElement(1))
    }

    def insert(htmlToInsert: String, pos: MessageInsertPosition): Unit = {
      pos match {
        case MessageInsertPosition.Leading  => doc.insertAfterEnd(pImplied, htmlToInsert)
        case MessageInsertPosition.Trailing => doc.insertBeforeEnd(contentParent, htmlToInsert)
      }
    }

    def clear(): Unit = {
      doc.remove(0, doc.getLength)
    }
  }

  val NameColors = Seq(
    // User
    "#6495ED", // CornflowerBlue
    // First interlocutor
    "#B22222", // FireBrick
    "#008000", // Green
    "#DAA520", // GoldenRod
    "#BA55D3", // MediumOrchid
    "#FF69B4", // HotPink
    "#808000", // Olive
    "#008080", // Teal
    "#9ACD32", // YellowGreen
    "#FF8C00", // DarkOrange
    "#00D0D0", // Cyan-ish
    "#BDB76B" // DarkKhaki
  )

  sealed trait MessageInsertPosition
  object MessageInsertPosition {
    case object Leading  extends MessageInsertPosition
    case object Trailing extends MessageInsertPosition
  }

  case class LoadStatus(
      firstId: Long,
      lastId: Long,
      beginReached: Boolean,
      endReached: Boolean
  )
}
