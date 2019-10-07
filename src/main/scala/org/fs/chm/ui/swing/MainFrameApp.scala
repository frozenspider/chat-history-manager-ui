package org.fs.chm.ui.swing

import java.io.StringReader

import scala.swing.Dimension
import scala.swing._
import scala.swing.event.ButtonClicked

import javax.swing.text.Element
import javax.swing.text.html.HTMLDocument
import javax.swing.text.html.HTMLEditorKit
import org.fs.chm.dao._
import org.fs.utility.Imports._

class MainFrameApp(dao: ChatHistoryDao) extends SimpleSwingApplication {
  var documentsCache: Map[Chat, MessageDocument] = Map.empty

  // TODO:
  // word-wrap and narrower width
  // dynamic loading
  // search

  override def top = new MainFrame {
//    import BuildInfo._
//    title    = s"$name ${version}b${buildInfoBuildNumber}"
    title = "Chat History Manager"
    contents = ui
    size = new Dimension(1000, 700)
    peer.setLocationRelativeTo(null)
  }

  val ui = new BorderPanel {
    import scala.swing.BorderPanel.Position._

//    val browseBtn = new Button("Browse")
//    listenTo(startBtn, browseBtn)
    layout(chatsPane) = West
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
        listenTo(el)

        contents += el

        reactions += {
          case ButtonClicked(`el`) => chatSelected(c)
        }
      }
    })) = Center
  }

  lazy val htmlKit = new HTMLEditorKit

  lazy val (messagesArea: TextPane, messagesPane) = {
    val ta = new TextPane()
    ta.peer.setEditorKit(htmlKit)
    ta.peer.setEditable(false)
    ta.peer.setSize(new Dimension(10, 10))

    (ta, new BorderPanel {
      import scala.swing.BorderPanel.Position._
      layout(new ScrollPane(ta)) = Center
    })
  }

  //
  // Events
  //

  def chatSelected(c: Chat): Unit = {
    // If the chat has been already rendered, restore previous document as-is
    // TODO: Add throbber, make async
    if (!documentsCache.contains(c)) {
      val md = createStubDoc
      md.doc.remove(0, md.doc.getLength)
      val messages = dao.messages(c, 0, 100)
      for (m <- messages) {
        renderMessage(md, c, m, MessageInsertPosition.Trailing)
      }
      documentsCache = documentsCache + ((c, md))
    }
    messagesArea.peer.setStyledDocument(documentsCache(c).doc)
  }

  //
  // Renderers and helpers
  //

  def createStubDoc: MessageDocument = {
    val doc     = htmlKit.createDefaultDocument().asInstanceOf[HTMLDocument]
    val content = """
                    |<html>
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
    val msgTitleHtmlString = s"${m.fromName} (${m.date.toString("yyyy-MM-dd HH:mm")})"
    val htmlToInsert       = s"""
                          |<div class="message" message_id="${m.id}">
                          |   <div class="message-title">${msgTitleHtmlString}</div>
                          |   <div class="message-body">${msgHtmlString}</div>
                          |</div>
                          |<p>
    """.stripMargin
    pos match {
      case MessageInsertPosition.Leading  => md.doc.insertAfterStart(md.contentParent, htmlToInsert)
      case MessageInsertPosition.Trailing => md.doc.insertBeforeEnd(md.contentParent, htmlToInsert)
    }
  }

  object RichTextRenderer {
    def renderHtml(rt: RichText): String = {
      val components = for (rtel <- rt.components) yield {
        renderComponent(rtel)
      }
      components.mkString
    }

    def renderComponent(rtel: RichText.Element): String = {
      rtel match {
        case rtel: RichText.Plain =>
          rtel.text replace ("\n", "<br>")
        case rtel: RichText.Bold =>
          s"<b>${rtel.text replace ("\n", "<br>")}</b>"
        case rtel: RichText.Italic =>
          s"<i>${rtel.text replace ("\n", "<br>")}</i>"
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

  sealed trait MessageInsertPosition
  object MessageInsertPosition {
    case object Leading  extends MessageInsertPosition
    case object Trailing extends MessageInsertPosition
  }

  case class MessageDocument(
      doc: HTMLDocument,
      contentParent: Element
  )
}
