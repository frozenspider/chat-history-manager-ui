package org.fs.chm.ui.swing

import java.awt.Color

import scala.swing._
import scala.swing.event.ButtonClicked

import javax.swing.text.AttributeSet
import javax.swing.text.Element
import javax.swing.text.SimpleAttributeSet
import javax.swing.text.StyleConstants
import javax.swing.text.html.HTMLDocument
import javax.swing.text.html.HTMLEditorKit
import org.fs.chm.dao._

class MainFrameApp(dao: ChatHistoryDao) extends SimpleSwingApplication {
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
          case ButtonClicked(`el`) => chatClicked(c)
        }
      }
    })) = Center
  }

  lazy val (messagesArea: TextPane, messagesDoc: HTMLDocument, messagesHtmlEl: Element, messagesPane) = {
    val ta  = new TextPane()
    val kit = new HTMLEditorKit
    ta.peer.setEditorKit(kit)
    // ta.peer.setContentType("text/html") // This is supposed to change StyleDocument from plain to HTMLDocument
    // ta.peer.setStyledDocument(new HTMLDocument())
    ta.text = """
                |<html>
                | <body>
                |   <div id="messages"></div>
                | </body>
                |</html>
      """.stripMargin
    val doc   = ta.styledDocument.asInstanceOf[HTMLDocument]
    val msgEl = doc.getElement("messages")
    (ta, doc, msgEl, new BorderPanel {
      import scala.swing.BorderPanel.Position._
      layout(new ScrollPane(ta)) = Center
    })
  }

  def chatClicked(c: Chat): Unit = {
//    messagesDoc.setInnerHTML(messagesHtmlEl, "<p>")
    messagesArea.text = ""
    val messages = dao.messages(c, 0, 100)
    for (m <- messages) {
      renderMessage(c, m, MessageInsertPosition.Trailing)
    }
  }

  def renderMessage(c: Chat, m: Message, pos: MessageInsertPosition): Unit = {
//    def currentOffset: Int = pos match {
//      case MessageInsertPosition.Leading  => 0
//      case MessageInsertPosition.Trailing => doc.getLength
//    }
//    def insert(s: String, attrs: AttributeSet): Unit = {
//      doc.insertString(currentOffset, s, attrs)
//    }
//    insert(s"${m.fromName} [${m.date.toString}]\n", null)
    val msgHtmlString: String = m match {
      case m: Message.Regular => m.textOption map (rt => RichTextRenderer.renderHtml(c, rt)) getOrElse ""
      case _                  => "[Unsupported]" // NOOP, FIXME
    }
    val htmlToInsert = s"""
                          |<div message_id="${m.id}">
                          |<p>${m.fromName} [${m.date.toString}]</p>
                          |<p>${msgHtmlString}</p>
                          |</div>
    """.stripMargin
    messagesDoc.insertBeforeEnd(messagesHtmlEl, htmlToInsert)
//    insert("\n\n", null)
  }

  object RichTextRenderer {
    def renderHtml(c: Chat, rt: RichText): String = {
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
          s"""<a href="${text}">${rtel.href}</a> """ // Space in the end is needed if link is followed by text
        case rtel: RichText.PrefmtBlock =>
          s"""<pre>${rtel.text}</pre>"""
        case rtel: RichText.PrefmtInline =>
          s"""<code>${rtel.text}</code>"""
      }
    }
  }

  sealed trait MessageInsertPosition
  object MessageInsertPosition {
    case object Leading  extends MessageInsertPosition
    case object Trailing extends MessageInsertPosition
  }
}
