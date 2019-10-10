package org.fs.chm.ui.swing

import java.io.StringReader

import javax.swing.text.Element
import javax.swing.text.html.HTMLDocument
import javax.swing.text.html.HTMLEditorKit
import org.fs.chm.dao._
import org.fs.utility.Imports._

class MessagesService(dao: ChatHistoryDao, htmlKit: HTMLEditorKit) {
  import org.fs.chm.ui.swing.MessagesService._

  def createStubDoc: MessageDocument = {
    val doc = htmlKit.createDefaultDocument().asInstanceOf[HTMLDocument]
    val content =
      """
        |<html>
        | <head>
        |   <style type="text/css">
        |     .title-name { font-weight: bold; }
        |     blockquote {
        |        border-left: 1px solid #ccc;
        |        margin: 5px 10px;
        |        padding: 5px 10px;
        |     }
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

  lazy val pleaseWaitDoc: HTMLDocument = {
    val md = createStubDoc
    md.insert("<div><h1>Please wait...</h1></div>", MessageInsertPosition.Leading)
    md.doc
  }

  //
  // Renderers and helpers
  //

  def renderMessageHtml(c: Chat, m: Message, isQuote: Boolean = false): String = {
    val msgHtmlString: String = m match {
      case m: Message.Regular =>
        val textHtmlOption = m.textOption map { rt =>
          s"""<div class="text">${RichTextRenderer.renderHtml(rt)}</div>"""
        }
        val contentHtmlOption = m.contentOption map { ct =>
          s"""<div class="content">${ContentRenderer.renderHtml(ct)}</div>"""
        }
        val fromHtmlOption =
          (if (isQuote)
             None
           else
             m.replyToMessageIdOption map { m2id =>
               val m2Option = dao.messageOption(c, m2id)
               m2Option match {
                 case None     => "[Deleted message]"
                 case Some(m2) => renderMessageHtml(c, m2, true)
               }
             }) map (html => s"""<blockquote>$html</blockquote> """)
        Seq(fromHtmlOption, textHtmlOption, contentHtmlOption).yieldDefined.mkString
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
      s"""<span class="title-name" style="$titleStyleCss">${m.fromName}</span> """ +
        s"(${m.date.toString("yyyy-MM-dd HH:mm")})"
    s"""
       |<div class="message" message_id="${m.id}">
       |   <div class="title">${titleHtmlString}</div>
       |   <div class="body">${msgHtmlString}</div>
       |</div>
       |${if (!isQuote) "<p>" else ""}
    """.stripMargin
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
}

object MessagesService {
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

  sealed trait MessageInsertPosition
  object MessageInsertPosition {
    case object Leading  extends MessageInsertPosition
    case object Trailing extends MessageInsertPosition
  }

}
