package org.fs.chm.ui.swing.messages.impl

import java.io.File
import java.io.StringReader

import javax.swing.text.Element
import javax.swing.text.html.HTML
import javax.swing.text.html.HTMLDocument
import javax.swing.text.html.HTMLEditorKit

import org.fs.chm.dao.Message.Service
import org.fs.chm.dao._
import org.fs.chm.dao.Helpers._
import org.fs.chm.protobuf.Content
import org.fs.chm.protobuf.ContentAnimation
import org.fs.chm.protobuf.ContentFile
import org.fs.chm.protobuf.ContentLocation
import org.fs.chm.protobuf.ContentPhoto
import org.fs.chm.protobuf.ContentPoll
import org.fs.chm.protobuf.ContentSharedContact
import org.fs.chm.protobuf.ContentSticker
import org.fs.chm.protobuf.ContentVideoMsg
import org.fs.chm.protobuf.ContentVoiceMsg
import org.fs.chm.ui.swing.general.SwingUtils._
import org.fs.chm.utility.EntityUtils
import org.fs.utility.Imports._

class MessagesService(htmlKit: HTMLEditorKit) {
  import MessagesService._

  def createStubDoc: MessageDocument = {
    // TODO: How to limit width and do word wraps?
    //       width, max-width acts weird,
    //       word-wrap, word-break, overflow-wrap are ignored
    val doc     = htmlKit.createDefaultDocument().asInstanceOf[HTMLDocument]
    val content = """<div id="messages"></div>"""
    htmlKit.read(new StringReader(content), doc, 0)
    val css = """|body {
                 |  font-family: arial,sans-serif;
                 |}
                 |.title {
                 |  padding-left: 5px;
                 |  padding-bottom: 5px;
                 |  font-size: 105%;
                 |}
                 |.forwarded-from {
                 |  padding-left: 5px;
                 |  padding-bottom: 5px;
                 |  font-size: 105%;
                 |  color: #909090;
                 |}
                 |.title-name {
                 |  font-weight: bold;
                 |}
                 |blockquote {
                 |  border-left: 1px solid #ccc;
                 |  margin: 5px 10px;
                 |  padding: 5px 10px;
                 |}
                 |.system-message {
                 |  border: 1px solid #A0A0A0;
                 |  margin: 5px 10px;
                 |  padding: 5px 10px;
                 |}
                 |#no-newer {
                 |  color: #909090;
                 |  font-size: 120%;
                 |  text-align: center;
                 |  border-bottom: 3px solid #909090;
                 |  margin: 5px 10px;
                 |  padding: 5px 10px;
                 |}
                 |""".stripMargin
    doc.getStyleSheet.addRule(css)
    doc.getStyleSheet.addRule("W3C_LENGTH_UNITS_ENABLE")
    val msgEl = doc.getElement("messages")
    MessageDocument(doc, msgEl)
  }

  lazy val pleaseWaitDoc: MessageDocument = {
    val md = createStubDoc
    md.insert("<div><h1>Please wait...</h1></div>", MessageInsertPosition.Leading)
    md
  }

  val loadingHtml: String =
    """|<div id="loading">
       |  <hr>
       |  <p>Loading...</p>
       |  <hr>
       |</div>""".stripMargin

  val nothingNewerHtml: String =
    """|<div id="no-newer">
       |  <p>Start of message history</p>
       |</div>""".stripMargin

  //
  // Renderers and helpers
  //

  def renderMessageHtml(dao: ChatHistoryDao, cwd: ChatWithDetails, m: Message, isQuote: Boolean = false): String = {
    val msgHtml: String = m match {
      case m: Message.Regular =>
        val textHtmlOption = renderTextOption(m.textOption)
        val contentHtmlOption = m.contentOption map { ct =>
          s"""<div class="content">${ContentHtmlRenderer.render(cwd, ct)}</div>"""
        }
        val fwdFromHtmlOption  = m.forwardFromNameOption map (n => renderFwdFrom(cwd, n))
        val replySrcHtmlOption = if (isQuote) None else m.replyToMessageIdOption map (id => renderSourceMessage(dao, cwd, id))
        Seq(fwdFromHtmlOption, replySrcHtmlOption, textHtmlOption, contentHtmlOption).yieldDefined.mkString
      case sm: Message.Service =>
        ServiceMessageHtmlRenderer.render(dao, cwd, sm)
    }
    val titleNameHtml = renderTitleName(cwd, Some(m.fromId), None)
    val titleHtml =
      s"""$titleNameHtml (${m.time.toString("yyyy-MM-dd HH:mm")})"""
    val sourceIdAttr = m.sourceIdOption map (id => s"""message_source_id="$id"""") getOrElse ""
    // "date" attribute is used by overlay to show topmost message date
    s"""
       |<div class="message" ${sourceIdAttr} date="${m.time.toString("yyyy-MM-dd")}">
       |   <div class="title">${titleHtml}</div>
       |   <div class="body">${msgHtml}</div>
       |</div>
       |${if (!isQuote) "<p>" else ""}
    """.stripMargin // TODO: Remove <p>
  }

  private def renderTitleName(cwd: ChatWithDetails, idOption: Option[Long], nameOption: Option[String]): String = {
    val idx = {
      val idx1 = idOption map (id => cwd.members indexWhere (_.id == id)) getOrElse -1
      val idx2 = cwd.members indexWhere (u => nameOption contains u.prettyName)
      if (idx1 != -1) idx1 else idx2
    }
    val color = if (idx >= 0) Colors.stringForIdx(idx) else "#000000"
    val resolvedName = nameOption getOrElse {
      EntityUtils.getOrUnnamed(idOption flatMap (id => cwd.members.find(_.id == id)) map (_.prettyName))
    }

    s"""<span class="title-name" style="color: $color;">${resolvedName}</span>"""
  }

  private def renderTextOption(textOption: Option[RichText]): Option[String] =
    textOption map { rt =>
      s"""<div class="text">${RichTextHtmlRenderer.render(rt)}</div>"""
    }

  private def renderFwdFrom(cwd: ChatWithDetails, fromName: String): String = {
    val titleNameHtml = renderTitleName(cwd, None, Some(fromName))
    s"""<div class="forwarded-from">Forwarded from $titleNameHtml</div>"""
  }

  private def renderSourceMessage(dao: ChatHistoryDao, cwd: ChatWithDetails, srcId: Message.SourceId): String = {
    val m2Option = dao.messageOption(cwd.chat, srcId)
    val html = m2Option match {
      case None     => "[Deleted message]"
      case Some(m2) => renderMessageHtml(dao, cwd, m2, true)
    }
    s"""<blockquote>$html</blockquote> """
  }

  private def renderPossiblyMissingContent(
      fileOption: Option[File],
      kindPrettyName: String
  )(renderContentFromFile: File => String): String = {
    fileOption match {
      case Some(file) if file.exists => renderContentFromFile(file)
      case Some(file)                => s"[$kindPrettyName not found]"
      case None                      => s"[$kindPrettyName not downloaded]"
    }
  }

  private def renderImage(fileOption: Option[File],
                          widthOption: Option[Int],
                          heightOption: Option[Int],
                          altTextOption: Option[String],
                          imagePrettyType: String): String = {
    renderPossiblyMissingContent(fileOption, imagePrettyType)(file => {
      val srcAttr    = Some(s"""src="${fileToLocalUriString(file)}"""")
      val widthAttr  = widthOption map (w => s"""width="${w / 2}"""")
      val heightAttr = heightOption map (h => s"""height="${h / 2}"""")
      val altAttr    = altTextOption map (e => s"""alt="$e"""")
      "<img " + Seq(srcAttr, widthAttr, heightAttr, altAttr).yieldDefined.mkString(" ") + "/>"
    })
  }

  private def fileToLocalUriString(file: File): String = {
    val path = file.getCanonicalPath.replace("\\", "/")
    "file://" + (if (path startsWith "/") "" else "/") + path
  }

  object ServiceMessageHtmlRenderer {
    def render(dao: ChatHistoryDao, cwd: ChatWithDetails, sm: Message.Service): String = {
      val textHtmlOption = renderTextOption(sm.textOption)
      val content = sm match {
        case sm: Message.Service.PhoneCall         => renderPhoneCall(sm)
        case sm: Message.Service.PinMessage        => "Pinned message" + renderSourceMessage(dao, cwd, sm.messageId)
        case sm: Message.Service.WithMembers       => renderMessageWithMembers(cwd, sm)
        case sm: Message.Service.ClearHistory      => s"History cleared"
        case sm: Message.Service.Group.EditTitle   => renderEditTitleMessage(sm)
        case sm: Message.Service.Group.EditPhoto   => renderEditPhotoMessage(sm)
        case sm: Message.Service.Group.MigrateFrom => renderMigratedFrom(sm)
        case sm: Message.Service.Group.MigrateTo   => "Migrated to another group"
      }
      Seq(Some(s"""<div class="system-message">$content</div>"""), textHtmlOption).yieldDefined.mkString
    }

    private def renderPhoneCall(sm: Service.PhoneCall) = {
      Seq(
        Some("Phone call"),
        sm.durationSecOption map (d => s"($d sec)"),
        sm.discardReasonOption map (r => s"($r)")
      ).yieldDefined.mkString(" ")
    }

    private def renderEditTitleMessage(sm: Service.Group.EditTitle) = {
      s"Changed group title to <b>${sm.title}</b>"
    }

    private def renderEditPhotoMessage(sm: Service.Group.EditPhoto) = {
      val image = renderImage(sm.pathOption, sm.widthOption, sm.heightOption, None, "Photo")
      s"Changed group photo<br>$image"
    }

    private def renderMigratedFrom(sm: Service.Group.MigrateFrom) = {
      ("Migrated" + sm.titleOption.map(s => s" from $s").getOrElse("")).trim
    }

    private def renderMessageWithMembers(cwd: ChatWithDetails, sm: Service.WithMembers) = {
      val members = sm.members
        .map(name => renderTitleName(cwd, None, Some(name)))
        .mkString("<ul><li>", "</li><li>", "</li></ul>")
      val content = sm match {
        case sm: Service.Group.Create        => s"Created group <b>${sm.title}</b>"
        case sm: Service.Group.InviteMembers => s"Invited"
        case sm: Service.Group.RemoveMembers => s"Removed"
        case sm: Service.Group.Call          => s"Group call"
      }
      s"$content$members"
    }
  }

  object RichTextHtmlRenderer {
    def render(rt: RichText): String = {
      val components = for (rtel <- rt.components) yield {
        renderComponent(rtel)
      }
      val hiddenLinkOption = rt.components collectFirst { case l: RichText.Link if l.hidden => l }
      val link = hiddenLinkOption map { l =>
        "<p> &gt; Link: " + renderComponent(l.copy(text = l.href, hidden = false))
      } getOrElse ""
      components.mkString + link
    }

    private def renderComponent(rtel: RichText.Element): String = {
      rtel match {
        case RichText.Plain(text)          => text replace ("\n", "<br>")
        case RichText.Bold(text)           => s"<b>${text replace ("\n", "<br>")}</b>"
        case RichText.Italic(text)         => s"<i>${text replace ("\n", "<br>")}</i>"
        case RichText.Underline(text)      => s"<u>${text replace ("\n", "<br>")}</u>"
        case RichText.Strikethrough(text)  => s"<strike>${text replace ("\n", "<br>")}</strike>"
        case link: RichText.Link           => renderLink(link)
        case RichText.PrefmtBlock(text, _) => s"""<pre>${text}</pre>"""
        case RichText.PrefmtInline(text)   => s"""<code>${text}</code>"""
      }
    }

    private def renderLink(rtel: RichText.Link) = {
      if (rtel.hidden) {
        rtel.plainSearchableString
      } else {
        // Space in the end is needed if link is followed by text
        val text = if (rtel.text.nonEmpty) rtel.text else rtel.href
        s"""<a href="${rtel.href}">${text}</a> """
      }
    }
  }

  object ContentHtmlRenderer {
    def render(cwd: ChatWithDetails, ct: Content): String = {
      ct.`val`.value match {
        case ct: ContentSticker       => renderSticker(ct)
        case ct: ContentPhoto         => renderPhoto(ct)
        case ct: ContentVoiceMsg      => renderVoiceMsg(ct)
        case ct: ContentVideoMsg      => renderVideoMsg(ct)
        case ct: ContentAnimation     => renderAnimation(ct)
        case ct: ContentFile          => renderFile(ct)
        case ct: ContentLocation      => renderLocation(ct)
        case ct: ContentPoll          => renderPoll(ct)
        case ct: ContentSharedContact => renderSharedContact(cwd, ct)
      }
    }

    private def renderVoiceMsg(ct: ContentVoiceMsg) = {
      renderPossiblyMissingContent(ct.pathFileOption, "Voice message")(file => {
        val mimeType = s"""type="${ct.mimeType}""""
        val durationOption = ct.durationSec map (d => s"""duration="$d"""")
        // <audio> tag is not impemented by default AWT toolkit, we're plugging custom view
        s"""<audio ${durationOption getOrElse ""} controls>
           |  <source src=${fileToLocalUriString(file)}" ${mimeType}>
           |</audio>""".stripMargin
      })
    }

    private def renderVideoMsg(ct: ContentVideoMsg): String = {
      renderPossiblyMissingContent(ct.pathFileOption, "Video message")(file => {
        // TODO
        "[Video messages not supported yet]"
      })
    }

    private def renderAnimation(ct: ContentAnimation): String = {
      renderPossiblyMissingContent(ct.pathFileOption, "Animation")(file => {
        // TODO
        "[Animations not supported yet]"
      })
    }

    private def renderFile(ct: ContentFile): String = {
      renderPossiblyMissingContent(ct.pathFileOption, "File")(file => {
        // TODO
        "[Files not supported yet]"
      })
    }

    def renderSticker(st: ContentSticker): String = {
      val pathOption = st.pathFileOption orElse st.thumbnailPathFileOption
      renderImage(pathOption, Some(st.width), Some(st.height), st.emoji, "Sticker")
    }

    def renderPhoto(ct: ContentPhoto): String = {
      renderImage(ct.pathFileOption, Some(ct.width), Some(ct.height), None, "Photo")
    }

    def renderLocation(ct: ContentLocation): String = {
      Seq(
        Some(s"<b>${ct.title}</b>"),
        ct.address,
        Some(s"<i>Location:</i> <b>${ct.lat}, ${ct.lon}</b>"),
        ct.durationSec map (s => s"(live for $s s)")
      ).yieldDefined.mkString("<blockquote>", "<br>", "</blockquote>")
    }

    def renderPoll(ct: ContentPoll): String = {
      s"""<blockquote><i>Poll:</i> ${ct.question}</blockquote>"""
    }

    def renderSharedContact(cwd: ChatWithDetails, ct: ContentSharedContact): String = {
      val name  = renderTitleName(cwd, None, Some(ct.prettyName))
      val phone = ct.phoneNumber map (pn => s"(phone: $pn)") getOrElse "(no phone number)"
      s"""<blockquote><i>Shared contact:</i> $name $phone</blockquote>"""
    }
  }
}

object MessagesService {
  case class MessageDocument(
      doc: HTMLDocument,
      contentParent: Element
  ) {

    // We need to account for "p-implied" parasitic element...
    private def pImplied = contentParent.getElement(0)

    def removeLoading(inTheBeginning: Boolean): Unit = {
      val count         = contentParent.getElementCount
      val elementsRange = if (inTheBeginning) (1 until count) else ((count - 1) to 1 by -1)
      val loadingEl = elementsRange.toStream
        .map({println("DUH!"); contentParent.getElement})
        .find(_.getAttributes.getAttribute(HTML.Attribute.ID) == "loading")
      loadingEl.foreach(doc.removeElement)
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
