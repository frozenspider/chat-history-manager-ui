package org.fs.chm.ui.swing

import java.io.File
import java.io.StringReader

import javax.swing.text.Element
import javax.swing.text.html.HTMLDocument
import javax.swing.text.html.HTMLEditorKit
import org.fs.chm.dao.Message.Service
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
        |     body {
        |       font-family: arial,sans-serif;
        |     }
        |     .title {
        |       padding-left: 5px;
        |       padding-bottom: 5px;
        |       font-size: 105%;
        |     }
        |     .forwarded-from {
        |       padding-left: 5px;
        |       padding-bottom: 5px;
        |       font-size: 105%;
        |       color: #909090;
        |     }
        |     .title-name {
        |       font-weight: bold;
        |     }
        |     blockquote {
        |        border-left: 1px solid #ccc;
        |        margin: 5px 10px;
        |        padding: 5px 10px;
        |     }
        |     .system-message {
        |        border: 1px solid #A0A0A0;
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

  lazy val pleaseWaitDoc: HTMLDocument = {
    val md = createStubDoc
    md.insert("<div><h1>Please wait...</h1></div>", MessageInsertPosition.Leading)
    md.doc
  }

  //
  // Renderers and helpers
  //

  def renderMessageHtml(c: Chat, m: Message, isQuote: Boolean = false): String = {
    val msgHtml: String = m match {
      case m: Message.Regular =>
        val textHtmlOption = renderTextOption(m.textOption)
        val contentHtmlOption = m.contentOption map { ct =>
          s"""<div class="content">${ContentHtmlRenderer.render(c, ct)}</div>"""
        }
        val fwdFromHtmlOption  = m.forwardFromNameOption map (n => renderFwdFrom(c, n))
        val replySrcHtmlOption = if (isQuote) None else m.replyToMessageIdOption map (id => renderSourceMessage(c, id))
        Seq(fwdFromHtmlOption, replySrcHtmlOption, textHtmlOption, contentHtmlOption).yieldDefined.mkString
      case sm: Message.Service =>
        ServiceMessageHtmlRenderer.render(c, sm)
    }
    val titleNameHtml = renderTitleName(c, Some(m.fromId), m.fromName)
    val titleHtml =
      s"""$titleNameHtml (${m.time.toString("yyyy-MM-dd HH:mm")})"""
    s"""
       |<div class="message" message_id="${m.id}">
       |   <div class="title">${titleHtml}</div>
       |   <div class="body">${msgHtml}</div>
       |</div>
       |${if (!isQuote) "<p>" else ""}
    """.stripMargin // TODO: Remove <p>
  }

  private def renderTitleName(c: Chat, idOption: Option[Long], name: String): String = {
    val intl = dao.interlocutors(c)
    val idx = {
      val idx1 = idOption map (id => intl indexWhere (_.id == id)) getOrElse -1
      val idx2 = intl indexWhere (_.prettyName == name)
      if (idx1 != -1) idx1 else idx2
    }
    val color = if (idx >= 0) NameColors(idx % NameColors.length) else "#000000"
    s"""<span class="title-name" style="color: $color;">$name</span>"""
  }

  private def renderTextOption(textOption: Option[RichText]): Option[String] =
    textOption map { rt =>
      s"""<div class="text">${RichTextHtmlRenderer.render(rt)}</div>"""
    }

  private def renderFwdFrom(c: Chat, fromName: String): String = {
    val titleNameHtml = renderTitleName(c, None, fromName)
    s"""<div class="forwarded-from">Forwarded from $titleNameHtml</div>"""
  }

  private def renderSourceMessage(c: Chat, srcId: Long): String = {
    val m2Option = dao.messageOption(c, srcId)
    val html = m2Option match {
      case None     => "[Deleted message]"
      case Some(m2) => renderMessageHtml(c, m2, true)
    }
    s"""<blockquote>$html</blockquote> """
  }

  private def renderPossiblyMissingContent(
      pathOption: Option[String],
      kindPrettyName: String
  )(renderContentFromFile: File => String): String = {
    val fileOption = pathOption map (new File(dao.dataPath, _))
    fileOption match {
      case Some(file) if file.exists => renderContentFromFile(file)
      case Some(file)                => s"[$kindPrettyName not found]"
      case None                      => s"[$kindPrettyName not downloaded]"
    }
  }

  private def renderImage(pathOption: Option[String],
                          widthOption: Option[Int],
                          heightOption: Option[Int],
                          altTextOption: Option[String],
                          imagePrettyType: String): String = {
    renderPossiblyMissingContent(pathOption, imagePrettyType)(file => {
      val srcAttr    = Some(s"""src="${fileToLocalUriString(file)}"""")
      val widthAttr  = widthOption map (w => s"""width="${w / 2}"""")
      val heightAttr = heightOption map (h => s"""height="${h / 2}"""")
      val altAttr    = altTextOption map (e => s"""alt="$e"""")
      "<img " + Seq(srcAttr, widthAttr, heightAttr, altAttr).yieldDefined.mkString(" ") + "/>"
    })
  }

  private def fileToLocalUriString(file: File): String = {
    "file:///" + file.getCanonicalPath.replace("\\", "/")
  }

  object ServiceMessageHtmlRenderer {
    def render(c: Chat, sm: Message.Service): String = {
      val textHtmlOption = renderTextOption(sm.textOption)
      val content = sm match {
        case sm: Message.Service.PhoneCall        => renderPhoneCall(sm)
        case sm: Message.Service.PinMessage       => "Pinned message" + renderSourceMessage(c, sm.messageId)
        case sm: Message.Service.MembershipChange => renderMembershipChangeMessage(c, sm)
        case sm: Message.Service.EditGroupPhoto   => renderEditGroupPhotoMessage(sm)
        case sm: Message.Service.ClearHistory     => s"History cleared"
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

    private def renderEditGroupPhotoMessage(sm: Service.EditGroupPhoto) = {
      val image = renderImage(sm.pathOption, sm.widthOption, sm.heightOption, None, "Photo")
      s"Changed group photo<br>$image"
    }

    private def renderMembershipChangeMessage(c: Chat, sm: Service.MembershipChange) = {
      val members = sm.members
        .map(name => renderTitleName(c, None, name))
        .mkString("<ul><li>", "</li><li>", "</li></ul>")
      val content = sm match {
        case sm: Service.CreateGroup        => s"Created group <b>${sm.title}</b>"
        case sm: Service.InviteGroupMembers => s"Invited"
        case sm: Service.RemoveGroupMembers => s"Removed"
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
        "<p> &gt; Link: " + renderComponent(l.copy(textOption = Some(l.href), hidden = false))
      } getOrElse ""
      components.mkString + link
    }

    private def renderComponent(rtel: RichText.Element): String = {
      rtel match {
        case rtel: RichText.Plain        => rtel.text replace ("\n", "<br>")
        case rtel: RichText.Bold         => s"<b>${rtel.text replace ("\n", "<br>")}</b>"
        case rtel: RichText.Italic       => s"<i>${rtel.text replace ("\n", "<br>")}</i>"
        case rtel: RichText.Link         => renderLink(rtel)
        case rtel: RichText.PrefmtBlock  => s"""<pre>${rtel.text}</pre>"""
        case rtel: RichText.PrefmtInline => s"""<code>${rtel.text}</code>"""
      }
    }

    private def renderLink(rtel: RichText.Link) = {
      if (rtel.hidden) {
        rtel.plainSearchableString
      } else {
        // Space in the end is needed if link is followed by text
        val text = rtel.textOption getOrElse rtel.href
        s"""<a href="${rtel.href}">${text}</a> """
      }
    }
  }

  object ContentHtmlRenderer {
    def render(c: Chat, ct: Content): String = {
      ct match {
        case ct: Content.Sticker       => renderSticker(ct)
        case ct: Content.Photo         => renderPhoto(ct)
        case ct: Content.VoiceMsg      => renderVoiceMsg(ct)
        case ct: Content.VideoMsg      => renderVideoMsg(ct)
        case ct: Content.Animation     => renderAnimation(ct)
        case ct: Content.File          => renderFile(ct)
        case ct: Content.Location      => renderLocation(ct)
        case ct: Content.Poll          => renderPoll(ct)
        case ct: Content.SharedContact => renderSharedContact(c, ct)
      }
    }

    private def renderVoiceMsg(ct: Content.VoiceMsg) = {
      renderPossiblyMissingContent(ct.pathOption, "Voice message")(file => {
        val mimeTypeOption = ct.mimeTypeOption map (mt => s"""type="$mt"""")
        val durationOption = ct.durationSecOption map (d => s"""duration="$d"""")
        // <audio> tag is not impemented by default AWT toolkit, we're plugging custom view
        s"""<audio ${durationOption getOrElse ""} controls>
           |  <source src=${fileToLocalUriString(file)}" ${mimeTypeOption getOrElse ""}>
           |</audio>""".stripMargin
      })
    }

    private def renderVideoMsg(ct: Content.VideoMsg): String = {
      // TODO
      "[Video messages not supported yet]"
    }

    private def renderAnimation(ct: Content.Animation): String = {
      // TODO
      "[Animations not supported yet]"
    }

    private def renderFile(ct: Content.File): String = {
      // TODO
      "[Files not supported yet]"
    }

    def renderSticker(st: Content.Sticker): String = {
      val pathOption = st.pathOption orElse st.thumbnailPathOption
      renderImage(pathOption, st.widthOption, st.heightOption, st.emojiOption, "Sticker")
    }

    def renderPhoto(ct: Content.Photo): String = {
      renderImage(ct.pathOption, Some(ct.width), Some(ct.height), None, "Photo")
    }

    def renderLocation(ct: Content.Location): String = {
      val liveString = ct.liveDurationSecOption map (s => s"(live for $s s)") getOrElse ""
      s"""<blockquote><i>Location:</i> <b>${ct.lat}, ${ct.lon}</b> $liveString<br></blockquote>"""
    }

    def renderPoll(ct: Content.Poll): String = {
      s"""<blockquote><i>Poll:</i> ${ct.question}</blockquote>"""
    }

    def renderSharedContact(c: Chat, ct: Content.SharedContact): String = {
      val name  = renderTitleName(c, None, ct.prettyName)
      val phone = ct.phoneNumberOption map (pn => s"(phone: $pn)") getOrElse "(no phone number)"
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
