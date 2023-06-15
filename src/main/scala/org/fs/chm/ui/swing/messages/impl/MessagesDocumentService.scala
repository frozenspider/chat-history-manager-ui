package org.fs.chm.ui.swing.messages.impl

import java.io.File
import java.io.StringReader

import javax.swing.text.Element
import javax.swing.text.html.HTML
import javax.swing.text.html.HTMLDocument
import javax.swing.text.html.HTMLEditorKit

import org.fs.chm.dao.ChatHistoryDao
import org.fs.chm.dao.Entities._
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
import org.fs.chm.protobuf.Message
import org.fs.chm.protobuf.MessageRegular
import org.fs.chm.protobuf.MessageService
import org.fs.chm.protobuf.MessageServiceClearHistory
import org.fs.chm.protobuf.MessageServiceGroupCall
import org.fs.chm.protobuf.MessageServiceGroupCreate
import org.fs.chm.protobuf.MessageServiceGroupEditPhoto
import org.fs.chm.protobuf.MessageServiceGroupEditTitle
import org.fs.chm.protobuf.MessageServiceGroupInviteMembers
import org.fs.chm.protobuf.MessageServiceGroupMigrateFrom
import org.fs.chm.protobuf.MessageServiceGroupMigrateTo
import org.fs.chm.protobuf.MessageServiceGroupRemoveMembers
import org.fs.chm.protobuf.MessageServicePhoneCall
import org.fs.chm.protobuf.MessageServicePinMessage
import org.fs.chm.protobuf.RichTextElement
import org.fs.chm.protobuf.RteBold
import org.fs.chm.protobuf.RteItalic
import org.fs.chm.protobuf.RteLink
import org.fs.chm.protobuf.RtePlain
import org.fs.chm.protobuf.RtePrefmtBlock
import org.fs.chm.protobuf.RtePrefmtInline
import org.fs.chm.protobuf.RteStrikethrough
import org.fs.chm.protobuf.RteUnderline
import org.fs.chm.ui.swing.general.SwingUtils._
import org.fs.chm.utility.EntityUtils
import org.fs.chm.utility.LangUtils._
import org.fs.utility.Imports._

class MessagesDocumentService(htmlKit: HTMLEditorKit) {
  import MessagesDocumentService._


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

  def renderMessageHtml(dao: ChatHistoryDao, cwd: ChatWithDetails, dsRoot: DatasetRoot, m: Message, isQuote: Boolean = false): String = {
    val msgHtml: String = m.typed.value match {
      case rm: MessageRegular =>
        val textHtmlOption = RichTextHtmlRenderer.render(m.text)
        val contentHtmlOption = rm.contentOption map { ct =>
          s"""<div class="content">${ContentHtmlRenderer.render(cwd, dsRoot, ct)}</div>"""
        }
        val fwdFromHtmlOption  = rm.forwardFromNameOption map (n => renderFwdFrom(cwd, n))
        val replySrcHtmlOption = if (isQuote) {
          None
        } else {
          rm.replyToMessageIdTypedOption map (id => renderSourceMessage(dao, cwd, dsRoot, id))
        }
        Seq(fwdFromHtmlOption, replySrcHtmlOption, textHtmlOption, contentHtmlOption).yieldDefined.mkString
      case sm: MessageService =>
        ServiceMessageHtmlRenderer.render(dao, cwd, dsRoot, m, sm)
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
      idOption flatMap (id => cwd.members.find(_.id == id)) map (_.prettyName) getOrElse Unnamed
    }

    s"""<span class="title-name" style="color: $color;">${resolvedName}</span>"""
  }

  private def renderFwdFrom(cwd: ChatWithDetails, fromName: String): String = {
    val titleNameHtml = renderTitleName(cwd, None, Some(fromName))
    s"""<div class="forwarded-from">Forwarded from $titleNameHtml</div>"""
  }

  private def renderSourceMessage(dao: ChatHistoryDao, cwd: ChatWithDetails, dsRoot: DatasetRoot, srcId: MessageSourceId): String = {
    val m2Option = dao.messageOption(cwd.chat, srcId)
    val html = m2Option match {
      case None     => "[Deleted message]"
      case Some(m2) => renderMessageHtml(dao, cwd, dsRoot, m2, true)
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
    def render(dao: ChatHistoryDao, cwd: ChatWithDetails, dsRoot: DatasetRoot, m: Message, sm: MessageService): String = {
      val textHtmlOption = RichTextHtmlRenderer.render(m.text)
      val content = sm.`val`.value match {
        case sm: MessageServicePhoneCall          => renderPhoneCall(sm)
        case sm: MessageServicePinMessage         => "Pinned message" + renderSourceMessage(dao, cwd, dsRoot, sm.messageIdTyped)
        case sm: MessageServiceClearHistory       => s"History cleared"
        case sm: MessageServiceGroupCreate        => renderCreateGroupMessage(cwd, sm)
        case sm: MessageServiceGroupEditTitle     => renderEditTitleMessage(sm)
        case sm: MessageServiceGroupEditPhoto     => renderEditPhotoMessage(sm, dsRoot)
        case sm: MessageServiceGroupInviteMembers => renderGroupInviteMembersMessage(cwd, sm)
        case sm: MessageServiceGroupRemoveMembers => renderGroupRemoveMembersMessage(cwd, sm)
        case sm: MessageServiceGroupMigrateFrom   => renderMigratedFrom(sm)
        case sm: MessageServiceGroupMigrateTo     => "Migrated to another group"
        case sm: MessageServiceGroupCall          => renderGroupCallMessage(cwd, sm)
      }
      Seq(Some(s"""<div class="system-message">$content</div>"""), textHtmlOption).yieldDefined.mkString
    }

    private def renderPhoneCall(sm: MessageServicePhoneCall) = {
      Seq(
        Some("Phone call"),
        sm.durationSecOption map (d => s"($d sec)"),
        sm.discardReasonOption map (r => s"($r)")
      ).yieldDefined.mkString(" ")
    }

    private def renderCreateGroupMessage(cwd: ChatWithDetails, sm: MessageServiceGroupCreate) = {
      val content = s"Created group <b>${sm.title}</b>"
      val members = renderMembers(cwd, sm.members)
      s"$content$members"
    }

    private def renderEditTitleMessage(sm: MessageServiceGroupEditTitle) = {
      s"Changed group title to <b>${sm.title}</b>"
    }

    private def renderEditPhotoMessage(sm: MessageServiceGroupEditPhoto, dsRoot: DatasetRoot) = {
      val image = renderImage(
        sm.photo.pathFileOption(dsRoot),
        Some(sm.photo.width),
        Some(sm.photo.height),
        None, "Photo")
      s"Changed group photo<br>$image"
    }

    private def renderGroupInviteMembersMessage(cwd: ChatWithDetails, sm: MessageServiceGroupInviteMembers) = {
      val content = s"Invited"
      val members = renderMembers(cwd, sm.members)
      s"$content$members"
    }

    private def renderGroupRemoveMembersMessage(cwd: ChatWithDetails, sm: MessageServiceGroupRemoveMembers) = {
      val content = s"Removed"
      val members = renderMembers(cwd, sm.members)
      s"$content$members"
    }

    private def renderMigratedFrom(sm: MessageServiceGroupMigrateFrom) = {
      s"Migrated from ${sm.title}}".trim
    }

    private def renderGroupCallMessage(cwd: ChatWithDetails, sm: MessageServiceGroupCall) = {
      val content = s"Group call"
      val members = renderMembers(cwd, sm.members)
      s"$content$members"
    }

    private def renderMembers(cwd: ChatWithDetails, members: Seq[String]) = {
      members
        .map(name => renderTitleName(cwd, None, Some(name)))
        .mkString("<ul><li>", "</li><li>", "</li></ul>")
    }
  }

  object RichTextHtmlRenderer {
    def render(rtes: Seq[RichTextElement]): Option[String] = {
      if (rtes.isEmpty) {
        None
      } else Some {
        val components = rtes map renderComponent
        val hiddenLink = rtes.zip(rtes.map(_.`val`.link)) collectFirst {
          case (rte, Some(link)) if link.hidden =>
            "<p> &gt; Link: " + renderLink(rte, link.copy(textOption = Some(link.href), hidden = false))
        } getOrElse ""
        s"""<div class="text">${components.mkString + hiddenLink}</div>"""
      }
    }

    private def renderComponent(rt: RichTextElement): String = {
      val text = rt.textOrEmptyString
      if (rt.`val`.isEmpty) {
        text
      } else rt.`val`.value match {
        case _: RtePlain         => text replace ("\n", "<br>")
        case _: RteBold          => s"<b>${text replace ("\n", "<br>")}</b>"
        case _: RteItalic        => s"<i>${text replace ("\n", "<br>")}</i>"
        case _: RteUnderline     => s"<u>${text replace ("\n", "<br>")}</u>"
        case _: RteStrikethrough => s"<strike>${text replace ("\n", "<br>")}</strike>"
        case link: RteLink       => renderLink(rt, link)
        case _: RtePrefmtBlock   => s"""<pre>${text}</pre>"""
        case _: RtePrefmtInline  => s"""<code>${text}</code>"""
      }
    }

    private def renderLink(rt: RichTextElement, link: RteLink): String = {
      if (link.hidden) {
        rt.searchableString.get
      } else {
        // Space in the end is needed if link is followed by text
        val text = link.textOption.getOrElse(link.href)
        s"""<a href="${link.href}">${text}</a> """
      }
    }
  }

  object ContentHtmlRenderer {
    def render(cwd: ChatWithDetails, dsRoot: DatasetRoot, ct: Content): String = {
      ct.`val`.value match {
        case ct: ContentSticker       => renderSticker(ct, dsRoot)
        case ct: ContentPhoto         => renderPhoto(ct, dsRoot)
        case ct: ContentVoiceMsg      => renderVoiceMsg(ct, dsRoot)
        case ct: ContentVideoMsg      => renderVideoMsg(ct, dsRoot)
        case ct: ContentAnimation     => renderAnimation(ct, dsRoot)
        case ct: ContentFile          => renderFile(ct, dsRoot)
        case ct: ContentLocation      => renderLocation(ct)
        case ct: ContentPoll          => renderPoll(ct)
        case ct: ContentSharedContact => renderSharedContact(cwd, ct)
      }
    }

    private def renderVoiceMsg(ct: ContentVoiceMsg, dsRoot: DatasetRoot) = {
      renderPossiblyMissingContent(ct.pathFileOption(dsRoot), "Voice message")(file => {
        val mimeType = s"""type="${ct.mimeType}""""
        val durationOption = ct.durationSecOption map (d => s"""duration="$d"""")
        // <audio> tag is not impemented by default AWT toolkit, we're plugging custom view
        s"""<audio ${durationOption getOrElse ""} controls>
           |  <source src=${fileToLocalUriString(file)}" ${mimeType}>
           |</audio>""".stripMargin
      })
    }

    private def renderVideoMsg(ct: ContentVideoMsg, dsRoot: DatasetRoot): String = {
      renderPossiblyMissingContent(ct.pathFileOption(dsRoot), "Video message")(file => {
        // TODO
        "[Video messages not supported yet]"
      })
    }

    private def renderAnimation(ct: ContentAnimation, dsRoot: DatasetRoot): String = {
      renderPossiblyMissingContent(ct.pathFileOption(dsRoot), "Animation")(file => {
        // TODO
        "[Animations not supported yet]"
      })
    }

    private def renderFile(ct: ContentFile, dsRoot: DatasetRoot): String = {
      renderPossiblyMissingContent(ct.pathFileOption(dsRoot), "File")(file => {
        // TODO
        "[Files not supported yet]"
      })
    }

    def renderSticker(st: ContentSticker, dsRoot: DatasetRoot): String = {
      val pathOption = st.pathFileOption(dsRoot) orElse st.thumbnailPathFileOption(dsRoot)
      renderImage(pathOption, Some(st.width), Some(st.height), st.emojiOption, "Sticker")
    }

    def renderPhoto(ct: ContentPhoto, dsRoot: DatasetRoot): String = {
      renderImage(ct.pathFileOption(dsRoot), Some(ct.width), Some(ct.height), None, "Photo")
    }

    def renderLocation(ct: ContentLocation): String = {
      Seq(
        Some(s"<b>${ct.titleOption}</b>"),
        ct.addressOption,
        Some(s"<i>Location:</i> <b>${ct.lat}, ${ct.lon}</b>"),
        ct.durationSecOption map (s => s"(live for $s s)")
      ).yieldDefined.mkString("<blockquote>", "<br>", "</blockquote>")
    }

    def renderPoll(ct: ContentPoll): String = {
      s"""<blockquote><i>Poll:</i> ${ct.question}</blockquote>"""
    }

    def renderSharedContact(cwd: ChatWithDetails, ct: ContentSharedContact): String = {
      val name  = renderTitleName(cwd, None, Some(ct.prettyName))
      val phone = ct.phoneNumberOption map (pn => s"(phone: $pn)") getOrElse "(no phone number)"
      s"""<blockquote><i>Shared contact:</i> $name $phone</blockquote>"""
    }
  }
}

object MessagesDocumentService {
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
