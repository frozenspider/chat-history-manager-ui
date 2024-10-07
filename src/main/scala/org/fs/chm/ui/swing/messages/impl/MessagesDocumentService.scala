package org.fs.chm.ui.swing.messages.impl

import java.io.File
import java.io.StringReader

import javax.swing.text.Element
import javax.swing.text.html.HTML

import com.github.nscala_time.time.Imports._
import org.fs.chm.dao.ChatHistoryDao
import org.fs.chm.dao.Entities._
import org.fs.chm.protobuf._
import org.fs.chm.ui.swing.general.CustomHtmlDocument
import org.fs.chm.ui.swing.general.ExtendedHtmlEditorKit
import org.fs.chm.ui.swing.general.SwingUtils._
import org.fs.chm.utility.EntityUtils
import org.fs.chm.utility.LangUtils._
import org.fs.utility.Imports._

class MessagesDocumentService(val htmlKit: ExtendedHtmlEditorKit) {
  import MessagesDocumentService._

  val stubCss =
    """|body {
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

  def createStubDoc: MessageDocument = {
    // TODO: How to limit width and do word wraps?
    //       width, max-width acts weird,
    //       word-wrap, word-break, overflow-wrap are ignored
    // TODO: Handle emojis!
    //       Importing font with
    //         @font-face {
    //           font-family: my-font;
    //           src: url(/full/path/to/font);
    //         }
    //       doesn't seem to work
    val doc     = htmlKit.createDefaultDocument
    val content = """<div id="messages"></div>"""
    htmlKit.read(new StringReader(content), doc, 0)
    doc.getStyleSheet.addRule(stubCss)
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

  def renderMessageHtml(dao: ChatHistoryDao,
                        cc: CombinedChat,
                        dsRoot: DatasetRoot,
                        m: Message,
                        showSeconds: Boolean,
                        isQuote: Boolean = false): String = {
    val (msgHtml: String, editDtOption: Option[DateTime], isDeleted: Boolean) = m.typed match {
      case Message.Typed.Regular(rm) =>
        val textHtmlOption = RichTextHtmlRenderer.render(m.text)
        val contentHtmlOption = (rm.contents map (_.get) map { ct =>
          s"""<div class="content">${ContentHtmlRenderer.render(cc, dsRoot, ct)}</div>"""
        }) match {
          case seq if seq.isEmpty => None
          case seq                => Some(seq mkString "")
        }
        val fwdFromHtmlOption  = rm.forwardFromNameOption map (n => renderFwdFrom(cc, n))
        val replySrcHtmlOption = if (isQuote) {
          None
        } else {
          rm.replyToMessageIdTypedOption map (id => renderSourceMessage(dao, cc, dsRoot, id))
        }
        val msgDeletedOption = if (rm.isDeleted) {
          Some(s"""<div class="system-message">Message deleted</div>""")
        } else None
        (Seq(msgDeletedOption, fwdFromHtmlOption, replySrcHtmlOption, textHtmlOption, contentHtmlOption).yieldDefined.mkString,
         rm.editTimeOption,
         rm.isDeleted)
      case Message.Typed.Service(Some(sm)) =>
        (ServiceMessageHtmlRenderer.render(dao, cc, dsRoot, m, sm),
         None,
         false)
    }
    val dtFormat = if (showSeconds) "yyyy-MM-dd HH:mm:ss" else "yyyy-MM-dd HH:mm"
    val editTimeHtml = editDtOption match {
      case Some(dt) => s""" <span style="color: gray;">(${if (isDeleted) "deleted" else "edited"} ${dt.toString(dtFormat)})</span>"""
      case None     => ""
    }
    val titleNameHtml = renderTitleName(cc, Some(m.fromId), None)
    val titleHtml =
      s"""$titleNameHtml (${m.time.toString(dtFormat)})$editTimeHtml"""
    val sourceIdAttr = m.sourceIdOption map (id => s"""message_source_id="$id"""") getOrElse ""
    // "date" attribute is used by overlay to show topmost message date
    s"""|<div class="message" ${sourceIdAttr} date="${m.time.toString("yyyy-MM-dd")}">
        |   <div class="title">${titleHtml}</div>
        |   <div class="body">${msgHtml}</div>
        |</div>
        |${if (!isQuote) "<p>" else ""}
    """.stripMargin // TODO: Remove <p>
  }

  private def renderTitleName(cc: CombinedChat, idOption: Option[Long], nameOption: Option[String]): String = {
    val idx = {
      idOption map (id => cc.members indexWhere (_.id == id)) getOrElse (nameOption.map(cc.resolveMemberIndex) getOrElse -1)
    }
    val color = if (idx >= 0) Colors.stringForIdx(idx) else "#000000"
    val resolvedName = nameOption getOrElse {
      idOption flatMap (id => cc.members.find(_.id == id)) map (_.prettyName) getOrElse Unnamed
    }

    s"""<span class="title-name" style="color: $color;">${toHtmlPlaintext(resolvedName)}</span>"""
  }

  private def renderFwdFrom(cc: CombinedChat, fromName: String): String = {
    val titleNameHtml = renderTitleName(cc, None, Some(fromName))
    s"""<div class="forwarded-from">Forwarded from $titleNameHtml</div>"""
  }

  private def renderSourceMessage(dao: ChatHistoryDao, cc: CombinedChat, dsRoot: DatasetRoot, srcId: MessageSourceId): String = {
    val m2Option = cc.cwds.map(cwd => dao.messageOption(cwd.chat, srcId)).yieldDefined.headOption
    val html = m2Option match {
      case None     => "[Deleted message]"
      case Some(m2) => renderMessageHtml(dao, cc, dsRoot, m2, true)
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

  private def renderPossiblyMissingPhoto(fileOption: Option[File],
                                         widthOption: Option[Int],
                                         heightOption: Option[Int],
                                         altTextOption: Option[String],
                                         isOneTime: Boolean): String = {
    val name = if (isOneTime) "One-time photo" else "Photo"
    renderPossiblyMissingContent(fileOption, name)(renderImage(_, widthOption, heightOption, altTextOption, isOneTime))
  }

  private def renderImage(file: File,
                          widthOption: Option[Int],
                          heightOption: Option[Int],
                          altTextOption: Option[String],
                          isOneTime: Boolean): String = {
    val srcAttr = Some(s"""src="${fileToLocalUriString(file)}"""")
    val widthAttr = widthOption map (w => s"""width="${w / 2}"""")
    val heightAttr = heightOption map (h => s"""height="${h / 2}"""")
    val altAttr = altTextOption map (e => s"""alt="$e"""")
    "<img " + Seq(srcAttr, widthAttr, heightAttr, altAttr).yieldDefined.mkString(" ") + "/>"
  }

  private def fileToLocalUriString(file: File): String = {
    val path = file.getCanonicalPath.replace("\\", "/")
    "file://" + (if (path startsWith "/") "" else "/") + path
  }

  /** Replace non-HTML-renderable characters with, well, renderable */
  private def toHtmlPlaintext(text: String): String = {
    text replace("<", "&lt;") replace(">", "&gt;") replace("\n", "<br>")
  }

  object ServiceMessageHtmlRenderer {
    def render(dao: ChatHistoryDao, cc: CombinedChat, dsRoot: DatasetRoot, m: Message, sm: MessageService): String = {
      val textHtmlOption = sm match {
        case _: MessageServiceStatusTextChanged => None // Will be rendered inline
        case _: MessageServiceNotice            => None // Will be rendered inline
        case _                                  => RichTextHtmlRenderer.render(m.text)
      }
      val content = sm match {
        case sm: MessageServicePhoneCall           => renderCallMessage(cc, sm)
        case sm: MessageServiceSuggestProfilePhoto => renderSuggestPhotoMessage(sm, dsRoot)
        case sm: MessageServicePinMessage          => "Pinned message" + renderSourceMessage(dao, cc, dsRoot, sm.messageIdTyped)
        case sm: MessageServiceClearHistory        => "History cleared"
        case sm: MessageServiceBlockUser           => s"User ${if (sm.isBlocked) "" else "un"}blocked"
        case sm: MessageServiceStatusTextChanged   => renderStatusTextChanged(m.text)
        case sm: MessageServiceNotice              => renderNotice(m.text)
        case sm: MessageServiceGroupCreate         => renderCreateGroupMessage(cc, sm)
        case sm: MessageServiceGroupEditTitle      => renderEditTitleMessage(sm)
        case sm: MessageServiceGroupEditPhoto      => renderEditPhotoMessage(sm, dsRoot)
        case sm: MessageServiceGroupDeletePhoto    => "Deleted group photo"
        case sm: MessageServiceGroupInviteMembers  => renderGroupInviteMembersMessage(cc, sm)
        case sm: MessageServiceGroupRemoveMembers  => renderGroupRemoveMembersMessage(cc, sm)
        case sm: MessageServiceGroupMigrateFrom    => renderMigratedFrom(sm)
        case sm: MessageServiceGroupMigrateTo      => "Migrated to another group"
      }
      Seq(Some(s"""<div class="system-message">$content</div>"""), textHtmlOption).yieldDefined.mkString
    }

    private def renderCallMessage(cc: CombinedChat, sm: MessageServicePhoneCall) = {
      Seq(
        Some("Call"),
        sm.durationSecOption map {
          case d if d < 60 =>
            s"($d sec)"
          case d if d < 3600 =>
            s"(${(d * 1000).hhMmSsString.dropWhile(c => c == '0' || c == ':')})"
          case d =>
            s"(${(d * 1000).hhMmSsString})"
        },
        renderMembers(cc, sm.members).toOption,
        sm.discardReasonOption filter (_ != "hangup") map (r => s"($r)")
      ).yieldDefined.mkString(" ")
    }

    private def renderStatusTextChanged(text: Seq[RichTextElement]) = {
      "<i>" + toHtmlPlaintext("<Status>") + "</i>" + RichTextHtmlRenderer.render(text).map(s => "<br>" + s).getOrElse("")
    }

    private def renderNotice(text: Seq[RichTextElement]) = {
      "<i>" + toHtmlPlaintext("<Notice>") + "</i>" + RichTextHtmlRenderer.render(text).map(s => "<br>" + s).getOrElse("")
    }

    private def renderCreateGroupMessage(cc: CombinedChat, sm: MessageServiceGroupCreate) = {
      val content = s"Created group <b>${sm.title}</b>"
      val members = renderMembers(cc, sm.members)
      s"$content$members"
    }

    private def renderEditTitleMessage(sm: MessageServiceGroupEditTitle) = {
      s"Changed group title to <b>${sm.title}</b>"
    }

    private def renderSuggestPhotoMessage(sm: MessageServiceSuggestProfilePhoto, dsRoot: DatasetRoot) = {
      val image = renderPossiblyMissingPhoto(
        sm.photo.pathFileOption(dsRoot),
        Some(sm.photo.width),
        Some(sm.photo.height),
        None,
        isOneTime = false)
      s"Suggested profile photo<br>$image"
    }

    private def renderEditPhotoMessage(sm: MessageServiceGroupEditPhoto, dsRoot: DatasetRoot) = {
      val image = renderPossiblyMissingPhoto(
        sm.photo.pathFileOption(dsRoot),
        Some(sm.photo.width),
        Some(sm.photo.height),
        None,
        isOneTime = false)
      s"Changed group photo<br>$image"
    }

    private def renderGroupInviteMembersMessage(cc: CombinedChat, sm: MessageServiceGroupInviteMembers) = {
      val content = s"Invited"
      val members = renderMembers(cc, sm.members)
      s"$content$members"
    }

    private def renderGroupRemoveMembersMessage(cc: CombinedChat, sm: MessageServiceGroupRemoveMembers) = {
      val content = s"Removed"
      val members = renderMembers(cc, sm.members)
      s"$content$members"
    }

    private def renderMigratedFrom(sm: MessageServiceGroupMigrateFrom) = {
      s"Migrated from ${sm.title}".trim
    }

    private def renderMembers(cc: CombinedChat, members: Seq[String]) = {
      if (members.isEmpty) ""
      else members
        .map(name => renderTitleName(cc, None, Some(name)))
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
        case _: RtePlain         => toHtmlPlaintext(text)
        case _: RteBold          => s"<b>${toHtmlPlaintext(text)}</b>"
        case _: RteItalic        => s"<i>${toHtmlPlaintext(text)}</i>"
        case _: RteUnderline     => s"<u>${toHtmlPlaintext(text)}</u>"
        case _: RteStrikethrough => s"<strike>${toHtmlPlaintext(text)}</strike>"
        case _: RteBlockquote    => s"<blockquote>${toHtmlPlaintext(text)}</blockquote>"
        case _: RteSpoiler       => s"<i><strike>${toHtmlPlaintext(text)}</strike></i>"
        case link: RteLink       => renderLink(rt, link)
        case _: RtePrefmtBlock   => s"""<pre>${text}</pre>"""
        case _: RtePrefmtInline  => s"""<code>${text}</code>"""
      }
    }

    private def renderLink(rt: RichTextElement, link: RteLink): String = {
      if (link.hidden) {
        rt.searchableString
      } else {
        // Space in the end is needed if link is followed by text
        val text = link.textOption.getOrElse(link.href)
        s"""<a href="${link.href}">${text}</a> """
      }
    }
  }

  object ContentHtmlRenderer {
    def render(cc: CombinedChat, dsRoot: DatasetRoot, ct: Content): String = {
      require(dsRoot != null, "dsRoot was null!")
      ct match {
        case ct: ContentSticker       => renderSticker(ct, dsRoot)
        case ct: ContentPhoto         => renderPhoto(ct, dsRoot)
        case ct: ContentVoiceMsg      => renderVoiceMsg(ct, dsRoot)
        case ct: ContentAudio         => renderAudio(ct, dsRoot)
        case ct: ContentVideoMsg      => renderVideoMsg(ct, dsRoot)
        case ct: ContentVideo         => renderVideo(ct, dsRoot)
        case ct: ContentFile          => renderFile(ct, dsRoot)
        case ct: ContentLocation      => renderLocation(ct)
        case ct: ContentPoll          => renderPoll(ct)
        case ct: ContentSharedContact => renderSharedContact(cc, ct)
      }
    }

    private def renderVoiceMsg(ct: ContentVoiceMsg, dsRoot: DatasetRoot) = {
      renderPossiblyMissingContent(ct.pathFileOption(dsRoot), "Voice message")(file => {
        val mimeType = s"""type="${ct.mimeType}""""
        val durationOption = ct.durationSecOption map (d => s"""duration="$d"""")
        // <audio> tag is not impemented by default AWT toolkit, we're plugging custom view
        s"""<audio ${durationOption getOrElse ""} controls>
           |  <source src="${fileToLocalUriString(file)}" ${mimeType}>
           |</audio>""".stripMargin
      })
    }

    private def renderAudio(ct: ContentAudio, dsRoot: DatasetRoot) = {
      val titleOption = Seq(ct.performerOption, ct.titleOption).yieldDefined match {
        case seq if seq.nonEmpty => Some(s"<b>${seq.mkString(" - ").trim}</b>")
        case _                   => None
      }
      val content = renderPossiblyMissingContent(ct.pathFileOption(dsRoot), "Audio")(file => {
        val mimeType = s"""type="${ct.mimeType}""""
        val durationOption = ct.durationSecOption map (d => s"""duration="$d"""")
        // <audio> tag is not impemented by default AWT toolkit, we're plugging custom view
        s"""<audio ${durationOption getOrElse ""} controls>
           |  <source src="${fileToLocalUriString(file)}" ${mimeType}>
           |</audio>""".stripMargin
      })
      Seq(titleOption, Some(content)).yieldDefined.mkString("<blockquote>", "<br>", "</blockquote>")
    }

    private def renderVideoMsg(ct: ContentVideoMsg, dsRoot: DatasetRoot): String = {
      // TODO: Support actual video messages, but for now, thumbnail will do
      val name = if (ct.isOneTime) "One-time video" else "Video"
      val fileOption = ct.pathFileOption(dsRoot);
      val thumbOption = ct.thumbnailPathFileOption(dsRoot)
      if (thumbOption.isEmpty && fileOption.isDefined && fileOption.get.exists) {
        s"[$name has no thumbnail]"
      } else renderPossiblyMissingContent(thumbOption, s"$name (thumbnail)")(file => {
        s"[$name]<br>" +
          renderImage(file, Some(ct.width), Some(ct.height), Some(name), ct.isOneTime)
      })
    }

    private def renderVideo(ct: ContentVideo, dsRoot: DatasetRoot): String = {
      val titleOption = Seq(ct.performerOption, ct.titleOption).yieldDefined match {
        case seq if seq.nonEmpty => Some(s"<b>${seq.mkString(" - ").trim}</b>")
        case _                   => None
      }
      // TODO: Support actual video, but for now, thumbnail will do
      val name = if (ct.isOneTime) "One-time video" else "Video"
      val fileOption = ct.pathFileOption(dsRoot);
      val thumbOption = ct.thumbnailPathFileOption(dsRoot)
      val content = if (thumbOption.isEmpty && fileOption.isDefined && fileOption.get.exists) {
        s"[$name has no thumbnail]"
      } else renderPossiblyMissingContent(thumbOption, s"$name (thumbnail)")(file => {
        s"[$name]<br>" +
          renderImage(file, Some(ct.width), Some(ct.height), Some(name), ct.isOneTime)
      })
      Seq(titleOption, Some(content)).yieldDefined.mkString("<blockquote>", "<br>", "</blockquote>")
    }

    private def renderFile(ct: ContentFile, dsRoot: DatasetRoot): String = {
      ct.thumbnailPathFileOption(dsRoot) match {
        case to @ Some(_) =>
          renderPossiblyMissingContent(to, "File (thumbnail)")(file => {
            "[File]<br>" +
              renderImage(file, None, None, Some("[File]"), isOneTime = false)
          })
        case None =>
          s"<blockquote>File: <b>${ct.fileNameOption.map(toHtmlPlaintext).getOrElse(Unnamed)}</b></blockquote>"
      }
    }

    def renderSticker(st: ContentSticker, dsRoot: DatasetRoot): String = {
      // TODO: Support actual animated stickers, but for now, thumbnail will do
      val isAnimatedSticker = st.pathFileOption(dsRoot).exists(f => {
        val name = f.getName.toLowerCase
        name.endsWith(".tgs") || name.endsWith(".webm")
      })
      val pathOption = st.pathFileOption(dsRoot).filter(_ => !isAnimatedSticker) orElse st.thumbnailPathFileOption(dsRoot)
      renderPossiblyMissingContent(pathOption, "Sticker")(sticker => {
        (if (isAnimatedSticker) "[Animated sticker]<br>" else "") +
          renderImage(sticker, Some(st.width), Some(st.height), st.emojiOption, isOneTime = false)
      })
    }

    def renderPhoto(ct: ContentPhoto, dsRoot: DatasetRoot): String = {
      renderPossiblyMissingPhoto(ct.pathFileOption(dsRoot), Some(ct.width), Some(ct.height), None, ct.isOneTime)
    }

    def renderLocation(ct: ContentLocation): String = {
      Seq(
        ct.titleOption.map(title => s"<b>$title</b>"),
        ct.addressOption,
        Some(s"<i>Location:</i> <b>${ct.lat}, ${ct.lon}</b>"),
        ct.durationSecOption map (s => s"(live for $s s)")
      ).yieldDefined.mkString("<blockquote>", "<br>", "</blockquote>")
    }

    def renderPoll(ct: ContentPoll): String = {
      s"""<blockquote><i>Poll:</i> ${ct.question}</blockquote>"""
    }

    def renderSharedContact(cc: CombinedChat, ct: ContentSharedContact): String = {
      val name  = renderTitleName(cc, None, Some(ct.prettyName))
      val phone = ct.phoneNumberOption map (pn => s"(phone: $pn)") getOrElse "(no phone number)"
      s"""<blockquote><i>Shared contact:</i> $name $phone</blockquote>"""
    }
  }
}

object MessagesDocumentService {
  case class MessageDocument(
      doc: CustomHtmlDocument,
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
