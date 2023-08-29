package org.fs.chm.loader.test_only

import java.io.File

import com.github.nscala_time.time.Imports._
import org.fs.chm.dao.Entities._
import org.fs.chm.protobuf._
import org.fs.chm.utility.LangUtils._
import org.json4s._

trait TelegramDataLoaderCommon {
  implicit protected val formats: Formats = DefaultFormats.withLong.withBigDecimal

  /** Starting with Telegram 2020-10, user IDs are shifted by this value */
  private val UserIdShift: Long = 0x100000000L

  /** Starting with Telegram 2021-05, personal chat IDs are un-shifted by this value */
  private val PersonalChatIdShift: Long = 0x100000000L

  /** Starting with Telegram 2021-05, personal chat IDs are un-shifted by this value */
  private val GroupChatIdShift: Long = PersonalChatIdShift * 2

  protected def normalize(u: User): User = {
    if (u.id >= UserIdShift) {
      u.copy(id = u.id - UserIdShift)
    } else {
      u
    }
  }

  protected def normalize(su: ShortUser): ShortUser = {
    if (su.id >= UserIdShift) {
      su.copy(id = su.id - UserIdShift)
    } else {
      su
    }
  }

  protected def normalize(m: Message): Message = {
    if (m.fromId >= UserIdShift) {
      m.withFromId(m.fromId - UserIdShift)
    } else {
      m
    }
  }

  protected object MessageParser {
    def parseMessageOption(jv: JValue): Option[Message] = {
      (getCheckedField[String](jv, "type") match {
        case "message" => Some(parseRegular(jv))
        case "service" => parseService(jv)
        case "unsupported" =>
          // Not enough data is provided even for a placeholder
          None
        case other =>
          throw new IllegalArgumentException(
            s"Don't know how to parse message of type '$other' for ${jv.toString.take(500)}")
      }) map (normalize)
    }

    private def parseRegular(jv: JValue): Message = {
      val text = RichTextParser.parseRichText(jv)
      val typed = Message.Typed.Regular(MessageRegular(
        editTimestampOption = stringOptToDateTimeOpt(getStringOpt(jv, "edited", false)).map(_.unixTimestamp),
        forwardFromNameOption = getForwardedFromOpt(jv, "forwarded_from"),
        replyToMessageIdOption = getFieldOpt[MessageSourceId](jv, "reply_to_message_id", false),
        contentOption = ContentParser.parseContentOption(jv),
      ))
      Message(
        internalId = NoInternalId,
        sourceIdOption = Some(getCheckedField[MessageSourceId](jv, "id")),
        timestamp = stringToDateTimeOpt(getCheckedField[String](jv, "date")).get.unixTimestamp,
        fromId = getUserId(jv, "from_id"),
        text = text,
        searchableString = Some(makeSearchableString(text, typed)),
        typed = typed
      )
    }

    private def parseService(jv: JValue): Option[Message] = {
      val serviceOption: Option[MessageService] = getCheckedField[String](jv, "action") match {
        case "phone_call" =>
          Some(MessageServicePhoneCall(
            durationSecOption = getFieldOpt[Int](jv, "duration_seconds", false),
            discardReasonOption = getStringOpt(jv, "discard_reason", false),
          ))
        case "group_call" =>
          // Treated the same as phone_call
          Some(MessageServicePhoneCall(
            durationSecOption = None,
            discardReasonOption = None,
          ))
        case "pin_message" =>
          Some(MessageServicePinMessage(
            messageId = getCheckedField[MessageSourceId](jv, "message_id"),
          ))
        case "clear_history" =>
          Some(MessageServiceClearHistory())
        case "create_group" =>
          Some(MessageServiceGroupCreate(
            title = getCheckedField[String](jv, "title"),
            members = getCheckedField[Seq[String]](jv, "members"),
          ))
        case "edit_group_photo" =>
          Some(MessageServiceGroupEditPhoto(
            ContentPhoto(
              pathOption = getFileOpt(jv, "photo", true),
              width = getCheckedField[Int](jv, "width"),
              height = getCheckedField[Int](jv, "height"),
            )
          ))
        case "edit_group_title" =>
          Some(MessageServiceGroupEditTitle(
            title = getCheckedField[String](jv, "title"),
          ))
        case "invite_members" =>
          Some(MessageServiceGroupInviteMembers(
            members = getCheckedField[Seq[String]](jv, "members"),
          ))
        case "remove_members" =>
          Some(MessageServiceGroupRemoveMembers(
            members = getCheckedField[Seq[String]](jv, "members"),
          ))
        case "join_group_by_link" =>
          // Maps into usual InviteMembers
          Some(MessageServiceGroupInviteMembers(
            members = Seq(getCheckedField[String](jv, "actor")),
          ))
        case "migrate_from_group" =>
          Some(MessageServiceGroupMigrateFrom(
            title = getCheckedField[String](jv, "title"),
          ))
        case "migrate_to_supergroup" =>
          Some(MessageServiceGroupMigrateTo())
        case "invite_to_group_call" =>
          Some(MessageServiceGroupCall(
            members = getCheckedField[Seq[String]](jv, "members"),
          ))
        case "edit_chat_theme" =>
          None
        case other =>
          throw new IllegalArgumentException(
            s"Don't know how to parse service message for action '$other' for ${jv.toString.take(500)}")
      }
      serviceOption map { service =>
        val text = RichTextParser.parseRichText(jv)
        val typed = Message.Typed.Service(Some(service))
        Message(
          internalId = NoInternalId,
          sourceIdOption = Some(getCheckedField[MessageSourceId](jv, "id")),
          timestamp = stringToDateTimeOpt(getCheckedField[String](jv, "date")).get.unixTimestamp,
          fromId = getUserId(jv, "actor_id"),
          text = text,
          searchableString = Some(makeSearchableString(text, typed)),
          typed = typed
        )
      }
    }
  }

  protected object RichTextParser {
    def parseRichText(jv: JValue): Seq[RichTextElement] = {
      val jText = getRawField(jv, "text", true)
      jText match {
        case arr: JArray =>
          arr.extract[Seq[JValue]] map parseElement
        case JString("") =>
          Seq.empty
        case s: JString =>
          Seq(parsePlain(s))
        case _ =>
          throw new IllegalArgumentException(s"Don't know how to parse RichText container '$jv'")
      }
    }

    private def parseElement(jv: JValue): RichTextElement = {
      jv match {
        case s: JString => parsePlain(s)
        case jo: JObject => parseElementObject(jo)
        case other => throw new IllegalArgumentException(s"Don't know how to parse RichText element '$other'")
      }
    }

    private def parseElementObject(jo: JObject): RichTextElement = {
      val values = jo.values
      values("type") match {
        case "bold" =>
          require(values.keys == Set("type", "text"), s"Unexpected bold format: $jo")
          RichText.makeBold(values("text").asInstanceOf[String])
        case "italic" =>
          require(values.keys == Set("type", "text"), s"Unexpected italic format: $jo")
          RichText.makeItalic(values("text").asInstanceOf[String])
        case "underline" =>
          require(values.keys == Set("type", "text"), s"Unexpected underline format: $jo")
          RichText.makeUnderline(values("text").asInstanceOf[String])
        case "strikethrough" =>
          require(values.keys == Set("type", "text"), s"Unexpected strikethrough format: $jo")
          RichText.makeStrikethrough(values("text").asInstanceOf[String])
        case "unknown" =>
          require(values.keys == Set("type", "text"), s"Unexpected unknown format: $jo")
          // Unknown is rendered as plaintext in telegram
          RichText.makePlain(values("text").asInstanceOf[String])
        case "code" =>
          require(values.keys == Set("type", "text"), s"Unexpected code format: $jo")
          RichText.makePrefmtInline((values("text").asInstanceOf[String]))
        case "pre" =>
          require(values.keys == Set("type", "text", "language"), s"Unexpected pre format: $jo")
          RichText.makePrefmtBlock(
            text = values("text").asInstanceOf[String],
            languageOption = stringToOption(values("language").asInstanceOf[String])
          )
        case "text_link" =>
          require(values.keys == Set("type", "text", "href"), s"Unexpected text_link format: $jo")
          val textOption = stringToOption(values("text").asInstanceOf[String])
          RichText.makeLink(
            textOption = textOption,
            href = values("href").asInstanceOf[String],
            hidden = isWhitespaceOrInvisible(textOption)
          )
        case "link" =>
          // Link format is hyperlink alone
          require(values.keys == Set("type", "text"), s"Unexpected link format: $jo")
          RichText.makeLink(
            textOption = Some(values("text").asInstanceOf[String]),
            href = values("text").asInstanceOf[String],
            hidden = false
          )
        case "email" =>
          // No special treatment for email
          require(values.keys == Set("type", "text"), s"Unexpected email format: $jo")
          RichText.makePlain(values("text").asInstanceOf[String])
        case "mention" =>
          // No special treatment for mention
          require(values.keys == Set("type", "text"), s"Unexpected mention format: $jo")
          RichText.makePlain(values("text").asInstanceOf[String])
        case "mention_name" =>
          // No special treatment for mention_name, but prepent @
          require(values.keys == Set("type", "text", "user_id"), s"Unexpected mention_name format: $jo")
          RichText.makePlain("@" + values("text").asInstanceOf[String])
        case "phone" =>
          // No special treatment for phone
          require(values.keys == Set("type", "text"), s"Unexpected phone format: $jo")
          RichText.makePlain(values("text").asInstanceOf[String])
        case "hashtag" =>
          // No special treatment for hashtag
          require(values.keys == Set("type", "text"), s"Unexpected hashtag format: $jo")
          RichText.makePlain(values("text").asInstanceOf[String])
        case "bot_command" =>
          // No special treatment for bot_command
          require(values.keys == Set("type", "text"), s"Unexpected bot_command format: $jo")
          RichText.makePlain(values("text").asInstanceOf[String])
        case "bank_card" =>
          // No special treatment for bank_card
          require(values.keys == Set("type", "text"), s"Unexpected bank_card format: $jo")
          RichText.makePlain(values("text").asInstanceOf[String])
        case "cashtag" =>
          // No special treatment for cashtag
          require(values.keys == Set("type", "text"), s"Unexpected cashtag format: $jo")
          RichText.makePlain(values("text").asInstanceOf[String])
        case other =>
          throw new IllegalArgumentException(
            s"Don't know how to parse RichText element of type '${values("type")}' for ${jo.toString.take(500)}")
      }
    }

    private def parsePlain(s: JString): RichTextElement = {
      RichText.makePlain(s.extract[String])
    }
  }

  protected object ContentParser {
    def parseContentOption(jv: JValue): Option[Content] = {
      val mediaTypeOption = getFieldOpt[String](jv, "media_type", false)
      val photoOption = getFieldOpt[String](jv, "photo", false)
      val fileOption = getFieldOpt[String](jv, "file", false)
      val locPresent = (jv \ "location_information") != JNothing
      val pollQuestionPresent = (jv \ "poll" \ "question") != JNothing
      val contactInfoPresent = (jv \ "contact_information") != JNothing
      (mediaTypeOption, photoOption, fileOption, locPresent, pollQuestionPresent, contactInfoPresent) match {
        case (None, None, None, false, false, false) => None
        case (Some("sticker"), None, Some(_), false, false, false) => Some(parseSticker(jv))
        case (Some("animation"), None, Some(_), false, false, false) => Some(parseAnimation(jv))
        case (Some("video_message"), None, Some(_), false, false, false) => Some(parseVideoMsg(jv))
        case (Some("voice_message"), None, Some(_), false, false, false) => Some(parseVoiceMsg(jv))
        case (Some("video_file"), None, Some(_), false, false, false) => Some(parseFile(jv, "<Video>"))
        case (Some("audio_file"), None, Some(_), false, false, false) => Some(parseFile(jv, "<Audio>"))
        case (None, Some(_), None, false, false, false) => Some(parsePhoto(jv))
        case (None, None, Some(_), false, false, false) => Some(parseFile(jv, "<File>"))
        case (None, None, None, true, false, false) => Some(parseLocation(jv))
        case (None, None, None, false, true, false) => Some(parsePoll(jv))
        case (None, None, None, false, false, true) => Some(parseSharedContact(jv))
        case _ =>
          throw new IllegalArgumentException(s"Couldn't determine content type for '$jv'")
      }
    }

    private def parseSticker(jv: JValue): ContentSticker = {
      ContentSticker(
        pathOption = getRelativePathOpt(jv, "file", true),
        thumbnailPathOption = getRelativePathOpt(jv, "thumbnail", true),
        emojiOption = getStringOpt(jv, "sticker_emoji", false),
        width = getCheckedField[Int](jv, "width"),
        height = getCheckedField[Int](jv, "height")
      )
    }

    private def parsePhoto(jv: JValue): ContentPhoto = {
      ContentPhoto(
        pathOption = getRelativePathOpt(jv, "photo", true),
        width = getCheckedField[Int](jv, "width"),
        height = getCheckedField[Int](jv, "height"),
      )
    }

    private def parseAnimation(jv: JValue): ContentAnimation = {
      ContentAnimation(
        pathOption = getRelativePathOpt(jv, "file", true),
        thumbnailPathOption = getRelativePathOpt(jv, "thumbnail", false),
        mimeType = getCheckedField[String](jv, "mime_type"),
        durationSecOption = getFieldOpt[Int](jv, "duration_seconds", false),
        width = getCheckedField[Int](jv, "width"),
        height = getCheckedField[Int](jv, "height"),
      )
    }

    private def parseVoiceMsg(jv: JValue): ContentVoiceMsg = {
      ContentVoiceMsg(
        pathOption = getRelativePathOpt(jv, "file", true),
        mimeType = getCheckedField[String](jv, "mime_type"),
        durationSecOption = getFieldOpt[Int](jv, "duration_seconds", false),
      )
    }

    private def parseVideoMsg(jv: JValue): ContentVideoMsg = {
      ContentVideoMsg(
        pathOption = getRelativePathOpt(jv, "file", true),
        thumbnailPathOption = getRelativePathOpt(jv, "thumbnail", true),
        mimeType = getCheckedField[String](jv, "mime_type"),
        durationSecOption = getFieldOpt[Int](jv, "duration_seconds", false),
        width = getCheckedField[Int](jv, "width"),
        height = getCheckedField[Int](jv, "height"),
      )
    }

    private def parseFile(jv: JValue, titleIfEmpty: String): ContentFile = {
      ContentFile(
        pathOption = getRelativePathOpt(jv, "file", true),
        thumbnailPathOption = getRelativePathOpt(jv, "thumbnail", false),
        mimeTypeOption = getStringOpt(jv, "mime_type", true),
        title = getStringOpt(jv, "title", false) getOrElse titleIfEmpty,
        performerOption = getStringOpt(jv, "performer", false),
        durationSecOption = getFieldOpt[Int](jv, "duration_seconds", false),
        widthOption = getFieldOpt[Int](jv, "width", false),
        heightOption = getFieldOpt[Int](jv, "height", false)
      )
    }

    private def parseLocation(jv: JValue): ContentLocation = {
      ContentLocation(
        titleOption = getStringOpt(jv, "place_name", false),
        addressOption = getStringOpt(jv, "address", false),
        latStr = getCheckedField[String](jv, "location_information", "latitude"),
        lonStr = getCheckedField[String](jv, "location_information", "longitude"),
        durationSecOption = getFieldOpt[Int](jv, "live_location_period_seconds", false)
      )
    }

    private def parsePoll(jv: JValue): ContentPoll = {
      ContentPoll(
        question = getCheckedField[String](jv, "poll", "question")
      )
    }

    private def parseSharedContact(jv: JValue): ContentSharedContact = {
      val ci = getRawField(jv, "contact_information", true)
      ContentSharedContact(
        firstNameOption = getStringOpt(ci, "first_name", true),
        lastNameOption = getStringOpt(ci, "last_name", true),
        phoneNumberOption = getStringOpt(ci, "phone_number", true),
        vcardPathOption = getRelativePathOpt(jv, "contact_vcard", false)
      )
    }
  }

  protected def parseChat(jv: JValue, dsUuid: PbUuid, memberIds: Set[Long], msgCount: Int): Chat = {
    val tpe = getCheckedField[String](jv, "type") match {
      case "personal_chat" => ChatType.Personal
      case "private_group" => ChatType.PrivateGroup
      case "private_supergroup" => ChatType.PrivateGroup
      case s => throw new IllegalArgumentException(s"Illegal format, unknown chat type '$s'")
    }

    // Undo the shifts introduced by Telegram 2021-05
    val id = (getCheckedField[Long](jv, "id"), tpe) match {
      case (v, ChatType.Personal) if v < PersonalChatIdShift => v + PersonalChatIdShift
      case (v, ChatType.PrivateGroup) if v < GroupChatIdShift => v + GroupChatIdShift
      case (v, _) => v
    }

    Chat(
      dsUuid = dsUuid,
      id = id,
      nameOption = getStringOpt(jv, "name", true),
      tpe = tpe,
      imgPathOption = None,
      memberIds = memberIds.toSeq,
      msgCount = msgCount
    )
  }

  protected def parseShortUserFromMessage(jv: JValue): ShortUser = {
    normalize(getCheckedField[String](jv, "type") match {
      case "message" =>
        ShortUser(getUserId(jv, "from_id"), getStringOpt(jv, "from", true))
      case "service" =>
        ShortUser(getUserId(jv, "actor_id"), getStringOpt(jv, "actor", true))
      case other =>
        throw new IllegalArgumentException(
          s"Don't know how to parse message of type '$other' for ${jv.toString.take(500)}")
    })
  }

  //
  // Utility
  //

  protected def stringToOption(s: String): Option[String] = {
    s match {
      case "" => None
      case "(File not included. Change data exporting settings to download.)" => None
      case other => Some(other)
    }
  }

  protected def isWhitespaceOrInvisible(s: String): Boolean = {
    // Accounts for invisible formatting indicator, e.g. zero-width space \u200B
    s matches "[\\s\\p{Cf}]*"
  }

  protected def isWhitespaceOrInvisible(s: Option[String]): Boolean = {
    s match {
      case None => true
      case Some(s) => isWhitespaceOrInvisible(s)
    }
  }

  /** Dates in TG history is exported in local timezone, so we'll try to import in current one as well */
  protected def stringToDateTimeOpt(s: String): Option[DateTime] = {
    DateTime.parse(s) match {
      case dt if dt.year.get == 1970 => None // TG puts minimum timestamp in place of absent
      case other => Some(other)
    }
  }

  protected def stringOptToDateTimeOpt(so: Option[String]): Option[DateTime] = {
    so flatMap stringToDateTimeOpt
  }

  protected def getRawField(jv: JValue, fieldName: String, mustPresent: Boolean): JValue = {
    val res = jv \ fieldName
    if (mustPresent) {
      require(res != JNothing, s"Incompatible format! Field '$fieldName' not found in ${jv.toString.take(500)}")
    }
    res
  }

  protected def getFieldOpt[A: Manifest](jv: JValue, fieldName: String, mustPresent: Boolean)(implicit formats: Formats): Option[A] = {
    getRawField(jv, fieldName, mustPresent).extractOpt[A]
  }

  protected def getStringOpt(jv: JValue, fieldName: String, mustPresent: Boolean)(implicit formats: Formats): Option[String] = {
    getRawField(jv, fieldName, mustPresent).extractOpt[String] flatMap stringToOption
  }

  // Does not d
  protected def getFileOpt(jv: JValue, fieldName: String, mustPresent: Boolean)(implicit formats: Formats): Option[String] = {
    getStringOpt(jv, fieldName, mustPresent).map(s => if (s.startsWith("/")) s.tail else s) map (_.makeRelativePath)
  }

  protected def getRelativePathOpt(jv: JValue, fieldName: String, mustPresent: Boolean)(implicit formats: Formats): Option[String] = {
    getStringOpt(jv, fieldName, mustPresent) map (p => p.makeRelativePath)
  }

  protected def getCheckedField[A: Manifest](jv: JValue, fieldName: String)(implicit formats: Formats): A = {
    getRawField(jv, fieldName, true).extract[A]
  }

  protected def getCheckedField[A: Manifest](jv: JValue, fn1: String, fn2: String)(implicit formats: Formats): A = {
    val res = jv \ fn1 \ fn2
    require(res != JNothing, s"Incompatible format! Path '$fn1 \\ $fn2' not found in $jv")
    res.extract[A]
  }

  protected def getForwardedFromOpt(jv: JValue, fieldName: String)(implicit formats: Formats): Option[String] = {
    getRawField(jv, fieldName, false) match {
      case JNothing => None
      case JNull => Some(Unknown)
      case ff: JString => Some(ff.values)
      case other =>
        throw new IllegalArgumentException(s"Don't know how to parse forwarded_from from '${other.toString.take(500)}'")
    }
  }

  protected def getUserId(jv: JValue, fieldName: String)(implicit formats: Formats): Long = {
    getRawField(jv, fieldName, true) match {
      case id: JInt => id.values.toLong
      case id: JLong => id.values
      case id: JString if id.values.matches("user\\d+") => id.values.substring(4).toLong
      case id: JString if id.values.matches("channel\\d+") => id.values.substring(7).toLong
      case other =>
        throw new IllegalArgumentException(s"Don't know how to get user ID from '${other.toString.take(500)}'")
    }
  }

  protected case class ShortUser(id: Long, fullNameOption: Option[String])
}
