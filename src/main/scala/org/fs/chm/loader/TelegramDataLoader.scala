package org.fs.chm.loader

import java.io.File
import java.io.FileNotFoundException
import java.util.UUID

import scala.collection.immutable.ListMap

import com.github.nscala_time.time.Imports.DateTime
import org.fs.chm.dao._
import org.fs.utility.Imports._
import org.json4s._
import org.json4s.jackson.JsonMethods

class TelegramDataLoader extends DataLoader {
  implicit private val formats: Formats = DefaultFormats.withLong.withBigDecimal

  /** Path should point to the folder with `result.json` and other stuff */
  override protected def loadDataInner(path: File): ChatHistoryDao = {
    implicit val dummyTracker = new FieldUsageTracker
    val resultJsonFile: File = new File(path, "result.json")
    if (!resultJsonFile.exists()) throw new FileNotFoundException("result.json not found in " + path.getAbsolutePath)

    val dataset = Dataset(
      uuid       = UUID.randomUUID(),
      alias      = "Telegram data @ " + DateTime.now().toString("yyyy-MM-dd"),
      sourceType = "telegram"
    )

    val parsed = JsonMethods.parse(resultJsonFile)
    val myself = parseMyself(getRawField(parsed, "personal_information", true), dataset.uuid)
    val users = for {
      contact <- getCheckedField[Seq[JValue]](parsed, "contacts", "list")
    } yield parseUser(contact, dataset.uuid)

    val chatsWithMessages = for {
      chat <- getCheckedField[Seq[JValue]](parsed, "chats", "list")
      if (getCheckedField[String](chat, "type") != "saved_messages")
    } yield {
      val messagesRes = (for {
        message <- getCheckedField[IndexedSeq[JValue]](chat, "messages")
      } yield MessageParser.parseMessageOption(message)).yieldDefined

      val chatRes = parseChat(chat, dataset.uuid, messagesRes.size)
      (chatRes, messagesRes)
    }
    val chatsWithMessagesLM = ListMap(chatsWithMessages: _*)

    new EagerChatHistoryDao(
      dataPathRoot      = path,
      dataset           = dataset,
      myself1           = myself,
      rawUsers          = users,
      chatsWithMessages = chatsWithMessagesLM
    )
  }

  //
  // Parsers
  //

  private def parseMyself(jv: JValue, dsUuid: UUID): User = {
    implicit val tracker = new FieldUsageTracker
    tracker.markUsed("bio") // Ignoring bio
    tracker.ensuringUsage(jv) {
      User(
        dsUuid             = dsUuid,
        id                 = getCheckedField[Long](jv, "user_id"),
        firstNameOption    = getStringOpt(jv, "first_name", true),
        lastNameOption     = getStringOpt(jv, "last_name", true),
        usernameOption     = getStringOpt(jv, "username", true),
        phoneNumberOption  = getStringOpt(jv, "phone_number", true),
        lastSeenTimeOption = None
      )
    }
  }

  private def parseUser(jv: JValue, dsUuid: UUID): User = {
    implicit val tracker = new FieldUsageTracker
    tracker.ensuringUsage(jv) {
      User(
        dsUuid             = dsUuid,
        id                 = getCheckedField[Long](jv, "user_id"),
        firstNameOption    = getStringOpt(jv, "first_name", true),
        lastNameOption     = getStringOpt(jv, "last_name", true),
        usernameOption     = None,
        phoneNumberOption  = getStringOpt(jv, "phone_number", true),
        lastSeenTimeOption = stringToDateTimeOpt(getCheckedField[String](jv, "date"))
      )
    }
  }

  private object MessageParser {
    def parseMessageOption(jv: JValue): Option[Message] = {
      implicit val tracker = new FieldUsageTracker
      tracker.markUsed("via_bot") // Ignored
      tracker.ensuringUsage(jv) {
        getCheckedField[String](jv, "type") match {
          case "message" => Some(parseRegular(jv))
          case "service" => Some(parseService(jv))
          case other =>
            throw new IllegalArgumentException(
              s"Don't know how to parse message of type '$other' for ${jv.toString.take(500)}")
        }
      }
    }

    private def parseRegular(jv: JValue)(implicit tracker: FieldUsageTracker): Message.Regular = {
      Message.Regular(
        id                     = getCheckedField[Long](jv, "id"),
        time                   = stringToDateTimeOpt(getCheckedField[String](jv, "date")).get,
        editTimeOption         = stringToDateTimeOpt(getCheckedField[String](jv, "edited")),
        fromName               = getCheckedField[String](jv, "from"),
        fromId                 = getCheckedField[Long](jv, "from_id"),
        forwardFromNameOption  = getStringOpt(jv, "forwarded_from", false),
        replyToMessageIdOption = getFieldOpt[Long](jv, "reply_to_message_id", false),
        textOption             = RichTextParser.parseRichTextOption(jv),
        contentOption          = ContentParser.parseContentOption(jv)
      )
    }

    private def parseService(jv: JValue)(implicit tracker: FieldUsageTracker): Message.Service = {
      tracker.markUsed("edited") // Service messages can't be edited
      getCheckedField[String](jv, "action") match {
        case "phone_call" =>
          Message.Service.PhoneCall(
            id                  = getCheckedField[Long](jv, "id"),
            time                = stringToDateTimeOpt(getCheckedField[String](jv, "date")).get,
            fromName            = getCheckedField[String](jv, "actor"),
            fromId              = getCheckedField[Long](jv, "actor_id"),
            durationSecOption   = getFieldOpt[Int](jv, "duration_seconds", false),
            discardReasonOption = getStringOpt(jv, "discard_reason", false),
            textOption          = RichTextParser.parseRichTextOption(jv)
          )
        case "pin_message" =>
          Message.Service.PinMessage(
            id         = getCheckedField[Long](jv, "id"),
            time       = stringToDateTimeOpt(getCheckedField[String](jv, "date")).get,
            fromName   = getCheckedField[String](jv, "actor"),
            fromId     = getCheckedField[Long](jv, "actor_id"),
            messageId  = getCheckedField[Long](jv, "message_id"),
            textOption = RichTextParser.parseRichTextOption(jv)
          )
        case "clear_history" =>
          Message.Service.ClearHistory(
            id         = getCheckedField[Long](jv, "id"),
            time       = stringToDateTimeOpt(getCheckedField[String](jv, "date")).get,
            fromName   = getCheckedField[String](jv, "actor"),
            fromId     = getCheckedField[Long](jv, "actor_id"),
            textOption = RichTextParser.parseRichTextOption(jv)
          )
        case "edit_group_photo" =>
          Message.Service.EditPhoto(
            id           = getCheckedField[Long](jv, "id"),
            time         = stringToDateTimeOpt(getCheckedField[String](jv, "date")).get,
            fromName     = getCheckedField[String](jv, "actor"),
            fromId       = getCheckedField[Long](jv, "actor_id"),
            pathOption   = getStringOpt(jv, "photo", true),
            widthOption  = getFieldOpt[Int](jv, "width", false),
            heightOption = getFieldOpt[Int](jv, "height", false),
            textOption   = RichTextParser.parseRichTextOption(jv)
          )
        case "create_group" =>
          Message.Service.Group.Create(
            id         = getCheckedField[Long](jv, "id"),
            time       = stringToDateTimeOpt(getCheckedField[String](jv, "date")).get,
            fromName   = getCheckedField[String](jv, "actor"),
            fromId     = getCheckedField[Long](jv, "actor_id"),
            title      = getCheckedField[String](jv, "title"),
            members    = getCheckedField[Seq[String]](jv, "members"),
            textOption = RichTextParser.parseRichTextOption(jv)
          )
        case "invite_members" =>
          Message.Service.Group.InviteMembers(
            id         = getCheckedField[Long](jv, "id"),
            time       = stringToDateTimeOpt(getCheckedField[String](jv, "date")).get,
            fromName   = getCheckedField[String](jv, "actor"),
            fromId     = getCheckedField[Long](jv, "actor_id"),
            members    = getCheckedField[Seq[String]](jv, "members"),
            textOption = RichTextParser.parseRichTextOption(jv)
          )
        case "remove_members" =>
          Message.Service.Group.RemoveMembers(
            id         = getCheckedField[Long](jv, "id"),
            time       = stringToDateTimeOpt(getCheckedField[String](jv, "date")).get,
            fromName   = getCheckedField[String](jv, "actor"),
            fromId     = getCheckedField[Long](jv, "actor_id"),
            members    = getCheckedField[Seq[String]](jv, "members"),
            textOption = RichTextParser.parseRichTextOption(jv)
          )
        case other =>
          throw new IllegalArgumentException(
            s"Don't know how to parse service message for action '$other' for ${jv.toString.take(500)}")
      }
    }
  }

  private object RichTextParser {
    def parseRichTextOption(jv: JValue)(implicit tracker: FieldUsageTracker): Option[RichText] = {
      val jText = getRawField(jv, "text", true)
      jText match {
        case arr: JArray =>
          val elements = arr.extract[Seq[JValue]] map parseElement
          // Make sure there no more than one hidden link
          val links = elements collect {
            case l: RichText.Link => l
          }
          if (links.size > 1) {
            require(links.tail forall (!_.hidden), s"Only the first link can be hidden! ${jv}")
          }
          Some(RichText(elements))
        case JString("") =>
          None
        case s: JString =>
          Some(RichText(Seq(parsePlain(s))))
        case other =>
          throw new IllegalArgumentException(s"Don't know how to parse RichText container '$jv'")
      }
    }

    private def parseElement(jv: JValue): RichText.Element = {
      jv match {
        case s: JString  => parsePlain(s)
        case jo: JObject => parseElementObject(jo)
        case other       => throw new IllegalArgumentException(s"Don't know how to parse RichText element '$jv'")
      }
    }

    private def parseElementObject(jo: JObject): RichText.Element = {
      val values = jo.values
      values("type") match {
        case "bold" =>
          require(values.keys == Set("type", "text"), s"Unexpected bold format: $jo")
          RichText.Bold(values("text").asInstanceOf[String])
        case "italic" =>
          require(values.keys == Set("type", "text"), s"Unexpected italic format: $jo")
          RichText.Italic(values("text").asInstanceOf[String])
        case "code" =>
          require(values.keys == Set("type", "text"), s"Unexpected code format: $jo")
          RichText.PrefmtInline(values("text").asInstanceOf[String])
        case "pre" =>
          require(values.keys == Set("type", "text", "language"), s"Unexpected pre format: $jo")
          RichText.PrefmtBlock(
            text           = values("text").asInstanceOf[String],
            languageOption = stringToOption(values("language").asInstanceOf[String])
          )
        case "text_link" =>
          require(values.keys == Set("type", "text", "href"), s"Unexpected text_link format: $jo")
          val text = stringToOption(values("text").asInstanceOf[String]).getOrElse("")
          RichText.Link(
            text   = text,
            href   = values("href").asInstanceOf[String],
            hidden = isWhitespaceOrInvisible(text)
          )
        case "link" =>
          // Link format is hyperlink alone
          require(values.keys == Set("type", "text"), s"Unexpected link format: $jo")
          RichText.Link(
            text   = values("text").asInstanceOf[String],
            href   = values("text").asInstanceOf[String],
            hidden = false
          )
        case "email" =>
          // No special treatment for email
          require(values.keys == Set("type", "text"), s"Unexpected email format: $jo")
          RichText.Plain(values("text").asInstanceOf[String])
        case "mention" =>
          // No special treatment for mention
          require(values.keys == Set("type", "text"), s"Unexpected mention format: $jo")
          RichText.Plain(values("text").asInstanceOf[String])
        case "mention_name" =>
          // No special treatment for mention_name, but prepent @
          require(values.keys == Set("type", "text", "user_id"), s"Unexpected mention_name format: $jo")
          RichText.Plain("@" + values("text").asInstanceOf[String])
        case "phone" =>
          // No special treatment for phone
          require(values.keys == Set("type", "text"), s"Unexpected phone format: $jo")
          RichText.Plain(values("text").asInstanceOf[String])
        case "hashtag" =>
          // No special treatment for hashtag
          require(values.keys == Set("type", "text"), s"Unexpected hashtag format: $jo")
          RichText.Plain(values("text").asInstanceOf[String])
        case "bot_command" =>
          // No special treatment for bot_command
          require(values.keys == Set("type", "text"), s"Unexpected bot_command format: $jo")
          RichText.Plain(values("text").asInstanceOf[String])
        case other =>
          throw new IllegalArgumentException(
            s"Don't know how to parse RichText element of type '${values("type")}' for ${jo.toString.take(500)}")
      }
    }

    private def parsePlain(s: JString): RichText.Plain = {
      RichText.Plain(s.extract[String])
    }
  }

  private object ContentParser {
    def parseContentOption(jv: JValue)(implicit tracker: FieldUsageTracker): Option[Content] = {
      val mediaTypeOption     = getFieldOpt[String](jv, "media_type", false)
      val photoOption         = getFieldOpt[String](jv, "photo", false)
      val fileOption          = getFieldOpt[String](jv, "file", false)
      val locPresent          = (jv \ "location_information") != JNothing
      val pollQuestionPresent = (jv \ "poll" \ "question") != JNothing
      val contactInfoPresent  = (jv \ "contact_information") != JNothing
      (mediaTypeOption, photoOption, fileOption, locPresent, pollQuestionPresent, contactInfoPresent) match {
        case (None, None, None, false, false, false)                     => None
        case (Some("sticker"), None, Some(_), false, false, false)       => Some(parseSticker(jv))
        case (Some("animation"), None, Some(_), false, false, false)     => Some(parseAnimation(jv))
        case (Some("video_message"), None, Some(_), false, false, false) => Some(parseVideoMsg(jv))
        case (Some("voice_message"), None, Some(_), false, false, false) => Some(parseVoiceMsg(jv))
        case (Some("video_file"), None, Some(_), false, false, false)    => Some(parseFile(jv))
        case (Some("audio_file"), None, Some(_), false, false, false)    => Some(parseFile(jv))
        case (None, Some(_), None, false, false, false)                  => Some(parsePhoto(jv))
        case (None, None, Some(_), false, false, false)                  => Some(parseFile(jv))
        case (None, None, None, true, false, false)                      => Some(parseLocation(jv))
        case (None, None, None, false, true, false)                      => Some(parsePoll(jv))
        case (None, None, None, false, false, true)                      => Some(parseSharedContact(jv))
        case _ =>
          throw new IllegalArgumentException(s"Couldn't determine content type for '$jv'")
      }
    }

    private def parseSticker(jv: JValue)(implicit tracker: FieldUsageTracker): Content.Sticker = {
      Content.Sticker(
        pathOption          = getStringOpt(jv, "file", true),
        thumbnailPathOption = getStringOpt(jv, "thumbnail", true),
        emojiOption         = getStringOpt(jv, "sticker_emoji", false),
        widthOption         = getFieldOpt[Int](jv, "width", false),
        heightOption        = getFieldOpt[Int](jv, "height", false)
      )
    }

    private def parsePhoto(jv: JValue)(implicit tracker: FieldUsageTracker): Content.Photo = {
      Content.Photo(
        pathOption = getStringOpt(jv, "photo", true),
        width      = getCheckedField[Int](jv, "width"),
        height     = getCheckedField[Int](jv, "height"),
      )
    }

    private def parseAnimation(jv: JValue)(implicit tracker: FieldUsageTracker): Content.Animation = {
      Content.Animation(
        pathOption          = getStringOpt(jv, "file", true),
        thumbnailPathOption = getStringOpt(jv, "thumbnail", true),
        mimeTypeOption      = Some(getCheckedField[String](jv, "mime_type")),
        durationSecOption   = getFieldOpt[Int](jv, "duration_seconds", false),
        width               = getCheckedField[Int](jv, "width"),
        height              = getCheckedField[Int](jv, "height"),
      )
    }

    private def parseVoiceMsg(jv: JValue)(implicit tracker: FieldUsageTracker): Content.VoiceMsg = {
      Content.VoiceMsg(
        pathOption        = getStringOpt(jv, "file", true),
        mimeTypeOption    = Some(getCheckedField[String](jv, "mime_type")),
        durationSecOption = getFieldOpt[Int](jv, "duration_seconds", false),
      )
    }

    private def parseVideoMsg(jv: JValue)(implicit tracker: FieldUsageTracker): Content.VideoMsg = {
      Content.VideoMsg(
        pathOption          = getStringOpt(jv, "file", true),
        thumbnailPathOption = getStringOpt(jv, "thumbnail", true),
        mimeTypeOption      = Some(getCheckedField[String](jv, "mime_type")),
        durationSecOption   = getFieldOpt[Int](jv, "duration_seconds", false),
        width               = getCheckedField[Int](jv, "width"),
        height              = getCheckedField[Int](jv, "height"),
      )
    }

    private def parseFile(jv: JValue)(implicit tracker: FieldUsageTracker): Content.File = {
      Content.File(
        pathOption          = getStringOpt(jv, "file", true),
        thumbnailPathOption = getStringOpt(jv, "thumbnail", false),
        mimeTypeOption      = getStringOpt(jv, "mime_type", true),
        titleOption         = getStringOpt(jv, "title", false),
        performerOption     = getStringOpt(jv, "performer", false),
        durationSecOption   = getFieldOpt[Int](jv, "duration_seconds", false),
        widthOption         = getFieldOpt[Int](jv, "width", false),
        heightOption        = getFieldOpt[Int](jv, "height", false)
      )
    }

    private def parseLocation(jv: JValue)(implicit tracker: FieldUsageTracker): Content.Location = {
      Content.Location(
        lat                   = getCheckedField[BigDecimal](jv, "location_information", "latitude"),
        lon                   = getCheckedField[BigDecimal](jv, "location_information", "longitude"),
        liveDurationSecOption = getFieldOpt[Int](jv, "live_location_period_seconds", false)
      )
    }

    private def parsePoll(jv: JValue)(implicit tracker: FieldUsageTracker): Content.Poll = {
      Content.Poll(
        question = getCheckedField[String](jv, "poll", "question")
      )
    }

    private def parseSharedContact(jv: JValue)(implicit tracker: FieldUsageTracker): Content.SharedContact = {
      val ci = getRawField(jv, "contact_information", true)
      Content.SharedContact(
        firstNameOption   = getStringOpt(ci, "first_name", true),
        lastNameOption    = getStringOpt(ci, "last_name", true),
        phoneNumberOption = getStringOpt(ci, "phone_number", true),
        vcardPathOption   = getStringOpt(jv, "contact_vcard", false)
      )
    }
  }

  private def parseChat(jv: JValue, dsUuid: UUID, msgCount: Int): Chat = {
    implicit val tracker = new FieldUsageTracker
    tracker.markUsed("messages")
    tracker.ensuringUsage(jv) {
      Chat(
        dsUuid        = dsUuid,
        id            = getCheckedField[Long](jv, "id"),
        nameOption    = getCheckedField[Option[String]](jv, "name"),
        tpe = getCheckedField[String](jv, "type") match {
          case "personal_chat" => ChatType.Personal
          case "private_group" => ChatType.PrivateGroup
          case s               => throw new IllegalArgumentException("Illegal format, unknown chat type '$s'")
        },
        imgPathOption = None,
        msgCount      = msgCount
      )
    }
  }

  //
  // Utility
  //

  private def stringToOption(s: String): Option[String] = {
    s match {
      case ""                                                                 => None
      case "(File not included. Change data exporting settings to download.)" => None
      case other                                                              => Some(other)
    }
  }

  private def isWhitespaceOrInvisible(s: String): Boolean = {
    // Accounts for invisible formatting indicator, e.g. zero-width space \u200B
    s matches "[\\s\\p{Cf}]*"
  }

  /** Dates in TG history is exported in local timezone, so we'll try to import in current one as well */
  private def stringToDateTimeOpt(s: String): Option[DateTime] = {
    DateTime.parse(s) match {
      case dt if dt.year.get == 1970 => None // TG puts minimum timestamp in place of absent
      case other                     => Some(other)
    }
  }

  private def getRawField(jv: JValue, fieldName: String, mustPresent: Boolean)(
      implicit tracker: FieldUsageTracker): JValue = {
    val res = jv \ fieldName
    tracker.markUsed(fieldName)
    if (mustPresent) require(res != JNothing, s"Incompatible format! Field '$fieldName' not found in $jv")
    res
  }

  private def getFieldOpt[A](jv: JValue, fieldName: String, mustPresent: Boolean)(
      implicit formats: Formats,
      mf: scala.reflect.Manifest[A],
      tracker: FieldUsageTracker): Option[A] = {
    getRawField(jv, fieldName, mustPresent).extractOpt[A]
  }

  private def getStringOpt(jv: JValue, fieldName: String, mustPresent: Boolean)(
      implicit formats: Formats,
      tracker: FieldUsageTracker): Option[String] = {
    val res = jv \ fieldName
    tracker.markUsed(fieldName)
    if (mustPresent) require(res != JNothing, s"Incompatible format! Field '$fieldName' not found in $jv")
    res.extractOpt[String] flatMap stringToOption
  }

  private def getCheckedField[A](jv: JValue, fieldName: String)(implicit formats: Formats,
                                                                mf: scala.reflect.Manifest[A],
                                                                tracker: FieldUsageTracker): A = {
    getRawField(jv, fieldName, true).extract[A]
  }

  private def getCheckedField[A](jv: JValue, fn1: String, fn2: String)(implicit formats: Formats,
                                                                       mf: scala.reflect.Manifest[A],
                                                                       tracker: FieldUsageTracker): A = {
    val res = jv \ fn1 \ fn2
    require(res != JNothing, s"Incompatible format! Path '$fn1 \\ $fn2' not found in $jv")
    tracker.markUsed(fn1)
    res.extract[A]
  }

  /** Tracks JSON fields being used and ensures that nothing is left unattended */
  class FieldUsageTracker {
    private var markedFields: Set[String] = Set.empty

    def markUsed(fieldName: String): Unit = {
      markedFields = markedFields + fieldName
    }

    def ensuringUsage[A](jv: JValue)(codeBlock: => A): A = {
      val res: A = codeBlock
      ensureUsage(jv)
      res
    }

    def ensureUsage(jv: JValue): Unit = {
      jv match {
        case JObject(children) =>
          val objFields = Set.empty ++ children.map(_._1)
          val unused    = objFields.diff(markedFields)
          if (unused.nonEmpty) {
            throw new IllegalArgumentException(s"Unused fields! $unused for ${jv.toString.take(500)}")
          }
        case _ =>
          throw new IllegalArgumentException("Not a JObject! " + jv)
      }
    }
  }
}
