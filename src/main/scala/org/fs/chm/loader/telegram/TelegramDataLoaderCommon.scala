package org.fs.chm.loader.telegram

import java.io.File
import java.io.FileNotFoundException
import java.util.UUID

import scala.collection.immutable.ListMap

import com.github.nscala_time.time.Imports._
import org.fs.chm.dao._
import org.fs.utility.Imports._
import org.json4s._
import org.json4s.jackson.JsonMethods

trait TelegramDataLoaderCommon {
  implicit protected val formats: Formats = DefaultFormats.withLong.withBigDecimal

  protected object MessageParser {
    def parseMessageOption(jv: JValue, rootFile: File): Option[Message] = {
      implicit val tracker = new FieldUsageTracker
      tracker.markUsed("via_bot") // Ignored
      tracker.ensuringUsage(jv) {
        getCheckedField[String](jv, "type") match {
          case "message"     => Some(parseRegular(jv, rootFile))
          case "service"     => Some(parseService(jv, rootFile))
          case "unsupported" =>
            // Not enough data is provided even for a placeholder
            tracker.markUsed("id")
            None
          case other =>
            throw new IllegalArgumentException(
              s"Don't know how to parse message of type '$other' for ${jv.toString.take(500)}")
        }
      }
    }

    private def parseRegular(jv: JValue, rootFile: File)(implicit tracker: FieldUsageTracker): Message.Regular = {
      tracker.markUsed("from") // Sending user name has been parsed during a separate pass
      Message.Regular(
        internalId             = Message.NoInternalId,
        sourceIdOption         = Some(getCheckedField[Message.SourceId](jv, "id")),
        time                   = stringToDateTimeOpt(getCheckedField[String](jv, "date")).get,
        editTimeOption         = stringToDateTimeOpt(getCheckedField[String](jv, "edited")),
        fromId                 = getCheckedField[Long](jv, "from_id"),
        forwardFromNameOption  = getStringOpt(jv, "forwarded_from", false),
        replyToMessageIdOption = getFieldOpt[Message.SourceId](jv, "reply_to_message_id", false),
        textOption             = RichTextParser.parseRichTextOption(jv),
        contentOption          = ContentParser.parseContentOption(jv, rootFile)
      )
    }

    private def parseService(jv: JValue, rootFile: File)(implicit tracker: FieldUsageTracker): Message.Service = {
      tracker.markUsed("edited") // Service messages can't be edited
      tracker.markUsed("actor")  // Sending user name has been parsed during a separate pass
      getCheckedField[String](jv, "action") match {
        case "phone_call" =>
          Message.Service.PhoneCall(
            internalId          = Message.NoInternalId,
            sourceIdOption      = Some(getCheckedField[Message.SourceId](jv, "id")),
            time                = stringToDateTimeOpt(getCheckedField[String](jv, "date")).get,
            fromId              = getCheckedField[Long](jv, "actor_id"),
            durationSecOption   = getFieldOpt[Int](jv, "duration_seconds", false),
            discardReasonOption = getStringOpt(jv, "discard_reason", false),
            textOption          = RichTextParser.parseRichTextOption(jv)
          )
        case "pin_message" =>
          Message.Service.PinMessage(
            internalId     = Message.NoInternalId,
            sourceIdOption = Some(getCheckedField[Message.SourceId](jv, "id")),
            time           = stringToDateTimeOpt(getCheckedField[String](jv, "date")).get,
            fromId         = getCheckedField[Long](jv, "actor_id"),
            messageId      = getCheckedField[Message.SourceId](jv, "message_id"),
            textOption     = RichTextParser.parseRichTextOption(jv)
          )
        case "clear_history" =>
          Message.Service.ClearHistory(
            internalId     = Message.NoInternalId,
            sourceIdOption = Some(getCheckedField[Message.SourceId](jv, "id")),
            time           = stringToDateTimeOpt(getCheckedField[String](jv, "date")).get,
            fromId         = getCheckedField[Long](jv, "actor_id"),
            textOption     = RichTextParser.parseRichTextOption(jv)
          )
        case "edit_group_photo" =>
          Message.Service.EditPhoto(
            internalId     = Message.NoInternalId,
            sourceIdOption = Some(getCheckedField[Message.SourceId](jv, "id")),
            time           = stringToDateTimeOpt(getCheckedField[String](jv, "date")).get,
            fromId         = getCheckedField[Long](jv, "actor_id"),
            pathOption     = getFileOpt(jv, "photo", true, rootFile),
            widthOption    = getFieldOpt[Int](jv, "width", false),
            heightOption   = getFieldOpt[Int](jv, "height", false),
            textOption     = RichTextParser.parseRichTextOption(jv)
          )
        case "create_group" =>
          Message.Service.Group.Create(
            internalId     = Message.NoInternalId,
            sourceIdOption = Some(getCheckedField[Message.SourceId](jv, "id")),
            time           = stringToDateTimeOpt(getCheckedField[String](jv, "date")).get,
            fromId         = getCheckedField[Long](jv, "actor_id"),
            title          = getCheckedField[String](jv, "title"),
            members        = getCheckedField[Seq[String]](jv, "members"),
            textOption     = RichTextParser.parseRichTextOption(jv)
          )
        case "invite_members" =>
          Message.Service.Group.InviteMembers(
            internalId     = Message.NoInternalId,
            sourceIdOption = Some(getCheckedField[Message.SourceId](jv, "id")),
            time           = stringToDateTimeOpt(getCheckedField[String](jv, "date")).get,
            fromId         = getCheckedField[Long](jv, "actor_id"),
            members        = getCheckedField[Seq[String]](jv, "members"),
            textOption     = RichTextParser.parseRichTextOption(jv)
          )
        case "remove_members" =>
          Message.Service.Group.RemoveMembers(
            internalId     = Message.NoInternalId,
            sourceIdOption = Some(getCheckedField[Message.SourceId](jv, "id")),
            time           = stringToDateTimeOpt(getCheckedField[String](jv, "date")).get,
            fromId         = getCheckedField[Long](jv, "actor_id"),
            members        = getCheckedField[Seq[String]](jv, "members"),
            textOption     = RichTextParser.parseRichTextOption(jv)
          )
        case "migrate_from_group" =>
          Message.Service.Group.MigrateFrom(
            internalId     = Message.NoInternalId,
            sourceIdOption = Some(getCheckedField[Message.SourceId](jv, "id")),
            time           = stringToDateTimeOpt(getCheckedField[String](jv, "date")).get,
            fromId         = getCheckedField[Long](jv, "actor_id"),
            titleOption    = Some(getCheckedField[String](jv, "title")),
            textOption     = RichTextParser.parseRichTextOption(jv)
          )
        case "migrate_to_supergroup" =>
          Message.Service.Group.MigrateTo(
            internalId     = Message.NoInternalId,
            sourceIdOption = Some(getCheckedField[Message.SourceId](jv, "id")),
            time           = stringToDateTimeOpt(getCheckedField[String](jv, "date")).get,
            fromId         = getCheckedField[Long](jv, "actor_id"),
            textOption     = RichTextParser.parseRichTextOption(jv)
          )
        case other =>
          throw new IllegalArgumentException(
            s"Don't know how to parse service message for action '$other' for ${jv.toString.take(500)}")
      }
    }
  }

  protected object RichTextParser {
    def parseRichTextOption(jv: JValue)(implicit tracker: FieldUsageTracker): Option[RichText] = {
      val jText = getRawField(jv, "text", true)
      jText match {
        case arr: JArray =>
          val elements = arr.extract[Seq[JValue]] map parseElement
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
        case "strikethrough" =>
          require(values.keys == Set("type", "text"), s"Unexpected strikethrough format: $jo")
          RichText.Strikethrough(values("text").asInstanceOf[String])
        case "unknown" =>
          require(values.keys == Set("type", "text"), s"Unexpected unknown format: $jo")
          // Unknown is rendered as plaintext in telegram
          RichText.Plain(values("text").asInstanceOf[String])
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

  protected object ContentParser {
    def parseContentOption(jv: JValue, rootFile: File)(implicit tracker: FieldUsageTracker): Option[Content] = {
      val mediaTypeOption = getFieldOpt[String](jv, "media_type", false)
      val photoOption = getFieldOpt[String](jv, "photo", false)
      val fileOption = getFieldOpt[String](jv, "file", false)
      val locPresent = (jv \ "location_information") != JNothing
      val pollQuestionPresent = (jv \ "poll" \ "question") != JNothing
      val contactInfoPresent = (jv \ "contact_information") != JNothing
      (mediaTypeOption, photoOption, fileOption, locPresent, pollQuestionPresent, contactInfoPresent) match {
        case (None, None, None, false, false, false)                     => None
        case (Some("sticker"), None, Some(_), false, false, false)       => Some(parseSticker(jv, rootFile))
        case (Some("animation"), None, Some(_), false, false, false)     => Some(parseAnimation(jv, rootFile))
        case (Some("video_message"), None, Some(_), false, false, false) => Some(parseVideoMsg(jv, rootFile))
        case (Some("voice_message"), None, Some(_), false, false, false) => Some(parseVoiceMsg(jv, rootFile))
        case (Some("video_file"), None, Some(_), false, false, false)    => Some(parseFile(jv, rootFile))
        case (Some("audio_file"), None, Some(_), false, false, false)    => Some(parseFile(jv, rootFile))
        case (None, Some(_), None, false, false, false)                  => Some(parsePhoto(jv, rootFile))
        case (None, None, Some(_), false, false, false)                  => Some(parseFile(jv, rootFile))
        case (None, None, None, true, false, false)                      => Some(parseLocation(jv))
        case (None, None, None, false, true, false)                      => Some(parsePoll(jv))
        case (None, None, None, false, false, true)                      => Some(parseSharedContact(jv, rootFile))
        case _ =>
          throw new IllegalArgumentException(s"Couldn't determine content type for '$jv'")
      }
    }

    private def parseSticker(jv: JValue, rootFile: File)(implicit tracker: FieldUsageTracker): Content.Sticker = {
      Content.Sticker(
        pathOption          = getFileOpt(jv, "file", true, rootFile),
        thumbnailPathOption = getFileOpt(jv, "thumbnail", true, rootFile),
        emojiOption         = getStringOpt(jv, "sticker_emoji", false),
        widthOption         = getFieldOpt[Int](jv, "width", false),
        heightOption        = getFieldOpt[Int](jv, "height", false)
      )
    }

    private def parsePhoto(jv: JValue, rootFile: File)(implicit tracker: FieldUsageTracker): Content.Photo = {
      Content.Photo(
        pathOption = getFileOpt(jv, "photo", true, rootFile),
        width      = getCheckedField[Int](jv, "width"),
        height     = getCheckedField[Int](jv, "height"),
      )
    }

    private def parseAnimation(jv: JValue, rootFile: File)(implicit tracker: FieldUsageTracker): Content.Animation = {
      Content.Animation(
        pathOption          = getFileOpt(jv, "file", true, rootFile),
        thumbnailPathOption = getFileOpt(jv, "thumbnail", true, rootFile),
        mimeTypeOption      = Some(getCheckedField[String](jv, "mime_type")),
        durationSecOption   = getFieldOpt[Int](jv, "duration_seconds", false),
        width               = getCheckedField[Int](jv, "width"),
        height              = getCheckedField[Int](jv, "height"),
      )
    }

    private def parseVoiceMsg(jv: JValue, rootFile: File)(implicit tracker: FieldUsageTracker): Content.VoiceMsg = {
      Content.VoiceMsg(
        pathOption        = getFileOpt(jv, "file", true, rootFile),
        mimeTypeOption    = Some(getCheckedField[String](jv, "mime_type")),
        durationSecOption = getFieldOpt[Int](jv, "duration_seconds", false),
      )
    }

    private def parseVideoMsg(jv: JValue, rootFile: File)(implicit tracker: FieldUsageTracker): Content.VideoMsg = {
      Content.VideoMsg(
        pathOption          = getFileOpt(jv, "file", true, rootFile),
        thumbnailPathOption = getFileOpt(jv, "thumbnail", true, rootFile),
        mimeTypeOption      = Some(getCheckedField[String](jv, "mime_type")),
        durationSecOption   = getFieldOpt[Int](jv, "duration_seconds", false),
        width               = getCheckedField[Int](jv, "width"),
        height              = getCheckedField[Int](jv, "height"),
      )
    }

    private def parseFile(jv: JValue, rootFile: File)(implicit tracker: FieldUsageTracker): Content.File = {
      Content.File(
        pathOption          = getFileOpt(jv, "file", true, rootFile),
        thumbnailPathOption = getFileOpt(jv, "thumbnail", false, rootFile),
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
        lat               = getCheckedField[BigDecimal](jv, "location_information", "latitude"),
        lon               = getCheckedField[BigDecimal](jv, "location_information", "longitude"),
        durationSecOption = getFieldOpt[Int](jv, "live_location_period_seconds", false)
      )
    }

    private def parsePoll(jv: JValue)(implicit tracker: FieldUsageTracker): Content.Poll = {
      Content.Poll(
        question = getCheckedField[String](jv, "poll", "question")
      )
    }

    private def parseSharedContact(jv: JValue, rootFile: File)(
        implicit tracker: FieldUsageTracker): Content.SharedContact = {
      val ci = getRawField(jv, "contact_information", true)
      Content.SharedContact(
        firstNameOption   = getStringOpt(ci, "first_name", true),
        lastNameOption    = getStringOpt(ci, "last_name", true),
        phoneNumberOption = getStringOpt(ci, "phone_number", true),
        vcardPathOption   = getFileOpt(jv, "contact_vcard", false, rootFile)
      )
    }
  }

  protected def parseChat(jv: JValue, dsUuid: UUID, msgCount: Int): Chat = {
    implicit val tracker = new FieldUsageTracker
    tracker.markUsed("messages")
    tracker.ensuringUsage(jv) {
      Chat(
        dsUuid     = dsUuid,
        id         = getCheckedField[Long](jv, "id"),
        nameOption = getStringOpt(jv, "name", true),
        tpe = getCheckedField[String](jv, "type") match {
          case "personal_chat"      => ChatType.Personal
          case "private_group"      => ChatType.PrivateGroup
          case "private_supergroup" => ChatType.PrivateGroup
          case s                    => throw new IllegalArgumentException(s"Illegal format, unknown chat type '$s'")
        },
        imgPathOption = None,
        msgCount      = msgCount
      )
    }
  }

  //
  // Utility
  //

  protected def stringToOption(s: String): Option[String] = {
    s match {
      case ""                                                                 => None
      case "(File not included. Change data exporting settings to download.)" => None
      case other                                                              => Some(other)
    }
  }

  protected def isWhitespaceOrInvisible(s: String): Boolean = {
    // Accounts for invisible formatting indicator, e.g. zero-width space \u200B
    s matches "[\\s\\p{Cf}]*"
  }

  /** Dates in TG history is exported in local timezone, so we'll try to import in current one as well */
  protected def stringToDateTimeOpt(s: String): Option[DateTime] = {
    DateTime.parse(s) match {
      case dt if dt.year.get == 1970 => None // TG puts minimum timestamp in place of absent
      case other                     => Some(other)
    }
  }

  protected def getRawField(jv: JValue, fieldName: String, mustPresent: Boolean)(
      implicit tracker: FieldUsageTracker): JValue = {
    val res = jv \ fieldName
    tracker.markUsed(fieldName)
    if (mustPresent) {
      require(res != JNothing, s"Incompatible format! Field '$fieldName' not found in ${jv.toString.take(500)}")
    }
    res
  }

  protected def getFieldOpt[A](jv: JValue, fieldName: String, mustPresent: Boolean)(
      implicit formats: Formats,
      mf: scala.reflect.Manifest[A],
      tracker: FieldUsageTracker): Option[A] = {
    getRawField(jv, fieldName, mustPresent).extractOpt[A]
  }

  protected def getStringOpt(jv: JValue, fieldName: String, mustPresent: Boolean)(
      implicit formats: Formats,
      tracker: FieldUsageTracker): Option[String] = {
    val res = jv \ fieldName
    tracker.markUsed(fieldName)
    if (mustPresent) {
      require(res != JNothing, s"Incompatible format! Field '$fieldName' not found in ${jv.toString.take(500)}")
    }
    res.extractOpt[String] flatMap stringToOption
  }

  protected def getFileOpt(jv: JValue, fieldName: String, mustPresent: Boolean, rootFile: File)(
      implicit formats: Formats,
      tracker: FieldUsageTracker): Option[File] = {
    getStringOpt(jv, fieldName, mustPresent) map (p => new File(rootFile, p).getAbsoluteFile)
  }

  protected def getCheckedField[A](jv: JValue, fieldName: String)(implicit formats: Formats,
                                                                  mf: scala.reflect.Manifest[A],
                                                                  tracker: FieldUsageTracker): A = {
    getRawField(jv, fieldName, true).extract[A]
  }

  protected def getCheckedField[A](jv: JValue, fn1: String, fn2: String)(implicit formats: Formats,
                                                                         mf: scala.reflect.Manifest[A],
                                                                         tracker: FieldUsageTracker): A = {
    val res = jv \ fn1 \ fn2
    require(res != JNothing, s"Incompatible format! Path '$fn1 \\ $fn2' not found in $jv")
    tracker.markUsed(fn1)
    res.extract[A]
  }

  protected case class ShortUser(id: Long, fullNameOption: Option[String])

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
          val unused = objFields.diff(markedFields)
          if (unused.nonEmpty) {
            throw new IllegalArgumentException(s"Unused fields! $unused for ${jv.toString.take(500)}")
          }
        case _ =>
          throw new IllegalArgumentException("Not a JObject! " + jv)
      }
    }
  }
}
