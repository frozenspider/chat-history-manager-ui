package org.fs.chm.dao

import java.io.{File => JFile}

import com.github.nscala_time.time.Imports._
import org.fs.chm.protobuf._
import org.fs.chm.utility.LangUtils._
import org.fs.utility.Imports._

object Entities {

  sealed trait InternalIdTag
  type MessageInternalId = Long with InternalIdTag

  sealed trait SourceIdTag
  type MessageSourceId = Long with SourceIdTag

  sealed trait DatasetRootTag
  type DatasetRoot = JFile with DatasetRootTag

  case class ChatWithDetails(chat: Chat,
                             lastMsgOption: Option[Message],

                             /** First element MUST be myself, the rest should be in some fixed order. */
                             members: Seq[User]) {
    val dsUuid: PbUuid = chat.dsUuid

    /** Used to resolve plaintext members */
    def resolveMemberIndex(memberName: String): Int =
      members indexWhere (_.prettyName == memberName)
  }

  case class CombinedChat(mainCwd: ChatWithDetails, slaveCwds: Seq[ChatWithDetails]) {
    val dsUuid: PbUuid = mainCwd.chat.dsUuid

    val mainChatId: Long = mainCwd.chat.id

    val cwds: Seq[ChatWithDetails] = mainCwd +: slaveCwds

    val members: Seq[User] = cwds.flatMap(_.members).distinct

    require(cwds.map(_.dsUuid).distinct.length == 1)

    /** Used to resolve plaintext members */
    def resolveMemberIndex(memberName: String): Int =
      members indexWhere (_.prettyName == memberName)

    override def equals(that: Any): Boolean = {
      that match {
        case that: CombinedChat => this.dsUuid == that.dsUuid && this.mainChatId == that.mainChatId
        case _ => false
      }
    }

    override def hashCode(): Int = {
      dsUuid.hashCode() * 31 + mainChatId.hashCode() * 7
    }
  }

  val NoInternalId: MessageInternalId = -1L.asInstanceOf[MessageInternalId]

  val Unnamed = "[unnamed]"

  def fromJavaUuid(uuid: java.util.UUID): PbUuid =
    PbUuid(uuid.toString.toLowerCase)

  def randomUuid: PbUuid =
    fromJavaUuid(java.util.UUID.randomUUID)

  //
  // Content
  //

  trait WithPathFileOption {
    def pathFileOption(datasetRoot: DatasetRoot): Option[JFile]
  }

  implicit class ExtendedContent(c: Content) extends WithPathFileOption {
    def pathFileOption(datasetRoot: DatasetRoot): Option[JFile] = {
      require(hasPath, "No path available!")
      c match {
        case c: ContentSticker    => c.pathFileOption(datasetRoot)
        case c: ContentPhoto      => c.pathFileOption(datasetRoot)
        case c: ContentVoiceMsg   => c.pathFileOption(datasetRoot)
        case c: ContentAudio      => c.pathFileOption(datasetRoot)
        case c: ContentVideoMsg   => c.pathFileOption(datasetRoot)
        case c: ContentVideo      => c.pathFileOption(datasetRoot)
        case c: ContentFile       => c.pathFileOption(datasetRoot)
        case c                    => None
      }
    }

    def hasPath: Boolean =
      c match {
        case _: ContentSticker   => true
        case _: ContentPhoto     => true
        case _: ContentVoiceMsg  => true
        case _: ContentAudio     => true
        case _: ContentVideoMsg  => true
        case _: ContentVideo     => true
        case _: ContentFile      => true
        case _                   => false
      }
  }

  implicit class ExtendedContentSticker(c: ContentSticker) extends WithPathFileOption {
    def pathFileOption(datasetRoot: DatasetRoot): Option[JFile] = c.pathOption.map(_.toFile(datasetRoot))

    def thumbnailPathFileOption(datasetRoot: DatasetRoot): Option[JFile] = c.thumbnailPathOption.map(_.toFile(datasetRoot))
  }

  implicit class ExtendedContentPhoto(c: ContentPhoto) extends WithPathFileOption {
    def pathFileOption(datasetRoot: DatasetRoot): Option[JFile] = c.pathOption.map(_.toFile(datasetRoot))
  }

  implicit class ExtendedContentVoiceMsg(c: ContentVoiceMsg) extends WithPathFileOption {
    def pathFileOption(datasetRoot: DatasetRoot): Option[JFile] = c.pathOption.map(_.toFile(datasetRoot))
  }

  implicit class ExtendedContentAudio(c: ContentAudio) extends WithPathFileOption {
    def pathFileOption(datasetRoot: DatasetRoot): Option[JFile] = c.pathOption.map(_.toFile(datasetRoot))
  }

  implicit class ExtendedContentVideoMsg(c: ContentVideoMsg) extends WithPathFileOption {
    def pathFileOption(datasetRoot: DatasetRoot): Option[JFile] = c.pathOption.map(_.toFile(datasetRoot))

    def thumbnailPathFileOption(datasetRoot: DatasetRoot): Option[JFile] = c.thumbnailPathOption.map(_.toFile(datasetRoot))
  }

  implicit class ExtendedContentVideo(c: ContentVideo) extends WithPathFileOption {
    def pathFileOption(datasetRoot: DatasetRoot): Option[JFile] = c.pathOption.map(_.toFile(datasetRoot))

    def thumbnailPathFileOption(datasetRoot: DatasetRoot): Option[JFile] = c.thumbnailPathOption.map(_.toFile(datasetRoot))
  }

  implicit class ExtendedContentFile(c: ContentFile) extends WithPathFileOption {
    def pathFileOption(datasetRoot: DatasetRoot): Option[JFile] = c.pathOption.map(_.toFile(datasetRoot))

    def thumbnailPathFileOption(datasetRoot: DatasetRoot): Option[JFile] = c.thumbnailPathOption.map(_.toFile(datasetRoot))
  }

  implicit class ExtendedContentLocation(c: ContentLocation) {
    def lat: BigDecimal = BigDecimal(c.latStr)

    def lon: BigDecimal = BigDecimal(c.lonStr)
  }

  implicit class ExtendedContentSharedContact(c: ContentSharedContact) {
    def vcardFileOption(datasetRoot: DatasetRoot): Option[JFile] = c.vcardPathOption.map(_.toFile(datasetRoot))

    // Same as ExtendedUser
    lazy val prettyNameOption: Option[String] = {
      (c.firstNameOption, c.lastNameOption, c.phoneNumberOption) match {
        case (Some(fn), Some(ln), _)   => Some(s"$fn $ln")
        case (Some(fn), None, _)       => Some(fn)
        case (None, Some(ln), _)       => Some(ln)
        case (None, None, Some(phone)) => Some(phone)
        case _                         => None
      }
    }

    lazy val prettyName: String =
      prettyNameOption getOrElse Unnamed
  }

  //
  //
  //

  implicit class ExtendedId(id: Long) {
    def toReadableId: String = id.toString.reverse.grouped(3).mkString(" ").reverse
  }

  implicit class ExtendedUser(u: User) {
    // Same as ExtendedContentSharedContact
    lazy val prettyNameOption: Option[String] = {
      (u.firstNameOption, u.lastNameOption, u.phoneNumberOption) match {
        case (Some(fn), Some(ln), _)   => Some(s"$fn $ln")
        case (Some(fn), None, _)       => Some(fn)
        case (None, Some(ln), _)       => Some(ln)
        case (None, None, Some(phone)) => Some(phone)
        case _                         => None
      }
    }

    lazy val prettyName: String =
      prettyNameOption getOrElse Unnamed
  }

  implicit class ExtendedChat(c: Chat) {
    def nameOrUnnamed: String = c.nameOption getOrElse Unnamed

    def qualifiedName: String = s"'$nameOrUnnamed' (#${c.id})"
  }

  implicit class ExtendedSourceType(st: SourceType) {
    def prettyString: String = st match {
      case SourceType.TextImport => "Text import"
      case SourceType.Telegram   => "Telegram"
      case SourceType.WhatsappDb => "WhatsApp"
      case SourceType.TinderDb   => "Tinder"
      case SourceType.BadooDb    => "Badoo"
    }
  }

  //
  // Message
  //

  implicit class ExtendedMessage(msg: Message) {
    def internalIdTyped: MessageInternalId =
      msg.internalId.asInstanceOf[MessageInternalId]

    def sourceIdTypedOption: Option[MessageSourceId] =
      msg.sourceIdOption.map(_.asInstanceOf[MessageSourceId])

    def time: DateTime =
      new DateTime(msg.timestamp * 1000)
  }

  implicit class ExtendedMessageRegular(msg: MessageRegular) {
    def editTimeOption: Option[DateTime] =
      msg.editTimestampOption map (ts => new DateTime(ts * 1000))

    def replyToMessageIdTypedOption: Option[MessageSourceId] =
      msg.replyToMessageIdOption.map(_.asInstanceOf[MessageSourceId])
  }

  implicit class ExtendedMessageServicePinMessage(msg: MessageServicePinMessage) {
    def messageIdTyped: MessageSourceId =
      msg.messageId.asInstanceOf[MessageSourceId]
  }

  //
  // Rich Text
  //

  implicit class ExtendedRichTextElement(rte: RichTextElement) {
    def textOption: Option[String] = {
      if (rte.`val`.isEmpty) {
        None
      } else rte.`val`.value match {
        case el: RtePlain         => el.text.toOption
        case el: RteBold          => el.text.toOption
        case el: RteItalic        => el.text.toOption
        case el: RteUnderline     => el.text.toOption
        case el: RteStrikethrough => el.text.toOption
        case el: RteBlockquote    => el.text.toOption
        case el: RteSpoiler       => el.text.toOption
        case el: RteLink          => el.textOption
        case el: RtePrefmtInline  => el.text.toOption
        case el: RtePrefmtBlock   => el.text.toOption
      }
    }

    def textOrEmptyString: String =
      textOption match {
        case None    => ""
        case Some(s) => s
      }
  }
}
