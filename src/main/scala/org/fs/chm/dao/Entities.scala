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

    /** Used to resolve plaintext members */
    def resolveMember(memberName: String): Option[User] =
     resolveMemberIndex(memberName) match {
       case -1 => None
       case x  => Some(members(x))
     }

    def resolveMembers(memberNames: Seq[String]): Seq[Option[User]] =
      memberNames.map(resolveMember)
  }

  val NoInternalId: MessageInternalId = -1L.asInstanceOf[MessageInternalId]

  val Unnamed = "[unnamed]"
  val Unknown = "[unknown]"

  def fromJavaUuid(uuid: java.util.UUID): PbUuid =
    PbUuid(uuid.toString.toLowerCase)

  def randomUuid: PbUuid =
    fromJavaUuid(java.util.UUID.randomUUID)

  def createDataset(srcAlias: String, srcType: String): Dataset =
    Dataset(
      uuid = randomUuid,
      alias = s"${srcAlias} data loaded @ " + DateTime.now().toString("yyyy-MM-dd"),
    )

  private def makeSearchableString(rteVal: RichTextElement.Val): String = {
    normalizeSeachableString(rteVal.value match {
      case RtePlain(text, _)                 => text
      case RteBold(text, _)                  => text
      case RteItalic(text, _)                => text
      case RteUnderline(text, _)             => text
      case RteStrikethrough(text, _)         => text
      case RteBlockquote(text, _)            => text
      case RteSpoiler(text, _)               => text
      case RteLink(textOpt, href, hidden, _) => textOpt.filter(_ != href).getOrElse(" ") + " " + href
      case RtePrefmtInline(text, _)          => text
      case RtePrefmtBlock(text, langOpt, _)  => text
    })
  }

  def makeSearchableString(components: Seq[RichTextElement], typed: Message.Typed): String = {
    val joinedText: String = (components.map(_.searchableString) mkString " ")

    val typedComponentText: Seq[String] = typed match {
      case Message.Typed.Regular(m) =>
        m.contentOption match {
          case Some(c: ContentSticker) =>
            Seq(c.emojiOption).yieldDefined
          case Some(c: ContentAudio) =>
            Seq(c.titleOption, c.performerOption).yieldDefined
          case Some(c: ContentVideo) =>
            Seq(c.titleOption, c.performerOption).yieldDefined
          case Some(c: ContentFile) =>
            Seq(c.fileNameOption).yieldDefined
          case Some(c: ContentLocation) =>
            Seq(c.addressOption, c.titleOption, Some(c.latStr), Some(c.lonStr)).yieldDefined
          case Some(c: ContentPoll) =>
            Seq(c.question)
          case Some(c: ContentSharedContact) =>
            Seq(c.firstNameOption, c.lastNameOption, c.phoneNumberOption).yieldDefined
          case _ =>
            // Text is enough.
            Seq.empty
        }
      case Message.Typed.Service(Some(m)) =>
        m match {
          case m: MessageServiceGroupCreate        => m.title +: m.members
          case m: MessageServiceGroupInviteMembers => m.members
          case m: MessageServiceGroupRemoveMembers => m.members
          case m: MessageServiceGroupMigrateFrom   => Seq(m.title)
          case m: MessageServiceGroupCall          => m.members
          case _                                   => Seq.empty
        }
      case Message.Typed.Empty | Message.Typed.Service(None) =>
        unexpectedCase(typed)
    }

    // Adding all links to the end to enable search over hrefs/hidden links too.
    val linksSeq: Seq[String] = components.map(_.`val`.link).collect({ case Some(l) => l.href })

    Seq(joinedText, typedComponentText.mkString(" "), linksSeq.mkString(" ")).map(_.trim).mkString(" ").trim
  }

  def normalizeSeachableString(s: String): String = {
    // \p is unicode category
    // \p{Z} is any separator (including \u00A0 no-break space)
    // \p{Cf} is any invisible formatting character (including \u200B zero-width space)
    s.replaceAll("[\\p{Z}\\p{Cf}\n]+", " ").trim
  }

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
  // PracticallyEquals implementation
  //

  def membersPracticallyEquals(members1: Seq[String], cwd1: ChatWithDetails,
                               members2: Seq[String], cwd2: ChatWithDetails) =
    cwd1.resolveMembers(members1).map(_.map(_.id)).toSet == cwd2.resolveMembers(members2).map(_.map(_.id)).toSet

  implicit object ContentStickerPracticallyEquals extends PracticallyEquals[(ContentSticker, DatasetRoot)] {
    override def practicallyEquals(v1: (ContentSticker, DatasetRoot), v2: (ContentSticker, DatasetRoot)): Boolean = {
      v1._1.pathFileOption(v1._2) =~= v2._1.pathFileOption(v2._2) &&
        v1._1.thumbnailPathFileOption(v1._2) =~= v2._1.thumbnailPathFileOption(v2._2) &&
        v1._1.copy(pathOption = None, thumbnailPathOption = None) == v2._1.copy(pathOption = None, thumbnailPathOption = None)
    }
  }

  implicit object ContentPhotoPracticallyEquals extends PracticallyEquals[(ContentPhoto, DatasetRoot)] {
    override def practicallyEquals(v1: (ContentPhoto, DatasetRoot), v2: (ContentPhoto, DatasetRoot)): Boolean = {
      v1._1.pathFileOption(v1._2) =~= v2._1.pathFileOption(v2._2) &&
        v1._1.copy(pathOption = None) == v2._1.copy(pathOption = None)
    }
  }

  implicit object ContentVoiceMsgPracticallyEquals extends PracticallyEquals[(ContentVoiceMsg, DatasetRoot)] {
    override def practicallyEquals(v1: (ContentVoiceMsg, DatasetRoot), v2: (ContentVoiceMsg, DatasetRoot)): Boolean = {
      v1._1.pathFileOption(v1._2) =~= v2._1.pathFileOption(v2._2) &&
        v1._1.copy(pathOption = None) == v2._1.copy(pathOption = None)
    }
  }

  implicit object ContentAudioPracticallyEquals extends PracticallyEquals[(ContentAudio, DatasetRoot)] {
    override def practicallyEquals(v1: (ContentAudio, DatasetRoot), v2: (ContentAudio, DatasetRoot)): Boolean = {
      v1._1.pathFileOption(v1._2) =~= v2._1.pathFileOption(v2._2) &&
        v1._1.copy(pathOption = None) == v2._1.copy(pathOption = None)
    }
  }

  implicit object ContentVideoMsgPracticallyEquals extends PracticallyEquals[(ContentVideoMsg, DatasetRoot)] {
    override def practicallyEquals(v1: (ContentVideoMsg, DatasetRoot), v2: (ContentVideoMsg, DatasetRoot)): Boolean = {
      v1._1.pathFileOption(v1._2) =~= v2._1.pathFileOption(v2._2) &&
        v1._1.thumbnailPathFileOption(v1._2) =~= v2._1.thumbnailPathFileOption(v2._2) &&
        v1._1.copy(pathOption = None, thumbnailPathOption = None) == v2._1.copy(pathOption = None, thumbnailPathOption = None)
    }
  }

  implicit object ContentVideoPracticallyEquals extends PracticallyEquals[(ContentVideo, DatasetRoot)] {
    override def practicallyEquals(v1: (ContentVideo, DatasetRoot), v2: (ContentVideo, DatasetRoot)): Boolean = {
      v1._1.pathFileOption(v1._2) =~= v2._1.pathFileOption(v2._2) &&
        v1._1.thumbnailPathFileOption(v1._2) =~= v2._1.thumbnailPathFileOption(v2._2) &&
        v1._1.copy(pathOption = None, thumbnailPathOption = None) == v2._1.copy(pathOption = None, thumbnailPathOption = None)
    }
  }

  implicit object ContentFilePracticallyEquals extends PracticallyEquals[(ContentFile, DatasetRoot)] {
    override def practicallyEquals(v1: (ContentFile, DatasetRoot), v2: (ContentFile, DatasetRoot)): Boolean = {
      v1._1.pathFileOption(v1._2) =~= v2._1.pathFileOption(v2._2) &&
        v1._1.thumbnailPathFileOption(v1._2) =~= v2._1.thumbnailPathFileOption(v2._2) &&
        v1._1.copy(pathOption = None, thumbnailPathOption = None) == v2._1.copy(pathOption = None, thumbnailPathOption = None)
    }
  }

  implicit object ContentLocationPracticallyEquals extends PracticallyEquals[(ContentLocation, DatasetRoot)] {
    override def practicallyEquals(v1: (ContentLocation, DatasetRoot), v2: (ContentLocation, DatasetRoot)): Boolean = {
      // lat/lon are strings, trailing zeros should be ignored,
      v1._1.lat == v2._1.lat && v1._1.lon == v2._1.lon &&
        v1._1.copy(latStr = "", lonStr = "") == v2._1.copy(latStr = "", lonStr = "")
    }
  }

  implicit object ContentPollPracticallyEquals extends PracticallyEquals[(ContentPoll, DatasetRoot)] {
    override def practicallyEquals(v1: (ContentPoll, DatasetRoot), v2: (ContentPoll, DatasetRoot)): Boolean = {
      // We don't really care about poll result
      v1._1 == v2._1
    }
  }

  implicit object ContentSharedContactPracticallyEquals extends PracticallyEquals[(ContentSharedContact, DatasetRoot)] {
    override def practicallyEquals(v1: (ContentSharedContact, DatasetRoot), v2: (ContentSharedContact, DatasetRoot)): Boolean = {
      v1._1.vcardFileOption(v1._2) =~= v2._1.vcardFileOption(v2._2) &&
        v1._1.copy(vcardPathOption = None) == v2._1.copy(vcardPathOption = None)
    }
  }

  implicit object ContentPracticallyEquals extends PracticallyEquals[(Content, DatasetRoot)] {
    override def practicallyEquals(v1: (Content, DatasetRoot), v2: (Content, DatasetRoot)): Boolean = {
      (v1._1, v2._1) match {
        case (c1: ContentSticker,       c2: ContentSticker)       => (c1, v1._2) =~= (c2, v2._2)
        case (c1: ContentPhoto,         c2: ContentPhoto)         => (c1, v1._2) =~= (c2, v2._2)
        case (c1: ContentVoiceMsg,      c2: ContentVoiceMsg)      => (c1, v1._2) =~= (c2, v2._2)
        case (c1: ContentAudio,         c2: ContentAudio)         => (c1, v1._2) =~= (c2, v2._2)
        case (c1: ContentVideoMsg,      c2: ContentVideoMsg)      => (c1, v1._2) =~= (c2, v2._2)
        case (c1: ContentVideo,         c2: ContentVideo)         => (c1, v1._2) =~= (c2, v2._2)
        case (c1: ContentFile,          c2: ContentFile)          => (c1, v1._2) =~= (c2, v2._2)
        case (c1: ContentLocation,      c2: ContentLocation)      => (c1, v1._2) =~= (c2, v2._2)
        case (c1: ContentPoll,          c2: ContentPoll)          => (c1, v1._2) =~= (c2, v2._2)
        case (c1: ContentSharedContact, c2: ContentSharedContact) => (c1, v1._2) =~= (c2, v2._2)
        case _                                                    => false
      }
    }
  }

  implicit object ContentOptionPracticallyEquals extends PracticallyEquals[(Option[Content], DatasetRoot)] {
    override def practicallyEquals(v1: (Option[Content], DatasetRoot), v2: (Option[Content], DatasetRoot)): Boolean = {
      (v1._1, v2._1) match {
        case (Some(c1), Some(c2)) => (c1, v1._2) =~= (c2, v2._2)
        case (None, None)         => true
        case _                    => false
      }
    }
  }

  implicit object MessageRegularPracticallyEquals extends PracticallyEquals[(MessageRegular, DatasetRoot)] {
    override def practicallyEquals(v1: (MessageRegular, DatasetRoot), v2: (MessageRegular, DatasetRoot)): Boolean = {
      val contentEquals = (v1._1.contentOption, v2._1.contentOption) match {
        case (None, None)           => true
        case (Some(v1c), Some(v2c)) => (v1c, v1._2) =~= (v2c, v2._2)
        case _                      => false
      }
      contentEquals &&
        v1._1.copy(
          forwardFromNameOption = None,
          contentOption = None
        ) == v2._1.copy(
          forwardFromNameOption = None,
          contentOption = None
        )
    }
  }

  implicit object MessageServicePracticallyEquals extends PracticallyEquals[(MessageService, DatasetRoot, ChatWithDetails)] {
    override def practicallyEquals(v1: (MessageService, DatasetRoot, ChatWithDetails),
                                   v2: (MessageService, DatasetRoot, ChatWithDetails)): Boolean = {
      if (v1._1.getClass != v2._1.getClass) {
        false
      } else (v1._1, v2._1) match {
        case (m1: MessageServicePhoneCall,           m2: MessageServicePhoneCall)           => m1 == m2
        case (m1: MessageServiceSuggestProfilePhoto, m2: MessageServiceSuggestProfilePhoto) => (m1.photo, v1._2) =~= (m2.photo, v2._2)
        case (m1: MessageServicePinMessage,          m2: MessageServicePinMessage)          => m1 == m2
        case (m1: MessageServiceClearHistory,        m2: MessageServiceClearHistory)        => m1 == m2
        case (m1: MessageServiceBlockUser,           m2: MessageServiceBlockUser)           => m1 == m2
        case (m1: MessageServiceGroupCreate,         m2: MessageServiceGroupCreate)         =>
          m1.copy(members = Seq.empty) == m2.copy(members = Seq.empty) &&
            membersPracticallyEquals(m1.members, v1._3, m2.members, v2._3)
        case (m1: MessageServiceGroupEditTitle,      m2: MessageServiceGroupEditTitle)      => m1 == m2
        case (m1: MessageServiceGroupEditPhoto,      m2: MessageServiceGroupEditPhoto)      => (m1.photo, v1._2) =~= (m2.photo, v2._2)
        case (m1: MessageServiceGroupDeletePhoto,    m2: MessageServiceGroupDeletePhoto)    => m1 == m2
        case (m1: MessageServiceGroupInviteMembers,  m2: MessageServiceGroupInviteMembers)  =>
          m1.copy(members = Seq.empty) == m2.copy(members = Seq.empty) &&
            membersPracticallyEquals(m1.members, v1._3, m2.members, v2._3)
        case (m1: MessageServiceGroupRemoveMembers,  m2: MessageServiceGroupRemoveMembers)  =>
          m1.copy(members = Seq.empty) == m2.copy(members = Seq.empty) &&
            membersPracticallyEquals(m1.members, v1._3, m2.members, v2._3)
        case (m1: MessageServiceGroupMigrateFrom,    m2: MessageServiceGroupMigrateFrom)    => m1 == m2
        case (m1: MessageServiceGroupMigrateTo,      m2: MessageServiceGroupMigrateTo)      => m1 == m2
        case (m1: MessageServiceGroupCall,           m2: MessageServiceGroupCall)           =>
          m1.copy(members = Seq.empty) == m2.copy(members = Seq.empty) &&
            membersPracticallyEquals(m1.members, v1._3, m2.members, v2._3)
        case _ => unexpectedCase((v1._1, v2._1))
      }
    }
  }

  implicit object TypedMessagePracticallyEquals extends PracticallyEquals[(Message.Typed, DatasetRoot, ChatWithDetails)] {
    override def practicallyEquals(v1: (Message.Typed, DatasetRoot, ChatWithDetails),
                                   v2: (Message.Typed, DatasetRoot, ChatWithDetails)): Boolean = {
      (v1._1, v2._1) match {
        case (Message.Typed.Regular(v1Regular), Message.Typed.Regular(v2Regular)) =>
          (v1Regular, v1._2) =~= (v2Regular, v2._2)
        case (Message.Typed.Service(Some(v1Service)), Message.Typed.Service(Some(v2Service))) =>
          (v1Service, v1._2, v1._3) =~= (v2Service, v2._2, v2._3)
        case _ => false
      }
    }
  }

  implicit object MessagePracticallyEquals extends PracticallyEquals[(Message, DatasetRoot, ChatWithDetails)] {
    /** "Practical equality" that ignores internal ID, paths of files with equal content, and some unimportant fields */
    override def practicallyEquals(v1: (Message, DatasetRoot, ChatWithDetails),
                                   v2: (Message, DatasetRoot, ChatWithDetails)): Boolean =
      v1._1.copy(
        internalId       = NoInternalId,
        searchableString = "",
        typed            = Message.Typed.Empty
      ) == v2._1.copy(
        internalId       = NoInternalId,
        searchableString = "",
        typed            = Message.Typed.Empty
      ) && (v1._1.typed, v1._2, v1._3) =~= (v2._1.typed, v2._2, v2._3)
  }

  //
  //
  //

  implicit class ExtendedId(id: Long) {
    def toReadableId: String = id.toString.reverse.grouped(3).mkString(" ").reverse
  }

  implicit class ExtendedUser(u: User) {
    def firstNameOrUnnamed: String = u.firstNameOption getOrElse Unnamed

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

  //
  // Message
  //

  implicit class ExtendedMessage(msg: Message) {
    def internalIdTyped: MessageInternalId =
      msg.internalId.asInstanceOf[MessageInternalId]

    def sourceIdTypedOption: Option[MessageSourceId] =
      msg.sourceIdOption.map(_.asInstanceOf[MessageSourceId])

    def files(datasetRoot: DatasetRoot): Set[JFile] = {
      val optionsSet: Set[Option[String]] = msg.typed match {
        case Message.Typed.Regular(msgRegular) =>
          msgRegular.contentOption match {
            case None => Set.empty
            case Some(v: ContentSticker)       => Set(v.pathOption, v.thumbnailPathOption)
            case Some(v: ContentPhoto)         => Set(v.pathOption)
            case Some(v: ContentVoiceMsg)      => Set(v.pathOption)
            case Some(v: ContentAudio)         => Set(v.pathOption)
            case Some(v: ContentVideoMsg)      => Set(v.pathOption, v.thumbnailPathOption)
            case Some(v: ContentVideo)         => Set(v.pathOption, v.thumbnailPathOption)
            case Some(v: ContentFile)          => Set(v.pathOption, v.thumbnailPathOption)
            case Some(v: ContentLocation)      => Set.empty
            case Some(v: ContentPoll)          => Set.empty
            case Some(v: ContentSharedContact) => Set(v.vcardPathOption)
          }
        case Message.Typed.Service(Some(ms)) =>
          ms match {
            case _: MessageServicePhoneCall           => Set.empty
            case m: MessageServiceSuggestProfilePhoto => Set(m.photo.pathOption)
            case _: MessageServicePinMessage          => Set.empty
            case _: MessageServiceClearHistory        => Set.empty
            case _: MessageServiceBlockUser           => Set.empty
            case _: MessageServiceGroupCreate         => Set.empty
            case _: MessageServiceGroupEditTitle      => Set.empty
            case m: MessageServiceGroupEditPhoto      => Set(m.photo.pathOption)
            case _: MessageServiceGroupDeletePhoto    => Set.empty
            case _: MessageServiceGroupInviteMembers  => Set.empty
            case _: MessageServiceGroupRemoveMembers  => Set.empty
            case _: MessageServiceGroupMigrateFrom    => Set.empty
            case _: MessageServiceGroupMigrateTo      => Set.empty
            case _: MessageServiceGroupCall           => Set.empty
          }
        case Message.Typed.Empty | Message.Typed.Service(None) =>
          unexpectedCase(msg)
      }
      optionsSet.yieldDefined.map(_.toFile(datasetRoot))
    }

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

  object RichText {
    def makePlain(text: String): RichTextElement =
      make(RichTextElement.Val.Plain(RtePlain(text)))

    def makeBold(text: String): RichTextElement =
      make(RichTextElement.Val.Bold(RteBold(text)))

    def makeItalic(text: String): RichTextElement =
      make(RichTextElement.Val.Italic(RteItalic(text)))

    def makeUnderline(text: String): RichTextElement =
      make(RichTextElement.Val.Underline(RteUnderline(text)))

    def makeStrikethrough(text: String): RichTextElement =
      make(RichTextElement.Val.Strikethrough(RteStrikethrough(text)))

    def makeBlockquote(text: String): RichTextElement =
      make(RichTextElement.Val.Blockquote(RteBlockquote(text)))

    def makeSpoiler(text: String): RichTextElement =
      make(RichTextElement.Val.Spoiler(RteSpoiler(text)))

    def makeLink(textOption: Option[String], href: String, hidden: Boolean): RichTextElement =
      make(RichTextElement.Val.Link(RteLink(textOption, href, hidden)))

    def makePrefmtInline(text: String): RichTextElement =
      make(RichTextElement.Val.PrefmtInline(RtePrefmtInline(text)))

    def makePrefmtBlock(text: String, languageOption: Option[String]): RichTextElement =
      make(RichTextElement.Val.PrefmtBlock(RtePrefmtBlock(text = text, languageOption = languageOption)))

    private def make(rteVal: RichTextElement.Val): RichTextElement = {
      RichTextElement(rteVal, makeSearchableString(rteVal))
    }
  }

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
