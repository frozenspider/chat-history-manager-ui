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
  }

  val NoInternalId: MessageInternalId = -1L.asInstanceOf[MessageInternalId]

  val Unnamed = "[unnamed]"

  def fromJavaUuid(uuid: java.util.UUID): PbUuid =
    PbUuid(uuid.toString.toLowerCase)

  def randomUuid: PbUuid =
    fromJavaUuid(java.util.UUID.randomUUID)

  def createDataset(srcAlias: String, srcType: String): Dataset =
    Dataset(
      uuid = randomUuid,
      alias = s"${srcAlias} data loaded @ " + DateTime.now().toString("yyyy-MM-dd"),
      sourceType = srcType
    )

  /** Should be kept in sync with RichTet.make*! */
  def makeSearchableString(rte: RichTextElement): String = {
    rte.`val`.value match {
      case RtePlain(text, _)                 => text
      case RteBold(text, _)                  => text
      case RteItalic(text, _)                => text
      case RteUnderline(text, _)             => text
      case RteStrikethrough(text, _)         => text
      case RteLink(textOpt, href, hiddem, _) => (textOpt getOrElse "") + href
      case RtePrefmtInline(text, _)          => text
      case RtePrefmtBlock(text, langOpt, _)  => text
    }
  }

  def makeSearchableString(components: Seq[RichTextElement], typed: Message.Typed): String = {
    val joinedText: String = (components.map(_.searchableString).yieldDefined mkString " ")

    val typedComponentText: Seq[String] = typed match {
      case _: Message.Typed.Regular =>
        // Text is enough.
        Seq.empty
      case m: Message.Typed.Service =>
        m.value.`val` match {
          case m: MessageService.Val.GroupCreate        => m.value.title +: m.value.members
          case m: MessageService.Val.GroupInviteMembers => m.value.members
          case m: MessageService.Val.GroupRemoveMembers => m.value.members
          case m: MessageService.Val.GroupMigrateFrom   => Seq(m.value.title)
          case m: MessageService.Val.GroupCall          => m.value.members
          case _                                        => Seq.empty
        }
    }

    // Adding all links to the end to enable search over hrefs/hidden links too.
    val linksSeq: Seq[String] = components.map(_.`val`.link).collect({ case Some(l) => l.href })

    Seq(joinedText, typedComponentText.mkString(" "), linksSeq.mkString(" ")).map(_.trim).mkString(" ").trim
  }

  def normalizeSeachableString(s: String): String =
    s.replaceAll("[\\s\\p{Cf}\n]+", " ").trim

  //
  // PracticallyEquals implementation
  //

  implicit object ContentStickerPracticallyEquals extends PracticallyEquals[(ContentSticker, DatasetRoot)] {
    override def practicallyEquals(v1: (ContentSticker, DatasetRoot), v2: (ContentSticker, DatasetRoot)): Boolean = {
      v1._1.pathFileOption(v1._2) =~= v2._1.pathFileOption(v1._2) &&
        v1._1.thumbnailPathFileOption(v1._2) =~= v2._1.thumbnailPathFileOption(v1._2) &&
        v1._1.copy(pathOption = None, thumbnailPathOption = None) == v2._1.copy(pathOption = None, thumbnailPathOption = None)
    }
  }

  implicit object ContentPhotoPracticallyEquals extends PracticallyEquals[(ContentPhoto, DatasetRoot)] {
    override def practicallyEquals(v1: (ContentPhoto, DatasetRoot), v2: (ContentPhoto, DatasetRoot)): Boolean = {
      v1._1.pathFileOption(v1._2) =~= v2._1.pathFileOption(v1._2) &&
        v1._1.copy(pathOption = None) == v2._1.copy(pathOption = None)
    }
  }

  implicit object ContentVoiceMsgPracticallyEquals extends PracticallyEquals[(ContentVoiceMsg, DatasetRoot)] {
    override def practicallyEquals(v1: (ContentVoiceMsg, DatasetRoot), v2: (ContentVoiceMsg, DatasetRoot)): Boolean = {
      v1._1.pathFileOption(v1._2) =~= v2._1.pathFileOption(v1._2) &&
        v1._1.copy(pathOption = None) == v2._1.copy(pathOption = None)
    }
  }

  implicit object ContentVideoMsgPracticallyEquals extends PracticallyEquals[(ContentVideoMsg, DatasetRoot)] {
    override def practicallyEquals(v1: (ContentVideoMsg, DatasetRoot), v2: (ContentVideoMsg, DatasetRoot)): Boolean = {
      v1._1.pathFileOption(v1._2) =~= v2._1.pathFileOption(v1._2) &&
        v1._1.thumbnailPathFileOption(v1._2) =~= v2._1.thumbnailPathFileOption(v1._2) &&
        v1._1.copy(pathOption = None, thumbnailPathOption = None) == v2._1.copy(pathOption = None, thumbnailPathOption = None)
    }
  }

  implicit object ContentAnimationPracticallyEquals extends PracticallyEquals[(ContentAnimation, DatasetRoot)] {
    override def practicallyEquals(v1: (ContentAnimation, DatasetRoot), v2: (ContentAnimation, DatasetRoot)): Boolean = {
      v1._1.pathFileOption(v1._2) =~= v2._1.pathFileOption(v1._2) &&
        v1._1.thumbnailPathFileOption(v1._2) =~= v2._1.thumbnailPathFileOption(v1._2) &&
        v1._1.copy(pathOption = None, thumbnailPathOption = None) == v2._1.copy(pathOption = None, thumbnailPathOption = None)
    }
  }

  implicit object ContentFilePracticallyEquals extends PracticallyEquals[(ContentFile, DatasetRoot)] {
    override def practicallyEquals(v1: (ContentFile, DatasetRoot), v2: (ContentFile, DatasetRoot)): Boolean = {
      v1._1.pathFileOption(v1._2) =~= v2._1.pathFileOption(v1._2) &&
        v1._1.thumbnailPathFileOption(v1._2) =~= v2._1.thumbnailPathFileOption(v1._2) &&
        v1._1.copy(pathOption = None, thumbnailPathOption = None) == v2._1.copy(pathOption = None, thumbnailPathOption = None)
    }
  }

  implicit object ContentLocationPracticallyEquals extends PracticallyEquals[(ContentLocation, DatasetRoot)] {
    override def practicallyEquals(v1: (ContentLocation, DatasetRoot), v2: (ContentLocation, DatasetRoot)): Boolean =
      v1._1 == v2._1
  }

  implicit object ContentPollPracticallyEquals extends PracticallyEquals[(ContentPoll, DatasetRoot)] {
    override def practicallyEquals(v1: (ContentPoll, DatasetRoot), v2: (ContentPoll, DatasetRoot)): Boolean = {
      // We don't really care about poll result
      v1._1 == v2._1
    }
  }

  implicit object ContentSharedContactPracticallyEquals extends PracticallyEquals[(ContentSharedContact, DatasetRoot)] {
    override def practicallyEquals(v1: (ContentSharedContact, DatasetRoot), v2: (ContentSharedContact, DatasetRoot)): Boolean = {
      v1._1.vcardFileOption(v1._2) =~= v2._1.vcardFileOption(v1._2) &&
        v1._1.copy(vcardPathOption = None) == v2._1.copy(vcardPathOption = None)
    }
  }

  implicit object ContentPracticallyEquals extends PracticallyEquals[(Content, DatasetRoot)] {
    override def practicallyEquals(v1: (Content, DatasetRoot), v2: (Content, DatasetRoot)): Boolean = {
      import Content.Val._;
      (v1._1.`val`, v2._1.`val`) match {
        case (Sticker(c1),       Sticker(c2))       => (c1, v1._2) =~= (c2, v2._2)
        case (Photo(c1),         Photo(c2))         => (c1, v1._2) =~= (c2, v2._2)
        case (VoiceMsg(c1),      VoiceMsg(c2))      => (c1, v1._2) =~= (c2, v2._2)
        case (VideoMsg(c1),      VideoMsg(c2))      => (c1, v1._2) =~= (c2, v2._2)
        case (Animation(c1),     Animation(c2))     => (c1, v1._2) =~= (c2, v2._2)
        case (File(c1),          File(c2))          => (c1, v1._2) =~= (c2, v2._2)
        case (Location(c1),      Location(c2))      => (c1, v1._2) =~= (c2, v2._2)
        case (Poll(c1),          Poll(c2))          => (c1, v1._2) =~= (c2, v2._2)
        case (SharedContact(c1), SharedContact(c2)) => (c1, v1._2) =~= (c2, v2._2)
        case _                                      => false
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

  implicit object MessageServiceValPracticallyEquals extends PracticallyEquals[(MessageService.Val, DatasetRoot)] {
    override def practicallyEquals(v1: (MessageService.Val, DatasetRoot), v2: (MessageService.Val, DatasetRoot)): Boolean = {
      import MessageService.Val._
      if (v1._1.getClass != v2._1.getClass) {
        false
      } else (v1._1, v2._1) match {
        case (m1: PhoneCall,          m2: PhoneCall)          => m1 == m2
        case (m1: PinMessage,         m2: PinMessage)         => m1 == m2
        case (m1: ClearHistory,       m2: ClearHistory)       => m1 == m2
        case (m1: GroupCreate,        m2: GroupCreate)        => m1 == m2
        case (m1: GroupEditTitle,     m2: GroupEditTitle)     => m1 == m2
        case (m1: GroupEditPhoto,     m2: GroupEditPhoto)     => (m1.value.photo, v1._2) =~= (m2.value.photo, v2._2)
        case (m1: GroupInviteMembers, m2: GroupInviteMembers) => m1 == m2
        case (m1: GroupRemoveMembers, m2: GroupRemoveMembers) => m1 == m2
        case (m1: GroupMigrateFrom,   m2: GroupMigrateFrom)   => m1 == m2
        case (m1: GroupMigrateTo,     m2: GroupMigrateTo)     => m1 == m2
        case (m1: GroupCall,          m2: GroupCall)          => m1 == m2
      }
    }
  }

  implicit object MessageServicePracticallyEquals extends PracticallyEquals[(MessageService, DatasetRoot)] {
    override def practicallyEquals(v1: (MessageService, DatasetRoot), v2: (MessageService, DatasetRoot)): Boolean =
      (v1._1.`val`, v1._2) =~= (v2._1.`val`, v2._2)
  }

  implicit object TypedMessagePracticallyEquals extends PracticallyEquals[(Message.Typed, DatasetRoot)] {
    override def practicallyEquals(v1: (Message.Typed, DatasetRoot), v2: (Message.Typed, DatasetRoot)): Boolean = {
      (v1._1, v2._1) match {
        case (Message.Typed.Regular(v1Regular), Message.Typed.Regular(v2Regular)) =>
          (v1Regular, v1._2) =~= (v2Regular, v2._2)
        case (Message.Typed.Service(v1Service), Message.Typed.Service(v2Service)) =>
          (v1Service, v1._2) =~= (v2Service, v2._2)
        case _ => false
      }
    }
  }

  implicit object MessagePracticallyEquals extends PracticallyEquals[(Message, DatasetRoot)] {
    /** "Practical equality" that ignores internal ID, paths of files with equal content, and some unimportant fields */
    override def practicallyEquals(v1: (Message, DatasetRoot), v2: (Message, DatasetRoot)): Boolean =
      v1._1.copy(
        internalId       = NoInternalId,
        searchableString = None,
        typed            = Message.Typed.Empty
      ) == v2._1.copy(
        internalId       = NoInternalId,
        searchableString = None,
        typed            = Message.Typed.Empty
      ) && (v1._1.typed, v1._2) =~= (v2._1.typed, v1._2)
  }

  //
  //
  //

  implicit class ExtendedUser(u: User) {
    def firstNameOrUnnamed: String = u.firstNameOption getOrElse Unnamed

    // Same as ExtendedContentSharedContact
    lazy val prettyNameOption: Option[String] = {
      val parts = Seq(u.firstNameOption, u.lastNameOption).yieldDefined
      if (parts.isEmpty) None else Some(parts.mkString(" ").trim)
    }

    lazy val prettyName: String =
      prettyNameOption getOrElse Unnamed
  }

  implicit class ExtendedChat(c: Chat) {
    def nameOrUnnamed: String = c.nameOption getOrElse Unnamed
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
      val optionsSet: Set[Option[String]] = msg.typed.value match {
        case msgRegular: MessageRegular  =>
          msgRegular.contentOption match {
            case None => Set.empty
            case Some(content) =>
              content.`val` match {
                case Content.Val.Sticker(v)       => Set(v.pathOption, v.thumbnailPathOption)
                case Content.Val.Photo(v)         => Set(v.pathOption)
                case Content.Val.VoiceMsg(v)      => Set(v.pathOption)
                case Content.Val.VideoMsg(v)      => Set(v.pathOption, v.thumbnailPathOption)
                case Content.Val.Animation(v)     => Set(v.pathOption, v.thumbnailPathOption)
                case Content.Val.File(v)          => Set(v.pathOption, v.thumbnailPathOption)
                case Content.Val.Location(_)      => Set.empty
                case Content.Val.Poll(_)          => Set.empty
                case Content.Val.SharedContact(v) => Set(v.vcardPathOption)
                case Content.Val.Empty            => throw new IllegalArgumentException("Empty content!")
              }
          }
        case MessageService(v, _) =>
          v match {
            case _: MessageService.Val.PhoneCall          => Set.empty
            case _: MessageService.Val.PinMessage         => Set.empty
            case _: MessageService.Val.ClearHistory       => Set.empty
            case _: MessageService.Val.GroupCreate        => Set.empty
            case _: MessageService.Val.GroupEditTitle     => Set.empty
            case m: MessageService.Val.GroupEditPhoto     => Set(m.value.photo.pathOption)
            case _: MessageService.Val.GroupInviteMembers => Set.empty
            case _: MessageService.Val.GroupRemoveMembers => Set.empty
            case _: MessageService.Val.GroupMigrateFrom   => Set.empty
            case _: MessageService.Val.GroupMigrateTo     => Set.empty
            case _: MessageService.Val.GroupCall          => Set.empty
          }
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

  /** Should be kept in sync with makeSearchableString! */
  object RichText {
    def makePlain(text: String): RichTextElement =
      RichTextElement(RichTextElement.Val.Plain(RtePlain(text)), Some(normalizeSeachableString(text)))

    def makeBold(text: String): RichTextElement =
      RichTextElement(RichTextElement.Val.Bold(RteBold(text)), Some(normalizeSeachableString(text)))

    def makeItalic(text: String): RichTextElement =
      RichTextElement(RichTextElement.Val.Italic(RteItalic(text)), Some(normalizeSeachableString(text)))

    def makeUnderline(text: String): RichTextElement =
      RichTextElement(RichTextElement.Val.Underline(RteUnderline(text)), Some(normalizeSeachableString(text)))

    def makeStrikethrough(text: String): RichTextElement =
      RichTextElement(RichTextElement.Val.Strikethrough(RteStrikethrough(text)), Some(normalizeSeachableString(text)))

    def makeLink(textOption: Option[String], href: String, hidden: Boolean): RichTextElement = {
      val searchableString = (normalizeSeachableString(textOption getOrElse "") + " " + href).trim
      RichTextElement(RichTextElement.Val.Link(RteLink(
        textOption = textOption,
        href       = href,
        hidden     = hidden
      )), Some(normalizeSeachableString(searchableString)))
    }

    def makePrefmtInline(text: String): RichTextElement =
      RichTextElement(RichTextElement.Val.PrefmtInline(RtePrefmtInline(text)), Some(normalizeSeachableString(text)))

    def makePrefmtBlock(text: String, languageOption: Option[String]): RichTextElement =
      RichTextElement(
        RichTextElement.Val.PrefmtBlock(RtePrefmtBlock(text = text, languageOption = languageOption)),
        Some(normalizeSeachableString(text))
      )
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

  //
  // Content
  //

  implicit class ExtendedContent(c: Content) {
    def pathFileOption(datasetRoot: DatasetRoot): Option[JFile] = {
      require(hasPath, "No path available!")
      c.`val`.value match {
        case c: ContentSticker    => c.pathFileOption(datasetRoot)
        case c: ContentPhoto      => c.pathFileOption(datasetRoot)
        case c: ContentVoiceMsg   => c.pathFileOption(datasetRoot)
        case c: ContentVideoMsg   => c.pathFileOption(datasetRoot)
        case c: ContentAnimation  => c.pathFileOption(datasetRoot)
        case c: ContentFile       => c.pathFileOption(datasetRoot)
        case c                    => None
      }
    }

    def hasPath: Boolean =
      c.`val`.value match {
        case _: ContentSticker   => true
        case _: ContentPhoto     => true
        case _: ContentVoiceMsg  => true
        case _: ContentVideoMsg  => true
        case _: ContentAnimation => true
        case _: ContentFile      => true
        case _                   => false
      }
  }

  implicit class ExtendedContentSticker(c: ContentSticker) {
    def pathFileOption(datasetRoot: DatasetRoot): Option[JFile] = c.pathOption.map(_.toFile(datasetRoot))

    def thumbnailPathFileOption(datasetRoot: DatasetRoot): Option[JFile] = c.thumbnailPathOption.map(_.toFile(datasetRoot))
  }

  implicit class ExtendedContentPhoto(c: ContentPhoto) {
    def pathFileOption(datasetRoot: DatasetRoot): Option[JFile] = c.pathOption.map(_.toFile(datasetRoot))
  }

  implicit class ExtendedContentVoiceMsg(c: ContentVoiceMsg) {
    def pathFileOption(datasetRoot: DatasetRoot): Option[JFile] = c.pathOption.map(_.toFile(datasetRoot))
  }

  implicit class ExtendedContentVideoMsg(c: ContentVideoMsg) {
    def pathFileOption(datasetRoot: DatasetRoot): Option[JFile] = c.pathOption.map(_.toFile(datasetRoot))

    def thumbnailPathFileOption(datasetRoot: DatasetRoot): Option[JFile] = c.thumbnailPathOption.map(_.toFile(datasetRoot))
  }

  implicit class ExtendedContentAnimation(c: ContentAnimation) {
    def pathFileOption(datasetRoot: DatasetRoot): Option[JFile] = c.pathOption.map(_.toFile(datasetRoot))

    def thumbnailPathFileOption(datasetRoot: DatasetRoot): Option[JFile] = c.thumbnailPathOption.map(_.toFile(datasetRoot))
  }

  implicit class ExtendedContentFile(c: ContentFile) {
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
      val parts = Seq(Some(c.firstName), c.lastNameOption).yieldDefined
      if (parts.isEmpty) None else Some(parts.mkString(" ").trim)
    }

    lazy val prettyName: String =
      prettyNameOption getOrElse Unnamed
  }
}
