package org.fs.chm.dao

import java.io.File
import java.util.UUID

import com.github.nscala_time.time.Imports._
import org.fs.utility.Imports._

/**
 * Everything except for messages should be pre-cached and readily available.
 * Should support equality.
 */
trait ChatHistoryDao extends AutoCloseable {
  sys.addShutdownHook(close())

  /** User-friendly name of a loaded data */
  def name: String

  def datasets: Seq[Dataset]

  /** Base of relative paths specified in messages */
  def dataPath(dsUuid: UUID): File

  def myself(dsUuid: UUID): User

  /** Contains myself as well. Order must be stable. */
  def users(dsUuid: UUID): Seq[User]

  def userOption(dsUuid: UUID, id: Long): Option[User]

  def chats(dsUuid: UUID): Seq[Chat]

  def chatOption(dsUuid: UUID, id: Long): Option[Chat]

  /**
   * First returned element MUST be myself, the rest should be in some fixed order.
   * This method should be fast.
   */
  def interlocutors(chat: Chat): Seq[User]

  /** Return N messages after skipping first M of them. Trivial pagination in a nutshell. */
  def scrollMessages(chat: Chat, offset: Int, limit: Int): IndexedSeq[Message]

  def firstMessages(chat: Chat, limit: Int): IndexedSeq[Message] =
    scrollMessages(chat, 0, limit)

  def lastMessages(chat: Chat, limit: Int): IndexedSeq[Message]

  /**
   * Return N messages before the given one (inclusive).
   * Message must be present, so the result would contain at least one element.
   */
  final def messagesBefore(chat: Chat, msg: Message, limit: Int): IndexedSeq[Message] =
    messagesBeforeImpl(chat, msg, limit) ensuring (seq => seq.nonEmpty && seq.size <= limit && seq.last =~= msg)

  protected def messagesBeforeImpl(chat: Chat, msg: Message, limit: Int): IndexedSeq[Message]

  /**
   * Return N messages after the given one (inclusive).
   * Message must be present, so the result would contain at least one element.
   */
  final def messagesAfter(chat: Chat, msg: Message, limit: Int): IndexedSeq[Message] =
    messagesAfterImpl(chat, msg, limit) ensuring (seq => seq.nonEmpty && seq.size <= limit && seq.head =~= msg)

  protected def messagesAfterImpl(chat: Chat, msg: Message, limit: Int): IndexedSeq[Message]

  /**
   * Return N messages between the given ones (inclusive).
   * Messages must be present, so the result would contain at least one element (if both are the same message).
   */
  final def messagesBetween(chat: Chat, msg1: Message, msg2: Message): IndexedSeq[Message] =
    messagesBetweenImpl(chat, msg1, msg2) ensuring (seq => seq.nonEmpty && seq.head =~= msg1 && seq.last =~= msg2)

  protected def messagesBetweenImpl(chat: Chat, msg1: Message, msg2: Message): IndexedSeq[Message]

  /**
   * Count messages between the given ones (exclusive, unlike messagesBetween).
   * Messages must be present.
   */
  def countMessagesBetween(chat: Chat, msg1: Message, msg2: Message): Int

  def messageOption(chat: Chat, id: Message.SourceId): Option[Message]

  def messageOptionByInternalId(chat: Chat, id: Message.InternalId): Option[Message]

  def isMutable: Boolean = this.isInstanceOf[MutableChatHistoryDao]

  def mutable = this.asInstanceOf[MutableChatHistoryDao]

  override def close(): Unit = {}

  /** Whether given data path is the one loaded in this DAO */
  def isLoaded(dataPathRoot: File): Boolean
}

trait MutableChatHistoryDao extends ChatHistoryDao {
  def insertDataset(ds: Dataset): Unit

  def renameDataset(dsUuid: UUID, newName: String): Dataset

  def deleteDataset(dsUuid: UUID): Unit

  /** Insert a new user. It should not yet exist. */
  def insertUser(user: User, isMyself: Boolean): Unit

  /** Sets the data (names and phone only) for a user with the given `id` and `dsUuid` to the given state */
  def updateUser(user: User): Unit

  /**
   * Merge absorbed user into base user, replacing base user's names and phone. Last "last seen time" is selected.
   * Their personal chats will also be merged into one (named after the "new" user).
   */
  def mergeUsers(baseUser: User, absorbedUser: User): Unit

  /** Insert a new chat. It should not yet exist. */
  def insertChat(chat: Chat): Unit

  def deleteChat(chat: Chat): Unit

  /** Insert a new message for the given chat. Internal ID will be ignored. */
  def insertMessages(chat: Chat, msgs: Seq[Message]): Unit

  /** Don't do automatic backups on data changes until re-enabled */
  def disableBackups(): Unit

  /** Start doing backups automatically once again. Do a backup immediately. */
  def enableBackups(): Unit

  /** Create a backup, if enabled, otherwise do nothing */
  protected def backup(): Unit
}

object ChatHistoryDao {
  val Unnamed = "[unnamed]"
}

sealed trait WithId {
  def id: Long
}

case class Dataset(
    uuid: UUID,
    alias: String,
    sourceType: String
) {
  override def equals(that: Any): Boolean = that match {
    case that: Dataset => this.uuid == that.uuid
    case _             => false
  }
  override def hashCode: Int = this.uuid.hashCode
}

object Dataset {
  def createDefault(srcAlias: String, srcType: String) = Dataset(
    uuid       = UUID.randomUUID(),
    alias      = s"${srcAlias} data loaded @ " + DateTime.now().toString("yyyy-MM-dd"),
    sourceType = srcType
  )
}

sealed trait PersonInfo {
  val firstNameOption: Option[String]
  val lastNameOption: Option[String]
  val phoneNumberOption: Option[String]

  lazy val prettyNameOption: Option[String] = {
    val parts = Seq(firstNameOption, lastNameOption).yieldDefined
    if (parts.isEmpty) None else Some(parts.mkString(" ").trim)
  }

  lazy val prettyName: String =
    prettyNameOption getOrElse ChatHistoryDao.Unnamed
}

case class User(
    dsUuid: UUID,
    /** Unique within a dataset */
    id: Long,
    /** If there's no first/last name separation, everything will be in first name */
    firstNameOption: Option[String],
    lastNameOption: Option[String],
    usernameOption: Option[String],
    phoneNumberOption: Option[String],
    lastSeenTimeOption: Option[DateTime]
) extends PersonInfo
    with WithId

sealed abstract class ChatType(val name: String)
object ChatType {
  case object Personal     extends ChatType("personal")
  case object PrivateGroup extends ChatType("private_group")
}

case class Chat(
    dsUuid: UUID,
    /** Unique within a dataset */
    id: Long,
    nameOption: Option[String],
    tpe: ChatType,
    imgPathOption: Option[String],
    msgCount: Int
) extends WithId

trait Searchable {
  def plainSearchableString: String
}

//
// Rich Text
//

case class RichText(
    components: Seq[RichText.Element]
) extends Searchable {
  override val plainSearchableString: String = {
    val joined = (components map (_.plainSearchableString) mkString " ")
      .replaceAll("[\\s\\p{Cf}\n]+", " ")
      .trim
    // Adding all links to the end to enable search over hrefs/hidden links too
    val links = components.collect({ case l: RichText.Link => l.href }).mkString(" ")
    joined + links
  }
}
object RichText {
  sealed trait Element extends Searchable {
    def text: String
  }

  case class Plain(
      text: String
  ) extends Element {
    override val plainSearchableString = text.replaceAll("[\\s\\p{Cf}\n]+", " ")
  }

  case class Bold(
      text: String
  ) extends Element {
    override val plainSearchableString = text.trim
  }

  case class Italic(
      text: String
  ) extends Element {
    override val plainSearchableString = text.trim
  }

  case class Strikethrough(
      text: String
  ) extends Element {
    override val plainSearchableString = text.trim
  }

  case class Link(
      /** Empty text would mean that this link is hidden - but it can still be hidden even if it's not */
      text: String,
      href: String,
      /** Some TG chats use text_links with empty/invisible text to be shown as preview but not appear in text */
      hidden: Boolean
  ) extends Element {
    override val plainSearchableString = (text + " " + href).replaceAll("[\\s\\p{Cf}\n]+", " ").trim
  }

  case class PrefmtInline(
      text: String
  ) extends Element {
    override val plainSearchableString = text.trim
  }

  case class PrefmtBlock(
      text: String,
      languageOption: Option[String]
  ) extends Element {
    override val plainSearchableString = text.replaceAll("[\\s\\p{Cf}\n]+", " ")
  }

}

//
// Message
//

/*
 * Design goal for this section - try to reuse as many fields as possible to comfortably store
 * the whole Message hierarchy in one table.
 *
 * Same applies to Content.
 */

sealed trait Message extends Searchable {

  /**
   * Unique ID assigned to this message by a DAO storage engine, should be -1 until saved.
   * No ordering guarantees are provided in general case.
   * Might change on dataset/chat mutation operations.
   * Should NEVER be compared across different DAOs!
   */
  val internalId: Message.InternalId

  /**
   * Unique within a chat, serves as a persistent ID when merging with older/newer DB version.
   * If it's not useful for this purpose, should be empty.
   */
  val sourceIdOption: Option[Message.SourceId]
  val time: DateTime
  val fromId: Long
  val textOption: Option[RichText]

  /**
   * Should return self type, but F-bound polymorphism turns type inference into pain - so no type-level enforcement, unfortunately.
   * Just make sure subclasses have correct return type.
   */
  def withInternalId(internalId: Message.InternalId): Message

  /** We can't use "super" on vals/lazy vals, so... */
  protected val plainSearchableMsgString =
    textOption map (_.plainSearchableString) getOrElse ""

  override val plainSearchableString = plainSearchableMsgString

  /** Equals not regarding internal IDs */
  def =~=(that: Message) =
    this.withInternalId(Message.NoInternalId) == that.withInternalId(Message.NoInternalId)

  def !=~=(that: Message) = !(this =~= that)

  override def toString: String = s"Message($plainSearchableString)"
}

object Message {
  sealed trait InternalIdTag
  type InternalId = Long with InternalIdTag

  sealed trait SourceIdTag
  type SourceId = Long with SourceIdTag

  val NoInternalId: InternalId = -1L.asInstanceOf[InternalId]

  case class Regular(
      internalId: InternalId,
      sourceIdOption: Option[SourceId],
      time: DateTime,
      editTimeOption: Option[DateTime],
      fromId: Long,
      forwardFromNameOption: Option[String],
      replyToMessageIdOption: Option[SourceId],
      textOption: Option[RichText],
      contentOption: Option[Content]
  ) extends Message {
    override def withInternalId(internalId: Message.InternalId): Regular = copy(internalId = internalId)
  }

  sealed trait Service extends Message

  object Service {
    sealed trait MembershipChange extends Service {
      val members: Seq[String]
    }

    case class PhoneCall(
        internalId: InternalId,
        sourceIdOption: Option[SourceId],
        time: DateTime,
        fromId: Long,
        textOption: Option[RichText],
        durationSecOption: Option[Int],
        discardReasonOption: Option[String]
    ) extends Service {
      override def withInternalId(internalId: Message.InternalId): PhoneCall = copy(internalId = internalId)
    }

    case class PinMessage(
        internalId: InternalId,
        sourceIdOption: Option[SourceId],
        time: DateTime,
        fromId: Long,
        textOption: Option[RichText],
        messageId: SourceId
    ) extends Service {
      override def withInternalId(internalId: Message.InternalId): PinMessage = copy(internalId = internalId)
    }

    /** Note: for Telegram, `from...` is not always meaningful */
    case class ClearHistory(
        internalId: InternalId,
        sourceIdOption: Option[SourceId],
        time: DateTime,
        fromId: Long,
        textOption: Option[RichText]
    ) extends Service {
      override def withInternalId(internalId: Message.InternalId): ClearHistory = copy(internalId = internalId)
    }

    case class EditPhoto(
        internalId: InternalId,
        sourceIdOption: Option[SourceId],
        time: DateTime,
        fromId: Long,
        textOption: Option[RichText],
        pathOption: Option[String],
        widthOption: Option[Int],
        heightOption: Option[Int]
    ) extends Service {
      override def withInternalId(internalId: Message.InternalId): EditPhoto = copy(internalId = internalId)
    }

    object Group {
      case class Create(
          internalId: InternalId,
          sourceIdOption: Option[SourceId],
          time: DateTime,
          fromId: Long,
          textOption: Option[RichText],
          title: String,
          members: Seq[String]
      ) extends MembershipChange {
        override def withInternalId(internalId: Message.InternalId): Create = copy(internalId = internalId)

        override val plainSearchableString =
          (plainSearchableMsgString +: title +: members).mkString(" ").trim
      }

      case class InviteMembers(
          internalId: InternalId,
          sourceIdOption: Option[SourceId],
          time: DateTime,
          fromId: Long,
          textOption: Option[RichText],
          members: Seq[String]
      ) extends MembershipChange {
        override def withInternalId(internalId: Message.InternalId): InviteMembers = copy(internalId = internalId)

        override val plainSearchableString =
          (plainSearchableMsgString +: members).mkString(" ").trim
      }

      case class RemoveMembers(
          internalId: InternalId,
          sourceIdOption: Option[SourceId],
          time: DateTime,
          fromId: Long,
          textOption: Option[RichText],
          members: Seq[String]
      ) extends MembershipChange {
        override def withInternalId(internalId: Message.InternalId): RemoveMembers = copy(internalId = internalId)

        override val plainSearchableString =
          (plainSearchableMsgString +: members).mkString(" ").trim
      }

      case class MigrateFrom(
          internalId: InternalId,
          sourceIdOption: Option[SourceId],
          time: DateTime,
          fromId: Long,
          titleOption: Option[String],
          textOption: Option[RichText]
      ) extends Service {
        override def withInternalId(internalId: Message.InternalId): MigrateFrom = copy(internalId = internalId)

        override val plainSearchableString =
          (plainSearchableMsgString + " " + titleOption.getOrElse("")).trim
      }

      case class MigrateTo(
          internalId: InternalId,
          sourceIdOption: Option[SourceId],
          time: DateTime,
          fromId: Long,
          textOption: Option[RichText]
      ) extends Service {
        override def withInternalId(internalId: Message.InternalId): MigrateTo = copy(internalId = internalId)
      }
    }
  }
}

//
// Content
//

sealed trait Content {}

object Content {
  case class Sticker(
      pathOption: Option[String],
      thumbnailPathOption: Option[String],
      emojiOption: Option[String],
      widthOption: Option[Int],
      heightOption: Option[Int]
  ) extends Content

  case class Photo(
      pathOption: Option[String],
      width: Int,
      height: Int
  ) extends Content

  case class VoiceMsg(
      pathOption: Option[String],
      mimeTypeOption: Option[String],
      durationSecOption: Option[Int],
  ) extends Content

  case class VideoMsg(
      pathOption: Option[String],
      thumbnailPathOption: Option[String],
      mimeTypeOption: Option[String],
      durationSecOption: Option[Int],
      width: Int,
      height: Int
  ) extends Content

  case class Animation(
      pathOption: Option[String],
      thumbnailPathOption: Option[String],
      mimeTypeOption: Option[String],
      durationSecOption: Option[Int],
      width: Int,
      height: Int
  ) extends Content

  case class File(
      pathOption: Option[String],
      thumbnailPathOption: Option[String],
      mimeTypeOption: Option[String],
      titleOption: Option[String],
      performerOption: Option[String],
      durationSecOption: Option[Int],
      widthOption: Option[Int],
      heightOption: Option[Int]
  ) extends Content

  case class Location(
      lat: BigDecimal,
      lon: BigDecimal,
      durationSecOption: Option[Int],
  ) extends Content

  /** We don't really care about poll result */
  case class Poll(
      question: String,
  ) extends Content

  case class SharedContact(
      firstNameOption: Option[String],
      lastNameOption: Option[String],
      phoneNumberOption: Option[String],
      vcardPathOption: Option[String]
  ) extends Content
      with PersonInfo
}
