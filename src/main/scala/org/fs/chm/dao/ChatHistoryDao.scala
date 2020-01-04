package org.fs.chm.dao

import java.io.File
import java.util.UUID

import com.github.nscala_time.time.Imports._
import org.fs.utility.Imports._

/**
 * Everything except for messages should be pre-cached and readily available.
 */
trait ChatHistoryDao extends AutoCloseable {
  sys.addShutdownHook(close())

  /** Base of relative paths specified in messages */
  def dataPathRoot: File

  def datasets: Seq[Dataset]

  def myself(dsUuid: UUID): User

  /** Contains myself as well */
  def users(dsUuid: UUID): Seq[User]

  def chats(dsUuid: UUID): Seq[Chat]

  /**
   * First returned element MUST be myself, the rest should be in some fixed order.
   * This method should be fast.
   */
  def interlocutors(chat: Chat): Seq[User]

  def lastMessages(chat: Chat, limit: Int): IndexedSeq[Message]

  /** Return N messages before the given one (exclusive). Returns None if message isn't found */
  def messagesBefore(chat: Chat, msgId: Long, limit: Int): Option[IndexedSeq[Message]]

  def messageOption(chat: Chat, id: Long): Option[Message]

  override def close(): Unit = {}
}

case class Dataset(
    uuid: UUID,
    alias: String,
    sourceType: String,
)

sealed trait PersonInfo {
  val firstNameOption: Option[String]
  val lastNameOption: Option[String]
  val phoneNumberOption: Option[String]

  lazy val prettyName: String = {
    val parts = Seq(firstNameOption, lastNameOption).yieldDefined
    if (parts.isEmpty) "[nameless]" else parts.mkString(" ").trim
  }
}

case class User(
    dsUuid: UUID,
    /** Might be 0, otherwise - is presumed to be unique */
    id: Long,
    /** If there's no first/last name separation, everything will be in first name */
    firstNameOption: Option[String],
    lastNameOption: Option[String],
    usernameOption: Option[String],
    phoneNumberOption: Option[String],
    lastSeenTimeOption: Option[DateTime]
) extends PersonInfo

sealed abstract class ChatType(val name: String)
object ChatType {
  case object Personal     extends ChatType("personal")
  case object PrivateGroup extends ChatType("private_group")
}

case class Chat(
    dsUuid: UUID,
    id: Long,
    nameOption: Option[String],
    tpe: ChatType,
    imgPathOption: Option[String],
    msgCount: Int
)

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
  val id: Long
  val time: DateTime
  val fromName: String
  val fromId: Long
  val textOption: Option[RichText]

  /** We can't use "super" on vals/lazy vals, so... */
  protected val plainSearchableMsgString =
    textOption map (_.plainSearchableString) getOrElse ""

  override val plainSearchableString = plainSearchableMsgString
}

object Message {
  case class Regular(
      id: Long,
      time: DateTime,
      editTimeOption: Option[DateTime],
      fromName: String,
      fromId: Long,
      forwardFromNameOption: Option[String],
      replyToMessageIdOption: Option[Long],
      textOption: Option[RichText],
      contentOption: Option[Content]
  ) extends Message

  sealed trait Service extends Message

  object Service {
    sealed trait MembershipChange extends Service {
      val members: Seq[String]
    }

    case class PhoneCall(
        id: Long,
        time: DateTime,
        fromName: String,
        fromId: Long,
        textOption: Option[RichText],
        durationSecOption: Option[Int],
        discardReasonOption: Option[String]
    ) extends Service

    case class PinMessage(
        id: Long,
        time: DateTime,
        fromName: String,
        fromId: Long,
        textOption: Option[RichText],
        messageId: Long
    ) extends Service

    /** Note: for Telegram, from is not always meaningful */
    case class ClearHistory(
        id: Long,
        time: DateTime,
        fromName: String,
        fromId: Long,
        textOption: Option[RichText]
    ) extends Service

    case class EditPhoto(
        id: Long,
        time: DateTime,
        fromName: String,
        fromId: Long,
        textOption: Option[RichText],
        pathOption: Option[String],
        widthOption: Option[Int],
        heightOption: Option[Int]
    ) extends Service

    object Group {
      case class Create(
          id: Long,
          time: DateTime,
          fromName: String,
          fromId: Long,
          textOption: Option[RichText],
          title: String,
          members: Seq[String]
      ) extends MembershipChange {
        override val plainSearchableString =
          (plainSearchableMsgString +: title +: members).mkString(" ").trim
      }

      case class InviteMembers(
          id: Long,
          time: DateTime,
          fromName: String,
          fromId: Long,
          textOption: Option[RichText],
          members: Seq[String]
      ) extends MembershipChange {
        override val plainSearchableString =
          (plainSearchableMsgString +: members).mkString(" ").trim
      }

      case class RemoveMembers(
          id: Long,
          time: DateTime,
          fromName: String,
          fromId: Long,
          textOption: Option[RichText],
          members: Seq[String]
      ) extends MembershipChange {
        override val plainSearchableString =
          (plainSearchableMsgString +: members).mkString(" ").trim
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
