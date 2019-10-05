package org.fs.chm.dao

import com.github.nscala_time.time.Imports._

trait ChatHistoryDao {
  def contacts: Seq[Contact]

  def chats: Seq[Chat]

  def messages(chat: Chat, offset: Int, limit: Int): IndexedSeq[Message]
}

case class Contact(
    /** Not guaranteed to be unique -- or even meaningful */
    id: Long,
    firstNameOption: Option[String],
    lastNameOption: Option[String],
    phoneNumberOption: Option[String],
    lastSeenDateOption: Option[DateTime]
) {
  lazy val prettyName: String = {
    Seq(firstNameOption getOrElse "", lastNameOption getOrElse "").mkString(" ").trim
  }
}

sealed trait ChatType
object ChatType {
  case object Personal     extends ChatType
  case object PrivateGroup extends ChatType
}

case class Chat(
    id: Long,
    nameOption: Option[String],
    tpe: ChatType,
    msgNum: Long
)

sealed trait Message {
  val id: Long
  val date: DateTime
  val editDateOption: Option[DateTime]
  val fromName: String
  val fromId: Long
}
object Message {
  case class Regular(
      id: Long,
      date: DateTime,
      editDateOption: Option[DateTime],
      fromName: String,
      fromId: Long,
      forwardFromNameOption: Option[String],
      replyToMessageIdOption: Option[Long],
      textOption: Option[String], // TODO: RichText
      contentOption: Option[Content]
  ) extends Message

  sealed trait Service extends Message
  case class CreateGroup(
      id: Long,
      date: DateTime,
      editDateOption: Option[DateTime],
      fromName: String,
      fromId: Long,
      title: String,
      members: Seq[String],
      textOption: Option[String]
  ) extends Service
  //case class CreateMessage
  // FIXME: Other types of msgs
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
      liveDurationSecOption: Option[Int],
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
}
