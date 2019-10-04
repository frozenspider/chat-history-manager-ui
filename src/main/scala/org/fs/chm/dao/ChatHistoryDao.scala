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

sealed trait Message
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
  //case class CreateMessage
  // FIXME: Other types of msgs
}

//
// Content
//

sealed trait Content {}

object Content {
  case class Sticker(
      path: String,
      thumbnailPath: String,
      emoji: String,
      width: Int,
      height: Int,
  ) extends Content

  case class Voice(
      path: String,
      thumbnailPath: String,
      durationSec: Int,
      mimeTypeOption: Option[String]
  ) extends Content

  case class Media(
      path: String,
      thumbnailPath: String,
      durationSecOption: Option[Int],
      mimeTypeOption: Option[String],
      width: Int,
      height: Int
  ) extends Content
}
