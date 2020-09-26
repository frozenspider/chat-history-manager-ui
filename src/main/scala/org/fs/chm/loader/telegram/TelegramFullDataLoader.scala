package org.fs.chm.loader.telegram

import java.io.File
import java.io.FileNotFoundException
import java.util.UUID

import scala.collection.immutable.ListMap

import org.fs.chm.dao._
import org.fs.utility.Imports._
import org.json4s._
import org.json4s.jackson.JsonMethods

class TelegramFullDataLoader extends TelegramDataLoader with TelegramDataLoaderCommon {

  override def doesLookRight(rootFile: File): Option[String] ={
    checkFormatLooksRight(rootFile, Seq("personal_information", "contacts", "chats"))
  }

  /** Path should point to the folder with `result.json` and other stuff */
  override protected def loadDataInner(rootFile: File): EagerChatHistoryDao = {
    implicit val dummyTracker = new FieldUsageTracker
    val resultJsonFile: File = new File(rootFile, "result.json").getAbsoluteFile
    if (!resultJsonFile.exists()) {
      throw new FileNotFoundException("result.json not found in " + rootFile.getAbsolutePath)
    }

    val dataset = Dataset.createDefault("Telegram", "telegram")

    val parsed = JsonMethods.parse(resultJsonFile)

    val (myself, users) = parseAndCombineUsers(parsed, dataset.uuid)

    val chatsWithMessages = for {
      chat <- getCheckedField[Seq[JValue]](parsed, "chats", "list")
      if (getCheckedField[String](chat, "type") != "saved_messages")
    } yield {
      val messagesRes = (for {
        message <- getCheckedField[IndexedSeq[JValue]](chat, "messages")
      } yield MessageParser.parseMessageOption(message, rootFile)).yieldDefined

      val chatRes = parseChat(chat, dataset.uuid, messagesRes.size)
      (chatRes, messagesRes)
    }
    val chatsWithMessagesLM = ListMap(chatsWithMessages: _*)

    new EagerChatHistoryDao(
      name               = "Telegram export data from " + rootFile.getName,
      _dataRootFile      = rootFile.getAbsoluteFile,
      dataset            = dataset,
      myself1            = myself,
      users1             = users,
      _chatsWithMessages = chatsWithMessagesLM
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

  private def parseShortUserFromMessage(jv: JValue): ShortUser = {
    implicit val dummyTracker = new FieldUsageTracker
    getCheckedField[String](jv, "type") match {
      case "message" =>
        ShortUser(
          id             = getCheckedField[Long](jv, "from_id"),
          fullNameOption = getStringOpt(jv, "from", true)
        )
      case "service" =>
        ShortUser(
          id             = getCheckedField[Long](jv, "actor_id"),
          fullNameOption = getStringOpt(jv, "actor", true)
        )
      case other =>
        throw new IllegalArgumentException(
          s"Don't know how to parse message of type '$other' for ${jv.toString.take(500)}")
    }
  }

  /**
   * Parse users from contact list and chat messages and combine them to get as much info as possible.
   * Returns `(myself, users)`
   */
  private def parseAndCombineUsers(parsed: JValue, dsUuid: UUID): (User, Seq[User]) = {
    implicit val dummyTracker = new FieldUsageTracker

    val myself = parseMyself(getRawField(parsed, "personal_information", true), dsUuid)

    val contactUsers = for {
      contact <- getCheckedField[Seq[JValue]](parsed, "contacts", "list")
    } yield parseUser(contact, dsUuid)

    // Doing additional pass over messages to fetch all users
    val messageUsers = (
      for {
        chat <- getCheckedField[Seq[JValue]](parsed, "chats", "list")
        if (getCheckedField[String](chat, "type") != "saved_messages")
        message <- getCheckedField[IndexedSeq[JValue]](chat, "messages")
        if (getCheckedField[String](message, "type") != "unsupported")
      } yield parseShortUserFromMessage(message)
    ).toSet

    require(messageUsers.forall(_.id > 0), "All user IDs in messages must be positive!")
    val combinedUsers = messageUsers.map {
      case ShortUser(id, _) if id == myself.id =>
        myself
      case ShortUser(id, fullNameOption) => //
        {
          contactUsers find (_.id == id)
        }.orElse {
          contactUsers find (fullNameOption contains _.prettyName)
        }.getOrElse {
          User(
            dsUuid             = dsUuid,
            id                 = id,
            firstNameOption    = fullNameOption,
            lastNameOption     = None,
            usernameOption     = None,
            phoneNumberOption  = None,
            lastSeenTimeOption = None
          )
        }.copy(id = id)
    }

    // Append myself if not encountered in messages
    val combinedUsers2 = if (combinedUsers contains myself) combinedUsers else (combinedUsers + myself)
    val combinedUsers3 = combinedUsers2.toSeq sortBy (u => (u.id, u.prettyName))
    (myself, combinedUsers3)
  } ensuring { p =>
    // Ensuring all IDs are unique
    val (myself, users) = p
    (users.toStream.map(_.id).toSet + myself.id).size == users.size
  }
}
