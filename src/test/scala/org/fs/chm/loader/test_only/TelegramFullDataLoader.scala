package org.fs.chm.loader.test_only

import java.io.File
import java.io.FileNotFoundException

import scala.collection.immutable.ListMap

import org.fs.chm.dao.EagerChatHistoryDao
import org.fs.chm.dao.Entities._
import org.fs.chm.loader.telegram.TelegramDataLoader
import org.fs.chm.protobuf.PbUuid
import org.fs.chm.protobuf.User
import org.fs.chm.utility.PerfUtils._
import org.fs.utility.Imports._
import org.json4s._
import org.json4s.jackson.JsonMethods

class TelegramFullDataLoader extends TelegramDataLoader with TelegramDataLoaderCommon {

  override def doesLookRight(rootFile: File): Option[String] ={
    checkFormatLooksRight(rootFile, Seq("personal_information", "contacts", "chats"))
  }

  /** Path should point to the folder with `result.json` and other stuff */
  override protected def loadDataInner(rootFile: File, createNew: Boolean): EagerChatHistoryDao = {
    require(!createNew, "Creating new dataset is not supported for Telegram (as it makes no sense)")
    logPerformance {
      val resultJsonFile: File = new File(rootFile, "result.json").getAbsoluteFile
      if (!resultJsonFile.exists()) {
        throw new FileNotFoundException("result.json not found in " + rootFile.getAbsolutePath)
      }

      val dataset = createDataset("Telegram", "telegram")

      val parsed = JsonMethods.parse(resultJsonFile)

      val (myself, users) = parseAndCombineUsers(parsed, dataset.uuid)

      val chatsWithMessages = for {
        chat <- getCheckedField[Seq[JValue]](parsed, "chats", "list")
        if (getCheckedField[String](chat, "type") != "saved_messages")
      } yield {
        logPerformance {
          val messagesRes = (for {
            message <- getCheckedField[IndexedSeq[JValue]](chat, "messages")
          } yield MessageParser.parseMessageOption(message)).yieldDefined

          val memberIds = (myself.id +: messagesRes.toStream.map(_.fromId)).toSet

          val chatRes = parseChat(chat, dataset.uuid, memberIds, messagesRes.size)
          (chatRes, messagesRes)
        }((res, ms) => s"Chat '${res._1.nameOption.getOrElse("#" + res._1.id)}' loaded in ${ms} ms")
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
    }((res, ms) => s"Telegram history loaded in ${ms} ms")
  }

  //
  // Parsers
  //

  private def parseMyself(jv: JValue, dsUuid: PbUuid): User = {
    normalize(User(
      dsUuid             = dsUuid,
      id                 = getCheckedField[Long](jv, "user_id"),
      firstNameOption    = getStringOpt(jv, "first_name", true),
      lastNameOption     = getStringOpt(jv, "last_name", true),
      usernameOption     = getStringOpt(jv, "username", true),
      phoneNumberOption  = getStringOpt(jv, "phone_number", true),
    ))
  }

  private def parseUser(jv: JValue, dsUuid: PbUuid): User = {
    // Before 2021-05, Telegram has "user_id" field that was largely rudimentary - it was always 0
    val idOption = getFieldOpt[Long](jv, "user_id", false)
    normalize(User(
      dsUuid             = dsUuid,
      id                 = idOption.getOrElse(0L),
      firstNameOption    = getStringOpt(jv, "first_name", true),
      lastNameOption     = getStringOpt(jv, "last_name", true),
      usernameOption     = None,
      phoneNumberOption  = getStringOpt(jv, "phone_number", true)
    ))
  }

  /**
   * Parse users from contact list and chat messages and combine them to get as much info as possible.
   * Returns `(myself, users)`
   */
  private def parseAndCombineUsers(parsed: JValue, dsUuid: PbUuid): (User, Seq[User]) = {
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
            phoneNumberOption  = None
          )
        }.copy(id = id)
    }

    // Make sure self is the first user, sort all the rest
    val combinedUsers2 = myself +: combinedUsers.filter(_ != myself).toSeq.sortBy(u => (u.id, u.prettyName))
    (myself, combinedUsers2)
  } ensuring { p =>
    // Ensuring all IDs are unique
    val (myself, users) = p
    (users.toStream.map(_.id).toSet + myself.id).size == users.size
  }
}
