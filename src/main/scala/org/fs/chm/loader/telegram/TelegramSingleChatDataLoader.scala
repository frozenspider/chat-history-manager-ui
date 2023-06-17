package org.fs.chm.loader.telegram

import java.io.File
import java.io.FileNotFoundException

import scala.collection.immutable.ListMap

import org.fs.chm.dao.EagerChatHistoryDao
import org.fs.chm.dao.Entities._
import org.fs.chm.protobuf.PbUuid
import org.fs.chm.protobuf.User
import org.fs.chm.utility.EntityUtils
import org.fs.utility.Imports._
import org.json4s._
import org.json4s.jackson.JsonMethods

class TelegramSingleChatDataLoader extends TelegramDataLoader with TelegramDataLoaderCommon {

  override def doesLookRight(rootFile: File): Option[String] = {
    checkFormatLooksRight(rootFile, Seq("name", "type", "id", "messages"))
  }

  /** Path should point to the folder with `result.json` and other stuff */
  override protected def loadDataInner(rootFile: File, createNew: Boolean): EagerChatHistoryDao = {
    require(!createNew, "Creating new dataset is not supported for Telegram (as it makes no sense)")
    implicit val dummyTracker = new FieldUsageTracker
    val resultJsonFile: File = new File(rootFile, "result.json").getAbsoluteFile
    if (!resultJsonFile.exists()) {
      throw new FileNotFoundException("result.json not found in " + rootFile.getAbsolutePath)
    }

    val dataset = createDataset("Telegram", "telegram")

    val parsed = JsonMethods.parse(resultJsonFile)

    val preUsers = parseUsers(parsed, dataset.uuid)
    val myself = preUsers(EntityUtils.chooseMyself(preUsers))
    val users = myself +: preUsers.filter(_ != myself)

    val messagesRes = (for {
      message <- getCheckedField[IndexedSeq[JValue]](parsed, "messages")
    } yield MessageParser.parseMessageOption(message, rootFile)).yieldDefined

    val memberIds = (myself.id +: messagesRes.toStream.map(_.fromId)).toSet

    val chatRes = parseChat(parsed, dataset.uuid, memberIds, messagesRes.size)

    new EagerChatHistoryDao(
      name               = "Telegram export data from " + rootFile.getName,
      _dataRootFile      = rootFile.getAbsoluteFile,
      dataset            = dataset,
      myself1            = myself,
      users1             = users,
      _chatsWithMessages = ListMap(chatRes -> messagesRes)
    )
  }

  //
  // Parsers
  //

  /** Parse users from chat messages to get as much info as possible. */
  private def parseUsers(parsed: JValue, dsUuid: PbUuid): Seq[User] = {
    implicit val dummyTracker = new FieldUsageTracker

    // Doing additional pass over messages to fetch all users
    val messageUsers = (
      for {
        message <- getCheckedField[IndexedSeq[JValue]](parsed, "messages")
        if (getCheckedField[String](message, "type") != "unsupported")
      } yield parseShortUserFromMessage(message)
    ).toSet
    require(messageUsers.forall(_.id > 0), "All user IDs in messages must be positive!")

    val fullUsers = messageUsers.map {
      case ShortUser(id, fullNameOption) => //
        User(
          dsUuid             = dsUuid,
          id                 = id,
          firstNameOption    = fullNameOption,
          lastNameOption     = None,
          usernameOption     = None,
          phoneNumberOption  = None
        )
    }

    fullUsers.toSeq sortBy (u => (u.id, u.prettyName))
  } ensuring { users =>
    // Ensuring all IDs are unique
    users.to(LazyList).map(_.id).toSet.size == users.size
  }
}
