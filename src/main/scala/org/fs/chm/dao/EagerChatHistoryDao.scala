package org.fs.chm.dao

import java.io.File
import java.util.UUID

import scala.collection.immutable.ListMap
import scala.collection.immutable.TreeMap

class EagerChatHistoryDao(
    override val name: String,
    dataPathRoot: File,
    dataset: Dataset,
    myself1: User,
    rawUsers: Seq[User],
    chatsWithMessages: ListMap[Chat, IndexedSeq[Message]]
) extends ChatHistoryDao {

  override def datasets: Seq[Dataset] = Seq(dataset)

  override def dataPath(dsUuid: UUID): File = dataPathRoot

  override def myself(dsUuid: UUID): User = myself1

  // We can't use mapValues because it's lazy...
  val messagesMap: ListMap[Chat, TreeMap[Long, Message]] = chatsWithMessages map {
    case (c, ms) => (c, TreeMap(ms.map(m => (m.id, m)): _*))
  }

  val users1: Seq[User] = {
    val allUsersFromIdName =
      chatsWithMessages.values.flatten.toSet.map((m: Message) => (m.fromId, m.fromNameOption))
    ({
      val result = allUsersFromIdName.toSeq.map {
        case (fromId, _) if fromId == myself1.id =>
          myself1
        case (fromId, fromNameOption) =>
          rawUsers
            .find(_.id == fromId)
            .orElse(rawUsers.find(u => fromNameOption contains u.prettyName))
            .getOrElse(
              User(
                dsUuid             = dataset.uuid,
                id                 = fromId,
                firstNameOption    = fromNameOption,
                lastNameOption     = None,
                usernameOption     = None,
                phoneNumberOption  = None,
                lastSeenTimeOption = None
              ))
            .copy(id = fromId)
      }
      // Append myself if not encountered in messages
      if (result contains myself1) result else (result :+ myself1)
    }).sortBy(c => (c.id, c.prettyName))
  }

  override def users(dsUuid: UUID): Seq[User] = users1

  val interlocutorsMap: Map[Chat, Seq[User]] = chatsWithMessages map {
    case (c, ms) =>
      val usersWithoutMe: Seq[User] =
        ms.toSet
          .map((m: Message) => (m.fromId, m.fromNameOption))
          .toSeq
          .filter(_._1 != myself1.id)
          .map {
            case (fromId, fromNameOption) =>
              users1
                .find(_.id == fromId)
                .orElse(users1.find(u => fromNameOption contains u.prettyName))
                .get
          }

      (c, myself1 +: usersWithoutMe.sortBy(c => (c.id, c.prettyName)))
  }

  val chats1 = chatsWithMessages.keys.toSeq

  override def chats(dsUuid: UUID) = chats1

  override def interlocutors(chat: Chat): Seq[User] = interlocutorsMap(chat)

  override def messagesBefore(chat: Chat, msgId: Long, limit: Int): IndexedSeq[Message] = {
    val messages   = chatsWithMessages(chat)
    val idx        = messages.lastIndexWhere(_.id <= msgId)
    val lowerLimit = (idx - limit + 1) max 0
    messages.slice(lowerLimit, idx + 1)
  }

  override def messagesAfter(chat: Chat, msgId: Long, limit: Int): IndexedSeq[Message] = {
    val messages   = chatsWithMessages(chat)
    val idx        = messages.indexWhere(_.id >= msgId)
    val upperLimit = (idx + limit) min messages.size
    messages.slice(idx, upperLimit)
  }

  override def messagesBetween(chat: Chat, msgId1: Long, msgId2: Long): IndexedSeq[Message] = {
    val messages = chatsWithMessages(chat)
    val idx1     = messages.indexWhere(_.id >= msgId1)
    val idx2     = messages.lastIndexWhere(_.id <= msgId2)
    messages.slice(idx1, idx2 + 1)
  }

  override def scrollMessages(chat: Chat, offset: Int, limit: Int): IndexedSeq[Message] = {
    chatsWithMessages.get(chat) map (_.slice(offset, offset + limit)) getOrElse IndexedSeq.empty
  }

  override def lastMessages(chat: Chat, limit: Int): IndexedSeq[Message] = {
    chatsWithMessages.get(chat) map (_.takeRight(limit)) getOrElse IndexedSeq.empty
  }

  override def messageOption(chat: Chat, id: Long): Option[Message] =
    messagesMap.get(chat) flatMap (_ get id)

  override def toString: String = {
    Seq(
      "EagerChatHistoryDao(",
      "  myself:",
      "    " + myself1.toString + "\n",
      "  users:",
      users1.mkString("    ", "\n    ", "\n"),
      "  chats:",
      chats1.mkString("    ", "\n    ", "\n"),
      ")"
    ).mkString("\n")
  }

  override def isLoaded(f: File): Boolean = {
    f != null && this.dataPathRoot == f.getParentFile
  }

  override def equals(that: Any): Boolean = that match {
    case that: EagerChatHistoryDao => this.name == that.name && that.isLoaded(this.dataPathRoot)
    case _                         => false
  }

  override def hashCode(): Int = this.name.hashCode + 17 * this.dataPathRoot.hashCode
}
