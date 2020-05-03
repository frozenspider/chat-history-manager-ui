package org.fs.chm.dao

import java.io.File
import java.util.UUID

import scala.collection.immutable.ListMap

/**
 * Serves as in-memory ChatHistoryDao.
 * Guarantees `internalId`` ordering to match order in  `_chatsWithMessages`.
 */
class EagerChatHistoryDao(
    override val name: String,
    dataPathRoot: File,
    dataset: Dataset,
    myself1: User,
    users1: Seq[User],
    _chatsWithMessages: ListMap[Chat, IndexedSeq[Message]]
) extends ChatHistoryDao {
  require(users1 contains myself1)

  private val chatsWithMessages: ListMap[Chat, IndexedSeq[Message]] = {
    var internalId = 0L
    // We can't use mapValues because it's lazy
    _chatsWithMessages map {
      case (c, ms) =>
        (c, ms map { (m: Message) =>
          internalId += 1
          m.withInternalId(internalId.asInstanceOf[Message.InternalId])
        })
    }
  }

  override def datasets: Seq[Dataset] = Seq(dataset)

  override def dataPath(dsUuid: UUID): File = dataPathRoot

  override def myself(dsUuid: UUID): User = myself1

  override def users(dsUuid: UUID): Seq[User] = users1

  override def userOption(dsUuid: UUID, id: Long): Option[User] = users1.find(_.id == id)

  val interlocutorsMap: Map[Chat, Seq[User]] = chatsWithMessages map {
    case (c, ms) =>
      val usersWithoutMe: Seq[User] =
        (ms.toSet.map((_: Message).fromId) - myself1.id).toSeq
          .map(fromId => users1.find(_.id == fromId).get)

      (c, myself1 +: usersWithoutMe.sortBy(c => (c.id, c.prettyName)))
  }

  val chats1 = chatsWithMessages.keys.toSeq

  override def chats(dsUuid: UUID) = chats1

  override def chatOption(dsUuid: UUID, id: Long): Option[Chat] = chats1 find (_.id == id)

  override def interlocutors(chat: Chat): Seq[User] = interlocutorsMap(chat)

  override def messagesBefore(chat: Chat, msg: Message, limit: Int): IndexedSeq[Message] = {
    val messages = chatsWithMessages(chat)
    val idx      = messages.lastIndexWhere(_.internalId <= msg.internalId)
    if (idx == -1) {
      IndexedSeq.empty
    } else {
      val lowerLimit = (idx - limit + 1) max 0
      messages.slice(lowerLimit, idx + 1)
    }
  }

  override def messagesAfter(chat: Chat, msg: Message, limit: Int): IndexedSeq[Message] = {
    val messages = chatsWithMessages(chat)
    val idx      = messages.indexWhere(_.internalId >= msg.internalId)
    if (idx == -1) {
      IndexedSeq.empty
    } else {
      val upperLimit = (idx + limit) min messages.size
      messages.slice(idx, upperLimit)
    }
  }

  override def messagesBetween(chat: Chat, msg1: Message, msg2: Message): IndexedSeq[Message] = {
    require(msg1.internalId <= msg2.internalId)
    val messages = chatsWithMessages(chat)
    val idx1     = messages.indexWhere(_.internalId >= msg1.internalId)
    val idx2     = messages.lastIndexWhere(_.internalId <= msg2.internalId)
    if (idx1 == -1) {
      // All messages are less than lower bound
      assert(idx2 == messages.size - 1)
      IndexedSeq.empty
    } else if (idx2 == -1) {
      // All messages are greater than upper bound
      assert(idx1 == 0)
      IndexedSeq.empty
    } else {
      assert(idx2 >= idx1)
      messages.slice(idx1, idx2 + 1)
    }
  }

  override def countMessagesBetween(chat: Chat, msg1: Message, msg2: Message): Int = {
    require(msg1.internalId <= msg2.internalId)
    val between = messagesBetween(chat, msg1, msg2)
    var size    = between.size
    if (size == 0) {
      0
    } else {
      if (between.head.internalId == msg1.internalId) size -= 1
      if (between.last.internalId == msg2.internalId) size -= 1
      math.max(size, 0)
    }
  }

  override def scrollMessages(chat: Chat, offset: Int, limit: Int): IndexedSeq[Message] = {
    chatsWithMessages.get(chat) map (_.slice(offset, offset + limit)) getOrElse IndexedSeq.empty
  }

  override def lastMessages(chat: Chat, limit: Int): IndexedSeq[Message] = {
    chatsWithMessages.get(chat) map (_.takeRight(limit)) getOrElse IndexedSeq.empty
  }

  override def messageOption(chat: Chat, id: Message.SourceId): Option[Message] =
    chatsWithMessages.get(chat) flatMap (_ find (_.sourceIdOption contains id))

  override def messageOptionByInternalId(chat: Chat, id: Message.InternalId): Option[Message] =
    chatsWithMessages.get(chat) flatMap (_ find (_.internalId == id))

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

  override def isLoaded(dataPathRoot: File): Boolean = {
    dataPathRoot != null && this.dataPathRoot == dataPathRoot
  }

  override def equals(that: Any): Boolean = that match {
    case that: EagerChatHistoryDao => this.name == that.name && that.isLoaded(this.dataPathRoot)
    case _                         => false
  }

  override def hashCode(): Int = this.name.hashCode + 17 * this.dataPathRoot.hashCode
}
