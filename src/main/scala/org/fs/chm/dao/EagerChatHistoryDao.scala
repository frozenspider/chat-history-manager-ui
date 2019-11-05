package org.fs.chm.dao

import java.io.File

import scala.collection.immutable.ListMap
import scala.collection.immutable.TreeMap

import org.apache.commons.lang3.StringUtils

class EagerChatHistoryDao(
    override val dataPath: File,
    override val myself: Contact,
    contactsRaw: Seq[Contact],
    chatsWithMessages: ListMap[Chat, IndexedSeq[Message]]
) extends ChatHistoryDao {

  // We can't use mapValues because it's lazy...
  val messagesMap: ListMap[Chat, TreeMap[Long, Message]] = chatsWithMessages map {
    case (c, ms) => (c, TreeMap(ms.map(m => (m.id, m)): _*))
  }

  override val contacts: Seq[Contact] = {
    val allContactsFromIdName =
      chatsWithMessages.values.flatten.toSet.map((m: Message) => (m.fromId, m.fromName))
    allContactsFromIdName.toSeq
      .map({
        case (fromId, _) if fromId == myself.id =>
          myself
        case (fromId, fromName) =>
          contactsRaw
            .find(_.id == fromId)
            .orElse(contactsRaw.find(_.prettyName == fromName))
            .getOrElse(
              Contact(
                id                 = fromId,
                firstNameOption    = Some(fromName),
                lastNameOption     = None,
                usernameOption     = None,
                phoneNumberOption  = None,
                lastSeenDateOption = None
              ))
            .copy(id = fromId)
      })
      .sortBy(c => (c.id, c.prettyName))
  }

  val interlocutorsMap: Map[Chat, Seq[Contact]] = chatsWithMessages map {
    case (c, ms) =>
      val cs: Seq[Contact] =
        ms.toSet
          .map((m: Message) => (m.fromId, m.fromName))
          .toSeq
          .map {
            case (fromId, _) if fromId == myself.id =>
              myself
            case (fromId, fromName) =>
              contacts
                .find(_.id == fromId)
                .orElse(contacts.find(_.prettyName == fromName))
                .get
          }
      (c, cs.sortBy(c => if (c == myself) (Long.MinValue, "") else (c.id, c.prettyName)))
  }

  override def chats = chatsWithMessages.keys.toSeq

  override def interlocutors(chat: Chat): Seq[Contact] = interlocutorsMap(chat)

  //
  // Direct messages fetch
  //

  override def messagesBefore(chat: Chat, msgId: Long, limit: Int): IndexedSeq[Message] = {
    val messages = chatsWithMessages(chat)
    val idx      = messages.indexWhere(_.id == msgId)
    require(idx > -1, "Message not found, that's unexpected")
    val upperBound = (idx - limit) max 0
    messages.slice(upperBound, idx)
  }

  override def messagesAround(chat: Chat, msgId: Long, limit: Int): IndexedSeq[Message] = {
    val messages = chatsWithMessages(chat)
    val idx      = messages.indexWhere(_.id == msgId)
    require(idx > -1, "Message not found, that's unexpected")
    val upperBound = (idx - (limit / 2)) max 0
    messages.slice(upperBound, upperBound + limit)
  }

  override def lastMessages(chat: Chat, limit: Int): IndexedSeq[Message] = {
    chatsWithMessages.get(chat) map (_.takeRight(limit)) getOrElse IndexedSeq.empty
  }

  override def messageOption(chat: Chat, id: Long): Option[Message] =
    messagesMap.get(chat) flatMap (_ get id)

  //
  // Search
  //

  override def search(chat: Chat, text: String, exact: Boolean): IndexedSeq[Message] = {
    require(exact, "Non-exact search not supported yet")
    val messages = chatsWithMessages(chat)
    messages.filter(m => StringUtils.containsIgnoreCase(m.plainSearchableString, text))
  }

  //
  // Other
  //

  override def distance(chat: Chat, msgId1: Long, msgId2: Long): Int = {
    val messages = chatsWithMessages(chat)
    // TODO: Make efficient!
    val idx1 = messages.indexWhere(_.id == msgId1)
    val idx2 = messages.indexWhere(_.id == msgId1)
    if (idx1 == -1 || idx2 == -1) {
      -1
    } else {
      math.abs(idx1 - idx2)
    }
  }

  override def toString: String = {
    Seq(
      "EagerChatHistoryDao(",
      "  myself:",
      "    " + myself.toString + "\n",
      "  contacts:",
      contacts.mkString("    ", "\n    ", "\n"),
      "  chats:",
      chats.mkString("    ", "\n    ", "\n"),
      ")"
    ).mkString("\n")
  }
}
