package org.fs.chm.dao

import scala.collection.immutable.ListMap
import scala.collection.immutable.TreeMap

class EagerChatHistoryDao(
    override val contacts: Seq[Contact],
    chatsWithMessages: ListMap[Chat, IndexedSeq[Message]]
) extends ChatHistoryDao {

  // We can't use mapValues because it's lazy...
  val messagesMap: ListMap[Chat, TreeMap[Long, Message]] = chatsWithMessages map {
    case (c, ms) => (c, TreeMap(ms.map(m => (m.id, m)): _*))
  }

  override def chats = chatsWithMessages.keys.toSeq

  override def lastMessages(chat: Chat, limit: Int): IndexedSeq[Message] = {
    chatsWithMessages.get(chat) map (_.takeRight(limit)) getOrElse IndexedSeq.empty
  }

  override def messageOption(chat: Chat, id: Long): Option[Message] =
    messagesMap.get(chat) flatMap (_ get id)

  override def toString: String = {
    Seq(
      "EagerChatHistoryDao(",
      "  contacts:",
      contacts.mkString("    ", "\n    ", "\n"),
      "  chats:",
      chats.mkString("    ", "\n    ", "\n"),
      ")"
    ).mkString("\n")
  }
}
