package org.fs.chm.dao

import scala.collection.immutable.ListMap

class EagerChatHistoryDao(
    override val contacts: Seq[Contact],
    chatsWithMessages: ListMap[Chat, IndexedSeq[Message]]
) extends ChatHistoryDao {

  override def chats = chatsWithMessages.keys.toSeq

  override def messages(chat: Chat, offset: Int, limit: Int) = {
    chatsWithMessages.get(chat) map (_.slice(offset, offset + limit)) getOrElse IndexedSeq.empty
  }

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
