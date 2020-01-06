package org.fs.chm.ui.swing.general

import org.fs.chm.dao.Chat
import org.fs.chm.dao.ChatHistoryDao

case class ChatWithDao(chat: Chat, dao: ChatHistoryDao) {
  val dsUuid = chat.dsUuid

  override def equals(that: Any): Boolean = that match {
    case ChatWithDao(chat2, dao2) => (chat == chat2) && (dao eq dao2)
    case _                        => false
  }
}
