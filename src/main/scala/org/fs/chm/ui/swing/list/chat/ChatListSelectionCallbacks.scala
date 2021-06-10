package org.fs.chm.ui.swing.list.chat

import org.fs.chm.dao.Chat
import org.fs.chm.dao.ChatHistoryDao
import org.fs.chm.dao.ChatWithDetails

trait ChatListSelectionCallbacks {
  def chatSelected(dao: ChatHistoryDao, cwd: ChatWithDetails): Unit
  def deleteChat(dao: ChatHistoryDao, chat: Chat): Unit
}
