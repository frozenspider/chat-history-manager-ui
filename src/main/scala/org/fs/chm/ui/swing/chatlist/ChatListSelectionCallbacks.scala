package org.fs.chm.ui.swing.chatlist

import java.util.UUID

import org.fs.chm.dao._
import org.fs.chm.ui.swing.general.ChatWithDao

trait ChatListSelectionCallbacks {
  def chatSelected(cc: ChatWithDao): Unit

  def renameDataset(dsUuid: UUID, newName: String, dao: ChatHistoryDao): Unit

  def alterUser(user: User, dao: ChatHistoryDao): Unit
}
