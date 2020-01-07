package org.fs.chm.ui.swing.chatlist

import java.util.UUID

import org.fs.chm.dao.ChatHistoryDao
import org.fs.chm.ui.swing.general.ChatWithDao

trait ChatListSelectionCallbacks {
  def chatSelected(cc: ChatWithDao): Unit

  def renameDataset(dsUuid: UUID, newName: String, dao: ChatHistoryDao): Unit
}
