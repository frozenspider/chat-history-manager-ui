package org.fs.chm.ui.swing.list
import java.util.UUID

import org.fs.chm.dao.ChatHistoryDao

trait DaoDatasetSelectionCallbacks {
  def renameDataset(dao: ChatHistoryDao, dsUuid: UUID, newName: String): Unit

  def deleteDataset(dao: ChatHistoryDao, dsUuid: UUID): Unit

  def shiftDatasetTime(dao: ChatHistoryDao, dsUuid: UUID, hrs: Int): Unit
}
