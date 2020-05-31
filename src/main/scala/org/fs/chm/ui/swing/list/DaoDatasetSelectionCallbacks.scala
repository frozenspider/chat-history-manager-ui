package org.fs.chm.ui.swing.list
import java.util.UUID

import org.fs.chm.dao.ChatHistoryDao

trait DaoDatasetSelectionCallbacks {
  def renameDataset(dsUuid: UUID, newName: String, dao: ChatHistoryDao): Unit

  def deleteDataset(dsUuid: UUID, dao: ChatHistoryDao): Unit
}
