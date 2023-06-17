package org.fs.chm.ui.swing.list

import org.fs.chm.dao.ChatHistoryDao
import org.fs.chm.protobuf.PbUuid

trait DaoDatasetSelectionCallbacks {
  def renameDataset(dao: ChatHistoryDao, dsUuid: PbUuid, newName: String): Unit

  def deleteDataset(dao: ChatHistoryDao, dsUuid: PbUuid): Unit

  def shiftDatasetTime(dao: ChatHistoryDao, dsUuid: PbUuid, hrs: Int): Unit
}
