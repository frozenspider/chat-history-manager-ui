package org.fs.chm.ui.swing

import org.fs.chm.dao.ChatHistoryDao
import org.fs.chm.protobuf.PbUuid

object Callbacks {
  trait RenameDataset {
    def renameDataset(dao: ChatHistoryDao, dsUuid: PbUuid, newName: String): Unit
  }

  trait DeleteDataset {
    def deleteDataset(dao: ChatHistoryDao, dsUuid: PbUuid): Unit
  }

  trait ShiftDatasetTime {
    def shiftDatasetTime(dao: ChatHistoryDao, dsUuid: PbUuid, hrs: Int): Unit
  }
}
