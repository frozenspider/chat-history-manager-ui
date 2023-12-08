package org.fs.chm.ui.swing

import com.github.nscala_time.time.Imports.DateTime
import org.fs.chm.dao.ChatHistoryDao
import org.fs.chm.dao.Entities.CombinedChat
import org.fs.chm.protobuf.Chat
import org.fs.chm.protobuf.PbUuid
import org.fs.chm.protobuf.User

object Callbacks {
  //
  // Dataset
  //

  trait RenameDatasetCb {
    def renameDataset(dao: ChatHistoryDao, dsUuid: PbUuid, newName: String): Unit
  }

  trait DeleteDatasetCb {
    def deleteDataset(dao: ChatHistoryDao, dsUuid: PbUuid): Unit
  }

  trait ShiftDatasetTimeCb {
    def shiftDatasetTime(dao: ChatHistoryDao, dsUuid: PbUuid, hrs: Int): Unit
  }

  //
  // User
  //

  trait UserDetailsMenuCb {
    def userEdited(user: User, dao: ChatHistoryDao): Unit
  }

  //
  // Chat
  //

  trait ChatCb {
    def selectChat(dao: ChatHistoryDao, cc: CombinedChat): Unit

    def deleteChat(dao: ChatHistoryDao, cc: CombinedChat): Unit

    def combineChats(dao: ChatHistoryDao, masterChat: Chat, slaveChat: Chat): Unit

    def compareChats(dao: ChatHistoryDao, baseChat: Chat, secondaryChat: Chat): Unit
  }


  //
  // Message history
  //

  trait MessageHistoryCb {

    def navigateToBeginning(): Unit

    def navigateToEnd(): Unit

    /**
     * Go to a first message at the given date.
     * If none found, go to the first one after.
     * If none found again, go to the last message.
     */
    def navigateToDate(date: DateTime): Unit
  }

}
