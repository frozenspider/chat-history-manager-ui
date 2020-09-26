package org.fs.chm.loader.telegram

import java.io.File

import org.fs.chm.dao.EagerChatHistoryDao
import org.fs.chm.loader.DataLoader

trait TelegramDataLoader extends DataLoader[EagerChatHistoryDao] {

  /** Check whether the given file looks to have a right format, show what's wrong if it isn't */
  def doesLookRight(rootFile: File): Option[String]
}
