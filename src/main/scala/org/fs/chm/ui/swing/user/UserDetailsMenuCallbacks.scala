package org.fs.chm.ui.swing.user

import org.fs.chm.dao.ChatHistoryDao
import org.fs.chm.dao.User

trait UserDetailsMenuCallbacks {
  def userEdited(user: User, dao: ChatHistoryDao): Unit
}
