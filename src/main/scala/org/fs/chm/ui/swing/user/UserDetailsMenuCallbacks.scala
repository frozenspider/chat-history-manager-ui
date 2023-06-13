package org.fs.chm.ui.swing.user

import org.fs.chm.dao.ChatHistoryDao
import org.fs.chm.dao.Entities.User

trait UserDetailsMenuCallbacks {
  def userEdited(user: User, dao: ChatHistoryDao): Unit
  def usersMerged(baseUser: User, absorbedUser: User, dao: ChatHistoryDao): Unit
}
