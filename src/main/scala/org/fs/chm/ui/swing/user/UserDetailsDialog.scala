package org.fs.chm.ui.swing.user

import scala.swing._

import org.fs.chm.dao.ChatHistoryDao
import org.fs.chm.dao.User
import org.fs.chm.ui.swing.general.CustomDialog

class UserDetailsDialog(
    dao: ChatHistoryDao,
    user: User
) extends CustomDialog[User] {

  {
    title = user.prettyName
  }

  override protected def okButtonText: String = "Apply"

  override protected lazy val dialogComponent: UserDetailsPane = {
    new UserDetailsPane(dao, user, true, None)
  }

  override protected def validateChoices(): Option[User] = {
    Some(dialogComponent.data)
  }

  private case class Entry(label: String, component: TextArea)
}
