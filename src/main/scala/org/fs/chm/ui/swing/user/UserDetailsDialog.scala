package org.fs.chm.ui.swing.user

import scala.swing._

import org.fs.chm.dao.ChatHistoryDao
import org.fs.chm.dao.User
import org.fs.chm.ui.swing.general.CustomDialog

class UserDetailsDialog(
    user: User,
    dao: ChatHistoryDao
) extends CustomDialog[User] {

  {
    title = user.prettyName
  }

  override protected def okButtonText: String = "Apply"

  override protected lazy val dialogComponent: UserDetailsPane = {
    new UserDetailsPane(user, dao, true, None)
  }

  override protected def validateChoices(): Option[User] = {
    Some(
      user.copy(
        firstNameOption   = dialogComponent.firstNameC.value,
        lastNameOption    = dialogComponent.lastNameC.value,
        usernameOption    = dialogComponent.usernameC.value,
        phoneNumberOption = dialogComponent.phoneNumberC.value
      )
    )
  }

  private case class Entry(label: String, component: TextArea)
}
