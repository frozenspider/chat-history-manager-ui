package org.fs.chm.ui.swing.user

import scala.swing._

import org.fs.chm.dao.User
import org.fs.chm.ui.swing.general.CustomDialog

class UserDetailsDialog(
    user: User
) extends CustomDialog[User] {

  {
    title = user.prettyName
  }

  override protected lazy val dialogComponent: UserDetailsPane = {
    new UserDetailsPane(user, true)
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
