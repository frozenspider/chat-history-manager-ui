package org.fs.chm.ui.swing.chatlist

import scala.swing.GridBagPanel.Anchor
import scala.swing.GridBagPanel.Fill
import scala.swing._

import org.fs.chm.dao.User
import org.fs.chm.ui.swing.general.SwingUtils._

class UserDetailsPane(
    user: User
) extends GridBagPanel {
  {
    val data: Seq[(String, String)] = Seq(
      ("First Name:", Some(user.firstNameOption getOrElse "")),
      ("Last Name:", Some(user.lastNameOption getOrElse "")),
      ("Username:", Some(user.usernameOption getOrElse "")),
      ("Phone:", Some(user.phoneNumberOption getOrElse "")),
      ("", Some("")),
      ("ID:", Some(user.id.toString)),
      ("Last Seen:", Some(user.lastSeenTimeOption map (_.toString("yyyy-MM-dd HH:mm:ss")) getOrElse ""))
    ).collect {
      case ((x, Some(y))) => (x, y)
    }

    val c = new Constraints
    c.fill  = Fill.None
    c.ipadx = 10
    c.ipady = 3

    // Labels column
    c.gridx  = 0
    c.anchor = Anchor.NorthEast
    for (((t, _), idx) <- data.zipWithIndex) {
      c.gridy = idx
      add(new Label(t) {
        this.fontSize = 15
      }, c)
    }

    // Data column
    c.gridx  = 1
    c.anchor = Anchor.NorthWest
    for (((_, v), idx) <- data.zipWithIndex) {
      c.gridy = idx
      add(new TextArea(v) {
        this.fontSize   = 15
        this.editable   = false
        this.background = null
        this.border     = null
      }, c)
    }
  }
}
