package org.fs.chm.ui.swing.user

import java.awt.Color

import scala.swing.GridBagPanel.Anchor
import scala.swing.GridBagPanel.Fill
import scala.swing._

import javax.swing.border.MatteBorder
import org.fs.chm.dao.User
import org.fs.chm.ui.swing.general.SwingUtils._
import org.fs.chm.ui.swing.general.field.TextComponent
import org.fs.chm.ui.swing.general.field.TextOptionComponent

class UserDetailsPane(
    user: User,
    mutable: Boolean
) extends GridBagPanel {

  val firstNameC   = new TextOptionComponent(user.firstNameOption, mutable)
  val lastNameC    = new TextOptionComponent(user.lastNameOption, mutable)
  val usernameC    = new TextOptionComponent(user.usernameOption, false)
  val phoneNumberC = new TextOptionComponent(user.phoneNumberOption, mutable)
  val lastSeenTimeC = // No need to render date/time properly yet
    new TextComponent(user.lastSeenTimeOption map (_.toString("yyyy-MM-dd HH:mm:ss")) getOrElse "", false)

  {
    val data: Seq[(String, Component)] = Seq(
      ("First Name:", firstNameC),
      ("Last Name:", lastNameC),
      ("Username:", usernameC),
      ("Phone:", phoneNumberC),
      ("", new TextComponent("", false)),
      ("ID:", new TextComponent(user.id.toString, false)),
      ("Last Seen:", lastSeenTimeC)
    )

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
      add(v, c)
    }

    border = new MatteBorder(0, 0, 1, 0, Color.GRAY)
  }
}
