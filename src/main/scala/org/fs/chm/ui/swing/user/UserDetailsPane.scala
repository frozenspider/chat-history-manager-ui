package org.fs.chm.ui.swing.user

import java.awt.Color

import scala.swing.GridBagPanel.Anchor
import scala.swing.GridBagPanel.Fill
import scala.swing._
import scala.swing.event.MouseReleased

import javax.swing.SwingUtilities
import javax.swing.border.MatteBorder
import org.fs.chm.dao.ChatHistoryDao
import org.fs.chm.dao.User
import org.fs.chm.ui.swing.general.SwingUtils._
import org.fs.chm.ui.swing.general.field.TextComponent
import org.fs.chm.ui.swing.general.field.TextOptionComponent

class UserDetailsPane(
    private var user: User,
    dao: ChatHistoryDao,
    mutable: Boolean,
    menuCallbacksOption: Option[UserDetailsMenuCallbacks]
) extends GridBagPanel {

  val firstNameC   = new TextOptionComponent(user.firstNameOption, mutable)
  val lastNameC    = new TextOptionComponent(user.lastNameOption, mutable)
  val usernameC    = new TextOptionComponent(user.usernameOption, false)
  val phoneNumberC = new TextOptionComponent(user.phoneNumberOption, mutable)

  {
    val data: Seq[(String, Component)] = Seq(
      ("ID:", new TextComponent(user.id.toString, false)),
      ("First Name:", firstNameC),
      ("Last Name:", lastNameC),
      ("Username:", usernameC),
      ("Phone:", phoneNumberC),
    )

    val c = new Constraints
    c.ipadx = 10
    c.ipady = 3

    // Labels column
    c.gridx  = 0
    c.anchor = Anchor.NorthEast
    c.fill   = Fill.None
    for ((t, _) <- data) {
      layout(new Label(t) {
        this.fontSize = 15
      }) = c
    }

    // Data column
    c.gridx   = 1
    c.anchor  = Anchor.NorthWest
    c.weightx = 1
    c.fill    = Fill.Horizontal
    for ((_, v) <- data) {
      layout(v) = c
    }

    border = new MatteBorder(0, 0, 1, 0, Color.GRAY)

    menuCallbacksOption foreach addRightClickMenu
  }

  private def addRightClickMenu(callbacks: UserDetailsMenuCallbacks): Unit = {
    val popupMenu = new PopupMenu {
      contents += menuItem("Edit", enabled = dao.isMutable)(edit())
    }

    // Reactions
    listenTo(this, mouse.clicks)
    reactions += {
      case e @ MouseReleased(src, pt, _, _, _) if SwingUtilities.isRightMouseButton(e.peer) && enabled =>
        popupMenu.show(this, pt.x, pt.y)
    }
  }

  private def edit(): Unit = {
    val dialog = new UserDetailsDialog(user, dao)
    dialog.width   = this.width
    dialog.visible = true
    dialog.selection foreach { result =>
      this.user = result
      reload()
      menuCallbacksOption foreach (_.userEdited(user, dao))
    }
  }

  private def reload(): Unit = {
    firstNameC.value   = user.firstNameOption
    lastNameC.value    = user.lastNameOption
    usernameC.value    = user.usernameOption
    phoneNumberC.value = user.phoneNumberOption
  }
}
