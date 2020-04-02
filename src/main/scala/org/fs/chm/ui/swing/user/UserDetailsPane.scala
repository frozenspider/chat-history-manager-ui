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
import org.fs.chm.ui.swing.general.field.ValueComponent

class UserDetailsPane(
    dao: ChatHistoryDao,
    private var user: User,
    editable: Boolean,
    menuCallbacksOption: Option[UserDetailsMenuCallbacks]
) extends GridBagPanel {

  val firstNameC   = new TextOptionComponent(user.firstNameOption, editable)
  val lastNameC    = new TextOptionComponent(user.lastNameOption, editable)
  val usernameC    = new TextOptionComponent(user.usernameOption, false)
  val phoneNumberC = new TextOptionComponent(user.phoneNumberOption, editable)

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

    menuCallbacksOption foreach { _ => // Adding right-click menu
      val popupMenu = new PopupMenu {
        contents += menuItem("Edit", enabled = dao.isMutable)(edit())
        contents += menuItem("Merge With...", enabled = dao.isMutable)(merge())
      }

      // Reactions
      listenTo(this, mouse.clicks)
      this.contents.collect {
        case c: ValueComponent[_] => listenTo(c.innerComponent, c.innerComponent.mouse.clicks)
      }
      reactions += {
        case e @ MouseReleased(src, pt, _, _, _) if SwingUtilities.isRightMouseButton(e.peer) && enabled =>
          popupMenu.show(src, pt.x, pt.y)
      }
    }
  }

  /** User from current field values */
  def data: User = {
    user.copy(
      firstNameOption   = firstNameC.value,
      lastNameOption    = lastNameC.value,
      usernameOption    = usernameC.value,
      phoneNumberOption = phoneNumberC.value
    )
  }

  private def edit(): Unit = {
    val dialog = new UserDetailsDialog(dao, user)
    dialog.width   = this.width
    dialog.visible = true
    dialog.selection foreach { user2 =>
      menuCallbacksOption foreach (_.userEdited(user2, dao))
      this.user = user2
      reload()
    }
  }

  private def merge(): Unit = {
    val dialog = new SelectUserToMergeDialog(dao, user)
    dialog.visible = true
    dialog.selection foreach { user2 =>
      menuCallbacksOption foreach (_.usersMerged(user, user2, dao))
      this.user = user2
      reload()
    }
  }

  private def reload(): Unit = {
    firstNameC.value   = user.firstNameOption
    lastNameC.value    = user.lastNameOption
    usernameC.value    = user.usernameOption
    phoneNumberC.value = user.phoneNumberOption
  }
}
