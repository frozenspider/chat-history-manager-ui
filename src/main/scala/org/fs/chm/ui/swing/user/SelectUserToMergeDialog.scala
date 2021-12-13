package org.fs.chm.ui.swing.user

import scala.swing.GridBagPanel.Anchor
import scala.swing.GridBagPanel.Fill
import scala.swing._

import org.fs.chm.dao._
import org.fs.chm.ui.swing.general.CustomDialog
import org.fs.chm.ui.swing.general.SwingUtils._

class SelectUserToMergeDialog(
    dao: ChatHistoryDao,
    baseUser: User
) extends CustomDialog[User](takeFullHeight = true) {
  {
    title = s"Select user to merge with ${baseUser.prettyName}"
  }

  private lazy val elementsSeq: Seq[(RadioButton, UserDetailsPane)] = {
    val group = new ButtonGroup()
    dao.users(baseUser.dsUuid).zipWithIndex.collect {
      case (u, idx) if u.id != baseUser.id =>
        val radio = new RadioButton()
        group.buttons += radio

        val pane = new UserDetailsPane(dao, u, true, None)
        for (el <- Seq(pane.firstNameC, pane.lastNameC)) {
          el.innerComponent.foreground = Colors.forIdx(idx)
          el.innerComponent.fontStyle  = Font.Style.Bold
        }
        (radio, pane)
    }
  }

  override protected lazy val dialogComponent: Component = {
    val containerPanel = new GridBagPanel {
      val c = new Constraints
      c.ipady = 3

      // Labels column
      c.insets = new Insets(0, 10, 0, 10)
      c.gridx  = 0
      c.anchor = Anchor.East
      c.fill   = Fill.None
      for ((rb, _) <- elementsSeq) {
        layout(rb) = c
      }

      // Data column
      c.insets  = new Insets(0, 0, 0, 0)
      c.gridx   = 1
      c.anchor  = Anchor.NorthWest
      c.weightx = 1
      c.fill    = Fill.Horizontal
      for ((_, pane) <- elementsSeq) {
        layout(pane) = c
      }
    }
    containerPanel.wrapInScrollpaneAndAdjustWidth()
  }

  override protected def validateChoices(): Option[User] = {
    elementsSeq collectFirst {
      case (radio, detailsPane) if radio.selected => detailsPane.data
    }
  }
}
