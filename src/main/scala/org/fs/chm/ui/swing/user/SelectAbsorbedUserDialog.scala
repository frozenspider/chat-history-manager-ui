package org.fs.chm.ui.swing.user

import scala.swing.GridBagPanel.Anchor
import scala.swing.GridBagPanel.Fill
import scala.swing._

import org.fs.chm.dao.ChatHistoryDao
import org.fs.chm.dao.Entities._
import org.fs.chm.protobuf.User
import org.fs.chm.ui.swing.general.CustomDialog
import org.fs.chm.ui.swing.general.SwingUtils._

class SelectAbsorbedUserDialog(
    dao: ChatHistoryDao,
    baseUser: User
) extends CustomDialog[User](takeFullHeight = true) {
  {
    title = s"Select user to be absorbed into ${baseUser.prettyName}"
  }

  override protected def headerText: String = "WARNING: Absorbed user will be removed, and its messages merge info will be lost.\n" +
    "Only do this for users that will have no more history to be merged.\n" +
    "Base user messages will keep their source IDs intact and this will be mergeable as before."

  private lazy val elementsSeq: Seq[(RadioButton, UserDetailsPane)] = {
    val group = new ButtonGroup()
    dao.users(baseUser.dsUuid).zipWithIndex.collect {
      case (u, idx) if u.id != baseUser.id =>
        val radio = new RadioButton()
        group.buttons += radio

        val pane = new UserDetailsPane(dao, u, editable = false, menuCallbacksOption = None)
        pane.stylizeFirstLastName(Colors.forIdx(idx))
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
