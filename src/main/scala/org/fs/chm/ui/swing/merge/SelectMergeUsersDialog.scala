package org.fs.chm.ui.swing.merge

import java.awt.Color
import java.util.UUID

import scala.collection.immutable.ListMap
import scala.swing._

import org.fs.chm.dao._
import org.fs.chm.ui.swing.general.CustomDialog
import org.fs.chm.ui.swing.general.SwingUtils._
import org.fs.chm.ui.swing.user.UserDetailsPane
import org.fs.chm.utility.EntityUtils._
import org.fs.utility.Imports._

class SelectMergeUsersDialog(
    masterDao: H2ChatHistoryDao,
    masterDs: Dataset,
    slaveDao: ChatHistoryDao,
    slaveDs: Dataset,
) extends CustomDialog[ListMap[User, User]] {
  {
    title = "Select users to merge"
  }

  private lazy val table = new SelectMergesTable[User, (User, User)](
    new Models(masterDao.users(masterDs.uuid), slaveDao.users(slaveDs.uuid))
  )

  override protected lazy val dialogComponent: Component = {
    new BorderPanel {
      import BorderPanel.Position._
      layout(new Label("Note: New users will me merged regardless")) = North
      layout(table.wrapInScrollpane())                               = Center
    }
  }

  override protected def validateChoices(): Option[ListMap[User, User]] = {
    Some(ListMap(table.selected: _*))
  }

  import org.fs.chm.ui.swing.merge.SelectMergesTable._

  private class Models(masterUsers: Seq[User], slaveUsers: Seq[User]) extends MergeModels[User, (User, User)] {

    override val allElems: Seq[RowData[User]] = {
      val masterUsersMap = groupById(masterUsers)

      val merges: Seq[RowData[User]] =
        for (su <- slaveUsers) yield {
          masterUsersMap.get(su.id) match {
            case None     => RowData.InSlaveOnly(su)
            case Some(mu) => RowData.InBoth(mu, su)
          }
        }

      var mergesAcc: Seq[RowData[User]] = Seq.empty

      // 1) Combined and unchanged chats
      val combinesMasterToDataMap: Map[User, RowData.InBoth[User]] =
        merges.collect { case rd @ RowData.InBoth(mu, su) => (mu, rd) }.toMap
      for (mu <- masterUsers) {
        combinesMasterToDataMap.get(mu) match {
          case Some(rd) => mergesAcc = mergesAcc :+ rd
          case None     => mergesAcc = mergesAcc :+ RowData.InMasterOnly(mu)
        }
      }

      // 2) Added chats
      val additionsSlaveToDataMap: Map[User, RowData.InSlaveOnly[User]] =
        merges.collect { case rd @ RowData.InSlaveOnly(su) => (su, rd) }.toMap
      for (su <- slaveUsers if additionsSlaveToDataMap.contains(su)) {
        mergesAcc = mergesAcc :+ additionsSlaveToDataMap(su)
      }

      mergesAcc
    }

    override val renderer = (renderable: ChatRenderable[User]) => {
      val r = new UserDetailsPane(renderable.v, false)
      if (!renderable.isSelectable) {
        r.background = Color.WHITE
      } else if (renderable.isCombine) {
        r.background = Color.YELLOW
      } else if (renderable.isAdd) {
        r.background = Color.GREEN
      } else {
        r.background = Color.WHITE
      }
      new BorderPanel {
        layout(r) = BorderPanel.Position.West
        background = r.background
      }
    }

    /** Only selectable if user content differs */
    override protected def isInBothSelectable(mu: User, su: User): Boolean = mu != su.copy(dsUuid = mu.dsUuid)
    override protected def isInSlaveSelectable(su: User):          Boolean = false
    override protected def isInMasterSelectable(mu: User):         Boolean = false

    override protected def rowDataToResultOption(
        rd: RowData[User],
        isSelected: Boolean
    ): Option[(User, User)] = {
      rd match {
        case _ if !isSelected        => None
        case RowData.InBoth(mu, su)  => Some(mu -> su)
        case RowData.InSlaveOnly(_)  => None
        case RowData.InMasterOnly(_) => None
      }
    }
  }
}
