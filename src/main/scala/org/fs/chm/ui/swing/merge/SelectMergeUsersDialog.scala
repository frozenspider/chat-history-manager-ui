package org.fs.chm.ui.swing.merge

import scala.collection.immutable.ListMap
import scala.swing._

import org.fs.chm.dao._
import org.fs.chm.ui.swing.general.CustomDialog
import org.fs.chm.ui.swing.general.SwingUtils._
import org.fs.chm.ui.swing.user.UserDetailsPane
import org.fs.chm.utility.EntityUtils._
import org.fs.utility.Imports._

class SelectMergeUsersDialog(
    masterDao: MutableChatHistoryDao,
    masterDs: Dataset,
    slaveDao: ChatHistoryDao,
    slaveDs: Dataset,
) extends CustomDialog[ListMap[User, User]] {
  private lazy val originalTitle = "Select users to merge"

  {
    title = originalTitle
  }

  private lazy val models =  new Models(masterDao.users(masterDs.uuid), slaveDao.users(slaveDs.uuid))

  private lazy val table = new SelectMergesTable[UserWithDao, (User, User)](models)

  override protected lazy val dialogComponent: Component = {
    new BorderPanel {
      import BorderPanel.Position._
      layout(new Label("Note: New users will me merged regardless")) = North
      layout(table.wrapInScrollpaneAndAdjustWidth())                 = Center
    }
  }

  override protected def validateChoices(): Option[ListMap[User, User]] = {
    Some(ListMap(table.selected: _*))
  }

  import org.fs.chm.ui.swing.merge.SelectMergesTable._

  private class Models(masterUsers: Seq[User], slaveUsers: Seq[User]) extends MergeModels[UserWithDao, (User, User)] {

    override val allElems: Seq[RowData[UserWithDao]] = {
      val masterUsersMap = groupById(masterUsers)

      val merges: Seq[RowData[UserWithDao]] =
        for (su <- slaveUsers) yield {
          masterUsersMap.get(su.id) match {
            case None     => RowData.InSlaveOnly(UserWithDao(su, slaveDao))
            case Some(mu) => RowData.InBoth(UserWithDao(mu, masterDao), UserWithDao(su, slaveDao))
          }
        }

      var mergesAcc: Seq[RowData[UserWithDao]] = Seq.empty

      // 1) Combined and unchanged chats
      val combinesMasterToDataMap: Map[User, RowData.InBoth[UserWithDao]] =
        merges.collect { case rd @ RowData.InBoth(mu, su) => (mu.user, rd) }.toMap
      for (mu <- masterUsers) {
        combinesMasterToDataMap.get(mu) match {
          case Some(rd) => mergesAcc = mergesAcc :+ rd
          case None     => mergesAcc = mergesAcc :+ RowData.InMasterOnly(UserWithDao(mu, masterDao))
        }
      }

      // 2) Added chats
      val additionsSlaveToDataMap: Map[User, RowData.InSlaveOnly[UserWithDao]] =
        merges.collect { case rd @ RowData.InSlaveOnly(su) => (su.user, rd) }.toMap
      for (su <- slaveUsers if additionsSlaveToDataMap.contains(su)) {
        mergesAcc = mergesAcc :+ additionsSlaveToDataMap(su)
      }

      mergesAcc
    }

    override val cellsAreInteractive = false

    override val renderer = (renderable: ListItemRenderable[UserWithDao]) => {
      val r = new UserDetailsPane(renderable.v.dao, renderable.v.user, false, None)
      if (!renderable.isSelectable) {
        r.background = Colors.NoBg
      } else if (renderable.isCombine) {
        r.background = Colors.CombineBg
      } else if (renderable.isAdd) {
        r.background = Colors.AdditionBg
      } else {
        r.background = Colors.NoBg
      }
      new BorderPanel {
        layout(r) = BorderPanel.Position.West
        background = r.background
      }
    }

    /** Only selectable if user content differs */
    override protected def isInBothSelectable(mu: UserWithDao, su: UserWithDao): Boolean =
      mu.user != su.user.copy(dsUuid = mu.user.dsUuid)
    override protected def isInSlaveSelectable(su: UserWithDao):  Boolean = false
    override protected def isInMasterSelectable(mu: UserWithDao): Boolean = false

    override protected def rowDataToResultOption(
        rd: RowData[UserWithDao],
        isSelected: Boolean
    ): Option[(User, User)] = {
      rd match {
        case _ if !isSelected        => None
        case RowData.InBoth(mu, su)  => Some(mu.user -> su.user)
        case RowData.InSlaveOnly(_)  => None
        case RowData.InMasterOnly(_) => None
      }
    }
  }

  private case class UserWithDao(user: User, dao: ChatHistoryDao)
}
