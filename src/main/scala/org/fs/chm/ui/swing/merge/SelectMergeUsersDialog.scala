package org.fs.chm.ui.swing.merge

import scala.swing._

import org.fs.chm.dao.ChatHistoryDao
import org.fs.chm.dao.Entities._
import org.fs.chm.dao.MutableChatHistoryDao
import org.fs.chm.dao.merge.DatasetMerger.UserMergeOption
import org.fs.chm.protobuf.Dataset
import org.fs.chm.protobuf.User
import org.fs.chm.ui.swing.general.CustomDialog
import org.fs.chm.ui.swing.general.SwingUtils._
import org.fs.chm.ui.swing.user.UserDetailsPane
import org.fs.chm.utility.EntityUtils._
import org.fs.utility.Imports._

/**
 * Show dialog for merging users.
 * Rules:
 * - There will be a merge option in the output for every user in master and slave DSs
 * - Master users will precede slave-only users
 * - Checkbox option will ONLY be present for users that actually changed
 * - Users whose chat was skipped entirely won't have a checkbox
 * - Result will contain an entry for every user in either datasets
 * This means that the only thing that could be controlled is whether to merge changed users or keep an old ones
 */
class SelectMergeUsersDialog(
    masterDao: MutableChatHistoryDao,
    masterDs: Dataset,
    slaveDao: ChatHistoryDao,
    slaveDs: Dataset,
    activeUserIds: Set[Long]
) extends CustomDialog[Seq[UserMergeOption]](takeFullHeight = true) {

  // Values here are lazy because they are used from the parent init code.

  private lazy val originalTitle = "Select users to merge"

  {
    title = originalTitle
  }

  private lazy val models = new Models(masterDao.users(masterDs.uuid), slaveDao.users(slaveDs.uuid))

  private lazy val table = new SelectMergesTable[UserWithDao, UserMergeOption](models)

  override protected lazy val dialogComponent: Component = {
    new BorderPanel {
      import BorderPanel.Position._
      layout(new Label("Note: New users will me merged regardless")) = North
      layout(table.wrapInScrollpaneAndAdjustWidth())                 = Center
    }
  }

  override protected def validateChoices(): Option[Seq[UserMergeOption]] = {
    Some(table.selected)
  }

  import org.fs.chm.ui.swing.merge.SelectMergesTable._

  private class Models(masterUsers: Seq[User], slaveUsers: Seq[User])
      extends MergeModels[UserWithDao, UserMergeOption] {

    override val allElems: Seq[RowData[UserWithDao]] = {
      val masterUsersMap = groupById(masterUsers)

      // Exclude non-active users
      val merges: Seq[RowData[UserWithDao]] =
        for (su <- slaveUsers) yield {
          masterUsersMap.get(su.id) match {
            case None =>
              RowData.InSlaveOnly(UserWithDao(su, slaveDao), selectable = false)
            case Some(mu) =>
              // Only selectable if user content conflicting/additional content
              def hasDifferentValue[T](lens: User => Option[T]): Boolean =
                lens(su).isDefined && lens(su) != lens(mu)

              val selectable = hasDifferentValue(u => u.firstNameOption) ||
                hasDifferentValue(u => u.lastNameOption) ||
                hasDifferentValue(u => u.usernameOption) ||
                hasDifferentValue(u => u.phoneNumberOption)

              RowData.InBoth(UserWithDao(mu, masterDao), UserWithDao(su, slaveDao), selectable)
          }
        }

      var mergesAcc: Seq[RowData[UserWithDao]] = Seq.empty

      // 1) Combined and unchanged users
      val combinesMasterToDataMap: Map[User, RowData.InBoth[UserWithDao]] =
        merges.collect { case rd @ RowData.InBoth(mu, _, _) => (mu.user, rd) }.toMap
      for (mu <- masterUsers) {
        combinesMasterToDataMap.get(mu) match {
          case Some(rd) => mergesAcc = mergesAcc :+ rd
          case None     => mergesAcc = mergesAcc :+ RowData.InMasterOnly(UserWithDao(mu, masterDao), selectable = false)
        }
      }

      // 2) Added users
      val additionsSlaveToDataMap: Map[User, RowData.InSlaveOnly[UserWithDao]] =
        merges.collect { case rd @ RowData.InSlaveOnly(su, _) => (su.user, rd) }.toMap
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

    override protected def rowDataToResultOption(
        rd: RowData[UserWithDao],
        isSelected: Boolean
    ): Option[UserMergeOption] = {
      Some(rd match {
        case RowData.InMasterOnly(muwd, _)                                        => UserMergeOption.Retain(muwd.user)
        case RowData.InSlaveOnly(suwd, _) if activeUserIds.contains(suwd.user.id) => UserMergeOption.Add(suwd.user)
        case RowData.InSlaveOnly(suwd, _)                                         => UserMergeOption.DontAdd(suwd.user)
        case RowData.InBoth(muwd, suwd, _) if isSelected                          => UserMergeOption.Replace(muwd.user, suwd.user)
        case RowData.InBoth(muwd, suwd, _)                                        => UserMergeOption.MatchOrDontReplace(muwd.user, suwd.user)
      })
    }
  }

  private case class UserWithDao(user: User, dao: ChatHistoryDao)
}

object SelectMergeUsersDialog {
  def main(args: Array[String]): Unit = {
    import scala.collection.immutable.ListMap

    import org.fs.chm.utility.test.TestUtils._
    import org.fs.chm.utility.test.EagerChatHistoryDao

    def createMultiUserDao(usersProducer: Dataset => Seq[User]): MutableChatHistoryDao = {
      val ds = Dataset(
        uuid       = randomUuid,
        alias      = "Dataset",
      )
      val users        = usersProducer(ds)
      val chat         = createGroupChat(ds.uuid, 1, "One", users.map(_.id), 0)
      val dataPathRoot = makeTempDir("eager")
      new EagerChatHistoryDao(
        name               = "Dao",
        _dataRootFile      = dataPathRoot,
        dataset            = ds,
        myself1            = users.head,
        users1             = users,
        _chatsWithMessages = ListMap(chat -> IndexedSeq.empty)
      ) with EagerMutableDaoTrait
    }

    val mDao = createMultiUserDao { ds =>
      (1 to 5) map (i => createUser(ds.uuid, i))
    }
    val (mDs, _, _, _, _) = getSimpleDaoEntities(mDao)
    val sDao = createMultiUserDao { ds =>
      (2 to 6 by 2) map { i =>
        val u = createUser(ds.uuid, i)
        if (i == 2) u.copy(firstNameOption = Some("Aha!")) else u
      }
    }
    val (sDs, _, _, _, _) = getSimpleDaoEntities(sDao)

    val activeUserIds = Seq(mDao.users(mDs.uuid), sDao.users(sDs.uuid)).flatten.map(_.id).sorted.dropRight(1).toSet

    val dialog = new SelectMergeUsersDialog(mDao, mDs, sDao, sDs, activeUserIds)
    dialog.visible = true
    dialog.peer.setLocationRelativeTo(null)
    println(dialog.selection map (_.mkString("\n  ", "\n  ", "\n")))
  }
}
