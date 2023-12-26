package org.fs.chm.ui.swing.merge

import scala.swing._

import org.fs.chm.dao.ChatHistoryDao
import org.fs.chm.dao.Entities._
import org.fs.chm.dao.MutableChatHistoryDao
import org.fs.chm.dao.merge.DatasetMerger._
import org.fs.chm.protobuf.Chat
import org.fs.chm.protobuf.Dataset
import org.fs.chm.protobuf.User
import org.fs.chm.ui.swing.general.CustomDialog
import org.fs.chm.ui.swing.general.SwingUtils._
import org.fs.chm.ui.swing.list.chat.ChatListItem
import org.fs.chm.utility.EntityUtils._
import org.fs.utility.Imports._

/**
 * Show dialog for merging chats.
 * Rules:
 * - There will be a merge option in the output for every chat in master DS
 * - Chats main chat ID will be ignored, each primitive chat is merged separately
 * - `Combine` options would NOT have mismatches defined
 * - Master chats will precede slave-only chats
 * - Checkbox option will be present for all chats in slave DS - i.e. whether to add/combine them or not
 */
class SelectMergeChatsDialog(
    masterDao: MutableChatHistoryDao,
    masterDs: Dataset,
    slaveDao: ChatHistoryDao,
    slaveDs: Dataset,
) extends CustomDialog[Seq[SelectedChatMergeOption]](takeFullHeight = true) {
  private lazy val originalTitle = "Select chats to merge"

  {
    title = originalTitle
  }

  private lazy val models = new Models(masterDao.chats(masterDs.uuid), slaveDao.chats(slaveDs.uuid))

  private lazy val table = {
    checkEdt()
    new SelectMergesTable[(ChatHistoryDao, ChatWithDetails), SelectedChatMergeOption](models)
  }

  override protected lazy val dialogComponent: Component = {
    table.wrapInScrollpaneAndAdjustWidth()
  }

  override protected def validateChoices(): Option[Seq[SelectedChatMergeOption]] = {
    Some(table.selected)
  }

  import org.fs.chm.ui.swing.merge.SelectMergesTable._

  private class Models(masterChats: Seq[ChatWithDetails], slaveChats: Seq[ChatWithDetails])
      extends MergeModels[(ChatHistoryDao, ChatWithDetails), SelectedChatMergeOption] {

    override val allElems: Seq[RowData[(ChatHistoryDao, ChatWithDetails)]] = {
      val masterChatsMap = groupById(masterChats)
      val slaveChatsMap  = groupById(slaveChats)

      // 1) Combined and unchanged chats
      val res1 =
        masterChats map { mc =>
          slaveChatsMap.get(mc.chat.id) match {
            case Some(sc) => RowData.InBoth(masterDao -> mc, slaveDao -> sc, selectable = true)
            case None     => RowData.InMasterOnly((masterDao: ChatHistoryDao) -> mc, selectable = false)
          }
        }

      // 2) Added chats
      val res2 = slaveChats.filter(sc => !masterChatsMap.contains(sc.chat.id)).map { sc =>
        RowData.InSlaveOnly(slaveDao -> sc, selectable = true)
      }

      res1 ++ res2
    }

    override val cellsAreInteractive = false

    override val renderer = (renderable: ListItemRenderable[(ChatHistoryDao, ChatWithDetails)]) => {
      val r = new ChatListItem(renderable.v._1, CombinedChat(renderable.v._2, Seq.empty), None, None)
      if (renderable.isCombine) {
        r.inactiveColor = Colors.CombineBg
      } else if (renderable.isAdd) {
        r.inactiveColor = Colors.AdditionBg
      }
      r.markDeselected()
      r
    }

    override protected def rowDataToResultOption(
        rd: RowData[(ChatHistoryDao, ChatWithDetails)],
        isSelected: Boolean
    ): Option[SelectedChatMergeOption] = {
      Some(rd match {
        case RowData.InMasterOnly((_, mcwd), _) =>
          ChatMergeOption.Keep(mcwd)
        case RowData.InBoth((_, mcwd), (_, scwd), _) =>
          if (isSelected) {
            ChatMergeOption.SelectedCombine(mcwd, scwd)
          } else {
            ChatMergeOption.DontCombine(mcwd, scwd)
          }
        case RowData.InSlaveOnly((_, scwd), _) =>
          if (isSelected) {
            ChatMergeOption.Add(scwd)
          } else {
            ChatMergeOption.DontAdd(scwd)
          }
      })
    }
  }
}

object SelectMergeChatsDialog {
  def main(args: Array[String]): Unit = {
    import java.nio.file.Files

    import scala.collection.immutable.ListMap

    import org.fs.chm.utility.test.TestUtils._
    import org.fs.chm.utility.test.EagerChatHistoryDao

    def createMultiChatDao(chatsProducer: (Dataset, Seq[User]) => Seq[Chat]): MutableChatHistoryDao = {
      val ds = Dataset(
        uuid  = randomUuid,
        alias = "Dataset",
      )
      val users        = (1 to 2) map (createUser(ds.uuid, _))
      val chats        = chatsProducer(ds, users)
      val dataPathRoot = makeTempDir("eager")
      new EagerChatHistoryDao(
        name               = "Dao",
        _dataRootFile      = dataPathRoot,
        dataset            = ds,
        myself1            = users.head,
        users1             = users,
        _chatsWithMessages = ListMap(chats.map(c => (c, IndexedSeq.empty)): _*)
      ) with EagerMutableDaoTrait
    }

    val mDao = createMultiChatDao(
      (ds, us) => for (i <- 1 to 5 if i != 4) yield createGroupChat(ds.uuid, i, i.toString, us.map(_.id), 2))
    val (mDs, _, _, _, _) = getSimpleDaoEntities(mDao)
    val sDao = createMultiChatDao(
      (ds, us) => for (i <- 2 to 6 by 2) yield createGroupChat(ds.uuid, i, i.toString, us.map(_.id), 0))
    val (sDs, _, _, _, _) = getSimpleDaoEntities(sDao)

    Swing.onEDTWait {
      val dialog = new SelectMergeChatsDialog(mDao, mDs, sDao, sDs)
      dialog.visible = true
      println(dialog.selection map (_.mkString("\n  ", "\n  ", "\n")))
    }
  }
}
