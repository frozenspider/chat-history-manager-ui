package org.fs.chm.ui.swing.merge

import scala.swing._

import org.fs.chm.dao.ChatHistoryDao
import org.fs.chm.dao.EagerChatHistoryDao
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
 * - `Combine` options would NOT have mismatches defined
 * - Master chats will precede slave-only chats
 * - Checkbox option will be present for all chats in slave DS - i.e. whether to add/combine them or not
 */
class SelectMergeChatsDialog(
    masterDao: MutableChatHistoryDao,
    masterDs: Dataset,
    slaveDao: ChatHistoryDao,
    slaveDs: Dataset,
) extends CustomDialog[Seq[ChatMergeOption]](takeFullHeight = true) {
  private lazy val originalTitle = "Select chats to merge"

  {
    title = originalTitle
  }

  private lazy val models = new Models(masterDao.chats(masterDs.uuid), slaveDao.chats(slaveDs.uuid))

  private lazy val table = {
    checkEdt()
    new SelectMergesTable[(ChatHistoryDao, ChatWithDetails), ChatMergeOption](models)
  }

  override protected lazy val dialogComponent: Component = {
    table.wrapInScrollpaneAndAdjustWidth()
  }

  override protected def validateChoices(): Option[Seq[ChatMergeOption]] = {
    Some(table.selected)
  }

  import org.fs.chm.ui.swing.merge.SelectMergesTable._

  private class Models(masterChats: Seq[ChatWithDetails], slaveChats: Seq[ChatWithDetails])
      extends MergeModels[(ChatHistoryDao, ChatWithDetails), ChatMergeOption] {

    override val allElems: Seq[RowData[(ChatHistoryDao, ChatWithDetails)]] = {
      val masterChatsMap = groupById(masterChats)

      val merges: Seq[RowData[(ChatHistoryDao, ChatWithDetails)]] =
        for (sc <- slaveChats) yield {
          masterChatsMap.get(sc.chat.id) match {
            case None     => RowData.InSlaveOnly(slaveDao -> sc)
            case Some(mc) => RowData.InBoth(masterDao -> mc, slaveDao -> sc)
          }
        }

      var mergesAcc: Seq[RowData[(ChatHistoryDao, ChatWithDetails)]] = Seq.empty

      // 1) Combined and unchanged chats
      val combinesMasterToDataMap: Map[Chat, RowData.InBoth[(ChatHistoryDao, ChatWithDetails)]] =
        merges.collect { case rd @ RowData.InBoth((md, mc), (sd, sc)) => (mc.chat, rd) }.toMap
      for (mc <- masterChats) {
        combinesMasterToDataMap.get(mc.chat) match {
          case Some(rd) => mergesAcc = mergesAcc :+ rd
          case None     => mergesAcc = mergesAcc :+ RowData.InMasterOnly((masterDao: ChatHistoryDao) -> mc)
        }
      }

      // 2) Added chats
      val additionsSlaveToDataMap: Map[Chat, RowData.InSlaveOnly[(ChatHistoryDao, ChatWithDetails)]] =
        merges.collect { case rd @ RowData.InSlaveOnly((sd, sc)) => (sc.chat, rd) }.toMap
      for (sc <- slaveChats if additionsSlaveToDataMap.contains(sc.chat)) {
        mergesAcc = mergesAcc :+ additionsSlaveToDataMap(sc.chat)
      }

      mergesAcc
    }

    override val cellsAreInteractive = false

    override val renderer = (renderable: ListItemRenderable[(ChatHistoryDao, ChatWithDetails)]) => {
      val r = new ChatListItem(renderable.v._1, renderable.v._2, None, None)
      if (renderable.isCombine) {
        r.inactiveColor = Colors.CombineBg
      } else if (renderable.isAdd) {
        r.inactiveColor = Colors.AdditionBg
      }
      r.markDeselected()
      r
    }

    override protected def isInBothSelectable(mv: (ChatHistoryDao, ChatWithDetails),
                                              sv: (ChatHistoryDao, ChatWithDetails)): Boolean =
      true

    override protected def isInSlaveSelectable(sv: (ChatHistoryDao, ChatWithDetails)): Boolean =
      true

    override protected def isInMasterSelectable(mv: (ChatHistoryDao, ChatWithDetails)): Boolean =
      false

    override protected def rowDataToResultOption(
        rd: RowData[(ChatHistoryDao, ChatWithDetails)],
        isSelected: Boolean
    ): Option[ChatMergeOption] = {
      rd match {
        case RowData.InMasterOnly((_, mcwd)) =>
          Some(ChatMergeOption.Keep(mcwd))
        case RowData.InBoth((_, mcwd), (_, scwd)) =>
          Some(
            if (isSelected) {
              // Mismatches have to be analyzed by DatasetMerger
              ChatMergeOption.Combine(mcwd, scwd, IndexedSeq.empty)
            } else {
              ChatMergeOption.Keep(scwd)
            }
          )
        case _ if !isSelected =>
          None
        case RowData.InSlaveOnly((_, scwd)) =>
          Some(ChatMergeOption.Add(scwd))
      }
    }
  }
}

object SelectMergeChatsDialog {
  def main(args: Array[String]): Unit = {
    import java.nio.file.Files

    import scala.collection.immutable.ListMap

    import org.fs.chm.utility.TestUtils._

    def createMultiChatDao(chatsProducer: (Dataset, Seq[User]) => Seq[Chat]): MutableChatHistoryDao = {
      val ds = Dataset(
        uuid       = randomUuid,
        alias      = "Dataset",
        sourceType = "test source"
      )
      val users        = (1 to 2) map (createUser(ds.uuid, _))
      val chats        = chatsProducer(ds, users)
      val dataPathRoot = Files.createTempDirectory("java_chm-eager_").toFile
      dataPathRoot.deleteOnExit()
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
