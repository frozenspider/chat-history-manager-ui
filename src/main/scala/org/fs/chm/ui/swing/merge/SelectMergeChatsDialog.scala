package org.fs.chm.ui.swing.merge

import scala.swing._

import org.fs.chm.dao._
import org.fs.chm.dao.merge.DatasetMerger._
import org.fs.chm.ui.swing.general.ChatWithDao
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
) extends CustomDialog[Seq[ChatMergeOption]] {
  private lazy val originalTitle = "Select chats to merge"

  {
    title = originalTitle
  }

  private lazy val models = new Models(masterDao.chats(masterDs.uuid), slaveDao.chats(slaveDs.uuid))

  private lazy val table = {
    checkEdt()
    new SelectMergesTable[ChatWithDao, ChatMergeOption](models)
  }

  override protected lazy val dialogComponent: Component = {
    table.wrapInScrollpaneAndAdjustWidth()
  }

  override protected def validateChoices(): Option[Seq[ChatMergeOption]] = {
    Some(table.selected)
  }

  import org.fs.chm.ui.swing.merge.SelectMergesTable._

  private class Models(masterChats: Seq[Chat], slaveChats: Seq[Chat])
      extends MergeModels[ChatWithDao, ChatMergeOption] {

    def wrapMasterValue(mv: Chat): ChatWithDao = ChatWithDao(mv, masterDao)
    def wrapSlaveValue(sv: Chat):  ChatWithDao = ChatWithDao(sv, slaveDao)

    override val allElems: Seq[RowData[ChatWithDao]] = {
      val masterChatsMap = groupById(masterChats)

      val merges: Seq[RowData[ChatWithDao]] =
        for (sc <- slaveChats) yield {
          masterChatsMap.get(sc.id) match {
            case None     => RowData.InSlaveOnly(wrapSlaveValue(sc))
            case Some(mc) => RowData.InBoth(wrapMasterValue(mc), wrapSlaveValue(sc))
          }
        }

      var mergesAcc: Seq[RowData[ChatWithDao]] = Seq.empty

      // 1) Combined and unchanged chats
      val combinesMasterToDataMap: Map[Chat, RowData.InBoth[ChatWithDao]] =
        merges.collect { case rd @ RowData.InBoth(mc, sc) => (mc.chat, rd) }.toMap
      for (mc <- masterChats) {
        combinesMasterToDataMap.get(mc) match {
          case Some(rd) => mergesAcc = mergesAcc :+ rd
          case None     => mergesAcc = mergesAcc :+ RowData.InMasterOnly(wrapMasterValue(mc))
        }
      }

      // 2) Added chats
      val additionsSlaveToDataMap: Map[Chat, RowData.InSlaveOnly[ChatWithDao]] =
        merges.collect { case rd @ RowData.InSlaveOnly(sc) => (sc.chat, rd) }.toMap
      for (sc <- slaveChats if additionsSlaveToDataMap.contains(sc)) {
        mergesAcc = mergesAcc :+ additionsSlaveToDataMap(sc)
      }

      mergesAcc
    }

    override val cellsAreInteractive = false

    override val renderer = (renderable: ListItemRenderable[ChatWithDao]) => {
      val r = new ChatListItem(renderable.v, None, None)
      if (renderable.isCombine) {
        r.inactiveColor = Colors.CombineBg
      } else if (renderable.isAdd) {
        r.inactiveColor = Colors.AdditionBg
      }
      r.markDeselected()
      r
    }

    override protected def isInBothSelectable(mv: ChatWithDao, sv: ChatWithDao): Boolean = true
    override protected def isInSlaveSelectable(sv: ChatWithDao):                 Boolean = true
    override protected def isInMasterSelectable(mv: ChatWithDao):                Boolean = false

    override protected def rowDataToResultOption(
        rd: RowData[ChatWithDao],
        isSelected: Boolean
    ): Option[ChatMergeOption] = {
      rd match {
        case RowData.InMasterOnly(ChatWithDao(mc, _)) =>
          Some(ChatMergeOption.Keep(mc))
        case RowData.InBoth(ChatWithDao(mc, _), ChatWithDao(sc, _)) =>
          Some(
            if (isSelected) {
              // Mismatches have to be analyzed by DatasetMerger
              ChatMergeOption.Combine(mc, sc, IndexedSeq.empty)
            } else {
              ChatMergeOption.Keep(sc)
            }
          )
        case _ if !isSelected =>
          None
        case RowData.InSlaveOnly(ChatWithDao(sc, _)) =>
          Some(ChatMergeOption.Add(sc))
      }
    }
  }
}

object SelectMergeChatsDialog {
  def main(args: Array[String]): Unit = {
    import java.nio.file.Files
    import java.util.UUID

    import scala.collection.immutable.ListMap

    import org.fs.chm.utility.TestUtils._

    def createMultiChatDao(chatsProducer: Dataset => Seq[Chat]): MutableChatHistoryDao = {
      val ds = Dataset(
        uuid = UUID.randomUUID(),
        alias = "Dataset",
        sourceType = "test source"
      )
      val chats        = chatsProducer(ds)
      val user         = createUser(ds.uuid, 1)
      val dataPathRoot = Files.createTempDirectory(null).toFile
      dataPathRoot.deleteOnExit()
      new EagerChatHistoryDao(
        name = "Dao",
        _dataRootFile = dataPathRoot,
        dataset = ds,
        myself1 = user,
        users1 = Seq(user),
        _chatsWithMessages = ListMap(chats.map(c => (c, IndexedSeq.empty)): _*)
      ) with EagerMutableDaoTrait
    }

    val mDao           = createMultiChatDao(ds => for (i <- 1 to 5 if i != 4) yield createChat(ds.uuid, i, i.toString, 0))
    val (mDs, _, _, _) = getSimpleDaoEntities(mDao)
    val sDao           = createMultiChatDao(ds => for (i <- 2 to 6 by 2) yield createChat(ds.uuid, i, i.toString, 0))
    val (sDs, _, _, _) = getSimpleDaoEntities(sDao)

    Swing.onEDTWait {
      val dialog = new SelectMergeChatsDialog(mDao, mDs, sDao, sDs)
      dialog.visible = true
      dialog.peer.setLocationRelativeTo(null)
      println(dialog.selection map (_.mkString("\n  ", "\n  ", "\n")))
    }
  }
}
