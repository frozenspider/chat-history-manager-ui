package org.fs.chm.ui.swing.merge

import java.awt.Color

import scala.swing._

import org.fs.chm.dao._
import org.fs.chm.dao.merge.ChatHistoryMerger._
import org.fs.chm.ui.swing.general.ChatWithDao
import org.fs.chm.ui.swing.general.CustomDialog
import org.fs.chm.ui.swing.general.SwingUtils._
import org.fs.chm.ui.swing.list.chat.ChatListItem
import org.fs.chm.utility.EntityUtils._
import org.fs.utility.Imports._

class SelectMergeChatsDialog(
    masterDao: H2ChatHistoryDao,
    masterDs: Dataset,
    slaveDao: ChatHistoryDao,
    slaveDs: Dataset,
) extends CustomDialog[Seq[ChangedChatMergeOption]] {
  {
    title = "Select chats to merge"
  }

  private lazy val table = new SelectMergesTable[ChatWithDao, ChangedChatMergeOption](
    new Models(masterDao.chats(masterDs.uuid), slaveDao.chats(slaveDs.uuid))
  )

  override protected lazy val dialogComponent: Component = {
    table.wrapInScrollpane()
  }

  override protected def validateChoices(): Option[Seq[ChangedChatMergeOption]] = {
    Some(table.selected)
  }

  import SelectMergesTable._

  private class Models(masterChats: Seq[Chat], slaveChats: Seq[Chat])
      extends MergeModels[ChatWithDao, ChangedChatMergeOption] {

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

    override val renderer = (renderable: ChatRenderable[ChatWithDao]) => {
      val r = new ChatListItem(renderable.v, None, None)
      if (renderable.isCombine) {
        r.inactiveColor = Color.YELLOW
      } else if (renderable.isAdd) {
        r.inactiveColor = Color.GREEN
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
    ): Option[ChangedChatMergeOption] = {
      rd match {
        case _ if !isSelected                                       => None
        case RowData.InBoth(ChatWithDao(mc, _), ChatWithDao(sc, _)) => Some(ChatMergeOption.Combine(mc, sc))
        case RowData.InSlaveOnly(ChatWithDao(sc, _))                => Some(ChatMergeOption.Add(sc))
        case RowData.InMasterOnly(_)                                => None
      }
    }
  }
}
