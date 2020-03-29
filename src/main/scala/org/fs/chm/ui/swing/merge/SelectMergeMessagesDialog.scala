package org.fs.chm.ui.swing.merge

import java.awt.Color

import scala.collection.mutable.ArrayBuffer
import scala.swing._

import javax.swing.text.html.HTMLEditorKit
import org.fs.chm.dao._
//import org.fs.chm.dao.merge.ChatHistoryMerger.Mismatch
//import org.fs.chm.dao.merge.ChatHistoryMerger.MismatchResolution
import org.fs.chm.ui.swing.MessagesAreaContainer
import org.fs.chm.ui.swing.MessagesService
import org.fs.chm.ui.swing.MessagesService.MessageInsertPosition
import org.fs.chm.ui.swing.general.ChatWithDao
import org.fs.chm.ui.swing.general.CustomDialog
import org.fs.chm.ui.swing.general.SwingUtils._
import org.fs.chm.utility.EntityUtils._
import org.fs.utility.Imports._

/*
class SelectMergeMessagesDialog(
    masterDao: ChatHistoryDao,
    masterChat: Chat,
    slaveDao: ChatHistoryDao,
    slaveChat: Chat,
    mismatches: IndexedSeq[Mismatch],
    htmlKit: HTMLEditorKit,
    msgService: MessagesService
) extends CustomDialog[Map[Mismatch, MismatchResolution]] {
  import SelectMergeMessagesDialog._

  {
    title = "Select messages to merge"
  }

  private lazy val table = new SelectMergesTable[RenderableMismatch, (Mismatch, MismatchResolution)](new Models)

  override protected lazy val dialogComponent: Component = {
    table.wrapInScrollpaneAndAdjustWidth()
  }

  override protected def validateChoices(): Option[Map[Mismatch, MismatchResolution]] = {
    Some(table.selected.toMap)
  }

  import SelectMergesTable._

  private class Models extends MergeModels[RenderableMismatch, (Mismatch, MismatchResolution)] {
    override val allElems: Seq[RowData[RenderableMismatch]] = {
      require(mismatches.nonEmpty)
      val masterCwd = ChatWithDao(masterChat, masterDao)
      val slaveCwd  = ChatWithDao(slaveChat, slaveDao)

      def cxtToRowDataOption(masterFetchResult: CxtFetchResult,
                             slaveFetchResult: CxtFetchResult): Option[RowData[RenderableMismatch]] = {
        def cxtToRaw(fetchResult: CxtFetchResult) = fetchResult match {
          case CxtFetchResult.Discrete(msf, n, msl) =>
            (msf map Right.apply) ++ Seq(Left(n)) ++ (msl map Right.apply)
          case CxtFetchResult.Continuous(ms) =>
            ms map Right.apply
        }
        val masterRaw = cxtToRaw(masterFetchResult)
        val slaveRaw  = cxtToRaw(slaveFetchResult)
        if (masterRaw.isEmpty && slaveRaw.isEmpty) {
          None
        } else {
          Some(
            RowData.InBoth(
              masterValue = RenderableMismatch(None, masterRaw, masterCwd),
              slaveValue  = RenderableMismatch(None, slaveRaw, slaveCwd)
            )
          )
        }
      }

      def messageToRowData(mismatch: Mismatch): RowData[RenderableMismatch] = {
        /*
        val slaveMessages = slaveDao
          .messagesBetween(slaveChat, mismatch.firstSlaveMsgId, mismatch.lastSlaveMsgId) map Right.apply
        mismatch match {
          case Mismatch.Addition(prevMasterMsgId, _) =>
            RowData.InSlaveOnly(RenderableMismatch(Some(mismatch), slaveMessages, slaveCwd))
          case mismatch: Mismatch.Conflict =>
            val masterMessages = masterDao
              .messagesBetween(masterChat, mismatch.firstMasterMsgId, mismatch.lastMasterMsgId) map Right.apply
            RowData.InBoth(
              masterValue = RenderableMismatch(Some(mismatch), masterMessages, masterCwd),
              slaveValue  = RenderableMismatch(Some(mismatch), slaveMessages, slaveCwd)
            )
        }
         */
        ???
      }

      def messageIdToOption(id: Message.DaoId) = if (id == -1) None else Some(id)

      val masterCxtFetcher = new ContextFetcher(masterDao, masterChat)
      val slaveCxtFetcher  = new ContextFetcher(slaveDao, slaveChat)

      val acc = ArrayBuffer.empty[RowData[RenderableMismatch]]

      val masterCxtBefore = masterCxtFetcher(None, messageIdToOption(mismatches.head.firstMasterMsgId))
      val slaveCxtBefore  = slaveCxtFetcher(None, messageIdToOption(mismatches.head.firstSlaveMsgId))
      cxtToRowDataOption(masterCxtBefore, slaveCxtBefore) foreach (acc += _)

      if (mismatches.size >= 2) {
        // Display message with its following context
        mismatches.sliding(2).foreach {
          case Seq(currMismatch, nextMismatch) =>
            val current = messageToRowData(currMismatch)
            acc += current

            val masterCxtAfter = masterCxtFetcher(
              messageIdToOption(currMismatch.lastMasterMsgId),
              messageIdToOption(nextMismatch.firstMasterMsgId))
            val slaveCxtAfter = masterCxtFetcher(
              messageIdToOption(currMismatch.lastSlaveMsgId),
              messageIdToOption(nextMismatch.firstSlaveMsgId))
            cxtToRowDataOption(masterCxtAfter, slaveCxtAfter) foreach (acc += _)
        }
      }

      // Last element (might also be the only element)
      val current = messageToRowData(mismatches.last)
      acc += current

      val masterCxtAfter = masterCxtFetcher(messageIdToOption(mismatches.last.lastMasterMsgId), None)
      val slaveCxtAfter  = masterCxtFetcher(messageIdToOption(mismatches.last.lastSlaveMsgId), None)
      cxtToRowDataOption(masterCxtAfter, slaveCxtAfter) foreach (acc += _)

      acc.toIndexedSeq
    }

    override val renderer = (renderable: ChatRenderable[RenderableMismatch]) => {
      val msgAreaContainer = new MessagesAreaContainer(htmlKit)
      val msgDoc           = msgService.createStubDoc
      if (renderable.isSelectable) {
        val color = if (renderable.isAdd) Colors.AdditionBg else Colors.CombineBg
        msgAreaContainer.textPane.background = color
      }
      for (either <- renderable.v.messageOptions) {
        val rendered = either match {
          case Right(msg) => msgService.renderMessageHtml(renderable.v.cwd, msg)
          case Left(num)  => s"<hr>${num} messages<hr><p>"
        }
        msgDoc.insert(rendered, MessageInsertPosition.Trailing)
      }
      msgAreaContainer.document = msgDoc.doc
      msgAreaContainer.scrollPane
    }

    override protected def isInBothSelectable(mv: RenderableMismatch, sv: RenderableMismatch): Boolean = !mv.isContext
    override protected def isInSlaveSelectable(sv: RenderableMismatch):                        Boolean = true
    override protected def isInMasterSelectable(mv: RenderableMismatch):                       Boolean = false

    override protected def rowDataToResultOption(
        rd: RowData[RenderableMismatch],
        isSelected: Boolean
    ): Option[(Mismatch, MismatchResolution)] = {
      val res = if (isSelected) MismatchResolution.Apply else MismatchResolution.Reject
      rd match {
        case RowData.InBoth(mmd, _)   => mmd.mismatchOption map (_ -> res)
        case RowData.InSlaveOnly(smd) => smd.mismatchOption map (_ -> res)
        case RowData.InMasterOnly(_)  => None
      }
    }
  }

  private case class RenderableMismatch(
      /** Mismatch to be rendered, None means this only a context */
      mismatchOption: Option[Mismatch],
      /** Messages to be rendered, or number of messages abbreviated out */
      messageOptions: Seq[Either[Int, Message[_]]],
      cwd: ChatWithDao
  ) {
    def isContext = mismatchOption.isEmpty
  }
}

private object SelectMergeMessagesDialog {
  private val MaxContinuousMsgsLength = 20
  private val MaxCutoffMsgsPartLength = 7

  class ContextFetcher(dao: ChatHistoryDao, chat: Chat) {
    private type FirstId = Message.DaoId
    private type LastId  = Message.DaoId

    // We don't necessarily need a lock, but it's still nice to avoid double-fetches
    val cacheLock = new Object

    def apply(
        lastIdBeforeOption: Option[FirstId],
        firstIdAfterOption: Option[LastId]
    ): CxtFetchResult = {
      val lastIdBeforeVal = lastIdBeforeOption getOrElse Long.MinValue
      val firstIdAfterVal = firstIdAfterOption getOrElse Long.MaxValue

      val fetch1 = (lastIdBeforeOption map { lastIdBefore =>
        dao.messagesAfter(chat, lastIdBefore, MaxContinuousMsgsLength + 2) dropWhile (_.sourceIdOption contains lastIdBefore)
      } getOrElse {
        dao.firstMessages(chat, MaxContinuousMsgsLength + 1)
      }) take MaxContinuousMsgsLength

      ??? // FIXME
//      if (fetch1.isEmpty) {
//        CxtFetchResult.Continuous(Seq.empty)
//      } else if (firstIdAfterOption.isDefined && (fetch1 exists (_.idOption exists (_ >= firstIdAfterVal)))) {
//        // Continuous sequence
//        CxtFetchResult.Continuous(fetch1 takeWhile (m => m.idOption.isDefined && m.idOption.get < firstIdAfterVal))
//      } else {
//        val fetch1Ids = fetch1.map(_.id).toSet
//        val fetch2 = (firstIdAfterOption map { firstIdAfter =>
//          dao.messagesBefore(chat, firstIdAfter, MaxCutoffMsgsPartLength + 1)
//        } getOrElse {
//          dao.lastMessages(chat, MaxCutoffMsgsPartLength)
//        }) dropWhile (m => (m.id <= lastIdBeforeVal) || (fetch1Ids contains m.id)) takeRight MaxCutoffMsgsPartLength
//
//        if (fetch2.isEmpty) {
//          CxtFetchResult.Continuous(fetch1)
//        } else {
//          val subfetch1 = fetch1.take(MaxCutoffMsgsPartLength)
//          val nBetween  = dao.countMessagesBetween(chat, subfetch1.last.id, fetch2.head.id)
//          CxtFetchResult.Discrete(subfetch1, nBetween, fetch2)
//        }
//      }
      ???
    }
  }

  sealed trait CxtFetchResult
  object CxtFetchResult {
    case class Discrete(firstMsgs: Seq[Message], between: Int, lastMsgs: Seq[Message]) extends CxtFetchResult
    case class Continuous(msgs: Seq[Message])                                          extends CxtFetchResult
  }

  def main(args: Array[String]): Unit = {
    import java.awt.Desktop
    import org.fs.chm.ui.swing.general.ExtendedHtmlEditorKit
    import org.fs.chm.utility.TestUtils._

    val desktopOption = if (Desktop.isDesktopSupported) Some(Desktop.getDesktop) else None
    val htmlKit       = new ExtendedHtmlEditorKit(desktopOption)
    val msgService    = new MessagesService(htmlKit)

    val numUsers = 3
    val msgs     = (1 to 9) map (id => createRegularMessage(id, (id % numUsers) + 1))

    val (mMsgs, sMsgs, mismatches) = {
      // Addtion before first
//      val mMsgs = msgs filter (Seq(4, 5) contains _.sourceIdOption.get)
//      val sMsgs = msgs filter (Seq(1, 2, 3, 4, 5) contains _.sourceIdOption.get)
//      val mismatches = IndexedSeq(
//        Mismatch.Addition(prevMasterMsgId = -1, slaveMsgIds = (1, 3))
//      )

      // Conflicts
      val mMsgs = msgs filter (Seq(1, 2, 3, 4, 5) contains _.sourceIdOption.get)
      val sMsgs = msgs filter (Seq(1, 2, 3, 4, 5) contains _.sourceIdOption.get)
      val mismatches = IndexedSeq(
        Mismatch.Conflict(masterMsgIds = (2, 3), slaveMsgIds = (2, 3)),
        Mismatch.Conflict(masterMsgIds = (4, 5), slaveMsgIds = (4, 5))
      )

      (mMsgs, sMsgs, mismatches)
    }

    val mDao          = createSimpleDao("Master", mMsgs, numUsers)
    val (_, _, mChat) = getSimpleDaoEntities(mDao)
    val sDao          = createSimpleDao("Slave", sMsgs, numUsers)
    val (_, _, sChat) = getSimpleDaoEntities(sDao)

    val dialog = new SelectMergeMessagesDialog(mDao, mChat, sDao, sChat, mismatches, htmlKit, msgService)
    dialog.visible = true
    dialog.peer.setLocationRelativeTo(null)
    println(dialog.selection)
  }
}
*/

class SelectMergeMessagesDialog()
