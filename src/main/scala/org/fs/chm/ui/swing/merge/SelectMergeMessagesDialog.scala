package org.fs.chm.ui.swing.merge

import java.awt.Color

import scala.collection.mutable.ArrayBuffer
import scala.swing._

import com.github.nscala_time.time.Imports._
import javax.swing.text.html.HTMLEditorKit
import org.fs.chm.dao._
import org.fs.chm.dao.merge.ChatHistoryMerger.Mismatch
import org.fs.chm.dao.merge.ChatHistoryMerger.MismatchResolution
import org.fs.chm.ui.swing.MessagesAreaContainer
import org.fs.chm.ui.swing.MessagesService
import org.fs.chm.ui.swing.MessagesService.MessageInsertPosition
import org.fs.chm.ui.swing.general.ChatWithDao
import org.fs.chm.ui.swing.general.CustomDialog
import org.fs.chm.ui.swing.general.SwingUtils._
import org.fs.chm.utility.EntityUtils._
import org.fs.utility.Imports._

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
    table.wrapInScrollpane()
  }

  override protected def validateChoices(): Option[Map[Mismatch, MismatchResolution]] = {
    Some(table.selected.toMap)
  }

  import SelectMergesTable._

  private class Models extends MergeModels[RenderableMismatch, (Mismatch, MismatchResolution)] {
    override val allElems: Seq[RowData[RenderableMismatch]] = {
      require(mismatches.nonEmpty)
      val masterCwd = ChatWithDao(masterChat, masterDao)
      val slaveCwd = ChatWithDao(slaveChat, slaveDao)

      def cxtToRowDataOption(masterFetchResult: CxtFetchResult,
                             slaveFetchResult: CxtFetchResult): Option[RowData[RenderableMismatch]] = {
        def cxtToRaw(fetchResult: CxtFetchResult) = fetchResult match {
          case CxtFetchResult.Discrete(msf, n, msl) =>
            (msf map Right.apply) ++ Seq(Left(n)) ++ (msl map Right.apply)
          case CxtFetchResult.Continuous(ms) =>
            ms map Right.apply
        }
        val masterRaw = cxtToRaw(masterFetchResult)
        val slaveRaw = cxtToRaw(slaveFetchResult)
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
        val slaveMessages = slaveDao
          .messagesBetween(slaveChat, mismatch.firstSlaveMsg, mismatch.lastSlaveMsg) map Right.apply
        mismatch match {
          case Mismatch.Addition(_, _, _) =>
            RowData.InSlaveOnly(RenderableMismatch(Some(mismatch), slaveMessages, slaveCwd))
          case mismatch: Mismatch.Conflict =>
            val masterMessages = masterDao
              .messagesBetween(masterChat, mismatch.firstMasterMsg, mismatch.lastMasterMsg) map Right.apply
            RowData.InBoth(
              masterValue = RenderableMismatch(Some(mismatch), masterMessages, masterCwd),
              slaveValue  = RenderableMismatch(Some(mismatch), slaveMessages, slaveCwd)
            )
        }
      }

      val masterCxtFetcher = new ContextFetcher(masterDao, masterChat)
      val slaveCxtFetcher = new ContextFetcher(slaveDao, slaveChat)

      val acc = ArrayBuffer.empty[RowData[RenderableMismatch]]

      val masterCxtBefore = masterCxtFetcher(None, mismatches.head.firstMasterMsgOption)
      val slaveCxtBefore = slaveCxtFetcher(None, Some(mismatches.head.firstSlaveMsg))
      cxtToRowDataOption(masterCxtBefore, slaveCxtBefore) foreach (acc += _)

      if (mismatches.size >= 2) {
        // Display message with its following context
        mismatches.sliding(2).foreach {
          case Seq(currMismatch, nextMismatch) =>
            val current = messageToRowData(currMismatch)
            acc += current

            val masterCxtAfter = masterCxtFetcher(
              currMismatch.lastMasterMsgOption,
              nextMismatch.firstMasterMsgOption
            )
            val slaveCxtAfter = slaveCxtFetcher(
              Some(currMismatch.lastSlaveMsg),
              Some(nextMismatch.firstSlaveMsg)
            )
            cxtToRowDataOption(masterCxtAfter, slaveCxtAfter) foreach (acc += _)
        }
      }

      // Last element (might also be the only element)
      val current = messageToRowData(mismatches.last)
      acc += current

      val masterCxtAfter = masterCxtFetcher(mismatches.last.lastMasterMsgOption, None)
      val slaveCxtAfter = slaveCxtFetcher(Some(mismatches.last.lastSlaveMsg), None)
      cxtToRowDataOption(masterCxtAfter, slaveCxtAfter) foreach (acc += _)

      acc.toIndexedSeq
    }

    override val renderer = (renderable: ChatRenderable[RenderableMismatch]) => {
      val msgAreaContainer = new MessagesAreaContainer(htmlKit)
//      msgAreaContainer.textPane.peer.putClientProperty(javax.swing.JEditorPane.HONOR_DISPLAY_PROPERTIES, Boolean.box(true))
      val msgDoc = msgService.createStubDoc
//      msgDoc.doc.getStyleSheet.addRule("#messages { background-color: #FFE0E0; }")
      if (renderable.isSelectable) {
        val color = if (renderable.isAdd) Colors.AdditionBg else Colors.CombineBg
        msgAreaContainer.textPane.background = color
      }
      val allRendered = for (either <- renderable.v.messageOptions) yield {
        val rendered = either match {
          case Right(msg) => msgService.renderMessageHtml(renderable.v.cwd, msg)
          case Left(num)  => s"<hr>${num} messages<hr><p>"
        }
        rendered
      }
      msgDoc.insert(allRendered.mkString.replaceAll("\n", ""), MessageInsertPosition.Trailing)
      msgAreaContainer.document = msgDoc.doc
      val ui = msgAreaContainer.textPane.peer.getUI
      val rootView = ui.getRootView(null)
      val view = rootView.getView(0)

      val prefSize = msgAreaContainer.textPane.preferredSize
      rootView.setSize(prefSize.width, prefSize.height)
//      val height = view.getPreferredSpan(1).round
      // = height

//      msgAreaContainer.textPane.preferredHeight = 1639
      msgAreaContainer.textPane
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
      messageOptions: Seq[Either[Int, Message]],
      cwd: ChatWithDao
  ) {
    def isContext = mismatchOption.isEmpty
  }
}

private object SelectMergeMessagesDialog {
  private val MaxContinuousMsgsLength = 20
  private val MaxCutoffMsgsPartLength = 7

  class ContextFetcher(dao: ChatHistoryDao, chat: Chat) {
    private type FirstMsg = Message
    private type LastMsg = Message

    // We don't necessarily need a lock, but it's still nice to avoid double-fetches
    val cacheLock = new Object

    def apply(
        lastBeforeOption: Option[FirstMsg],
        firstAfterOption: Option[LastMsg]
    ): CxtFetchResult = {
      if (lastBeforeOption.isEmpty && firstAfterOption.isEmpty) {
        CxtFetchResult.Continuous(Seq.empty)
      } else {
        val fetch1 = fetchMsgsAfterExc(lastBeforeOption, MaxContinuousMsgsLength)

        if (fetch1.isEmpty) {
          CxtFetchResult.Continuous(Seq.empty)
        } else if (firstAfterOption.isDefined && (fetch1 contains firstAfterOption.get)) {
          // Continuous sequence
          CxtFetchResult.Continuous(fetch1 takeWhile (_ != firstAfterOption.get))
        } else {
          val subfetch1 = fetch1.take(MaxCutoffMsgsPartLength)
          val subfetch1Set = subfetch1.toSet
          val fetch2 = fetchMsgsBeforeExc(firstAfterOption, MaxCutoffMsgsPartLength).dropWhile { m =>
            (subfetch1Set contains m) || m.time < subfetch1.last.time
          }

          if (fetch2.isEmpty) {
            CxtFetchResult.Continuous(fetch1)
          } else {
            val nBetween = dao.countMessagesBetween(chat, subfetch1.last, fetch2.head)
            CxtFetchResult.Discrete(subfetch1, nBetween, fetch2)
          }
        }
      }
    }

    private def fetchMsgsAfterExc(lastBeforeOption: Option[FirstMsg], howMany: Int): Seq[Message] = {
      lastBeforeOption map { lastBefore =>
        dao.messagesAfter(chat, lastBefore, howMany + 1).drop(1)
      } getOrElse {
        dao.firstMessages(chat, howMany)
      }
    }

    private def fetchMsgsBeforeExc(firstAfterOption: Option[LastMsg], howMany: Int): Seq[Message] = {
      firstAfterOption map { firstAfter =>
        dao.messagesBefore(chat, firstAfter, howMany + 1).dropRight(1)
      } getOrElse {
        dao.lastMessages(chat, howMany)
      }
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
    val htmlKit = new ExtendedHtmlEditorKit(desktopOption)
    val msgService = new MessagesService(htmlKit)

    val numUsers = 3
    val msgs = (0 to 9) map (id => createRegularMessage(id, (id % numUsers) + 1))

    val (mMsgs, sMsgs) = {
      // Addtion before first
      val mMsgs = msgs filter (Seq(4, 5) contains _.sourceIdOption.get)
      val sMsgs = msgs filter (Seq(1, 2, 3, 4, 5) contains _.sourceIdOption.get)

//      // Conflicts
//      val mMsgs = msgs filter (Seq(1, 2, 3, 4, 5) contains _.sourceIdOption.get)
//      val sMsgs = msgs filter (Seq(1, 2, 3, 4, 5) contains _.sourceIdOption.get)

//      // Master had no messages
//      val mMsgs = msgs filter (Seq() contains _.sourceIdOption.get)
//      val sMsgs = msgs filter (Seq(0, 1, 2, 3, 4, 5) contains _.sourceIdOption.get)

      (mMsgs, sMsgs)
    }

    val mDao = createSimpleDao("Master", mMsgs, numUsers)
    val (_, _, mChat, mMsgsI) = getSimpleDaoEntities(mDao)
    val sDao = createSimpleDao("Slave", sMsgs, numUsers)
    val (_, _, sChat, sMsgsI) = getSimpleDaoEntities(sDao)

    val mismatches = IndexedSeq(
      // Addtion before first
      Mismatch.Addition(
        prevMasterMsgOption = None,
        nextMasterMsgOption = Some(mMsgsI.bySrcId(4)),
        slaveMsgs           = (sMsgsI.bySrcId(1), sMsgsI.bySrcId(3))
      )

//      // Conflicts
//      Mismatch.Conflict(
//        masterMsgs = (mMsgsI.bySrcId(2), mMsgsI.bySrcId(3)),
//        slaveMsgs  = (sMsgsI.bySrcId(2), sMsgsI.bySrcId(3))
//      ),
//      Mismatch.Conflict(
//        masterMsgs = (mMsgsI.bySrcId(4), mMsgsI.bySrcId(5)),
//        slaveMsgs  = (sMsgsI.bySrcId(4), sMsgsI.bySrcId(5))
//      )

//      // Master had no messages
//      Mismatch.Addition(
//        prevMasterMsgOption = None,
//        nextMasterMsgOption = None,
//        slaveMsgs           = (sMsgsI.bySrcId(0), sMsgsI.bySrcId(5))
//      )
    )

    val dialog = new SelectMergeMessagesDialog(mDao, mChat, sDao, sChat, mismatches, htmlKit, msgService)
    dialog.visible = true
    dialog.peer.setLocationRelativeTo(null)
    println(dialog.selection)
  }
}
