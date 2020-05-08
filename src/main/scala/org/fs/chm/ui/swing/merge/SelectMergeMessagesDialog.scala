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

  private lazy val MaxMessageWidth = 500

  private lazy val originalTitle = "Select messages to merge"

  {
    title = originalTitle
  }

  private lazy val models = new Models

  private lazy val table = new SelectMergesTable[RenderableMismatch, (Mismatch, MismatchResolution)](
    models,
    (() => title = s"$originalTitle (${models.currentlySelectedCount} of ${models.totalSelectableCount})")
  )

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
        def abbreviateMessages(msgs: IndexedSeq[Message]): Seq[Either[Int, Message]] = {
          if (msgs.length <= MaxContinuousMsgsLength) {
            msgs map Right.apply
          } else {
            val l = MaxCutoffMsgsPartLength
            val between = msgs.length - (l * 2)
            ((msgs.take(l) map Right.apply) ++ Seq(Left(between)) ++ (msgs.takeRight(l) map Right.apply))
          }
        }

        val slaveMessages = slaveDao.messagesBetween(slaveChat, mismatch.firstSlaveMsg, mismatch.lastSlaveMsg)
        mismatch match {
          case mismatch: Mismatch.Addition =>
            RowData.InSlaveOnly(RenderableMismatch(Some(mismatch), abbreviateMessages(slaveMessages), slaveCwd))
          case mismatch: Mismatch.Conflict =>
            val masterMessages = masterDao.messagesBetween(masterChat, mismatch.firstMasterMsg, mismatch.lastMasterMsg)
            RowData.InBoth(
              masterValue = RenderableMismatch(Some(mismatch), masterMessages map Right.apply, masterCwd),
              slaveValue  = RenderableMismatch(Some(mismatch), slaveMessages map Right.apply, slaveCwd)
            )
        }
      }

      val masterCxtFetcher = new ContextFetcher(masterDao, masterChat)
      val slaveCxtFetcher = new ContextFetcher(slaveDao, slaveChat)

      val acc = ArrayBuffer.empty[RowData[RenderableMismatch]]

      val masterCxtBefore = masterCxtFetcher(None, mismatches.head.prevMasterMsgOption)
      val slaveCxtBefore = slaveCxtFetcher(None, mismatches.head.prevSlaveMsgOption)
      cxtToRowDataOption(masterCxtBefore, slaveCxtBefore) foreach (acc += _)

      if (mismatches.size >= 2) {
        // Display message with its following context
        mismatches.sliding(2).foreach {
          case Seq(currMismatch, nextMismatch) =>
            assert(currMismatch.nextSlaveMsgOption.isDefined)

            val current = messageToRowData(currMismatch)
            acc += current

            val masterCxtAfter =
              if (// Addition immediately followed by conflict
                  currMismatch.prevMasterMsgOption == nextMismatch.prevMasterMsgOption ||
                  // Conflict immediately followed by addition
                  currMismatch.nextMasterMsgOption == nextMismatch.nextMasterMsgOption) {
                CxtFetchResult.Continuous(Seq.empty)
              } else {
                masterCxtFetcher(
                  currMismatch.nextMasterMsgOption,
                  nextMismatch.prevMasterMsgOption
                )
              }

            val slaveCxtAfter =
              if (// One block immediately followed by the other
                  currMismatch.nextSlaveMsgOption contains nextMismatch.firstSlaveMsg) {
                CxtFetchResult.Continuous(Seq.empty)
              } else {
                slaveCxtFetcher(
                  currMismatch.nextSlaveMsgOption,
                  nextMismatch.prevSlaveMsgOption
                )
              }

            cxtToRowDataOption(masterCxtAfter, slaveCxtAfter) foreach (acc += _)
        }
      }

      // Last element (might also be the only element)
      val current = messageToRowData(mismatches.last)
      acc += current

      val masterCxtAfter = masterCxtFetcher(mismatches.last.nextMasterMsgOption, None)
      val slaveCxtAfter = slaveCxtFetcher(mismatches.last.nextSlaveMsgOption, None)
      cxtToRowDataOption(masterCxtAfter, slaveCxtAfter) foreach (acc += _)

      acc.toIndexedSeq
    }

    override val cellsAreInteractive = true

    override val renderer = (renderable: ListItemRenderable[RenderableMismatch]) => {
      // FIXME: Figure out what to do with a shitty layout!
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

//      val prefSize = msgAreaContainer.textPane.preferredSize
//      rootView.setSize(prefSize.width, prefSize.height)
//      val height = view.getPreferredSpan(1).round
      // = height

//      msgAreaContainer.textPane.preferredHeight = 1639

      // For some reason, maximumWidth is ignored
      msgAreaContainer.textPane.preferredWidth = MaxMessageWidth

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
        firstOption: Option[FirstMsg],
        lastOption: Option[LastMsg]
    ): CxtFetchResult = {
      if (firstOption.isEmpty && lastOption.isEmpty) {
        CxtFetchResult.Continuous(Seq.empty)
      } else {
        val fetch1 = fetchMsgsAfterInc(firstOption, MaxContinuousMsgsLength)

        if (fetch1.isEmpty) {
          CxtFetchResult.Continuous(Seq.empty)
        } else if (lastOption.isDefined && (fetch1 contains lastOption.get)) {
          // Continuous sequence
          CxtFetchResult.Continuous(fetch1 dropRightWhile (_ != lastOption.get))
        } else {
          val subfetch1 = fetch1.take(MaxCutoffMsgsPartLength)
          val subfetch1Set = subfetch1.toSet
          val fetch2 = fetchMsgsBeforeInc(lastOption, MaxCutoffMsgsPartLength).dropWhile { m =>
            (subfetch1Set contains m) || m.time < subfetch1.last.time
          }

          if (fetch2.isEmpty) {
            assert(lastOption.isEmpty)
            CxtFetchResult.Continuous(fetch1)
          } else {
            val nBetween = dao.countMessagesBetween(chat, subfetch1.last, fetch2.head)
            CxtFetchResult.Discrete(subfetch1, nBetween, fetch2)
          }
        }
      }
    }

    private def fetchMsgsAfterInc(firstOption: Option[FirstMsg], howMany: Int): Seq[Message] = {
      firstOption map { first =>
        dao.messagesAfter(chat, first, howMany)
      } getOrElse {
        dao.firstMessages(chat, howMany)
      }
    }

    private def fetchMsgsBeforeInc(lastOption: Option[LastMsg], howMany: Int): Seq[Message] = {
      lastOption map { last =>
        dao.messagesBefore(chat, last, howMany)
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
    val msgs = (0 to 1000) map (id => {
      val msg = createRegularMessage(id, (id % numUsers) + 1)
      if (id == 1) {
        val longText = (
          Seq.fill(100)("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").mkString(" ") + " " + Seq.fill(100)("abcdefg").mkString
        )
        msg.asInstanceOf[Message.Regular].copy(textOption = Some(RichText(Seq(RichText.Plain(longText)))))
      } else {
        msg
      }
    })

    val (mMsgs, sMsgs) = {
      // Addtion
      val mMsgs = msgs filter (Seq(1) contains _.sourceIdOption.get)
      val sMsgs = msgs filter ((2 to 100) contains _.sourceIdOption.get)

//      // Addtion before first
//      val mMsgs = msgs filter (Seq(4, 5) contains _.sourceIdOption.get)
//      val sMsgs = msgs filter (Seq(1, 2, 3, 4, 5) contains _.sourceIdOption.get)

//      // Conflicts
//      val mMsgs = msgs filter (Seq(1, 2, 3, 4, 5) contains _.sourceIdOption.get)
//      val sMsgs = msgs filter (Seq(1, 2, 3, 4, 5) contains _.sourceIdOption.get)

//      // Addition + conflict + addition, has before and after
//      val mMsgs = msgs filter (Seq(1, 3, 5) contains _.sourceIdOption.get)
//      val sMsgs = msgs filter (Seq(1, 2, 3, 4, 5) contains _.sourceIdOption.get)

//      // Addition + conflict + addition, no before and after
//      val mMsgs = msgs filter (Seq(3) contains _.sourceIdOption.get)
//      val sMsgs = msgs filter (Seq(2, 3, 4) contains _.sourceIdOption.get)

//      // Master had no messages
//      val mMsgs = msgs filter (Seq() contains _.sourceIdOption.get)
//      val sMsgs = msgs filter (Seq(1, 2, 3, 4, 5) contains _.sourceIdOption.get)

      (mMsgs, sMsgs)
    }

    val mDao = createSimpleDao("Master", mMsgs, numUsers)
    val (_, _, mChat, mMsgsI) = getSimpleDaoEntities(mDao)
    val sDao = createSimpleDao("Slave", sMsgs, numUsers)
    val (_, _, sChat, sMsgsI) = getSimpleDaoEntities(sDao)

    val mismatches = IndexedSeq(
      // Addtion
      Mismatch.Addition(
        prevMasterMsgOption = Some(mMsgsI.bySrcId(1)),
        nextMasterMsgOption = None,
        prevSlaveMsgOption  = None,
        slaveMsgs           = (sMsgsI.bySrcId(2), sMsgsI.bySrcId(100)),
        nextSlaveMsgOption  = None
      )

//      // Addtion before first
//      Mismatch.Addition(
//        prevMasterMsgOption = None,
//        nextMasterMsgOption = Some(mMsgsI.bySrcId(4)),
//        prevSlaveMsgOption  = None,
//        slaveMsgs           = (sMsgsI.bySrcId(1), sMsgsI.bySrcId(3)),
//        nextSlaveMsgOption  = Some(sMsgsI.bySrcId(4))
//      )

//      // Conflicts
//      Mismatch.Conflict(
//        prevMasterMsgOption = Some(mMsgsI.bySrcId(1)),
//        masterMsgs          = (mMsgsI.bySrcId(2), mMsgsI.bySrcId(2)),
//        nextMasterMsgOption = Some(mMsgsI.bySrcId(3)),
//        prevSlaveMsgOption  = Some(sMsgsI.bySrcId(1)),
//        slaveMsgs           = (sMsgsI.bySrcId(2), sMsgsI.bySrcId(2)),
//        nextSlaveMsgOption  = Some(sMsgsI.bySrcId(3))
//      ),
//      Mismatch.Conflict(
//        prevMasterMsgOption = Some(mMsgsI.bySrcId(3)),
//        masterMsgs          = (mMsgsI.bySrcId(4), mMsgsI.bySrcId(5)),
//        nextMasterMsgOption = None,
//        prevSlaveMsgOption  = Some(sMsgsI.bySrcId(3)),
//        slaveMsgs           = (sMsgsI.bySrcId(4), sMsgsI.bySrcId(5)),
//        nextSlaveMsgOption  = None
//      )

//      // Addition + conflict + addition, has before and after
//      Mismatch.Addition(
//        prevMasterMsgOption = Some(mMsgsI.bySrcId(1)),
//        nextMasterMsgOption = Some(mMsgsI.bySrcId(3)),
//        prevSlaveMsgOption  = Some(sMsgsI.bySrcId(1)),
//        slaveMsgs           = (sMsgsI.bySrcId(2), sMsgsI.bySrcId(2)),
//        nextSlaveMsgOption  = Some(sMsgsI.bySrcId(3))
//      ),
//      Mismatch.Conflict(
//        prevMasterMsgOption = Some(mMsgsI.bySrcId(1)),
//        masterMsgs          = (mMsgsI.bySrcId(3), mMsgsI.bySrcId(3)),
//        nextMasterMsgOption = Some(mMsgsI.bySrcId(5)),
//        prevSlaveMsgOption  = Some(sMsgsI.bySrcId(2)),
//        slaveMsgs           = (sMsgsI.bySrcId(3), sMsgsI.bySrcId(3)),
//        nextSlaveMsgOption  = Some(sMsgsI.bySrcId(4))
//      ),
//      Mismatch.Addition(
//        prevMasterMsgOption = Some(mMsgsI.bySrcId(3)),
//        nextMasterMsgOption = Some(mMsgsI.bySrcId(5)),
//        prevSlaveMsgOption  = Some(sMsgsI.bySrcId(3)),
//        slaveMsgs           = (sMsgsI.bySrcId(4), sMsgsI.bySrcId(4)),
//        nextSlaveMsgOption  = Some(sMsgsI.bySrcId(5))
//      )

//      // Addition + conflict + addition, no before and after
//      Mismatch.Addition(
//        prevMasterMsgOption = None,
//        nextMasterMsgOption = Some(mMsgsI.bySrcId(3)),
//        prevSlaveMsgOption  = None,
//        slaveMsgs           = (sMsgsI.bySrcId(2), sMsgsI.bySrcId(2)),
//        nextSlaveMsgOption  = Some(sMsgsI.bySrcId(3))
//      ),
//      Mismatch.Conflict(
//        prevMasterMsgOption = None,
//        masterMsgs          = (mMsgsI.bySrcId(3), mMsgsI.bySrcId(3)),
//        nextMasterMsgOption = None,
//        prevSlaveMsgOption  = Some(sMsgsI.bySrcId(2)),
//        slaveMsgs           = (sMsgsI.bySrcId(3), sMsgsI.bySrcId(3)),
//        nextSlaveMsgOption  = Some(sMsgsI.bySrcId(4))
//      ),
//      Mismatch.Addition(
//        prevMasterMsgOption = Some(mMsgsI.bySrcId(3)),
//        nextMasterMsgOption = None,
//        prevSlaveMsgOption  = Some(sMsgsI.bySrcId(3)),
//        slaveMsgs           = (sMsgsI.bySrcId(4), sMsgsI.bySrcId(4)),
//        nextSlaveMsgOption  = None
//      )

//      // Master had no messages
//      Mismatch.Addition(
//        prevMasterMsgOption = None,
//        nextMasterMsgOption = None,
//        prevSlaveMsgOption  = None,
//        slaveMsgs           = (sMsgsI.bySrcId(1), sMsgsI.bySrcId(5)),
//        nextSlaveMsgOption  = None
//      )
    )

    val dialog = new SelectMergeMessagesDialog(mDao, mChat, sDao, sChat, mismatches, htmlKit, msgService)
    dialog.visible = true
    dialog.peer.setLocationRelativeTo(null)
    println(dialog.selection)
  }
}
