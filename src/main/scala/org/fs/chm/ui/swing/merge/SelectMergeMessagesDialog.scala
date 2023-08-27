package org.fs.chm.ui.swing.merge

import scala.swing._

import com.github.nscala_time.time.Imports._
import javax.swing.text.html.HTMLEditorKit

import org.fs.chm.dao.ChatHistoryDao
import org.fs.chm.dao.Entities._
import org.fs.chm.dao.merge.DatasetMerger.MessagesMergeDecision
import org.fs.chm.dao.merge.DatasetMerger.MessagesMergeDiff
import org.fs.chm.protobuf.Chat
import org.fs.chm.protobuf.Message
import org.fs.chm.ui.swing.general.CustomDialog
import org.fs.chm.ui.swing.general.SwingUtils._
import org.fs.chm.ui.swing.messages.impl.MessagesAreaContainer
import org.fs.chm.ui.swing.messages.impl.MessagesDocumentService
import org.fs.chm.utility.EntityUtils._
import org.fs.utility.Imports._

import SelectMergeMessagesDialog._

/**
 * Show dialog for merging chat messages.
 * Unlike other merge dialogs, this one does not perform a mismatch analysis, and relies on the provided one instead.
 * Rules:
 * - Multiple `Match` diffs will be squished together to avoid cluttering
 * - Checkbox option will be present for all `Add`/`Replace` diffs
 * - `Add` mismatch that was unchecked will be removed from output
 * - `Replace` mismatch that was unchecked will be replaced by `Keep` mismatch
 * This means that master messages coverage should not change in the output
 */
class SelectMergeMessagesDialog(
    model: SelectMergeMessagesModel
) extends CustomDialog[IndexedSeq[MessagesMergeDecision]](takeFullHeight = true) {
  import SelectMergeMessagesDialog._

  {
    title = s"Select messages to merge (${model.name})"
  }

  private lazy val table = {
    checkEdt()
    new SelectMergesTable[RenderableDiff, MessagesMergeDecision](model)
  }

  override protected lazy val dialogComponent: Component = {
    table.wrapInScrollpaneAndAdjustWidth()
  }

  override protected def validateChoices(): Option[IndexedSeq[MessagesMergeDecision]] = {
    Some(table.selected.toIndexedSeq)
  }
}

object SelectMergeMessagesDialog {
  private val MaxContinuousMsgsLength = 20
  private val MaxCutoffMsgsPartLength = 7

  import SelectMergesTable._

  class SelectMergeMessagesModel(
    masterDao: ChatHistoryDao,
    masterCwd: ChatWithDetails,
    slaveDao: ChatHistoryDao,
    slaveCwd: ChatWithDetails,
    _diffs: IndexedSeq[MessagesMergeDecision], // Type is a hack!
    htmlKit: HTMLEditorKit
  ) extends MergeModels[RenderableDiff, MessagesMergeDecision] {
    // Values here are lazy because they are used from the parent init code.

    val name: String = masterCwd.chat.nameOrUnnamed

    private lazy val diffs = {
      require(_diffs.forall(_.isInstanceOf[MessagesMergeDiff]))
      _diffs.asInstanceOf[IndexedSeq[MessagesMergeDiff]]
    }

    private lazy val MaxMessageWidth = 500

    private lazy val masterRoot = masterDao.datasetRoot(masterCwd.dsUuid)
    private lazy val slaveRoot  = slaveDao.datasetRoot(slaveCwd.dsUuid)

    override val allElems: Seq[RowData[RenderableDiff]] = {
      require(diffs.nonEmpty)

      val masterCxtFetcher = new ContextFetcher(masterDao, masterCwd.chat)
      val slaveCxtFetcher  = new ContextFetcher(slaveDao, slaveCwd.chat)

      def cxtToRaw(fetchResult: CxtFetchResult): Seq[Either[Int, Message]] = fetchResult match {
        case CxtFetchResult.Discrete(msf, n, msl) =>
          (msf map Right.apply) ++ Seq(Left(n)) ++ (msl map Right.apply)
        case CxtFetchResult.Continuous(ms) =>
          ms map Right.apply
      }

      diffs map { diff =>
        val masterFetchResult = masterCxtFetcher(diff.firstMasterMsgOption, diff.lastMasterMsgOption)
        val slaveFetchResult  = slaveCxtFetcher(diff.firstSlaveMsgOption, diff.lastSlaveMsgOption)
        val masterValue = RenderableDiff(diff, cxtToRaw(masterFetchResult), masterDao, masterCwd, masterRoot)
        val slaveValue  = RenderableDiff(diff, cxtToRaw(slaveFetchResult),  slaveDao,  slaveCwd,  slaveRoot)
        diff match {
          case _: MessagesMergeDiff.Retain  => RowData.InMasterOnly(masterValue, selectable = false)
          case _: MessagesMergeDiff.Add     => RowData.InSlaveOnly(slaveValue, selectable = true)
          case _: MessagesMergeDiff.Replace => RowData.InBoth(masterValue, slaveValue, selectable = true)
          case _: MessagesMergeDiff.Match   => RowData.InBoth(masterValue, slaveValue, selectable = false)
        }
      }
    }

    override val cellsAreInteractive = true

    override lazy val renderer: ListItemRenderer[RenderableDiff, _] = (renderable: ListItemRenderable[RenderableDiff]) => {
      // FIXME: Figure out what to do with a shitty layout!
      checkEdt()
      val msgAreaContainer = new MessagesAreaContainer(htmlKit)
//      msgAreaContainer.textPane.peer.putClientProperty(javax.swing.JEditorPane.HONOR_DISPLAY_PROPERTIES, Boolean.box(true))
      val msgService = msgAreaContainer.msgService
      val md = msgService.createStubDoc
//      msgDoc.doc.getStyleSheet.addRule("#messages { background-color: #FFE0E0; }")
      if (renderable.isSelectable) {
        val color = if (renderable.isAdd) Colors.AdditionBg else Colors.CombineBg
        msgAreaContainer.textPane.background = color
      }
      val allRendered = for (either <- renderable.v.messageOptions) yield {
        val rendered = either match {
          case Right(msg) => msgService.renderMessageHtml(renderable.v.dao, renderable.v.cwd, renderable.v.dsRoot, msg)
          case Left(num)  => s"<hr>${num} messages<hr><p>"
        }
        rendered
      }
      md.insert(allRendered.mkString.replaceAll("\n", ""), MessagesDocumentService.MessageInsertPosition.Trailing)
      msgAreaContainer.render(md, showTop = true)
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

      val res = msgAreaContainer.textPane

      // If we don't call it here, we might get an NPE later, under some unknown and rare conditions. Magic!
      res.peer.getPreferredSize
      res
    }

    override protected def rowDataToResultOption(
        rd: RowData[RenderableDiff],
        selected: Boolean
    ): Option[MessagesMergeDecision] = {
      import MessagesMergeDiff._
      val diff: MessagesMergeDiff = rd match {
        case RowData.InBoth(mmd, _, _)    => mmd.diff
        case RowData.InMasterOnly(mmd, _) => mmd.diff
        case RowData.InSlaveOnly(smd, _)  => smd.diff
      }
      diff match {
        case diff: Retain               => Some(diff)
        case diff: Match                => Some(diff)

        case diff: Add if selected      => Some(diff)
        case diff: Add                  => None
        case diff: Replace if selected  => Some(diff)
        case diff: Replace              => Some(diff.asDontReplace)
      }
    }
  }

  case class RenderableDiff(
    /** Note that diff is the same for lhs and rhs */
    diff: MessagesMergeDiff,
    /** Messages to be rendered, or number of messages abbreviated out */
    messageOptions: Seq[Either[Int, Message]],
    dao: ChatHistoryDao,
    cwd: ChatWithDetails,
    dsRoot: DatasetRoot
 )

  sealed trait CxtFetchResult
  object CxtFetchResult {
    case class Discrete(firstMsgs: Seq[Message], between: Int, lastMsgs: Seq[Message]) extends CxtFetchResult
    case class Continuous(msgs: Seq[Message])                                          extends CxtFetchResult
  }

  class ContextFetcher(dao: ChatHistoryDao, chat: Chat) {
    private type FirstMsg = Message
    private type LastMsg  = Message

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
          val subfetch1    = fetch1.take(MaxCutoffMsgsPartLength)
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

  def main(args: Array[String]): Unit = {
    import java.awt.Desktop
    import org.fs.chm.ui.swing.general.ExtendedHtmlEditorKit
    import org.fs.chm.utility.TestUtils._

    val desktopOption = if (Desktop.isDesktopSupported) Some(Desktop.getDesktop) else None
    val htmlKit = new ExtendedHtmlEditorKit(desktopOption)
    val msgService = new MessagesDocumentService(htmlKit)

    val numUsers = 3
    val msgs = (0 to 1000) map (id => {
      val msg = createRegularMessage(id, (id % numUsers) + 1)
      if ((50 to 100) contains id) {
        val longText = (
          Seq.fill(100)("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").mkString(" ") + " " + Seq.fill(100)("abcdefg").mkString
        )
        msg.copy(text = Seq(RichText.makePlain(longText)))
      } else {
        msg
      }
    })

    val mDao = createSimpleDao(isMaster = true, "Master", msgs, numUsers)
    val (_, _, _, mCwd, mMsgsI) = getSimpleDaoEntities(mDao)
    val sDao = createSimpleDao(isMaster = false, "Slave", msgs, numUsers)
    val (_, _, _, sCwd, sMsgsI) = getSimpleDaoEntities(sDao)

    val mismatches = IndexedSeq(
      // Prefix
      MessagesMergeDiff.Retain(
        firstMasterMsg = mMsgsI.bySrcId(10),
        lastMasterMsg  = mMsgsI.bySrcId(15),
      ),

      MessagesMergeDiff.Match(
        firstMasterMsg = mMsgsI.bySrcId(15),
        lastMasterMsg  = mMsgsI.bySrcId(39),
        firstSlaveMsg  = sMsgsI.bySrcId(15),
        lastSlaveMsg   = sMsgsI.bySrcId(39)
      ),

      MessagesMergeDiff.Retain(
        firstMasterMsg = mMsgsI.bySrcId(40),
        lastMasterMsg  = mMsgsI.bySrcId(40),
      ),

//      // Addition
//      MessagesMergeDiff.Add(
//        firstSlaveMsg = sMsgsI.bySrcId(41),
//        lastSlaveMsg  = sMsgsI.bySrcId(60)
//      )

//      // Conflict
//      MessagesMergeDiff.Replace(
//        firstMasterMsg = mMsgsI.bySrcId(41),
//        lastMasterMsg  = mMsgsI.bySrcId(60),
//        firstSlaveMsg  = sMsgsI.bySrcId(41),
//        lastSlaveMsg   = sMsgsI.bySrcId(60)
//      ),

      // Addition + conflict + addition
      MessagesMergeDiff.Add(
        firstSlaveMsg = sMsgsI.bySrcId(41),
        lastSlaveMsg  = sMsgsI.bySrcId(42)
      ),
      MessagesMergeDiff.Replace(
        firstMasterMsg = mMsgsI.bySrcId(43),
        lastMasterMsg  = mMsgsI.bySrcId(44),
        firstSlaveMsg  = sMsgsI.bySrcId(43),
        lastSlaveMsg   = sMsgsI.bySrcId(44)
      ),
      MessagesMergeDiff.Add(
        firstSlaveMsg = sMsgsI.bySrcId(45),
        lastSlaveMsg  = sMsgsI.bySrcId(46)
      ),

      // Suffix
      MessagesMergeDiff.Match(
        firstMasterMsg = mMsgsI.bySrcId(200),
        lastMasterMsg  = mMsgsI.bySrcId(201),
        firstSlaveMsg  = sMsgsI.bySrcId(200),
        lastSlaveMsg   = sMsgsI.bySrcId(201)
      ),
      MessagesMergeDiff.Match(
        firstMasterMsg = mMsgsI.bySrcId(202),
        lastMasterMsg  = mMsgsI.bySrcId(400),
        firstSlaveMsg  = sMsgsI.bySrcId(202),
        lastSlaveMsg   = sMsgsI.bySrcId(400)
      )
    )

    val model = new SelectMergeMessagesModel(mDao, mCwd, sDao, sCwd, mismatches, htmlKit)
    Swing.onEDTWait {
      val dialog = new SelectMergeMessagesDialog(model)
      dialog.visible = true
      dialog.peer.setLocationRelativeTo(null)
      println(dialog.selection map (_.mkString("\n  ", "\n  ", "\n")))
    }
  }
}
