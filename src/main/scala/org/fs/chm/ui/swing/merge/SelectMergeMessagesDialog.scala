package org.fs.chm.ui.swing.merge

import scala.swing._

import com.github.nscala_time.time.Imports._
import javax.swing.text.html.HTMLEditorKit
import org.fs.chm.dao._
import org.fs.chm.dao.merge.DatasetMerger.MessagesMergeOption
import org.fs.chm.ui.swing.MessagesAreaContainer
import org.fs.chm.ui.swing.MessagesService
import org.fs.chm.ui.swing.MessagesService.MessageInsertPosition
import org.fs.chm.ui.swing.general.ChatWithDao
import org.fs.chm.ui.swing.general.CustomDialog
import org.fs.chm.ui.swing.general.SwingUtils._
import org.fs.chm.utility.EntityUtils._
import org.fs.utility.Imports._

/**
 * Show dialog for merging chat messages.
 * Unlike other merge dialogs, this one does not perform a mismatch analysis, and relies on the provided one instead.
 * Rules:
 * - Checkbox option will be present for all `Add`/`Replace` mismatches
 * - `Add` mismatch that was unchecked will be removed from output
 * - `Replace` mismatch that was unchecked will be replaced by `Keep` mismatch
 * This means that master messages coverage should not change in the output
 */
class SelectMergeMessagesDialog(
    masterDao: ChatHistoryDao,
    masterChat: Chat,
    slaveDao: ChatHistoryDao,
    slaveChat: Chat,
    mismatches: IndexedSeq[MessagesMergeOption],
    htmlKit: HTMLEditorKit,
    msgService: MessagesService
) extends CustomDialog[IndexedSeq[MessagesMergeOption]] {
  import SelectMergeMessagesDialog._

  private lazy val MaxMessageWidth = 500

  private lazy val originalTitle =
    s"Select messages to merge (${masterChat.nameOption.getOrElse(ChatHistoryDao.Unnamed)})"

  {
    title = originalTitle
  }

  private lazy val models = new Models

  private lazy val table = new SelectMergesTable[RenderableMismatch, MessagesMergeOption](models)

  override protected lazy val dialogComponent: Component = {
    table.wrapInScrollpaneAndAdjustWidth()
  }

  override protected def validateChoices(): Option[IndexedSeq[MessagesMergeOption]] = {
    Some(table.selected.toIndexedSeq)
  }

  import SelectMergesTable._

  private class Models extends MergeModels[RenderableMismatch, MessagesMergeOption] {
    override val allElems: Seq[RowData[RenderableMismatch]] = {
      require(mismatches.nonEmpty)
      val masterCwd = ChatWithDao(masterChat, masterDao)
      val slaveCwd = ChatWithDao(slaveChat, slaveDao)

      val masterCxtFetcher = new ContextFetcher(masterDao, masterChat)
      val slaveCxtFetcher = new ContextFetcher(slaveDao, slaveChat)

      def cxtToRaw(fetchResult: CxtFetchResult): Seq[Either[Int, Message]] = fetchResult match {
        case CxtFetchResult.Discrete(msf, n, msl) =>
          (msf map Right.apply) ++ Seq(Left(n)) ++ (msl map Right.apply)
        case CxtFetchResult.Continuous(ms) =>
          ms map Right.apply
      }

      mismatches map { mismatch =>
        val masterFetchResult = masterCxtFetcher(mismatch.firstMasterMsgOption, mismatch.lastMasterMsgOption)
        val slaveFetchResult  = slaveCxtFetcher(mismatch.firstSlaveMsgOption, mismatch.lastSlaveMsgOption)
        val masterValue       = RenderableMismatch(mismatch, cxtToRaw(masterFetchResult), masterCwd)
        val slaveValue        = RenderableMismatch(mismatch, cxtToRaw(slaveFetchResult), slaveCwd)
        mismatch match {
          case MessagesMergeOption.Keep(_, _, Some(_), Some(_)) => RowData.InBoth(masterValue, slaveValue)
          case _: MessagesMergeOption.Keep                      => RowData.InMasterOnly(masterValue)
          case _: MessagesMergeOption.Add                       => RowData.InSlaveOnly(slaveValue)
          case _: MessagesMergeOption.Replace                   => RowData.InBoth(masterValue, slaveValue)
        }
      }
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

    override protected def isInBothSelectable(mv: RenderableMismatch, sv: RenderableMismatch): Boolean = mv.isSelectable
    override protected def isInSlaveSelectable(sv: RenderableMismatch):                        Boolean = sv.isSelectable
    override protected def isInMasterSelectable(mv: RenderableMismatch):                       Boolean = mv.isSelectable

    override protected def rowDataToResultOption(
        rd: RowData[RenderableMismatch],
        isSelected: Boolean
    ): Option[MessagesMergeOption] = {
      import MessagesMergeOption._
      rd match {
        case RowData.InMasterOnly(mmd)                                => Some(mmd.mismatch)
        case RowData.InBoth(mmd, _) if isSelected                     => Some(mmd.mismatch)
        case RowData.InBoth(RenderableMismatch(mm: Keep, _, _), _)    => Some(mm)
        case RowData.InBoth(RenderableMismatch(mm: Replace, _, _), _) => Some(mm.asKeep)
        case _ if !isSelected                                         => None
        case RowData.InSlaveOnly(smd)                                 => Some(smd.mismatch)
      }
    }
  }

  private case class RenderableMismatch(
      /** Mismatch to be rendered */
      mismatch: MessagesMergeOption,
      /** Messages to be rendered, or number of messages abbreviated out */
      messageOptions: Seq[Either[Int, Message]],
      cwd: ChatWithDao
  ) {
    def isSelectable = mismatch match {
      case _: MessagesMergeOption.Keep    => false
      case _: MessagesMergeOption.Add     => true
      case _: MessagesMergeOption.Replace => true
    }
  }

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
}

private object SelectMergeMessagesDialog {
  private val MaxContinuousMsgsLength = 20
  private val MaxCutoffMsgsPartLength = 7

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
      if ((50 to 100) contains id) {
        val longText = (
          Seq.fill(100)("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").mkString(" ") + " " + Seq.fill(100)("abcdefg").mkString
        )
        msg.asInstanceOf[Message.Regular].copy(textOption = Some(RichText(Seq(RichText.Plain(longText)))))
      } else {
        msg
      }
    })

    val mDao = createSimpleDao("Master", msgs, numUsers)
    val (_, _, mChat, mMsgsI) = getSimpleDaoEntities(mDao)
    val sDao = createSimpleDao("Slave", msgs, numUsers)
    val (_, _, sChat, sMsgsI) = getSimpleDaoEntities(sDao)

    val mismatches = IndexedSeq(
      // Prefix
      MessagesMergeOption.Keep(
        firstMasterMsg      = mMsgsI.bySrcId(40),
        lastMasterMsg       = mMsgsI.bySrcId(40),
//        firstSlaveMsgOption = None,
//        lastSlaveMsgOption  = None
        firstSlaveMsgOption = Some(sMsgsI.bySrcId(40)),
        lastSlaveMsgOption  = Some(sMsgsI.bySrcId(40))
      ),

//      // Addition
//      MessagesMergeOption.Add(
//        firstSlaveMsg = sMsgsI.bySrcId(41),
//        lastSlaveMsg  = sMsgsI.bySrcId(60)
//      )

//      // Conflict
//      MessagesMergeOption.Replace(
//        firstMasterMsg = mMsgsI.bySrcId(41),
//        lastMasterMsg  = mMsgsI.bySrcId(60),
//        firstSlaveMsg  = sMsgsI.bySrcId(41),
//        lastSlaveMsg   = sMsgsI.bySrcId(60)
//      ),

      // Addition + conflict + addition
      MessagesMergeOption.Add(
        firstSlaveMsg = sMsgsI.bySrcId(41),
        lastSlaveMsg  = sMsgsI.bySrcId(42)
      ),
      MessagesMergeOption.Replace(
        firstMasterMsg = mMsgsI.bySrcId(43),
        lastMasterMsg  = mMsgsI.bySrcId(44),
        firstSlaveMsg  = sMsgsI.bySrcId(43),
        lastSlaveMsg   = sMsgsI.bySrcId(44)
      ),
      MessagesMergeOption.Add(
        firstSlaveMsg = sMsgsI.bySrcId(45),
        lastSlaveMsg  = sMsgsI.bySrcId(46)
      ),

      // Suffix
      MessagesMergeOption.Keep(
        firstMasterMsg      = mMsgsI.bySrcId(200),
        lastMasterMsg       = mMsgsI.bySrcId(201),
//        firstSlaveMsgOption = None,
//        lastSlaveMsgOption  = None
        firstSlaveMsgOption = Some(sMsgsI.bySrcId(200)),
        lastSlaveMsgOption  = Some(sMsgsI.bySrcId(201))
      ),
    )

    val dialog = new SelectMergeMessagesDialog(mDao, mChat, sDao, sChat, mismatches, htmlKit, msgService)
    dialog.visible = true
    dialog.peer.setLocationRelativeTo(null)
    println(dialog.selection map (_.mkString("\n  ", "\n  ", "\n")))
  }
}
