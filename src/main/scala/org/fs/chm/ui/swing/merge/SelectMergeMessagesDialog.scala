package org.fs.chm.ui.swing.merge

import scala.annotation.tailrec
import scala.swing._

import com.github.nscala_time.time.Imports._
import javax.swing.text.html.HTMLEditorKit

import org.fs.chm.dao._
import org.fs.chm.dao.merge.DatasetMerger.MessagesMergeOption
import org.fs.chm.protobuf.RichTextElement
import org.fs.chm.protobuf.RtePlain
import org.fs.chm.ui.swing.general.CustomDialog
import org.fs.chm.ui.swing.general.SwingUtils._
import org.fs.chm.ui.swing.messages.impl.MessagesAreaContainer
import org.fs.chm.ui.swing.messages.impl.MessagesService
import org.fs.chm.utility.EntityUtils._
import org.fs.utility.Imports._

/**
 * Show dialog for merging chat messages.
 * Unlike other merge dialogs, this one does not perform a mismatch analysis, and relies on the provided one instead.
 * Rules:
 * - Multiple `Keep` mismatches will be squished together to avoid cluttering
 * - Checkbox option will be present for all `Add`/`Replace` mismatches
 * - `Add` mismatch that was unchecked will be removed from output
 * - `Replace` mismatch that was unchecked will be replaced by `Keep` mismatch
 * This means that master messages coverage should not change in the output
 */
class SelectMergeMessagesDialog(
    masterDao: ChatHistoryDao,
    masterCwd: ChatWithDetails,
    slaveDao: ChatHistoryDao,
    slaveCwd: ChatWithDetails,
    mismatches: IndexedSeq[MessagesMergeOption],
    htmlKit: HTMLEditorKit
) extends CustomDialog[IndexedSeq[MessagesMergeOption]](takeFullHeight = true) {
  import SelectMergeMessagesDialog._

  private lazy val MaxMessageWidth = 500

  private lazy val originalTitle =
    s"Select messages to merge (${masterCwd.chat.nameOption.getOrElse(ChatHistoryDao.Unnamed)})"

  {
    title = originalTitle
  }

  private lazy val models = new Models

  private lazy val table = {
    checkEdt()
    new SelectMergesTable[RenderableMismatch, Seq[MessagesMergeOption]](models)
  }

  override protected lazy val dialogComponent: Component = {
    table.wrapInScrollpaneAndAdjustWidth()
  }

  override protected def validateChoices(): Option[IndexedSeq[MessagesMergeOption]] = {
    Some(table.selected.flatten.toIndexedSeq)
  }

  import SelectMergesTable._

  private class Models extends MergeModels[RenderableMismatch, Seq[MessagesMergeOption]] {
    override val allElems: Seq[RowData[RenderableMismatch]] = {
      require(mismatches.nonEmpty)

      val masterCxtFetcher = new ContextFetcher(masterDao, masterCwd.chat)
      val slaveCxtFetcher = new ContextFetcher(slaveDao, slaveCwd.chat)

      def cxtToRaw(fetchResult: CxtFetchResult): Seq[Either[Int, Message]] = fetchResult match {
        case CxtFetchResult.Discrete(msf, n, msl) =>
          (msf map Right.apply) ++ Seq(Left(n)) ++ (msl map Right.apply)
        case CxtFetchResult.Continuous(ms) =>
          ms map Right.apply
      }

      // Group consecutive Keeps
      val groupedMismatches: Seq[Seq[MessagesMergeOption]] = {
        @tailrec
        def process(acc: Seq[Seq[MessagesMergeOption]],
                    left: Seq[MessagesMergeOption]): Seq[Seq[MessagesMergeOption]] = {
          left.headOption match {
            case None => acc
            case Some(_: MessagesMergeOption.Keep) =>
              val (keeps, rest) = left.span(_.isInstanceOf[MessagesMergeOption.Keep])
              process(acc :+ keeps, rest)
            case Some(mmo) =>
              process(acc :+ Seq(mmo), left.tail)
          }
        }
        process(Seq.empty, mismatches)
      }

      groupedMismatches map { mismatches =>
        val fwdStream = mismatches.toStream
        val bckStream = mismatches.reverse.toStream
        val masterFetchResult =
          masterCxtFetcher(
            fwdStream.flatMap(_.firstMasterMsgOption).headOption,
            bckStream.flatMap(_.lastMasterMsgOption).headOption
          )
        val slaveFetchResult =
          slaveCxtFetcher(
            fwdStream.flatMap(_.firstSlaveMsgOption).headOption,
            bckStream.flatMap(_.lastSlaveMsgOption).headOption
          )
        val masterValue = RenderableMismatch(mismatches, cxtToRaw(masterFetchResult), masterDao, masterCwd)
        val slaveValue = RenderableMismatch(mismatches, cxtToRaw(slaveFetchResult), slaveDao, slaveCwd)
        mismatches match {
          // Those are keeps
          case xs if xs.tail.nonEmpty                                => RowData.InBoth(masterValue, slaveValue)
          case MessagesMergeOption.Keep(_, _, Some(_), Some(_)) +: _ => RowData.InBoth(masterValue, slaveValue)
          case (_: MessagesMergeOption.Keep) +: _                    => RowData.InMasterOnly(masterValue)
          case (_: MessagesMergeOption.Add) +: _                     => RowData.InSlaveOnly(slaveValue)
          case (_: MessagesMergeOption.Replace) +: _                 => RowData.InBoth(masterValue, slaveValue)
        }
      }
    }

    override val cellsAreInteractive = true

    override lazy val renderer = (renderable: ListItemRenderable[RenderableMismatch]) => {
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
          case Right(msg) => msgService.renderMessageHtml(renderable.v.dao, renderable.v.cwd, msg)
          case Left(num)  => s"<hr>${num} messages<hr><p>"
        }
        rendered
      }
      md.insert(allRendered.mkString.replaceAll("\n", ""), MessagesService.MessageInsertPosition.Trailing)
      msgAreaContainer.render(md, true)
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

    override protected def isInBothSelectable(mv: RenderableMismatch, sv: RenderableMismatch): Boolean = mv.isSelectable
    override protected def isInSlaveSelectable(sv: RenderableMismatch):                        Boolean = sv.isSelectable
    override protected def isInMasterSelectable(mv: RenderableMismatch):                       Boolean = mv.isSelectable

    override protected def rowDataToResultOption(
        rd: RowData[RenderableMismatch],
        isSelected: Boolean
    ): Option[Seq[MessagesMergeOption]] = {
      import MessagesMergeOption._
      rd match {
        case RowData.InMasterOnly(mmd)                                  => Some(mmd.mismatches)
        case RowData.InBoth(mmd, _) if isSelected                       => Some(mmd.mismatches)
        case RowData.InBoth(RenderableMismatch.Keep(mms, _, _, _), _)   => Some(mms)
        case RowData.InBoth(RenderableMismatch.Replace(mm, _, _, _), _) => Some(Seq(mm.asKeep))
        case _ if !isSelected                                           => None
        case RowData.InSlaveOnly(smd)                                   => Some(smd.mismatches)
      }
    }
  }

  private sealed abstract class RenderableMismatch(val isSelectable: Boolean) {
    /** Mismatches to be rendered */
    def mismatches: Seq[MessagesMergeOption]

    /** Messages to be rendered, or number of messages abbreviated out */
    def messageOptions: Seq[Either[Int, Message]]

    def dao: ChatHistoryDao
    def cwd: ChatWithDetails
  }

  private object RenderableMismatch {
    def apply(mismatches: Seq[MessagesMergeOption],
              messageOptions: Seq[Either[Int, Message]],
              dao: ChatHistoryDao,
              cwd: ChatWithDetails): RenderableMismatch = {
      mismatches match {
        case xs if xs.forall(_.isInstanceOf[MessagesMergeOption.Keep]) =>
          Keep(mismatches.asInstanceOf[Seq[MessagesMergeOption.Keep]], messageOptions, dao, cwd)
        case Seq(mmo: MessagesMergeOption.Add) =>
          Add(mmo, messageOptions, dao, cwd)
        case Seq(mmo: MessagesMergeOption.Replace) =>
          Replace(mmo, messageOptions, dao, cwd)
      }
    }
    case class Keep(
        mismatches: Seq[MessagesMergeOption.Keep],
        messageOptions: Seq[Either[Int, Message]],
        dao: ChatHistoryDao,
        cwd: ChatWithDetails
    ) extends RenderableMismatch(false)

    case class Add(
        mismatch: MessagesMergeOption.Add,
        messageOptions: Seq[Either[Int, Message]],
        dao: ChatHistoryDao,
        cwd: ChatWithDetails
    ) extends RenderableMismatch(true) {
      override def mismatches = Seq(mismatch)
    }

    case class Replace(
        mismatch: MessagesMergeOption.Replace,
        messageOptions: Seq[Either[Int, Message]],
        dao: ChatHistoryDao,
        cwd: ChatWithDetails
    ) extends RenderableMismatch(true) {
      override def mismatches = Seq(mismatch)
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
        msg.asInstanceOf[Message.Regular].copy(textOption =
          Some(RichText.fromPlainString(longText)))
      } else {
        msg
      }
    })

    val mDao = createSimpleDao("Master", msgs, numUsers)
    val (_, _, mCwd, mMsgsI) = getSimpleDaoEntities(mDao)
    val sDao = createSimpleDao("Slave", msgs, numUsers)
    val (_, _, sCwd, sMsgsI) = getSimpleDaoEntities(sDao)

    val mismatches = IndexedSeq(
      // Prefix
      MessagesMergeOption.Keep(
        firstMasterMsg      = mMsgsI.bySrcId(10),
        lastMasterMsg       = mMsgsI.bySrcId(15),
        firstSlaveMsgOption = None,
        lastSlaveMsgOption  = None
      ),

      MessagesMergeOption.Keep(
        firstMasterMsg      = mMsgsI.bySrcId(15),
        lastMasterMsg       = mMsgsI.bySrcId(39),
        firstSlaveMsgOption = Some(sMsgsI.bySrcId(15)),
        lastSlaveMsgOption  = Some(sMsgsI.bySrcId(39))
      ),

      MessagesMergeOption.Keep(
        firstMasterMsg      = mMsgsI.bySrcId(40),
        lastMasterMsg       = mMsgsI.bySrcId(40),
        firstSlaveMsgOption = None,
        lastSlaveMsgOption  = None
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
      MessagesMergeOption.Keep(
        firstMasterMsg      = mMsgsI.bySrcId(202),
        lastMasterMsg       = mMsgsI.bySrcId(400),
        firstSlaveMsgOption = Some(sMsgsI.bySrcId(202)),
        lastSlaveMsgOption  = Some(sMsgsI.bySrcId(400))
      )
    )

    Swing.onEDTWait {
      val dialog = new SelectMergeMessagesDialog(mDao, mCwd, sDao, sCwd, mismatches, htmlKit)
      dialog.visible = true
      dialog.peer.setLocationRelativeTo(null)
      println(dialog.selection map (_.mkString("\n  ", "\n  ", "\n")))
    }
  }
}
