package org.fs.chm.ui.swing

import java.awt.Desktop
import java.awt.event.AdjustmentEvent
import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.swing._
import scala.swing.event.ButtonClicked

import com.github.nscala_time.time.Imports._
import javax.swing.event.HyperlinkEvent
import org.fs.chm.dao._
import org.fs.chm.ui.swing.MessagesService._
import org.fs.chm.ui.swing.chatlist.ChatListSelectionCallbacks
import org.fs.chm.ui.swing.chatlist.DaoItem
import org.fs.chm.ui.swing.general.ChatWithDao
import org.fs.chm.ui.swing.general.ExtendedHtmlEditorKit
import org.fs.chm.ui.swing.general.SwingUtils._
import org.fs.chm.ui.swing.webp.Webp
import org.fs.utility.Imports._

class MainFrameApp extends SimpleSwingApplication { app =>
  val Lock             = new Object
  val MsgBatchLoadSize = 100

  var loadedDaos:      Seq[ChatHistoryDao]               = Seq.empty
  var documentsCache:  Map[ChatWithDao, MessageDocument] = Map.empty
  var loadStatusCache: Map[ChatWithDao, LoadStatus]      = Map.empty

  var currentChatOption:      Option[ChatWithDao]  = None
  var loadMessagesInProgress: AtomicBoolean = new AtomicBoolean(false)

  val desktopOption = if (Desktop.isDesktopSupported) Some(Desktop.getDesktop) else None
  val htmlKit       = new ExtendedHtmlEditorKit(desktopOption)
  val msgService    = new MessagesService(htmlKit)

  // TODO:
  // reply-to (make clickable)
  // word-wrap and narrower width
  // search
  // better pictures rendering
  // emoji and fonts

  Future {
    Webp.eagerInit()
  }

  override def top = new MainFrame {
    import org.fs.chm.BuildInfo._
    title    = s"$name v${version} b${new DateTime(builtAtMillis).toString("yyyyMMdd-HHmmss")}"
    contents = ui
    size     = new Dimension(1000, 700)
    peer.setLocationRelativeTo(null)
  }

  val ui = new BorderPanel {
    import scala.swing.BorderPanel.Position._

//    val browseBtn = new Button("Browse")
//    listenTo(startBtn, browseBtn)
    layout(chatsPane)              = West
    layout(msgAreaContainer.panel) = Center
//    reactions += {
//      case ButtonClicked(`startBtn`)  => eventStartClick
//      case ButtonClicked(`browseBtn`) => eventBrowseClick
//    }
  }

  lazy val (chatsPane, chatsListContents) = {
    val panePreferredWidth = 300

    val panel = new BoxPanel(Orientation.Vertical)
    panel.preferredWidth = panePreferredWidth

    new BorderPanel {
      import scala.swing.BorderPanel.Position._

      layout(new ScrollPane(panel) {
        verticalScrollBar.unitIncrement = 10
        verticalScrollBarPolicy = ScrollPane.BarPolicy.Always
        horizontalScrollBarPolicy = ScrollPane.BarPolicy.Never
      }) = Center
    } -> panel.contents
  }

  lazy val msgAreaContainer: MessagesAreaContainer = {
    val m = new MessagesAreaContainer(htmlKit)

    // Load older messages when sroll is near the top
    m.scrollPane.verticalScrollBar.peer.addAdjustmentListener((e: AdjustmentEvent) => {
      if (e.getValue < 500 && !e.getValueIsAdjusting) {
        tryLoadPreviousMessages()
      }
    })

    // Open clicked hyperlinks in browser
    m.textPane.peer.addHyperlinkListener((e: HyperlinkEvent) => {
      if (e.getEventType == HyperlinkEvent.EventType.ACTIVATED) {
        desktopOption map (_.browse(e.getURL.toURI))
      }
    })

    m
  }

  def changeChatsClickable(enabled: Boolean): Unit = {
    chatsPane.enabled = enabled
    def changeClickableRecursive(c: Component): Unit = c match {
      case i: DaoItem   => i.enabled = enabled
      case c: Container => c.contents foreach changeClickableRecursive
    }
    changeClickableRecursive(chatsPane)
  }

  //
  // Events
  //

  def loadDao(dao: ChatHistoryDao): Unit = {
    Swing.onEDTWait(Lock.synchronized {
      loadedDaos = dao +: loadedDaos
      chatsListContents += new DaoItem(new ChatListSelectionCallbacks {
        override def chatSelected(cc: ChatWithDao): Unit = app.chatSelected(cc)
      }, dao)
    })
  }

  def chatSelected(cc: ChatWithDao): Unit = {
    Lock.synchronized {
      currentChatOption         = None
      msgAreaContainer.document = msgService.pleaseWaitDoc
      changeChatsClickable(false)
    }
    val f = Future {
      // If the chat has been already rendered, restore previous document as-is
      if (!documentsCache.contains(cc)) {
        val md       = msgService.createStubDoc
        val messages = cc.dao.lastMessages(cc.chat, MsgBatchLoadSize)
        for (m <- messages) {
          md.insert(msgService.renderMessageHtml(cc, m), MessageInsertPosition.Trailing)
        }
        val loadStatus = LoadStatus(
          firstId      = messages.headOption map (_.id) getOrElse (-1),
          lastId       = messages.lastOption map (_.id) getOrElse (-1),
          beginReached = messages.size < MsgBatchLoadSize,
          endReached   = true
        )
        Lock.synchronized {
          loadStatusCache = loadStatusCache + ((cc, loadStatus))
          documentsCache  = documentsCache + ((cc, md))
        }
      }
    }
    f foreach { _ =>
      Swing.onEDTWait(Lock.synchronized {
        currentChatOption = Some(cc)
        val doc = documentsCache(cc).doc
        msgAreaContainer.document = doc
        msgAreaContainer.scroll.toEnd() // FIXME: Doesn't always work!
        changeChatsClickable(true)
      })
    }
  }

  def tryLoadPreviousMessages(): Unit =
    currentChatOption match {
      case _ if loadMessagesInProgress.get => // NOOP
      case None                            => // NOOP
      case Some(cc) =>
        Lock.synchronized {
          msgAreaContainer.caretUpdatesEnabled = false
          val loadStatus = loadStatusCache(cc)
          if (!loadStatus.beginReached) {
            changeChatsClickable(false)
            loadMessagesInProgress set true
            val md = documentsCache(cc)
            val f = Future {
              Lock.synchronized {
                val (viewPos1, viewSize1) = msgAreaContainer.view.posAndSize
                md.insert("<div id=\"loading\"><hr><p> Loading... </p><hr></div>", MessageInsertPosition.Leading)
                val addedMessages = cc.dao.messagesBefore(cc.chat, loadStatus.firstId, MsgBatchLoadSize).get
                loadStatusCache = loadStatusCache.updated(
                  cc,
                  loadStatusCache(cc).copy(
                    firstId      = addedMessages.headOption map (_.id) getOrElse (-1),
                    beginReached = addedMessages.size < MsgBatchLoadSize
                  )
                )
                // We're still holding the lock
                Swing.onEDTWait {
                  // TODO: Prevent flickering
                  // TODO: Preserve selection
                  md.removeFirst()
                  for (m <- addedMessages.reverse) {
                    md.insert(msgService.renderMessageHtml(cc, m), MessageInsertPosition.Leading)
                  }
                  val (_, viewSize2) = msgAreaContainer.view.posAndSize
                  val heightDiff     = viewSize2.height - viewSize1.height
                  msgAreaContainer.view.show(viewPos1.x, viewPos1.y + heightDiff)
                }
              }
            }
            f.onComplete(_ =>
              Swing.onEDTWait(Lock.synchronized {
                loadMessagesInProgress set false
                changeChatsClickable(true)
                msgAreaContainer.caretUpdatesEnabled = true
              }))
          }
        }
    }

  case class LoadStatus(
      firstId: Long,
      lastId: Long,
      beginReached: Boolean,
      endReached: Boolean
  )
}
