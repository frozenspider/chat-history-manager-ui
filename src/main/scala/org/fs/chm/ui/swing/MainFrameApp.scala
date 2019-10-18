package org.fs.chm.ui.swing

import java.awt.Desktop
import java.awt.event.AdjustmentEvent
import java.awt.{ Container => AwtContainer }
import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.swing._
import scala.swing.event.ButtonClicked

import com.github.nscala_time.time.Imports._
import javax.swing.event.HyperlinkEvent
import org.fs.chm.dao._
import org.fs.chm.ui.swing.MessagesService._
import org.fs.chm.ui.swing.general.ExtendedHtmlEditorKit
import org.fs.chm.ui.swing.webp.Webp
import org.fs.utility.Imports._

class MainFrameApp(dao: ChatHistoryDao) extends SimpleSwingApplication {
  val Lock             = new Object
  val MsgBatchLoadSize = 100

  var documentsCache:  Map[Chat, MessageDocument] = Map.empty
  var loadStatusCache: Map[Chat, LoadStatus]      = Map.empty

  var currentChatOption:      Option[Chat]  = None
  var loadMessagesInProgress: AtomicBoolean = new AtomicBoolean(false)

  val desktopOption = if (Desktop.isDesktopSupported) Some(Desktop.getDesktop) else None
  val htmlKit       = new ExtendedHtmlEditorKit(desktopOption)
  val msgService    = new MessagesService(dao, htmlKit)

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

  lazy val chatsPane = new BorderPanel {
    import scala.swing.BorderPanel.Position._

    layout(new ScrollPane(new BoxPanel(Orientation.Vertical) {
      for (c <- dao.chats) {
        val el = new Button(c.nameOption getOrElse "<Unnamed>")
        contents += el
        listenTo(el)
        reactions += {
          case ButtonClicked(`el`) => chatSelected(c)
        }
      }
    })) = Center
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
    def changeClickableRecursive(c: AwtContainer): Unit = {
      c.setEnabled(enabled)
      c.getComponents foreach {
        case c: AwtContainer => changeClickableRecursive(c)
        case _               => //NOOP
      }
    }
    changeClickableRecursive(chatsPane.peer)
  }

  //
  // Events
  //

  def chatSelected(c: Chat): Unit = {
    Lock.synchronized {
      currentChatOption         = None
      msgAreaContainer.document = msgService.pleaseWaitDoc
      changeChatsClickable(false)
    }
    val f = Future {
      // If the chat has been already rendered, restore previous document as-is
      if (!documentsCache.contains(c)) {
        val md       = msgService.createStubDoc
        val messages = dao.lastMessages(c, MsgBatchLoadSize)
        for (m <- messages) {
          md.insert(msgService.renderMessageHtml(c, m), MessageInsertPosition.Trailing)
        }
        val loadStatus = LoadStatus(
          firstId      = messages.headOption map (_.id) getOrElse (-1),
          lastId       = messages.lastOption map (_.id) getOrElse (-1),
          beginReached = messages.size < MsgBatchLoadSize,
          endReached   = true
        )
        Lock.synchronized {
          loadStatusCache = loadStatusCache + ((c, loadStatus))
          documentsCache  = documentsCache + ((c, md))
        }
      }
    }
    f foreach { _ =>
      Swing.onEDTWait(Lock.synchronized {
        currentChatOption = Some(c)
        val doc = documentsCache(c).doc
        msgAreaContainer.document = doc
        msgAreaContainer.scroll.toEnd()
        changeChatsClickable(true)
      })
    }
  }

  def tryLoadPreviousMessages(): Unit =
    currentChatOption match {
      case _ if loadMessagesInProgress.get => // NOOP
      case None                            => // NOOP
      case Some(c) =>
        Lock.synchronized {
          msgAreaContainer.caretUpdatesEnabled = false
          val loadStatus = loadStatusCache(c)
          if (!loadStatus.beginReached) {
            changeChatsClickable(false)
            loadMessagesInProgress set true
            val md = documentsCache(c)
            val f = Future {
              Lock.synchronized {
                val (viewPos1, viewSize1) = msgAreaContainer.view.posAndSize
                md.insert("<div id=\"loading\"><hr><p> Loading... </p><hr></div>", MessageInsertPosition.Leading)
                val addedMessages = dao.messagesBefore(c, loadStatus.firstId, MsgBatchLoadSize)
                loadStatusCache = loadStatusCache.updated(
                  c,
                  loadStatusCache(c).copy(
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
                    md.insert(msgService.renderMessageHtml(c, m), MessageInsertPosition.Leading)
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
