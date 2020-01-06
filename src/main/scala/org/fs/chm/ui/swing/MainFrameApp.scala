package org.fs.chm.ui.swing

import java.awt.Desktop
import java.awt.EventQueue
import java.awt.event.AdjustmentEvent
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.immutable.ListMap
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.swing._

import com.github.nscala_time.time.Imports._
import javax.swing.event.HyperlinkEvent
import org.fs.chm.BuildInfo
import org.fs.chm.dao._
import org.fs.chm.loader.H2DataManager
import org.fs.chm.loader.TelegramDataLoader
import org.fs.chm.ui.swing.MessagesService._
import org.fs.chm.ui.swing.chatlist.ChatListSelectionCallbacks
import org.fs.chm.ui.swing.chatlist.DaoItem
import org.fs.chm.ui.swing.general.ChatWithDao
import org.fs.chm.ui.swing.general.ExtendedHtmlEditorKit
import org.fs.chm.ui.swing.general.SwingUtils._
import org.fs.chm.ui.swing.webp.Webp
import org.fs.utility.Imports._

class MainFrameApp extends SimpleSwingApplication { app =>
  private val Lock             = new Object
  private val MsgBatchLoadSize = 100

  private var loadedDaos: ListMap[ChatHistoryDao, Map[Chat, ChatCache]] = ListMap.empty

  private var currentChatOption:      Option[ChatWithDao] = None
  private var loadMessagesInProgress: AtomicBoolean       = new AtomicBoolean(false)

  private val desktopOption = if (Desktop.isDesktopSupported) Some(Desktop.getDesktop) else None
  private val htmlKit       = new ExtendedHtmlEditorKit(desktopOption)
  private val msgService    = new MessagesService(htmlKit)

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

    layout(menuBar)                = North
    layout(chatsPane)              = West
    layout(msgAreaContainer.panel) = Center
  }

  lazy val menuBar = new MenuBar {
    val fileMenu = new Menu("File") {
      contents += menuItem("Open Database", () => showOpenDialog())
    }
    contents += fileMenu
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

  def showOpenDialog(): Unit = {
    // TODO: Show errors
    val chooser = DataLoaders.chooser
    chooser.showOpenDialog(null) match {
      case FileChooser.Result.Cancel => // NOOP
      case FileChooser.Result.Error  => // Mostly means that dialog was dismissed, also NOOP
      case FileChooser.Result.Approve => {
        changeChatsClickable(false)
        Future { // To release UI lock
          Swing.onEDT {
            val dao = if (DataLoaders.h2ff.accept(chooser.selectedFile)) {
              DataLoaders.h2.loadData(chooser.selectedFile.getParentFile)
            } else if (DataLoaders.tgFf.accept(chooser.selectedFile)) {
              DataLoaders.tg.loadData(chooser.selectedFile.getParentFile)
            } else {
              throw new IllegalStateException("Unknown file type!")
            }
            loadDaoFromEDT(dao)
          }
        }
      }
    }
  }

  def loadDaoFromEDT(dao: ChatHistoryDao): Unit = {
    require(EventQueue.isDispatchThread, "Should be called from EDT")
    Lock.synchronized {
      loadedDaos = loadedDaos + (dao -> Map.empty) // TODO: Reverse?
      chatsListContents += new DaoItem(new ChatListSelectionCallbacks {
        override def chatSelected(cc: ChatWithDao): Unit = app.chatSelected(cc)
      }, dao)
    }
    chatsPane.validate()
    changeChatsClickable(true)
  }

  def chatSelected(cc: ChatWithDao): Unit = {
    Lock.synchronized {
      currentChatOption         = None
      msgAreaContainer.document = msgService.pleaseWaitDoc
      if (!loadedDaos(cc.dao).contains(cc.chat)) {
        updateCache(cc.dao, cc.chat, ChatCache(None, None))
      }
      changeChatsClickable(false)
    }
    val f = Future {
      val cache = loadedDaos(cc.dao)(cc.chat)
      cache.msgDocOption match {
        case Some(_) =>
          () // If the chat has been already rendered, restore previous document as-is
        case None =>
          val msgDoc   = msgService.createStubDoc
          val messages = cc.dao.lastMessages(cc.chat, MsgBatchLoadSize)
          for (m <- messages) {
            msgDoc.insert(msgService.renderMessageHtml(cc, m), MessageInsertPosition.Trailing)
          }
          val loadStatus = LoadStatus(
            firstId = messages.headOption map (_.id) getOrElse (-1),
            lastId = messages.lastOption map (_.id) getOrElse (-1),
            beginReached = messages.size < MsgBatchLoadSize,
            endReached = true
          )
          updateCache(cc.dao, cc.chat, ChatCache(Some(msgDoc), Some(loadStatus)))
      }
    }
    f foreach { _ =>
      Swing.onEDTWait(Lock.synchronized {
        currentChatOption = Some(cc)
        val doc = loadedDaos(cc.dao)(cc.chat).msgDocOption.get.doc
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
          val cache = loadedDaos(cc.dao)(cc.chat)
          val loadStatus = cache.loadStatusOption.get
          if (!loadStatus.beginReached) {
            changeChatsClickable(false)
            loadMessagesInProgress set true
            val msgDoc = cache.msgDocOption.get
            val f = Future {
              Lock.synchronized {
                val (viewPos1, viewSize1) = msgAreaContainer.view.posAndSize
                msgDoc.insert("<div id=\"loading\"><hr><p> Loading... </p><hr></div>", MessageInsertPosition.Leading)
                val addedMessages = cc.dao.messagesBefore(cc.chat, loadStatus.firstId, MsgBatchLoadSize).get
                updateCache(
                  cc.dao,
                  cc.chat,
                  cache.copy(loadStatusOption = Some {
                    loadStatus.copy(
                      firstId = addedMessages.headOption map (_.id) getOrElse (-1),
                      beginReached = addedMessages.size < MsgBatchLoadSize
                    )
                  })
                )
                // We're still holding the lock
                Swing.onEDTWait {
                  // TODO: Prevent flickering
                  // TODO: Preserve selection
                  msgDoc.removeFirst()
                  for (m <- addedMessages.reverse) {
                    msgDoc.insert(msgService.renderMessageHtml(cc, m), MessageInsertPosition.Leading)
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

  def updateCache(dao: ChatHistoryDao, chat: Chat, cache: ChatCache): Unit =
    Lock.synchronized {
      loadedDaos = loadedDaos + (dao -> (loadedDaos(dao) + (chat -> cache)))
    }

  private case class ChatCache(
      msgDocOption: Option[MessageDocument],
      loadStatusOption: Option[LoadStatus]
  )

  private case class LoadStatus(
      firstId: Long,
      lastId: Long,
      beginReached: Boolean,
      endReached: Boolean
  )

  private object DataLoaders {
    val h2 = new H2DataManager
    val tg = new TelegramDataLoader

    val h2ff = easyFileFilter(s"${BuildInfo.name} database (*${H2DataManager.DefaultExt})") { f =>
      f.getName endsWith H2DataManager.DefaultExt
    }

    val tgFf = easyFileFilter("Telegram export JSON database (result.json)") { f =>
      f.getName == "result.json"
    }

    val chooser = new FileChooser(null) {
      peer.addChoosableFileFilter(h2ff)
      peer.addChoosableFileFilter(tgFf)
    }
  }
}
