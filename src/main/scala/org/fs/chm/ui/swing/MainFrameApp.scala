package org.fs.chm.ui.swing

import java.awt.Desktop
import java.awt.EventQueue
import java.awt.event.AdjustmentEvent
import java.io.File
import java.util.UUID
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
import org.fs.chm.utility.IoUtils._
import org.fs.chm.utility.SimpleConfigAware
import org.fs.utility.Imports._
import org.slf4s.Logging

class MainFrameApp //
    extends SimpleSwingApplication
    with ChatListSelectionCallbacks
    with SimpleConfigAware
    with Logging { app =>
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
    layout(chatsOuterPanel)        = West
    layout(msgAreaContainer.panel) = Center
  }

  lazy val (menuBar, saveAsMenuRoot) = {
    val saveAsMenuRoot = new Menu("Save Database As...")
    new MenuBar {
      val fileMenu = new Menu("File") {
        contents += menuItem("Open Database")(showOpenDialog())
        contents += saveAsMenuRoot
      }
      contents += fileMenu
    } -> saveAsMenuRoot
  }

  lazy val (chatsOuterPanel, chatsListContents) = {
    val panePreferredWidth = 300

    val panel = new BoxPanel(Orientation.Vertical)

    new BorderPanel {
      import scala.swing.BorderPanel.Position._

      val panel2 = new BorderPanel {
        layout(panel) = North
        layout {
          // That's the only solution I came up with that worked to set a minimum width of an empty chat list
          // (setting minimum size doesn't work, setting preferred size screws up scrollbar)
          new BorderPanel {
            this.preferredWidth = panePreferredWidth
          }
        } = South
      }

      layout(new ScrollPane(panel2) {
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
    chatsOuterPanel.enabled = enabled
    def changeClickableRecursive(c: Component): Unit = c match {
      case i: DaoItem   => i.enabled = enabled
      case c: Container => c.contents foreach changeClickableRecursive
    }
    changeClickableRecursive(chatsOuterPanel)
  }

  //
  // Events
  //

  def showOpenDialog(): Unit = {
    // TODO: Show errors
    val chooser = DataLoaders.openChooser
    for (lastFileString <- config.get(DataLoaders.LastFileKey)) {
      val lastFile = new File(lastFileString)
      chooser.peer.setCurrentDirectory(lastFile.existingDir)
      chooser.selectedFile = lastFile
    }
    chooser.showOpenDialog(null) match {
      case FileChooser.Result.Cancel => // NOOP
      case FileChooser.Result.Error  => // Mostly means that dialog was dismissed, also NOOP
      case FileChooser.Result.Approve if loadedDaos.keys.exists(_ isLoaded chooser.selectedFile) =>
        // TODO: Show error?
        log.warn(s"File ${chooser.selectedFile} is already loaded")
      case FileChooser.Result.Approve => {
        changeChatsClickable(false)
        config.update(DataLoaders.LastFileKey, chooser.selectedFile.getAbsolutePath)
        Future { // To release UI lock
          Swing.onEDT {
            val dao = DataLoaders.load(chooser.selectedFile)
            loadDaoFromEDT(dao)
          }
        }
      }
    }
  }

  def showSaveAsDialog(srcDao: ChatHistoryDao): Unit = {
    val chooser = DataLoaders.saveAsChooser
    for (lastFileString <- config.get(DataLoaders.LastFileKey)) {
      val lastFile = new File(lastFileString)
      chooser.peer.setCurrentDirectory(lastFile.existingDir)
    }
    chooser.showOpenDialog(null) match {
      case FileChooser.Result.Cancel => // NOOP
      case FileChooser.Result.Error  => // Mostly means that dialog was dismissed, also NOOP
      case FileChooser.Result.Approve => {
        changeChatsClickable(false)
        config.update(DataLoaders.LastFileKey, chooser.selectedFile.getAbsolutePath)
        Future { // To release UI lock
          Swing.onEDT {
            val dstDao = DataLoaders.saveAs(srcDao, chooser.selectedFile)
            // TODO: Unload srcDao
            loadDaoFromEDT(dstDao)
          }
        }
      }
    }
  }

  def loadDaoFromEDT(dao: ChatHistoryDao): Unit = {
    require(EventQueue.isDispatchThread, "Should be called from EDT")
    Lock.synchronized {
      loadedDaos = loadedDaos + (dao -> Map.empty) // TODO: Reverse?
      chatsListContents += new DaoItem(this, dao)
    }
    daoListChanged()
    changeChatsClickable(true)
  }

  def daoListChanged(): Unit = {
    saveAsMenuRoot.contents.clear()
    for (dao <- loadedDaos.keys) {
      saveAsMenuRoot.contents += menuItem(dao.name)(showSaveAsDialog(dao))
    }
    chatsOuterPanel.revalidate()
    chatsOuterPanel.repaint()
  }

  override def renameDataset(dsUuid: UUID, newName: String, dao: ChatHistoryDao): Unit = {
    require(EventQueue.isDispatchThread, "Should be called from EDT")
    changeChatsClickable(false)
    Swing.onEDT { // To release UI lock
      Lock.synchronized {
        dao.renameDataset(dsUuid, newName)
        chatsListContents.clear()
        for (dao <- loadedDaos.keys) {
          chatsListContents += new DaoItem(this, dao)
        }
      }
      chatsOuterPanel.revalidate()
      chatsOuterPanel.repaint()
      changeChatsClickable(true)
    }
  }

  override def chatSelected(cc: ChatWithDao): Unit = {
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
    val LastFileKey = "last_database_file"

    private val h2 = new H2DataManager
    private val tg = new TelegramDataLoader

    private val h2ff = easyFileFilter(s"${BuildInfo.name} database (*${H2DataManager.DefaultExt})") { f =>
      f.getName endsWith H2DataManager.DefaultExt
    }

    private val tgFf = easyFileFilter("Telegram export JSON database (result.json)") { f =>
      f.getName == "result.json"
    }

    val openChooser = new FileChooser(null) {
      title = "Select a database to open"
      peer.addChoosableFileFilter(h2ff)
      peer.addChoosableFileFilter(tgFf)
    }

    def load(file: File): ChatHistoryDao = {
      if (h2ff.accept(file)) {
        h2.loadData(file.getParentFile)
      } else if (tgFf.accept(file)) {
        tg.loadData(file.getParentFile)
      } else {
        throw new IllegalStateException("Unknown file type!")
      }
    }

    val saveAsChooser = new FileChooser(null) {
      title = "Choose a directory where the new database will be stored"
      fileSelectionMode = FileChooser.SelectionMode.DirectoriesOnly
      peer.setAcceptAllFileFilterUsed(false)
    }

    def saveAs(srcDao: ChatHistoryDao, dir: File): ChatHistoryDao = {
      h2.create(dir)
      val dstDao = h2.loadData(dir)
      dstDao.copyAllFrom(srcDao)
      dstDao
    }
  }
}
