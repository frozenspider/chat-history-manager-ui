package org.fs.chm.ui.swing

import java.awt.Desktop
import java.awt.event.AdjustmentEvent
import java.io.File
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean

import scala.annotation.tailrec
import scala.collection.immutable.ListMap
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.swing._

import com.github.nscala_time.time.Imports._
import javax.swing.event.HyperlinkEvent
import org.fs.chm.BuildInfo
import org.fs.chm.dao._
import org.fs.chm.dao.merge.DatasetMerger
import org.fs.chm.dao.merge.DatasetMerger._
import org.fs.chm.loader._
import org.fs.chm.ui.swing.MessagesService._
import org.fs.chm.ui.swing.general.ChatWithDao
import org.fs.chm.ui.swing.general.ExtendedHtmlEditorKit
import org.fs.chm.ui.swing.general.SwingUtils
import org.fs.chm.ui.swing.general.SwingUtils._
import org.fs.chm.ui.swing.list.DaoDatasetSelectionCallbacks
import org.fs.chm.ui.swing.list.DaoItem
import org.fs.chm.ui.swing.list.DaoList
import org.fs.chm.ui.swing.list.chat.ChatListItem
import org.fs.chm.ui.swing.list.chat.ChatListItemSelectionGroup
import org.fs.chm.ui.swing.list.chat.ChatListSelectionCallbacks
import org.fs.chm.ui.swing.merge._
import org.fs.chm.ui.swing.user.UserDetailsMenuCallbacks
import org.fs.chm.ui.swing.user.UserDetailsPane
import org.fs.chm.ui.swing.webp.Webp
import org.fs.chm.utility.IoUtils._
import org.fs.chm.utility.SimpleConfigAware
import org.slf4s.Logging

class MainFrameApp //
    extends SimpleSwingApplication
    with SimpleConfigAware
    with Logging
    with DaoDatasetSelectionCallbacks
    with ChatListSelectionCallbacks
    with UserDetailsMenuCallbacks { app =>

  /** A lock which needs to be taken to mutate local variables or DAO */
  private val MutationLock     = new Object
  private val MsgBatchLoadSize = 100

  private var loadedDaos: ListMap[ChatHistoryDao, Map[Chat, ChatCache]] = ListMap.empty

  private var currentChatOption:      Option[ChatWithDao] = None
  private var loadMessagesInProgress: AtomicBoolean       = new AtomicBoolean(false)

  private val desktopOption = if (Desktop.isDesktopSupported) Some(Desktop.getDesktop) else None
  private val htmlKit       = new ExtendedHtmlEditorKit(desktopOption)
  private val msgService    = new MessagesService(htmlKit)
  private val chatSelGroup  = new ChatListItemSelectionGroup

  // TODO:
  // reply-to (make clickable)
  // word-wrap and narrower width
  // search
  // better pictures rendering
  // emoji and fonts

  val preloadResult: Future[_] = {
    val futureSeq = Seq(
      DataLoaders.preload(),
      Seq(Webp.preload())
    ).flatten
    futureSeq.reduce((a, b) => a.flatMap(_ => b))
  }

  override def top = new MainFrame {
    import org.fs.chm.BuildInfo._
    title    = s"$name v${version} b${new DateTime(builtAtMillis).toString("yyyyMMdd-HHmmss")}"
    contents = ui
    size     = new Dimension(1000, 700)
    peer.setLocationRelativeTo(null)

    // Install EDT exception handler
    Swing.onEDTWait {
      Thread.currentThread.setUncaughtExceptionHandler(handleException)
    }
  }

  val ui = new BorderPanel {
    import scala.swing.BorderPanel.Position._

    layout(menuBar) = North
    layout(chatsOuterPanel) = West
    layout(msgAreaContainer.panel) = Center
    layout {
      new BorderPanel {
        layout(statusLabel) = West
      }
    } = South
  }

  lazy val (menuBar, dbEmbeddedMenu) = {
    val separatorBeforeDb = new Separator()
    val separatorAfterDb  = new Separator()
    val dbMenu = new Menu("Database") {
      contents += menuItem("Open")(showOpenDialog())
      contents += separatorBeforeDb
      contents += separatorAfterDb
    }
    val dbEmbeddedMenu = new EmbeddedMenu(dbMenu, separatorBeforeDb, separatorAfterDb)
    val menuBar = new MenuBar {
      contents += dbMenu
      contents += new Menu("Edit") {
        contents += menuItem("Users")(showUsersDialog())
        contents += menuItem("Merge Datasets")(showSelectDatasetsToMergeDialog())
      }
    }
    (menuBar, dbEmbeddedMenu)
  }

  lazy val chatList = new DaoList(dao => new DaoChatItem(dao))

  lazy val statusLabel = new Label(" ")

  lazy val chatsOuterPanel = {
    new BorderPanel {
      import scala.swing.BorderPanel.Position._

      val panel2 = new BorderPanel {
        layout(chatList.panel) = North
        layout {
          // That's the only solution I came up with that worked to set a minimum width of an empty chat list
          // (setting minimum size doesn't work, setting preferred size screws up scrollbar)
          new BorderPanel {
            this.preferredWidth = DaoItem.PanelWidth
          }
        } = South
      }

      layout(new ScrollPane(panel2) {
        verticalScrollBar.unitIncrement = ComfortableScrollSpeed

        verticalScrollBarPolicy   = ScrollPane.BarPolicy.Always
        horizontalScrollBarPolicy = ScrollPane.BarPolicy.Never
      }) = Center
    }
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

  def freezeTheWorld(statusMsg: String): Unit = {
    statusLabel.text = statusMsg
    menuBar.contents foreach (_.enabled = false)
    changeChatsClickable(false)
  }

  def unfreezeTheWorld(): Unit = {
    statusLabel.text = " " // Empty string to prevent collapse
    menuBar.contents foreach (_.enabled = true)
    changeChatsClickable(true)
  }

  def changeChatsClickable(enabled: Boolean): Unit = {
    chatsOuterPanel.enabled = enabled
    def changeClickableRecursive(c: Component): Unit = c match {
      case i: DaoItem[_]      => i.enabled = enabled
      case c: Container       => c.contents foreach changeClickableRecursive
      case f: FillerComponent => // NOOP
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
      case FileChooser.Result.Approve if loadedDaos.keys.exists(_ isLoaded chooser.selectedFile.getParentFile) =>
        showWarning(s"File '${chooser.selectedFile}' is already loaded")
      case FileChooser.Result.Approve => {
        freezeTheWorld("Loading data...")
        config.update(DataLoaders.LastFileKey, chooser.selectedFile.getAbsolutePath)
        Future { // To release UI lock
          Swing.onEDT {
            try {
              val dao = DataLoaders.load(chooser.selectedFile)
              loadDaoInEDT(dao)
            } finally {
              unfreezeTheWorld()
            }
          }
        }
      }
    }
  }

  def close(dao: ChatHistoryDao): Unit = {
    checkEdt()
    MutationLock.synchronized {
      loadedDaos = loadedDaos - dao
      chatList.replaceWith(loadedDaos.keys.toSeq)
      dao.close()
    }
    daoListChanged()
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
        freezeTheWorld("Saving data...")
        config.update(DataLoaders.LastFileKey, chooser.selectedFile.getAbsolutePath)
        Future { // To release UI lock
          try {
            val dstDao = DataLoaders.saveAs(srcDao, chooser.selectedFile)
            Swing.onEDTWait {
              loadDaoInEDT(dstDao, Some(srcDao))
            }
          } finally {
            Swing.onEDT {
              unfreezeTheWorld()
            }
          }
        }
      }
    }
  }

  def showUsersDialog(): Unit = {
    val userList = new DaoList({ dao =>
      new DaoItem(
        dao,
        None, { ds =>
          dao.users(ds.uuid).zipWithIndex map {
            case (u, i) =>
              val pane = new UserDetailsPane(dao, u, false, Some(this))
              for (el <- Seq(pane.firstNameC, pane.lastNameC)) {
                el.innerComponent.foreground = Colors.forIdx(i)
                el.innerComponent.fontStyle  = Font.Style.Bold
              }
              pane
          }
        }
      )
    })
    userList.replaceWith(loadedDaos.keys.toSeq)
    userList.panel.preferredWidth = DaoItem.PanelWidth

    val outerPanel = new BorderPanel {
      import scala.swing.BorderPanel.Position._

      layout(new ScrollPane(userList.panel) {
        verticalScrollBar.unitIncrement = ComfortableScrollSpeed

        verticalScrollBarPolicy   = ScrollPane.BarPolicy.Always
        horizontalScrollBarPolicy = ScrollPane.BarPolicy.Never
      }) = Center
    }

    outerPanel.preferredHeight = 500

    Dialog.showMessage(title = "Users", message = outerPanel.peer, messageType = Dialog.Message.Plain)
  }

  def showSelectDatasetsToMergeDialog(): Unit = {
    if (loadedDaos.isEmpty) {
      showWarning("Load a database first!")
    } else if (!loadedDaos.exists(_._1.isMutable)) {
      showWarning("You'll need an editable database first. Save the one you want to use as base.")
    } else if (loadedDaos.keys.flatMap(_.datasets).size == 1) {
      showWarning("Only one dataset is loaded - nothing to merge.")
    } else {
      val selectDsDialog = new SelectMergeDatasetDialog(loadedDaos.keys.toSeq)
      selectDsDialog.visible = true
      selectDsDialog.selection foreach {
        case ((masterDao, masterDs), (slaveDao, slaveDs)) =>
          val selectChatsDialog = new SelectMergeChatsDialog(masterDao, masterDs, slaveDao, slaveDs)
          selectChatsDialog.visible = true
          selectChatsDialog.selection foreach { chatsToMerge =>
            val selectUsersDialog = new SelectMergeUsersDialog(masterDao, masterDs, slaveDao, slaveDs)
            selectUsersDialog.visible = true
            selectUsersDialog.selection foreach { usersToMerge =>
              mergeDatasets(masterDao, masterDs, slaveDao, slaveDs, usersToMerge, chatsToMerge)
            }
          }
      }
    }
  }

  //
  // Other stuff
  //

  def mergeDatasets(
      masterDao: MutableChatHistoryDao,
      masterDs: Dataset,
      slaveDao: ChatHistoryDao,
      slaveDs: Dataset,
      usersToMerge: Seq[UserMergeOption],
      chatsToMerge: Seq[ChatMergeOption]
  ): Unit = {
    val merger = new DatasetMerger(masterDao, masterDs, slaveDao, slaveDs)
    Swing.onEDT {
      freezeTheWorld("Analyzing chat messages...")
    }
    Future {
      // Analyze
      val analyzed = chatsToMerge.map(merger.analyzeChatHistoryMerge)
      val (resolved, cancelled) = analyzed.foldLeft((Seq.empty[ChatMergeOption], false)) {
        case ((res, stop), _) if stop =>
          (res, true)
        case ((res, _), (cmo @ ChatMergeOption.Combine(mc, sc, mismatches))) =>
          // Resolve mismatches
          val dialog =
            new SelectMergeMessagesDialog(masterDao, mc, slaveDao, sc, mismatches, htmlKit, msgService)
          dialog.visible = true
          dialog.selection
            .map(resolution => (res :+ (cmo.copy(messageMergeOptions = resolution)), false))
            .getOrElse((res, true))
        case ((res, _), cmo) =>
          (res :+ cmo, false)
      }
      if (cancelled) None else Some(resolved)
    } map { chatsMergeResolutionsOption: Option[Seq[ChatMergeOption]] =>
      // Merge
      Swing.onEDTWait {
        freezeTheWorld("Merging...")
      }
      chatsMergeResolutionsOption foreach { chatsMergeResolutions =>
        merger.merge(usersToMerge, chatsMergeResolutions)
        // TODO: daoListChanged? loadDaoInEDT?
        chatsOuterPanel.revalidate()
        chatsOuterPanel.repaint()
      }
    } onComplete { res =>
      res.failed.foreach(handleException)
      Swing.onEDT {
        unfreezeTheWorld()
      }
    }
  }

  def loadDaoInEDT(dao: ChatHistoryDao, daoToReplaceOption: Option[ChatHistoryDao] = None): Unit = {
    checkEdt()
    MutationLock.synchronized {
      daoToReplaceOption match {
        case Some(srcDao) =>
          val seq  = loadedDaos.toSeq
          val seq2 = seq.updated(seq.indexWhere(_._1 == srcDao), (dao -> Map.empty[Chat, ChatCache]))
          loadedDaos = ListMap(seq2: _*)
          chatList.replaceWith(loadedDaos.keys.toSeq)
          srcDao.close()
        case None =>
          loadedDaos = loadedDaos + (dao -> Map.empty) // TODO: Reverse?
          chatList.append(dao)
      }
    }
    daoListChanged()
    unfreezeTheWorld()
  }

  def daoListChanged(): Unit = {
    dbEmbeddedMenu.clear()
    for (dao <- loadedDaos.keys) {
      val daoMenu = new Menu(dao.name) {
        contents += menuItem("Save As...")(showSaveAsDialog(dao))
        contents += menuItem("Close")(close(dao))
      }
      dbEmbeddedMenu.append(daoMenu)
    }
    chatsOuterPanel.revalidate()
    chatsOuterPanel.repaint()
  }

  override def renameDataset(dsUuid: UUID, newName: String, dao: ChatHistoryDao): Unit = {
    checkEdt()
    require(dao.isMutable, "DAO is immutable!")
    freezeTheWorld("Renaming...")
    Swing.onEDT { // To release UI lock
      try {
        MutationLock.synchronized {
          dao.mutable.renameDataset(dsUuid, newName)
          chatList.replaceWith(loadedDaos.keys.toSeq)
        }
        chatsOuterPanel.revalidate()
        chatsOuterPanel.repaint()
      } finally {
        unfreezeTheWorld()
      }
    }
  }

  override def userEdited(user: User, dao: ChatHistoryDao): Unit = {
    checkEdt()
    require(dao.isMutable, "DAO is immutable!")
    freezeTheWorld("Modifying...")
    asyncChangeUsers(dao, {
      dao.mutable.updateUser(user)
      Seq(user.id)
    })
  }

  override def usersMerged(baseUser: User, absorbedUser: User, dao: ChatHistoryDao): Unit = {
    checkEdt()
    require(dao.isMutable, "DAO is immutable!")
    require(baseUser.dsUuid == absorbedUser.dsUuid, "Users are from different datasets!")
    freezeTheWorld("Modifying...")
    asyncChangeUsers(dao, {
      dao.mutable.mergeUsers(baseUser, absorbedUser)
      Seq(baseUser.id, absorbedUser.id)
    })
  }

  override def deleteChat(cc: ChatWithDao): Unit = {
    freezeTheWorld("Deleting...")
    Swing.onEDT {
      try {
        MutationLock.synchronized {
          cc.dao.mutable.deleteChat(cc.chat)
          evictFromCache(cc.dao, cc.chat)
          chatList.replaceWith(loadedDaos.keys.toSeq)
        }
        chatsOuterPanel.revalidate()
        chatsOuterPanel.repaint()
      } finally {
        unfreezeTheWorld()
      }
    }
  }

  override def chatSelected(cc: ChatWithDao): Unit = {
    MutationLock.synchronized {
      currentChatOption         = None
      msgAreaContainer.document = msgService.pleaseWaitDoc
      if (!loadedDaos(cc.dao).contains(cc.chat)) {
        updateCache(cc.dao, cc.chat, ChatCache(None, None))
      }
      freezeTheWorld("Loading chat...")
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
            firstOption  = messages.headOption,
            lastOption   = messages.lastOption,
            beginReached = messages.size < MsgBatchLoadSize,
            endReached   = true
          )
          updateCache(cc.dao, cc.chat, ChatCache(Some(msgDoc), Some(loadStatus)))
      }
    }
    f foreach { _ =>
      Swing.onEDTWait(MutationLock.synchronized {
        currentChatOption = Some(cc)
        val doc = loadedDaos(cc.dao)(cc.chat).msgDocOption.get.doc
        msgAreaContainer.document = doc
        msgAreaContainer.scroll.toEnd() // FIXME: Doesn't always work!
        unfreezeTheWorld()
      })
    }
  }

  def tryLoadPreviousMessages(): Unit = {
    log.debug("Trying to load previous messages")
    currentChatOption match {
      case _ if loadMessagesInProgress.get => log.debug("Loading messages: Already in progress")
      case None                            => log.debug("Loading messages: No chat selected")
      case Some(cc) =>
        MutationLock.synchronized {
          msgAreaContainer.caretUpdatesEnabled = false
          val cache      = loadedDaos(cc.dao)(cc.chat)
          val loadStatus = cache.loadStatusOption.get
          log.debug(s"Loading messages: loadStatus = ${loadStatus}")
          if (!loadStatus.beginReached) {
            freezeTheWorld("Loading messages...")
            loadMessagesInProgress set true
            val msgDoc = cache.msgDocOption.get
            val f = Future {
              MutationLock.synchronized {
                val (viewPos1, viewSize1) = msgAreaContainer.view.posAndSize
                msgDoc.insert("<div id=\"loading\"><hr><p> Loading... </p><hr></div>", MessageInsertPosition.Leading)
                assert(loadStatus.firstOption.isDefined)
                val addedMessages = loadStatus.firstOption match {
                  case Some(first) => cc.dao.messagesBefore(cc.chat, first, MsgBatchLoadSize + 1).dropRight(1)
                  case None        => throw new MatchError("Uh-oh, this shouldn't happen!")
                }
                log.debug(s"Loading messages: Loaded ${addedMessages.size} previous messages")
                updateCache(
                  cc.dao,
                  cc.chat,
                  cache.copy(loadStatusOption = Some {
                    loadStatus.copy(
                      firstOption  = addedMessages.headOption,
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
                  log.debug("Loading messages: Reloaded message container")
                }
              }
            }
            f.onComplete(_ =>
              Swing.onEDTWait(MutationLock.synchronized {
                loadMessagesInProgress set false
                unfreezeTheWorld()
                msgAreaContainer.caretUpdatesEnabled = true
              }))
          }
        }
    }
  }

  /** Asynchronously apply the given change (under mutation lock) and refresh UI to reflect it */
  def asyncChangeUsers(dao: ChatHistoryDao, applyChangeAndReturnChangedIds: => Seq[Long]): Unit = {
    Future { // To release UI lock
      try {
        val userIds = MutationLock.synchronized {
          val userIds = applyChangeAndReturnChangedIds
          Swing.onEDTWait {
            chatList.replaceWith(loadedDaos.keys.toSeq)
          }
          userIds
        }
        chatsOuterPanel.revalidate()
        chatsOuterPanel.repaint()
        MutationLock.synchronized {
          // Evict chats containing edited user from cache
          val chatsToEvict = for {
            (chat, _) <- loadedDaos(dao)
            if dao.interlocutors(chat) exists (u => userIds contains u.id)
          } yield chat
          chatsToEvict foreach (c => evictFromCache(dao, c))

          // Reload currently selected chat
          val chatItemToReload = for {
            cwd  <- currentChatOption
            item <- chatList.innerItems.find(i => i.chat.id == cwd.chat.id && i.chat.dsUuid == cwd.dsUuid)
          } yield item

          chatItemToReload match {
            case Some(chatItem) =>
              // Redo current chat layout
              chatItem.select()
            case None =>
              // No need to do anything
              Swing.onEDT {
                unfreezeTheWorld()
              }
          }
        }
      } catch {
        case ex: Exception =>
          SwingUtils.showError(ex.getMessage)
          Swing.onEDT {
            unfreezeTheWorld()
          }
      }
    }
  }

  def updateCache(dao: ChatHistoryDao, chat: Chat, cache: ChatCache): Unit =
    MutationLock.synchronized {
      loadedDaos = loadedDaos + (dao -> (loadedDaos(dao) + (chat -> cache)))
    }

  def evictFromCache(dao: ChatHistoryDao, chat: Chat): Unit =
    MutationLock.synchronized {
      if (loadedDaos.contains(dao)) {
        loadedDaos = loadedDaos + (dao -> (loadedDaos(dao) - chat))
      }
    }

  private def handleException(thread: Thread, ex: Throwable): Unit =
    handleException(ex)

  @tailrec
  private def handleException(ex: Throwable): Unit =
    if (ex.getCause != null && ex.getCause != ex) {
      handleException(ex.getCause)
    } else {
      ex match {
        case ex: IllegalArgumentException =>
          log.warn("Caught an exception:", ex)
          SwingUtils.showWarning(ex.getMessage)
        case _ =>
          log.error("Caught an exception:", ex)
          SwingUtils.showError(ex.getMessage)
      }
    }

  private class DaoChatItem(dao: ChatHistoryDao)
      extends DaoItem(
        dao             = dao,
        callbacksOption = Some(this),
        getInnerItems = { ds =>
          dao.chats(ds.uuid) map (c => new ChatListItem(ChatWithDao(c, dao), Some(chatSelGroup), Some(this)))
        }
      )

  private case class ChatCache(
      msgDocOption: Option[MessageDocument],
      loadStatusOption: Option[LoadStatus]
  )

  private case class LoadStatus(
      firstOption: Option[Message],
      lastOption: Option[Message],
      beginReached: Boolean,
      endReached: Boolean
  )

  private object DataLoaders {
    val LastFileKey = "last_database_file"

    private val h2      = new H2DataManager
    private val tg      = new TelegramDataLoader
    private val gts5610 = new GTS5610DataLoader

    /** Initializes DAOs to speed up subsequent calls */
    def preload(): Seq[Future[_]] = {
      h2.preload()
    }

    private val h2ff = easyFileFilter(
      s"${BuildInfo.name} database (*.${H2DataManager.DefaultExt})"
    )(_.getName endsWith ("." + H2DataManager.DefaultExt))

    private val tgFf = easyFileFilter(
      "Telegram export JSON database (result.json)"
    )(_.getName == "result.json")

    private val gts5610Ff = easyFileFilter(
      s"Samsung GT-S5610 export vMessage files [choose any in folder] (*.${GTS5610DataLoader.DefaultExt})"
    ) { _.getName endsWith ("." + GTS5610DataLoader.DefaultExt) }

    val openChooser = new FileChooser(null) {
      title = "Select a database to open"
      peer.addChoosableFileFilter(h2ff)
      peer.addChoosableFileFilter(tgFf)
      peer.addChoosableFileFilter(gts5610Ff)
    }

    def load(file: File): ChatHistoryDao = {
      if (h2ff.accept(file)) {
        h2.loadData(file.getParentFile)
      } else if (tgFf.accept(file)) {
        tg.loadData(file.getParentFile)
      } else if (gts5610Ff.accept(file)) {
        gts5610.loadData(file.getParentFile)
      } else {
        throw new IllegalStateException("Unknown file type!")
      }
    }

    val saveAsChooser = new FileChooser(null) {
      title             = "Choose a directory where the new database will be stored"
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
