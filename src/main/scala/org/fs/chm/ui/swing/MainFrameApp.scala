package org.fs.chm.ui.swing

import java.awt.Desktop
import java.awt.Toolkit
import java.awt.event.AdjustmentEvent
import java.io.File
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean

import scala.annotation.tailrec
import scala.collection.immutable.ListMap
import scala.concurrent.CancellationException
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.swing._

import com.github.nscala_time.time.Imports._
import javax.swing.SwingUtilities
import javax.swing.event.HyperlinkEvent
import org.fs.chm.BuildInfo
import org.fs.chm.dao._
import org.fs.chm.dao.merge.DatasetMerger
import org.fs.chm.dao.merge.DatasetMerger._
import org.fs.chm.loader._
import org.fs.chm.loader.telegram._
import org.fs.chm.ui.swing.general.ExtendedHtmlEditorKit
import org.fs.chm.ui.swing.general.SwingUtils
import org.fs.chm.ui.swing.general.SwingUtils._
import org.fs.chm.ui.swing.list.DaoDatasetSelectionCallbacks
import org.fs.chm.ui.swing.list.DaoItem
import org.fs.chm.ui.swing.list.DaoList
import org.fs.chm.ui.swing.list.chat._
import org.fs.chm.ui.swing.merge._
import org.fs.chm.ui.swing.messages.MessageNavigationCallbacks
import org.fs.chm.ui.swing.messages.MessagesRenderingComponent
import org.fs.chm.ui.swing.messages.impl.MessagesAreaContainer
import org.fs.chm.ui.swing.user.UserDetailsMenuCallbacks
import org.fs.chm.ui.swing.user.UserDetailsPane
import org.fs.chm.utility.CliUtils
import org.fs.chm.utility.EntityUtils
import org.fs.chm.utility.InterruptableFuture._
import org.fs.chm.utility.IoUtils._
import org.fs.chm.utility.SimpleConfigAware
import org.slf4s.Logging

class MainFrameApp(grpcDataLoader: TelegramGRPCDataLoader) //
    extends SimpleSwingApplication
    with SimpleConfigAware
    with Logging
    with DaoDatasetSelectionCallbacks
    with ChatListSelectionCallbacks
    with UserDetailsMenuCallbacks
    with MessageNavigationCallbacks { app =>

  type MD = MessagesAreaContainer.MessageDocument

  /** A lock which needs to be taken to mutate local variables or DAO */
  private val MutationLock           = new Object
  private val MsgBatchLoadSize       = 100
  private val MinScrollToTriggerLoad = 1000

  private var initialFileOption: Option[File] = None

  private var loadedDaos: ListMap[ChatHistoryDao, Map[Chat, ChatCache]] = ListMap.empty

  private var currentChatOption:      Option[(ChatHistoryDao, ChatWithDetails)] = None
  private var loadMessagesInProgress: AtomicBoolean                             = new AtomicBoolean(false)

  private val desktopOption = if (Desktop.isDesktopSupported) Some(Desktop.getDesktop) else None
  private val htmlKit       = new ExtendedHtmlEditorKit(desktopOption)
  private val chatSelGroup  = new ChatListItemSelectionGroup

  /*
   * TODO:
   *  - merge only newer messages
   *  - reply-to (make clickable)
   *  - word-wrap and narrower width
   *  - search
   *  - better pictures rendering
   *  - emoji and fonts
   *  - fucked up merge layout
   *  - delete orphan users
   *  - better tabs?
   *  - go to date
   *  - cache document view position
   */

  val preloadResult: Future[_] = {
    val futureSeq = Seq(
      DataLoaders.preload(),
    ).flatten
    futureSeq.reduce((a, b) => a.flatMap(_ => b))
  }

  override lazy val top = new MainFrame {
    import org.fs.chm.BuildInfo._
    title    = s"$name v${version} b${new DateTime(builtAtMillis).toString("yyyyMMdd-HHmmss")}"
    contents = ui
    size     = new Dimension(1000, 700)
    peer.setLocationRelativeTo(null)

    Swing.onEDTWait {
      // Install EDT exception handler
      Thread.currentThread.setUncaughtExceptionHandler(handleException)

      if (initialFileOption.isDefined) {
        freezeTheWorld("")
      }
    }

    initialFileOption map (f => Future { Swing.onEDT { openDb(f) } })
  }

  lazy val ui = new BorderPanel {
    import scala.swing.BorderPanel.Position._

    layout(menuBar) = North
    layout(chatsOuterPanel) = West
    layout(msgRenderer.component) = Center
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
      contents += menuItem("Open")(showOpenDbDialog())
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

  lazy val msgRenderer: MessagesRenderingComponent[MD] = {
    import org.fs.chm.ui.swing.messages.impl.MessagesAreaEnhancedContainer

    val m = new MessagesAreaEnhancedContainer(htmlKit, this)

    // Load older messages when sroll is near the top
    val sb = m.scrollPane.verticalScrollBar.peer
    sb.addAdjustmentListener((e: AdjustmentEvent) => {
      sb.getMinimum
      if (!e.getValueIsAdjusting) {
        if (e.getValue < MinScrollToTriggerLoad) {
          tryLoadPreviousMessages()
        } else if (sb.getMaximum - sb.getVisibleAmount - e.getValue < MinScrollToTriggerLoad) {
          tryLoadNextMessages()
        }
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

  def setStatus(statusMsg: String): Unit = {
    log.info("Status: " + statusMsg)
    if (SwingUtilities.isEventDispatchThread) {
      statusLabel.text = statusMsg
    } else {
      Swing.onEDTWait {
        statusLabel.text = statusMsg
      }
    }
  }

  def freezeTheWorld(statusMsg: String): Unit = {
    checkEdt()
    setStatus(statusMsg)
    menuBar.contents foreach (_.enabled = false)
    changeChatsClickable(false)
  }

  def unfreezeTheWorld(): Unit = {
    checkEdt()
    setStatus(" ") // Empty string to prevent collapse
    menuBar.contents foreach (_.enabled = true)
    changeChatsClickable(true)
  }

  def worldFreezingIFuture[T](statusMsg: String)(body: => T): InterruptableFuture[T] = {
    val ifuture = Future.interruptibly {
      Swing.onEDT(freezeTheWorld(statusMsg))
      body
    }
    ifuture.future onComplete { res =>
      res.failed.foreach(handleException)
      Swing.onEDT {
        unfreezeTheWorld()
      }
    }
    ifuture
  }

  def changeChatsClickable(enabled: Boolean): Unit = {
    checkEdt()
    chatsOuterPanel.enabled = enabled
    def changeClickableRecursive(c: Component): Unit = c match {
      case i: DaoItem[_]      => i.enabled = enabled
      case c: Container       => c.contents foreach changeClickableRecursive
      case _: FillerComponent => // NOOP
    }
    changeClickableRecursive(chatsOuterPanel)
  }

  //
  // Events
  //

  def showOpenDbDialog(): Unit = {
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
      case FileChooser.Result.Approve =>
        openDb(chooser.selectedFile)
    }
  }

  def openDb(file: File): Unit = {
    checkEdt()
    freezeTheWorld("Loading data...")
    config.update(DataLoaders.LastFileKey, file.getAbsolutePath)
    Future { // To release UI lock
      try {
        val dao = DataLoaders.load(file)
        Swing.onEDT {
          try {
            loadDaoInEDT(dao)
          } finally {
            unfreezeTheWorld()
          }
        }
      } catch {
        case th: Throwable =>
          Swing.onEDT { unfreezeTheWorld() }
          log.error("Exception while opening a database:", th)
          showError(th.getMessage)
      }
    }
  }

  def closeDb(dao: ChatHistoryDao): Unit = {
    checkEdt()
    freezeTheWorld("Closing...")
    Future {
      Swing.onEDT {
        try {
          MutationLock.synchronized {
            loadedDaos = loadedDaos - dao
            chatList.replaceWith(loadedDaos.keys.toSeq)
            dao.close()
          }
          daoListChanged()
        } finally {
          unfreezeTheWorld()
        }
      }
    }
  }

  def showSaveDbAsDialog(srcDao: ChatHistoryDao): Unit = {
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

    outerPanel.preferredHeight = Toolkit.getDefaultToolkit.getScreenSize.height - 100

    Dialog.showMessage(title = "Users", message = outerPanel.peer, messageType = Dialog.Message.Plain)
  }

  def showSelectDatasetsToMergeDialog(): Unit = {
    checkEdt()
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
            val merger = new DatasetMerger(masterDao, masterDs, slaveDao, slaveDs)
            val analyzeChatsF = analyzeChatsFuture(merger, chatsToMerge)
            val selectUsersDialog = new SelectMergeUsersDialog(masterDao, masterDs, slaveDao, slaveDs)
            selectUsersDialog.visible = true
            selectUsersDialog.selection match {
              case Some(usersToMerge) =>
                analyzeChatsF.future.foreach(analyzed => mergeDatasets(merger, analyzed, usersToMerge))
              case None =>
                analyzeChatsF.cancel()
            }
          }
      }
    }
  }

  //
  // Other stuff
  //

  def analyzeChatsFuture(
      merger: DatasetMerger,
      chatsToMerge: Seq[ChatMergeOption]
  ): InterruptableFuture[Seq[ChatMergeOption]] =
    worldFreezingIFuture("Analyzing chat messages...") {
      chatsToMerge.map { cmo =>
        if (Thread.interrupted()) {
          throw new InterruptedException()
        }
        val chat = (cmo.slaveCwdOption orElse cmo.masterCwdOption).get.chat
        setStatus(s"Analyzing '${EntityUtils.getOrUnnamed(chat.nameOption)}' (${chat.msgCount} messages)...")
        merger.analyzeChatHistoryMerge(cmo)
      }
    }

  def mergeDatasets(
      merger: DatasetMerger,
      analyzed: Seq[ChatMergeOption],
      usersToMerge: Seq[UserMergeOption]
  ): Unit = {
    // TODO: Make async, with other chats merging working in the background while users makes the choice
    worldFreezingIFuture("Combining chats...") {
      val (resolved, cancelled) = analyzed.foldLeft((Seq.empty[ChatMergeOption], false)) {
        case ((res, stop), _) if stop =>
          (res, true)
        case ((res, _), (cmo @ ChatMergeOption.Combine(mcwd, scwd, mismatches))) =>
          setStatus(s"Combining '${EntityUtils.getOrUnnamed(scwd.chat.nameOption)}' (${scwd.chat.msgCount} messages)...")
          // Resolve mismatches
          if (mismatches.forall(_.isInstanceOf[MessagesMergeOption.Keep])) {
            // User has no choice - pass them as-is
            (res :+ cmo, false)
          } else {
            val dialog = onEdtReturning {
              new SelectMergeMessagesDialog(merger.masterDao, mcwd, merger.slaveDao, scwd, mismatches, htmlKit)
            }
            dialog.visible = true
            dialog.selection
              .map(resolution => (res :+ (cmo.copy(messageMergeOptions = resolution)), false))
              .getOrElse((res, true))
          }
        case ((res, _), cmo) =>
          (res :+ cmo, false)
      }
      if (cancelled) None else Some(resolved)
    }.future map { chatsMergeResolutionsOption: Option[Seq[ChatMergeOption]] =>
      chatsMergeResolutionsOption foreach { chatsMergeResolutions =>
        // Merge
        worldFreezingIFuture("Merging...") {
          MutationLock.synchronized {
            merger.merge(usersToMerge, chatsMergeResolutions)
            Swing.onEDTWait {
              chatList.replaceWith(loadedDaos.keys.toSeq)
              chatsOuterPanel.revalidate()
              chatsOuterPanel.repaint()
            }
          }
        }
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
        contents += menuItem("Save As...")(showSaveDbAsDialog(dao))
        contents += menuItem("Close")(closeDb(dao))
      }
      dbEmbeddedMenu.append(daoMenu)
    }
    chatsOuterPanel.revalidate()
    chatsOuterPanel.repaint()
  }

  override def renameDataset(dao: ChatHistoryDao, dsUuid: UUID, newName: String): Unit = {
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

  override def deleteDataset(dao: ChatHistoryDao, dsUuid: UUID): Unit = {
    checkEdt()
    require(dao.isMutable, "DAO is immutable!")
    freezeTheWorld("Deleting...")
    Swing.onEDT { // To release UI lock
      try {
        MutationLock.synchronized {
          dao.mutable.deleteDataset(dsUuid)
          chatList.replaceWith(loadedDaos.keys.toSeq)
        }
        chatsOuterPanel.revalidate()
        chatsOuterPanel.repaint()
      } finally {
        unfreezeTheWorld()
      }
    }
  }

  override def shiftDatasetTime(dao: ChatHistoryDao, dsUuid: UUID, hrs: Int): Unit = {
    checkEdt()
    require(dao.isMutable, "DAO is immutable!")
    freezeTheWorld("Shifting time...")
    Swing.onEDT { // To release UI lock
      try {
        MutationLock.synchronized {
          dao.mutable.shiftDatasetTime(dsUuid, hrs)
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

  override def deleteChat(dao: ChatHistoryDao, chat: Chat): Unit = {
    freezeTheWorld("Deleting...")
    Swing.onEDT {
      try {
        MutationLock.synchronized {
          dao.mutable.deleteChat(chat)
          evictFromCache(dao, chat)
          chatList.replaceWith(loadedDaos.keys.toSeq)
        }
        chatsOuterPanel.revalidate()
        chatsOuterPanel.repaint()
      } finally {
        unfreezeTheWorld()
      }
    }
  }

  override def chatSelected(dao: ChatHistoryDao, cwd: ChatWithDetails): Unit = {
    checkEdt()
    MutationLock.synchronized {
      currentChatOption = None
      msgRenderer.renderPleaseWait()
      if (!loadedDaos(dao).contains(cwd.chat)) {
        updateCache(dao, cwd.chat, ChatCache(None, None))
      }
      freezeTheWorld("Loading chat...")
    }
    Future {
      MutationLock.synchronized {
        currentChatOption = Some(dao -> cwd)
        loadMessagesInProgress set true
      }
      // If the chat has been already rendered, restore previous document as-is
      if (loadedDaos(dao)(cwd.chat).msgDocOption.isEmpty) {
        loadLastMessagesAndUpdateCache(dao, cwd)
      }
      Swing.onEDTWait(MutationLock.synchronized {
        val doc = loadedDaos(dao)(cwd.chat).msgDocOption.get
        msgRenderer.render(doc, false)
        loadMessagesInProgress set false
        unfreezeTheWorld()
      })
    }
  }

  override def navigateToBeginning(): Unit = {
    checkEdt()
    freezeTheWorld("Navigating...")
    Future {
      currentChatOption match {
        case Some((dao, cwd)) =>
          val cache = loadedDaos(dao)(cwd.chat)
          cache.loadStatusOption match {
            case Some(ls) if ls.beginReached =>
              // Just scroll
              Swing.onEDTWait(msgRenderer.render(cache.msgDocOption.get, true))
            case _ =>
              loadMessagesInProgress set true
              loadFirstMessagesAndUpdateCache(dao, cwd)
          }
        case None =>
          () // NOOP
      }
      Swing.onEDT {
        loadMessagesInProgress set false
        unfreezeTheWorld()
      }
    }
  }

  override def navigateToEnd(): Unit = {
    checkEdt()
    freezeTheWorld("Navigating...")
    Future {
      currentChatOption match {
        case Some((dao, cwd)) =>
          val cache = loadedDaos(dao)(cwd.chat)
          cache.loadStatusOption match {
            case Some(ls) if ls.endReached =>
              // Just scroll
              Swing.onEDTWait(msgRenderer.render(cache.msgDocOption.get, false))
            case _ =>
              loadMessagesInProgress set true
              loadLastMessagesAndUpdateCache(dao, cwd)
          }
        case None =>
          () // NOOP
      }
      Swing.onEDT {
        loadMessagesInProgress set false
        unfreezeTheWorld()
      }
    }
  }

  override def navigateToDate(date: DateTime): Unit = {
    // FIXME: This doesn't work!
    checkEdt()
    freezeTheWorld("Navigating...")
    Future {
      currentChatOption match {
        case Some((dao, cwd)) =>
          // TODO: Don't replace a document if currently cached document already contains message?
          loadMessagesInProgress set true
          loadDateMessagesAndUpdateCache(dao, cwd, date)
        case None =>
          () // NOOP
      }
      Swing.onEDT {
        loadMessagesInProgress set false
        unfreezeTheWorld()
      }
    }
  }

  def tryLoadPreviousMessages(): Unit = {
    log.debug("Trying to load previous messages")
    tryLoadMessages(
      ls => !ls.beginReached,
      (dao, cwd, ls) => {
        val newMsgs = dao.messagesBefore(cwd.chat, ls.firstOption.get, MsgBatchLoadSize + 1).dropRight(1)
        val ls2     = ls.copy(firstOption = newMsgs.headOption, beginReached = newMsgs.size < MsgBatchLoadSize)
        (newMsgs, ls2)
      },
      (dao, cwd, msgs, ls) => {
        msgRenderer.prepend(dao, cwd, msgs, ls.beginReached)
      }
    )
  }

  def tryLoadNextMessages(): Unit = {
    log.debug("Trying to load next messages")
    tryLoadMessages(
      ls => !ls.endReached,
      (dao, cwd, ls) => {
        val newMsgs = dao.messagesAfter(cwd.chat, ls.lastOption.get, MsgBatchLoadSize + 1).drop(1)
        val ls2     = ls.copy(lastOption = newMsgs.lastOption, endReached = newMsgs.size < MsgBatchLoadSize)
        (newMsgs, ls2)
      },
      (dao, cwd, msgs, ls) => {
        msgRenderer.append(dao, cwd, msgs, ls.endReached)
      }
    )
  }

  def tryLoadMessages(
      shouldLoad: LoadStatus => Boolean,
      load: (ChatHistoryDao, ChatWithDetails, LoadStatus) => (IndexedSeq[Message], LoadStatus),
      addToRender: (ChatHistoryDao, ChatWithDetails, IndexedSeq[Message], LoadStatus) => MD
  ): Unit = {
    currentChatOption match {
      case _ if loadMessagesInProgress.get => log.debug("Loading messages: Already in progress")
      case None                            => log.debug("Loading messages: No chat selected")
      case Some((dao, cwd)) =>
        val cache      = loadedDaos(dao)(cwd.chat)
        val loadStatus = cache.loadStatusOption.get
        log.debug(s"Loading messages: loadStatus = ${loadStatus}")
        if (shouldLoad(loadStatus)) {
          freezeTheWorld("Loading messages...")
          msgRenderer.updateStarted()
          loadMessagesInProgress set true
          val f = Future {
            Swing.onEDTWait(msgRenderer.prependLoading())
            assert(loadStatus.firstOption.isDefined)
            assert(loadStatus.lastOption.isDefined)
            val (addedMessages, loadStatus2) = load(dao, cwd, loadStatus)
            log.debug(s"Loading messages: Loaded ${addedMessages.size} messages")
            Swing.onEDTWait(MutationLock.synchronized {
              val md =  addToRender(dao, cwd, addedMessages, loadStatus2)
              log.debug("Loading messages: Reloaded message container")
              updateCache(dao, cwd.chat, ChatCache(Some(md), Some(loadStatus2)))
            })
          }
          f.onComplete(_ =>
            Swing.onEDTWait(MutationLock.synchronized {
              msgRenderer.updateFinished()
              loadMessagesInProgress set false
              unfreezeTheWorld()
            }))
        }
    }
  }

  def loadFirstMessagesAndUpdateCache(dao: ChatHistoryDao, cwd: ChatWithDetails): Unit = {
    val msgs = dao.firstMessages(cwd.chat, MsgBatchLoadSize)
    Swing.onEDTWait {
      val md = msgRenderer.render(dao, cwd, msgs, true, true)
      val loadStatus = LoadStatus(
        firstOption  = msgs.headOption,
        lastOption   = msgs.lastOption,
        beginReached = true,
        endReached   = msgs.size < MsgBatchLoadSize
      )
      updateCache(dao, cwd.chat, ChatCache(Some(md), Some(loadStatus)))
    }
  }

  def loadLastMessagesAndUpdateCache(dao: ChatHistoryDao, cwd: ChatWithDetails): Unit = {
    val msgs = dao.lastMessages(cwd.chat, MsgBatchLoadSize)
    Swing.onEDTWait {
      val md = msgRenderer.render(dao, cwd, msgs, msgs.size < MsgBatchLoadSize, false)
      val loadStatus = LoadStatus(
        firstOption  = msgs.headOption,
        lastOption   = msgs.lastOption,
        beginReached = msgs.size < MsgBatchLoadSize,
        endReached   = true
      )
      updateCache(dao, cwd.chat, ChatCache(Some(md), Some(loadStatus)))
    }
  }

  def loadDateMessagesAndUpdateCache(dao: ChatHistoryDao, cwd: ChatWithDetails, date: DateTime): Unit = {
    val (msgsB, msgsA) = dao.messagesAroundDate(cwd.chat, date, MsgBatchLoadSize)
    val msgs = msgsB ++ msgsA
    Swing.onEDTWait {
      val md = {
        msgRenderer.render(dao, cwd, msgsA, false, true)
        // FIXME: Viewport is not updated!
        msgRenderer.updateStarted()
        val md = msgRenderer.prepend(dao, cwd, msgsB, msgsB.size < MsgBatchLoadSize)
        msgRenderer.updateFinished()
        md
      }
      val loadStatus = LoadStatus(
        firstOption  = msgs.headOption,
        lastOption   = msgs.lastOption,
        beginReached = msgsB.size < MsgBatchLoadSize,
        endReached   = msgsA.size < MsgBatchLoadSize
      )
      updateCache(dao, cwd.chat, ChatCache(Some(md), Some(loadStatus)))
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
            if userIds.toSet.intersect(chat.memberIds).nonEmpty
          } yield chat
          chatsToEvict foreach (c => evictFromCache(dao, c))

          // Reload currently selected chat
          val chatItemToReload = for {
            (_, cwd) <- currentChatOption
            item     <- chatList.innerItems.find(i => i.chat.id == cwd.chat.id && i.chat.dsUuid == cwd.dsUuid)
          } yield item

          Swing.onEDT {
            chatItemToReload match {
              case Some(chatItem) =>
                // Redo current chat layout
                chatItem.select()
              case None =>
                // No need to do anything
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

  //
  // Utility and classes
  //

  override def startup(args: Array[String]): Unit = {
    try {
      initialFileOption = CliUtils.parse(args, "db", true).map(new File(_))
    } catch {
      case ex: Throwable => handleException(ex)
    }
    super.startup(args)
  }

  private def handleException(thread: Thread, ex: Throwable): Unit =
    handleException(ex)

  @tailrec
  private def handleException(ex: Throwable): Unit =
    if (ex.getCause != null && ex.getCause != ex) {
      handleException(ex.getCause)
    } else {
      ex match {
        case ex: CancellationException =>
          log.warn("Execution cancelled")
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
          dao.chats(ds.uuid) map (cwd => new ChatListItem(dao, cwd, Some(chatSelGroup), Some(this)))
        }
      )

  private case class ChatCache(
      msgDocOption: Option[MD],
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

    private val h2       = new H2DataManager
    private val tgFull   = new TelegramFullDataLoader
    private val tgSingle = new TelegramSingleChatDataLoader
    private val gts5610  = new GTS5610DataLoader

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
      val f = file.getParentFile
      if (h2ff.accept(file)) {
        h2.loadData(f)
      } else if (tgFf.accept(file)) {
        val loadersWithErrors =
          Seq(grpcDataLoader, tgFull, tgSingle).map(l => (l, l.doesLookRight(f)))
        loadersWithErrors.find(_._2.isEmpty) match {
          case Some((loader, _)) => loader.loadData(f)
          case None => {
            val errors = loadersWithErrors.map(_._1).toIndexedSeq
            throw new IllegalStateException(
              "Not a telegram format: Errors:\n"
                + s"(from gRPC loader) ${errors(0)}\n"
                + s"(as a full history) ${errors(1)}\n"
                + s"(as a single chat) ${errors(2)}")
          }
        }
      } else if (gts5610Ff.accept(file)) {
        gts5610.loadData(f)
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
      val dstDao = h2.create(dir)
      dstDao.copyAllFrom(srcDao)
      dstDao
    }
  }
}
