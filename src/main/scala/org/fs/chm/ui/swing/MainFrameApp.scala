package org.fs.chm.ui.swing

import java.awt.Desktop
import java.awt.Toolkit
import java.awt.event.AdjustmentEvent
import java.io.{File => JFile}

import scala.annotation.tailrec
import scala.collection.immutable.ListMap
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.swing._

import com.github.nscala_time.time.Imports._
import javax.swing.SwingUtilities
import javax.swing.event.HyperlinkEvent

import scala.util.Failure

import org.fs.chm.BuildInfo
import org.fs.chm.dao.ChatHistoryDao
import org.fs.chm.dao.Entities._
import org.fs.chm.dao.GrpcChatHistoryDao
import org.fs.chm.dao.merge.DatasetMerger
import org.fs.chm.dao.merge.DatasetMerger._
import org.fs.chm.dao.merge.DatasetMergerRemote
import org.fs.chm.loader._
import org.fs.chm.protobuf.Chat
import org.fs.chm.protobuf.Message
import org.fs.chm.protobuf.PbUuid
import org.fs.chm.protobuf.User
import org.fs.chm.ui.swing.general.ExtendedHtmlEditorKit
import org.fs.chm.ui.swing.general.SwingUtils
import org.fs.chm.ui.swing.general.SwingUtils._
import org.fs.chm.ui.swing.list.DaoItem
import org.fs.chm.ui.swing.list.DaoList
import org.fs.chm.ui.swing.list.chat._
import org.fs.chm.ui.swing.merge._
import org.fs.chm.ui.swing.messages.MessagesRenderingComponent
import org.fs.chm.ui.swing.messages.impl.MessagesAreaContainer
import org.fs.chm.ui.swing.user.UserDetailsPane
import org.fs.chm.utility.CliUtils
import org.fs.chm.utility.InterruptableFuture._
import org.fs.chm.utility.LangUtils._
import org.fs.chm.utility.SimpleConfigAware
import org.fs.utility.Imports._
import org.fs.utility.StopWatch
import org.slf4s.Logging

class MainFrameApp(grpcPort: Int) //
    extends SimpleSwingApplication
    with SimpleConfigAware
    with Logging
    with Callbacks.ChatCb
    with Callbacks.UserDetailsMenuCb
    with Callbacks.MessageHistoryCb { app =>

  type MD = MessagesAreaContainer.MessageDocument

  /** A lock which needs to be taken to mutate local variables or DAO */
  private val MutationLock           = new Object
  private val MsgBatchLoadSize       = 100
  private val MinScrollToTriggerLoad = 1000

  private var initialFileOption: Option[JFile] = None

  private var loadedDaos: ListMap[GrpcChatHistoryDao, Map[CombinedChat, ChatCache]] = ListMap.empty

  private var currentChatOption:      Option[(GrpcChatHistoryDao, CombinedChat)] = None
  private var loadMessagesInProgress: Boolean                                    = false

  private val desktopOption = if (Desktop.isDesktopSupported) Some(Desktop.getDesktop) else None
  private val htmlKit       = new ExtendedHtmlEditorKit(desktopOption)
  private val chatSelGroup  = new ChatListItemSelectionGroup

  private lazy val grpcHolder = {
    new GrpcDataLoaderHolder(grpcPort)
  }

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

  override lazy val top = new MainFrame {
    import org.fs.chm.BuildInfo._
    title    = s"$name v${version} b${new DateTime(builtAtMillis).toString("yyyyMMdd-HHmmss")}"
    contents = ui
    size     = new Dimension(1000, 700)
    peer.setLocationRelativeTo(null)
    Thread.setDefaultUncaughtExceptionHandler(handleException)

    Swing.onEDTWait {
      // Install EDT exception handler (may be unnecessary due to default handler)
      Thread.currentThread.setUncaughtExceptionHandler(handleException)

      if (initialFileOption.isDefined) {
        freezeTheWorld("")
      }
    }

    initialFileOption map (f => futureHandlingExceptions { Swing.onEDT { openDb(f) } })
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
        contents += menuItem("Compare Datasets")(showSelectDatasetsToCompareDialog())
      }
    }
    (menuBar, dbEmbeddedMenu)
  }

  lazy val chatList = new DaoList[ChatListItem, GrpcChatHistoryDao](dao => new DaoChatItem(dao))

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
      res.failed.toOption foreach {
        case _: CancellationException => showWarning("Cancelled")
        case th: Throwable            => handleException(th)
      }
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
    val chooser = DataLoaders.openChooser()
    for (lastFileString <- config.get(DataLoaders.LastFileKey)) {
      val lastFile = new JFile(lastFileString)
      chooser.peer.setCurrentDirectory(lastFile.nearestExistingDir)
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

  def openDb(file: JFile): Unit = {
    checkEdt()
    freezeTheWorld("Loading data...")
    config.update(DataLoaders.LastFileKey, file.getAbsolutePath)
    futureHandlingExceptions { // To release UI lock
      val dao = DataLoaders.load(file)
      Swing.onEDT {
        loadDaoInEDT(dao)
        unfreezeTheWorld()
      }
    }
  }

  def closeDb(dao: GrpcChatHistoryDao): Unit = {
    checkEdt()
    freezeTheWorld("Closing...")
    futureHandlingExceptions {
      Swing.onEDT {
        MutationLock.synchronized {
          loadedDaos = loadedDaos - dao
          chatList.replaceWith(loadedDaos.keys.toSeq)
          dao.close()
        }
        daoListChanged()
        unfreezeTheWorld()
      }
    }
  }

  def showPickNewDbNameDialog(title: String, initial: String, callbackStatus: String)(callback: String => Unit): Unit = {
    Dialog.showInput(
      title   = title,
      message = "Choose a name for a new database",
      initial = initial
    ) foreach (s => worldFreezingIFuture(callbackStatus) {
      callback(s)
    })
  }

  def showUsersDialog(): Unit = {
    val userList = new DaoList[UserDetailsPane, GrpcChatHistoryDao]({ dao =>
      new DaoItem(
        dao,
        { ds =>
          dao.users(ds.uuid).sortBy(_.id).zipWithIndex map {
            case (u, i) =>
              val pane = new UserDetailsPane(dao, u, false, Some(this))
              pane.stylizeFirstLastName(Colors.forIdx(i))
              pane
          }
        },
        popupEnabled = false,
        None, None, None
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
          val storagePath = masterDao.storagePath
          showPickNewDbNameDialog("Merge datasets", storagePath.getName, "Merging...") { newDbName =>
            val newDbPath = new JFile(storagePath.getParentFile, newDbName)
            if (newDbPath.exists && newDbPath.list().nonEmpty) {
              showError(s"Database directory ${newDbPath.getAbsolutePath} exists and is not empty")
            } else Swing.onEDTWait {
              val selectChatsDialog = new SelectMergeChatsDialog(masterDao, masterDs, slaveDao, slaveDs)
              selectChatsDialog.visible = true
              selectChatsDialog.selection foreach { chatsToMerge =>
                val merger = new DatasetMergerRemote(grpcHolder.channel, masterDao, masterDs, slaveDao, slaveDs)
                val analyzeChatsF = analyzeChatsFuture(merger, chatsToMerge)
                val activeUserIds = chatsToMerge
                  .filter(!_.isInstanceOf[ChatMergeOption.DontAdd])
                  .flatMap(ctm => Seq(ctm.masterCwdOption, ctm.slaveCwdOption))
                  .yieldDefined
                  .flatMap(_.chat.memberIds)
                  .toSet
                val selectUsersDialog = new SelectMergeUsersDialog(masterDao, masterDs, slaveDao, slaveDs, activeUserIds)
                selectUsersDialog.visible = true
                selectUsersDialog.selection match {
                  case Some(usersToMerge) =>
                    analyzeChatsF.future.foreach(analyzed => mergeDatasets(merger, masterDao, slaveDao, analyzed, usersToMerge, newDbPath))
                  case None =>
                    analyzeChatsF.cancel()
                }
              }
            }
          }
      }
    }
  }

  def showSelectDatasetsToCompareDialog(): Unit = {
    checkEdt()
    if (loadedDaos.size < 2) {
      showWarning("Load at least two databases first!")
    } else if (loadedDaos.keys.flatMap(_.datasets).size == 1) {
      showWarning("Only one dataset is loaded - nothing to compare.")
    } else {
      val selectDsDialog = new SelectCompareDatasetDialog(loadedDaos.keys.toSeq)
      selectDsDialog.visible = true
      selectDsDialog.selection foreach {
        case ((masterDao, masterDs), (slaveDao, slaveDs)) =>
          worldFreezingIFuture("Comparing datasets...") {
            grpcHolder.remoteLoader.ensureSame(masterDao.key, masterDs.uuid, slaveDao.key, slaveDs.uuid)
            showWarning("Datasets are same!")
          }
      }
    }
  }

  //
  // Other stuff
  //

  def analyzeChatsFuture(
      merger: DatasetMerger,
      chatsToMerge: Seq[SelectedChatMergeOption]
  ): InterruptableFuture[Seq[AnalyzedChatMergeOption]] =
    worldFreezingIFuture("Analyzing chat messages...") {
      chatsToMerge.map { cmo =>
        if (Thread.interrupted()) {
          throw new InterruptedException()
        }
        cmo match {
          case cmo: ChatMergeOption.SelectedCombine =>
            setStatus(s"Analyzing ${cmo.title}...")
            val diffs = merger.analyze(cmo.masterCwd, cmo.slaveCwd, cmo.title)
            // Sanity check
            if (diffs.size >= 10000) {
              throw new IllegalStateException(s"Found ${diffs.size} mismatches for ${cmo.title}!")
            }
            cmo.analyzed(diffs)
          case cmo: ChatMergeOption.Add => cmo
          case cmo: ChatMergeOption.DontAdd => cmo
          case cmo: ChatMergeOption.Keep => cmo
        }
      }
    }

  def mergeDatasets(
      merger: DatasetMergerRemote,
      masterDao: GrpcChatHistoryDao,
      slaveDao: GrpcChatHistoryDao,
      analyzed: Seq[AnalyzedChatMergeOption],
      usersToMerge: Seq[UserMergeOption],
      newDbPath: JFile
  ): Unit = {
    worldFreezingIFuture("Combining chats...") {
      type MergeModel = SelectMergeMessagesDialog.SelectMergeMessagesModel
      type LazyModel = () => MergeModel

      val (_resolved, cmosWithLazyModels) =
        analyzed.foldLeft((Seq.empty[ResolvedChatMergeOption], Seq.empty[(ChatMergeOption.AnalyzedCombine, LazyModel)])) {
          case ((resolved, cmosWithLazyModels), (cmo @ ChatMergeOption.AnalyzedCombine(mcwd, scwd, diffs))) =>
            // Resolve mismatches
            if (diffs.forall(d => d.isInstanceOf[MessagesMergeDiff.Match] || d.isInstanceOf[MessagesMergeDiff.Retain])) {
              // User has no choice - pass them as-is
              (resolved :+ cmo.resolveAsIs, cmosWithLazyModels)
            } else if (diffs.forall(d => d.isInstanceOf[MessagesMergeDiff.Add])) {
              // We're adding a whole chat, choosing is pointless
              (resolved :+ cmo.resolveAsIs, cmosWithLazyModels)
            } else {
              val next = (cmo, () => {
                if (Thread.interrupted()) throw new InterruptedException("Cancelled")
                StopWatch.measureAndCall {
                  // I *HOPE* that creating model alone outside of EDT doesn't cause issues
                  new MergeModel(masterDao, mcwd, slaveDao, scwd, diffs, htmlKit)
                }((_, t) => log.info(s"Model for chats merge ${cmo.title} created in $t ms"))
              })
              (resolved, cmosWithLazyModels :+ next)
            }
          case ((resolved, cmosWithLazyModels), cmo: ChatMergeOption.Add) =>
            (resolved :+ cmo, cmosWithLazyModels)
          case ((resolved, cmosWithLazyModels), cmo: ChatMergeOption.DontAdd) =>
            (resolved :+ cmo, cmosWithLazyModels)
          case ((resolved, cmosWithLazyModels), cmo: ChatMergeOption.Keep) =>
            (resolved :+ cmo, cmosWithLazyModels)
      }

      // Since model creation is costly, next model is made asynchronously

      def evaluate(element: (ChatMergeOption.AnalyzedCombine, LazyModel)) = {
        (element._1, Future.interruptibly { element._2() })
      }

      var resolved = _resolved.toIndexedSeq
      var nextModelFutureOption = cmosWithLazyModels.headOption map evaluate
      var i = 1
      var cancelled = false
      while (!cancelled && nextModelFutureOption.isDefined) {
        val (cmo, futureModel) = nextModelFutureOption.get
        setStatus(s"Processing ${cmo.title}...")

        nextModelFutureOption = if (cmosWithLazyModels.length <= i) {
          None
        } else {
          Some(evaluate(cmosWithLazyModels(i)))
        }

        val model = Await.result(futureModel.future, duration.Duration.Inf)

        val dialog = onEdtReturning {
          new SelectMergeMessagesDialog(model)
        }
        dialog.visible = true
        cancelled = dialog.selection match {
          case Some(resolution) =>
            resolved = resolved :+ cmo.resolved(resolution)
            false
          case None =>
            nextModelFutureOption.map(_._2.cancel())
            true
        }

        i += 1
      }

      if (cancelled)
        throw new CancellationException("Cancelled")

      resolved
    }.future.flatMap((chatsMergeResolutions: Seq[ResolvedChatMergeOption]) => {
      // Merge
      worldFreezingIFuture("Merging...") {
        newDbPath.mkdir()
        val (newDao, _) = merger.merge(usersToMerge, chatsMergeResolutions, newDbPath)
        Swing.onEDTWait {
          loadDaoInEDT(newDao)
        }
      }.future
    })
  }

  def loadDaoInEDT(dao: GrpcChatHistoryDao, daoToReplaceOption: Option[GrpcChatHistoryDao] = None): Unit = {
    checkEdt()
    MutationLock.synchronized {
      daoToReplaceOption match {
        case Some(srcDao) =>
          val seq  = loadedDaos.toSeq
          val seq2 = seq.updated(seq.indexWhere(_._1 == srcDao), (dao -> Map.empty[CombinedChat, ChatCache]))
          loadedDaos = ListMap(seq2: _*)
          chatList.replaceWith(loadedDaos.keys.toSeq)
          srcDao.close()
        case None =>
          chatList.append(dao)
          loadedDaos = loadedDaos + (dao -> Map.empty) // TODO: Reverse?
      }
    }
    daoListChanged()
    unfreezeTheWorld()
  }

  def daoListChanged(): Unit = {
    def saveAs(dao: GrpcChatHistoryDao): Unit = {
      showPickNewDbNameDialog("Save As", dao.storagePath.getName, "Saving data...") { newName =>
        val dstDao = dao.saveAsRemote(newName)
        Swing.onEDTWait {
          loadDaoInEDT(dstDao, Some(dao))
        }
      }
    }

    dbEmbeddedMenu.clear()
    for (dao <- loadedDaos.keys) {
      val daoMenu = new Menu(dao.name) {
        contents += menuItem("Save As...")(saveAs(dao))
        contents += new Separator()
        contents += menuItem("Close")(closeDb(dao))
      }
      dbEmbeddedMenu.append(daoMenu)
    }
    chatsOuterPanel.revalidate()
    chatsOuterPanel.repaint()
  }

  def renameDataset(_dao: ChatHistoryDao, dsUuid: PbUuid, newName: String): Unit = {
    checkEdt()
    require(_dao.isInstanceOf[GrpcChatHistoryDao])
    val dao = _dao.asInstanceOf[GrpcChatHistoryDao]
    freezeTheWorld("Renaming...")
    Swing.onEDT { // To release UI lock
      try {
        MutationLock.synchronized {
          dao.renameDataset(dsUuid, newName)
          chatList.replaceWith(loadedDaos.keys.toSeq)
        }
        chatsOuterPanel.revalidate()
        chatsOuterPanel.repaint()
      } finally {
        unfreezeTheWorld()
      }
    }
  }

  def deleteDataset(_dao: ChatHistoryDao, dsUuid: PbUuid): Unit = {
    checkEdt()
    require(_dao.isInstanceOf[GrpcChatHistoryDao])
    val dao = _dao.asInstanceOf[GrpcChatHistoryDao]
    freezeTheWorld("Deleting...")
    Swing.onEDT { // To release UI lock
      try {
        MutationLock.synchronized {
          dao.deleteDataset(dsUuid)
          chatList.replaceWith(loadedDaos.keys.toSeq)
        }
        chatsOuterPanel.revalidate()
        chatsOuterPanel.repaint()
      } finally {
        unfreezeTheWorld()
      }
    }
  }

  def shiftDatasetTime(_dao: ChatHistoryDao, dsUuid: PbUuid, hrs: Int): Unit = {
    checkEdt()
    require(_dao.isInstanceOf[GrpcChatHistoryDao])
    val dao = _dao.asInstanceOf[GrpcChatHistoryDao]
    freezeTheWorld("Shifting time...")
    Swing.onEDT { // To release UI lock
      try {
        MutationLock.synchronized {
          dao.shiftDatasetTime(dsUuid, hrs)
          MutationLock.synchronized {
            // Clear cache
            if (loadedDaos.contains(dao)) {
              loadedDaos = loadedDaos + (dao -> Map.empty)
            }
          }
          chatList.replaceWith(loadedDaos.keys.toSeq)
        }
        chatsOuterPanel.revalidate()
        chatsOuterPanel.repaint()
      } finally {
        unfreezeTheWorld()
      }
    }
  }

  override def userEdited(user: User, _dao: ChatHistoryDao): Unit = {
    checkEdt()
    require(_dao.isInstanceOf[GrpcChatHistoryDao])
    val dao = _dao.asInstanceOf[GrpcChatHistoryDao]
    freezeTheWorld("Modifying...")
    asyncChangeChats(dao, {
      dao.updateUser(user)
    })
  }

  override def deleteChat(_dao: ChatHistoryDao, cc: CombinedChat): Unit = {
    require(_dao.isInstanceOf[GrpcChatHistoryDao])
    val dao = _dao.asInstanceOf[GrpcChatHistoryDao]
    freezeTheWorld("Deleting...")
    Swing.onEDT {
      try {
        MutationLock.synchronized {
          for (cwd <- cc.cwds) {
            dao.deleteChat(cwd.chat)
          }
          evictFromCache(dao, cc)
          chatList.replaceWith(loadedDaos.keys.toSeq)
        }
        chatsOuterPanel.revalidate()
        chatsOuterPanel.repaint()
      } finally {
        unfreezeTheWorld()
      }
    }
  }

  override def selectChat(_dao: ChatHistoryDao, cc: CombinedChat): Unit = {
    checkEdt()
    require(_dao.isInstanceOf[GrpcChatHistoryDao])
    val dao = _dao.asInstanceOf[GrpcChatHistoryDao]
    MutationLock.synchronized {
      currentChatOption = None
      msgRenderer.renderPleaseWait()
      if (!loadedDaos(dao).contains(cc)) {
        updateCache(dao, cc, ChatCache(None, Map.empty))
      }
      freezeTheWorld("Loading chat...")
    }
    futureHandlingExceptions {
      MutationLock.synchronized {
        currentChatOption = Some(dao -> cc)
        loadMessagesInProgress = true
      }
      // If the chat has been already rendered, restore previous document as-is
      if (loadedDaos(dao)(cc).msgDocOption.isEmpty) {
        loadLastMessagesAndUpdateCache(dao, cc)
      }
      Swing.onEDTWait(MutationLock.synchronized {
        val doc = loadedDaos(dao)(cc).msgDocOption.get
        msgRenderer.render(doc, false)
        loadMessagesInProgress = false
        unfreezeTheWorld()
      })
    }
  }

  override def combineChats(_dao: ChatHistoryDao, masterChat: Chat, slaveChat: Chat): Unit = {
    checkEdt()
    require(_dao.isInstanceOf[GrpcChatHistoryDao])
    val dao = _dao.asInstanceOf[GrpcChatHistoryDao]
    freezeTheWorld("Combining...")
    asyncChangeChats(dao, {
      dao.combineChats(masterChat, slaveChat)
    })
  }

  override def navigateToBeginning(): Unit = {
    checkEdt()
    freezeTheWorld("Navigating...")
    futureHandlingExceptions {
      currentChatOption match {
        case Some((dao, cc)) =>
          // Ignore cache for this purpose
          MutationLock.synchronized {
            loadMessagesInProgress = true
          }
          loadFirstMessagesAndUpdateCache(dao, cc)
        case None =>
          () // NOOP
      }
      Swing.onEDT {
        MutationLock.synchronized {
          loadMessagesInProgress = false
        }
        unfreezeTheWorld()
      }
    }
  }

  override def navigateToEnd(): Unit = {
    checkEdt()
    freezeTheWorld("Navigating...")
    futureHandlingExceptions {
      currentChatOption match {
        case Some((dao, cc)) =>
          val cache = loadedDaos(dao)(cc)
          // Ignore cache for this purpose
          MutationLock.synchronized {
            loadMessagesInProgress = true
          }
          loadLastMessagesAndUpdateCache(dao, cc)
        case None =>
          () // NOOP
      }
      Swing.onEDT {
        MutationLock.synchronized {
          loadMessagesInProgress = false
        }
        unfreezeTheWorld()
      }
    }
  }

  override def navigateToDate(date: DateTime): Unit = {
    // FIXME: This doesn't work!
    ???
    //    checkEdt()
    //    freezeTheWorld("Navigating...")
    //    futureHandlingExceptions {
    //      currentChatOption match {
    //        case Some((dao, cwd)) =>
    //          // TODO: Don't replace a document if currently cached document already contains message?
    //          MutationLock.synchronized {
    //            loadMessagesInProgress = true
    //          }
    //          loadDateMessagesAndUpdateCache(dao, cwd, date)
    //        case None =>
    //          () // NOOP
    //      }
    //      Swing.onEDT {
    //        MutationLock.synchronized {
    //          loadMessagesInProgress = false
    //        }
    //        unfreezeTheWorld()
    //      }
    //    }
  }

  private def loadCombinedChatMessages(cc: CombinedChat,
                                       loadStatuses: LoadStatuses,
                                       isBackward: Boolean,
                                       fetch: (Chat, Option[LoadStatus]) => Seq[Message]): (Seq[Message], LoadStatuses) = {
    val allMsgsUnlimited = cc.cwds
      .flatMap { cwd =>
        println(s"=== Processing chat ${cwd.chat.id}")
        val loadStatusOption = loadStatuses.get(cwd.chat.id)
        println(s"loadStatusOption = $loadStatusOption")
        (if ((isBackward && loadStatusOption.exists(_.beginReached)) || (!isBackward && loadStatusOption.exists(_.endReached))) {
          Seq.empty
        } else {
          val res = fetch(cwd.chat, loadStatusOption)
          println(s"Fetched ${res.headOption.map(_.internalId)} to ${res.lastOption.map(_.internalId)}")
          res
        }).map(m => (m, cwd.chat))
      }
      .sortBy(p => (p._1.timestamp, p._1.internalId))
    val allMsgs = if (!isBackward) allMsgsUnlimited.take(MsgBatchLoadSize) else allMsgsUnlimited.takeRight(MsgBatchLoadSize)

    // If all messages combines is still less than batch load size, there will be nothing left to fetch
    val noMoreMessages = allMsgs.size < MsgBatchLoadSize

    val loadStatuses2 = cc.cwds.map(cwd => {
      println(s"=== Updating cache for chat ${cwd.chat.id}")
      val loadStatusOption = loadStatuses.get(cwd.chat.id)
      val newFirstOption = allMsgs.find    (_._2.id == cwd.chat.id).map(_._1)
      val newLastOption  = allMsgs.findLast(_._2.id == cwd.chat.id).map(_._1)
      println(s"Setting cache to ${newFirstOption.map(_.internalId)} to ${newLastOption.map(_.internalId)}")
      val newLoadStatus = loadStatusOption.map(loadStatus => LoadStatus(
        firstOption  = newFirstOption.orElse(loadStatus.firstOption),
        lastOption   = newLastOption.orElse(loadStatus.lastOption),
        beginReached = loadStatus.beginReached ||  (isBackward && noMoreMessages),
        endReached   = loadStatus.endReached   || (!isBackward && noMoreMessages)
      )).getOrElse(LoadStatus(
        firstOption  = newFirstOption,
        lastOption   = newLastOption,
        beginReached =  isBackward && noMoreMessages,
        endReached   = !isBackward && noMoreMessages
      ))
      cwd.chat.id -> newLoadStatus
    }).toMap

    (allMsgs.map(_._1), loadStatuses2)
  }

  def tryLoadPreviousMessages(): Unit = {
    log.debug("Trying to load previous messages")
    tryLoadMessages(
      ls => ls.exists(!_._2.beginReached),
      (dao, cc, ls) => {
        loadCombinedChatMessages(cc, ls, isBackward = true, (chat, loadStatusOption) => {
          val loadStatus = loadStatusOption.getOrElse(throw new IllegalStateException(s"Chat ${chat} is not in cache!"))
          dao.messagesBefore(chat, loadStatus.firstOption.get.internalIdTyped, MsgBatchLoadSize)
        })
      },
      (dao, cc, msgs, ls) => {
        msgRenderer.prepend(dao, cc, msgs, ls.values.forall(_.beginReached))
      }
    )
  }

  def tryLoadNextMessages(): Unit = {
    log.debug("Trying to load next messages")
    tryLoadMessages(
      ls => ls.exists(!_._2.endReached),
      (dao, cc, ls) => {
        loadCombinedChatMessages(cc, ls, isBackward = false, (chat, loadStatusOption) => {
          val loadStatus = loadStatusOption.getOrElse(throw new IllegalStateException(s"Chat ${chat} is not in cache!"))
          dao.messagesAfter(chat, loadStatus.lastOption.get.internalIdTyped, MsgBatchLoadSize)
        })
      },
      (dao, cc, msgs, ls) => {
        msgRenderer.append(dao, cc, msgs, ls.values.forall(_.endReached))
      }
    )
  }

  def tryLoadMessages(
      shouldLoad: LoadStatuses => Boolean,
      load: (GrpcChatHistoryDao, CombinedChat, LoadStatuses) => (Seq[Message], LoadStatuses),
      addToRender: (GrpcChatHistoryDao, CombinedChat, Seq[Message], LoadStatuses) => MD
  ): Unit = {
    val chatInfoOption = MutationLock.synchronized {
      currentChatOption match {
        case _ if loadMessagesInProgress =>
          log.debug("Loading messages: Already in progress")
          None
        case None =>
          log.debug("Loading messages: No chat selected")
          None
        case Some((dao, _)) if !loadedDaos.contains(dao) =>
          log.debug("Loading messages: DAO not loaded")
          None
        case Some((dao, cc)) =>
          val cache        = loadedDaos(dao)(cc)
          val loadStatuses = cache.loadStatuses
          log.debug(s"Loading messages: loadStatuses = ${loadStatuses}")
          if (!shouldLoad(loadStatuses)) {
            None
          } else {
            loadMessagesInProgress = true
            freezeTheWorld("Loading messages...")
            Some((loadStatuses, dao, cc))
          }
      }
    }
    chatInfoOption match {
      case Some((loadStatuses, dao, cc)) =>
        msgRenderer.updateStarted()
        val f = futureHandlingExceptions {
          Swing.onEDTWait(msgRenderer.prependLoading())
          val (addedMessages, loadStatuses2) = load(dao, cc, loadStatuses)
          log.debug(s"Loading messages: Loaded ${addedMessages.size} messages")
          Swing.onEDTWait(MutationLock.synchronized {
            val md = addToRender(dao, cc, addedMessages, loadStatuses2)
            log.debug("Loading messages: Reloaded message container")
            updateCache(dao, cc, ChatCache(Some(md), loadStatuses2))

            msgRenderer.updateFinished()
            loadMessagesInProgress = false
            unfreezeTheWorld()
          })
        }
      case None => /* NOOP */
    }
  }

  def loadFirstMessagesAndUpdateCache(dao: GrpcChatHistoryDao, cc: CombinedChat): Unit = {
    val (msgs, loadStatuses) = loadCombinedChatMessages(cc, Map.empty, isBackward = false, (chat, _) => {
      dao.firstMessages(chat, MsgBatchLoadSize)
    })

    Swing.onEDTWait {
      val md = msgRenderer.render(dao, cc, msgs, beginReached = true, showTop = true)
      updateCache(dao, cc, ChatCache(Some(md), loadStatuses))
    }
  }

  def loadLastMessagesAndUpdateCache(dao: GrpcChatHistoryDao, cc: CombinedChat): Unit = {
    val (msgs, loadStatuses) = loadCombinedChatMessages(cc, Map.empty, isBackward = true, (chat, _) => {
      dao.lastMessages(chat, MsgBatchLoadSize)
    })

    Swing.onEDTWait {
      val md = msgRenderer.render(dao, cc, msgs, loadStatuses.values.forall(_.beginReached), showTop = false)
      updateCache(dao, cc, ChatCache(Some(md), loadStatuses))
    }
  }

  def loadDateMessagesAndUpdateCache(dao: ChatHistoryDao, cwd: ChatWithDetails, date: DateTime): Unit = {
    ??? // Dead code as of now
    // val (msgsB, msgsA) = dao.messagesAroundDate(cwd.chat, date, MsgBatchLoadSize)
    // val msgs = msgsB ++ msgsA
    // Swing.onEDTWait {
    //   val md = {
    //     msgRenderer.render(dao, cwd, msgsA, false, true)
    //     // FIXME: Viewport is not updated!
    //     msgRenderer.updateStarted()
    //     val md = msgRenderer.prepend(dao, cwd, msgsB, msgsB.size < MsgBatchLoadSize)
    //     msgRenderer.updateFinished()
    //     md
    //   }
    //   val loadStatus = LoadStatus(
    //     firstOption  = msgs.headOption,
    //     lastOption   = msgs.lastOption,
    //     beginReached = msgsB.size < MsgBatchLoadSize,
    //     endReached   = msgsA.size < MsgBatchLoadSize
    //   )
    //   updateCache(dao, cwd.chat, ChatCache(Some(md), Some(loadStatus)))
    // }
  }

  /** Asynchronously apply the given change (under mutation lock) and refresh UI to reflect it */
  def asyncChangeChats(dao: GrpcChatHistoryDao, applyChange: => Unit): Unit = {
    Future { // To release UI lock
      try {
        MutationLock.synchronized {
          applyChange
          Swing.onEDTWait {
            chatList.replaceWith(loadedDaos.keys.toSeq)
          }
        }
        chatsOuterPanel.revalidate()
        chatsOuterPanel.repaint()
        MutationLock.synchronized {
          // Evict all dao chats from cache
          loadedDaos = loadedDaos + (dao -> Map.empty)

          // Reload currently selected chat
          val chatItemToReload = for {
            (_, cc) <- currentChatOption
            item    <- chatList.innerItems.find(i => i.mainChat.id == cc.mainChatId && i.mainChat.dsUuid == cc.dsUuid)
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
          Swing.onEDT { unfreezeTheWorld() }
          handleException(ex)
      }
    }
  }

  def updateCache(dao: GrpcChatHistoryDao, cc: CombinedChat, cache: ChatCache): Unit =
    MutationLock.synchronized {
      loadedDaos = loadedDaos + (dao -> (loadedDaos(dao) + (cc -> cache)))
    }

  def evictFromCache(dao: GrpcChatHistoryDao, cc: CombinedChat): Unit =
    MutationLock.synchronized {
      if (loadedDaos.contains(dao)) {
        loadedDaos = loadedDaos + (dao -> (loadedDaos(dao) - cc))
      }
    }

  //
  // Utility and classes
  //

  override def startup(args: Array[String]): Unit = {
    try {
      initialFileOption = CliUtils.parse(args, "db", true).map(new JFile(_))
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
      if (isEdt()) {
        unfreezeTheWorld()
      } else
        Swing.onEDT {
          unfreezeTheWorld()
        }
    }

  private def futureHandlingExceptions[T](code: => T): Future[T] = {
    val f = Future(code)
    f.onComplete {
      case Failure(th) => handleException(th)
      case _ => // NOOP
    }
    f
  }

  private class DaoChatItem(dao: GrpcChatHistoryDao)
      extends DaoItem[ChatListItem](
        dao             = dao,
        getInnerItems = { ds =>
          val allCwds = dao.chats(ds.uuid)
          allCwds.filter(_.chat.mainChatId.isEmpty).map(cwd => {
            val slaveChats = allCwds.filter(_.chat.mainChatId.contains(cwd.chat.id))
            new ChatListItem(dao, CombinedChat(cwd, slaveChats), Some(chatSelGroup), Some(this))
          })
        },
        popupEnabled                   = true,
        renameDatasetCallbackOption    = if (dao.isMutable) Some(renameDataset) else None,
        deleteDatasetCallbackOption    = if (dao.isMutable) Some(deleteDataset) else None,
        shiftDatasetTimeCallbackOption = Some(shiftDatasetTime)
      )

  /** Map from individual chat ID to load status */
  private type LoadStatuses = Map[Long, LoadStatus]

  private case class ChatCache(
    msgDocOption: Option[MD],

    /** Map from individual chat ID to load status */
    loadStatuses: LoadStatuses,
  )

  private case class LoadStatus(
    firstOption: Option[Message],
    lastOption: Option[Message],
    beginReached: Boolean,
    endReached: Boolean
  )

  private object DataLoaders {
    val LastFileKey = "last_database_file"

    private val sqliteFf = easyFileFilter(
      s"${BuildInfo.name} database (sqlite)"
    ) { f => f.getName == "data.sqlite" }

    private val tgFf = easyFileFilter(
      "Telegram export JSON database (result.json)"
    )(_.getName == "result.json")

    private val androidFf = easyFileFilter(
      s"Supported app's Android database"
    ) { _.getName.endsWith(".db") }

    private val waTextFf = easyFileFilter(
      s"WhatsApp text export"
    ) { f => f.getName.startsWith("WhatsApp Chat with ") && f.getName.endsWith(".txt") }

    def openChooser(): FileChooser = new FileChooser(null) {
      title = "Select a database to open"
      peer.addChoosableFileFilter(sqliteFf)
      peer.addChoosableFileFilter(tgFf)
      peer.addChoosableFileFilter(androidFf)
      peer.addChoosableFileFilter(waTextFf)
    }

    def load(file: JFile): GrpcChatHistoryDao = {
      if (sqliteFf.accept(file) || tgFf.accept(file) || androidFf.accept(file) || waTextFf.accept(file)) {
        grpcHolder.remoteLoader.loadData(file)
      } else {
        throw new IllegalStateException("Unknown file type!")
      }
    }
  }
}
