package org.fs.chm.ui.swing

import java.awt.Desktop
import java.awt.Toolkit
import java.awt.event.AdjustmentEvent
import java.io.{File => JFile}
import java.util.concurrent.atomic.AtomicBoolean

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
import org.fs.chm.dao.EagerChatHistoryDao
import org.fs.chm.dao.Entities._
import org.fs.chm.dao.GrpcChatHistoryDao
import org.fs.chm.dao.MutableChatHistoryDao
import org.fs.chm.dao.merge.DatasetMerger
import org.fs.chm.dao.merge.DatasetMerger._
import org.fs.chm.dao.merge.DatasetMergerLocal
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

  private var loadedDaos: ListMap[ChatHistoryDao, Map[Chat, ChatCache]] = ListMap.empty

  private var currentChatOption:      Option[(ChatHistoryDao, ChatWithDetails)] = None
  private var loadMessagesInProgress: Boolean                                   = false

  private val desktopOption = if (Desktop.isDesktopSupported) Some(Desktop.getDesktop) else None
  private val htmlKit       = new ExtendedHtmlEditorKit(desktopOption)
  private val chatSelGroup  = new ChatListItemSelectionGroup

  private lazy val grpcHolder = {
    val grpcDaoService = new GrpcDaoService(f => DataLoaders.load(f, remote = false));
    new GrpcDataLoaderHolder(grpcPort, grpcDaoService)
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
    Thread.setDefaultUncaughtExceptionHandler(handleException);

    Swing.onEDTWait {
      // Install EDT exception handler (may be unnecessary due to default handler)
      Thread.currentThread.setUncaughtExceptionHandler(handleException)

      if (initialFileOption.isDefined) {
        freezeTheWorld("")
      }
    }

    initialFileOption map (f => futureHandlingExceptions { Swing.onEDT { openDb(f, remote = false) } })
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
      contents += menuItem("Open")(showOpenDbDialog(remote = false))
      contents += menuItem("Open (Remote)")(showOpenDbDialog(remote = true))
      contents += separatorBeforeDb
      contents += separatorAfterDb
    }
    val dbEmbeddedMenu = new EmbeddedMenu(dbMenu, separatorBeforeDb, separatorAfterDb)
    val menuBar = new MenuBar {
      contents += dbMenu
      contents += new Menu("Edit") {
        contents += menuItem("Users")(showUsersDialog())
        contents += menuItem("Merge Datasets")(showSelectDatasetsToMergeDialog(remote = false))
        contents += menuItem("Merge Datasets (remote)")(showSelectDatasetsToMergeDialog(remote = true))
        contents += menuItem("Compare Datasets")(showSelectDatasetsToCompareDialog())
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

  def showOpenDbDialog(remote: Boolean): Unit = {
    val chooser = DataLoaders.openChooser(remote)
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
        openDb(chooser.selectedFile, remote)
    }
  }

  def openDb(file: JFile, remote: Boolean): Unit = {
    checkEdt()
    freezeTheWorld("Loading data...")
    config.update(DataLoaders.LastFileKey, file.getAbsolutePath)
    futureHandlingExceptions { // To release UI lock
      val dao = DataLoaders.load(file, remote)
      Swing.onEDT {
        loadDaoInEDT(dao)
        unfreezeTheWorld()
      }
    }
  }

  def closeDb(dao: ChatHistoryDao): Unit = {
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

  def verifyRemoteDao(rpcDao: GrpcChatHistoryDao): Unit = {
    checkEdt()
    freezeTheWorld("Verifying...")
    futureHandlingExceptions {
      val path = rpcDao.storagePath
      try {
        grpcHolder.grpcDaoService.getDao(path) match {
          case Some(localDao) =>
            for (ds <- localDao.datasets) {
              // This throws and exception if something is wrong
              ChatHistoryDao.ensureDataSourcesAreEqual(localDao, rpcDao, ds.uuid)
            }
            showWarning("DAOs match!")
          case None => showError(s"DAO with path ${path} wasn't found!")
        }
      } catch {
        case th: Throwable =>
          showError(s"Found a problem: ${th}")
      } finally {
        Swing.onEDT { unfreezeTheWorld() }
      }
    }
  }

  def showPickDirDialog(callback: JFile => Unit): Unit = {
    val chooser = DataLoaders.saveAsChooser
    for (lastFileString <- config.get(DataLoaders.LastFileKey)) {
      val lastFile = new JFile(lastFileString)
      chooser.peer.setCurrentDirectory(lastFile.nearestExistingDir)
    }
    chooser.showOpenDialog(null) match {
      case FileChooser.Result.Cancel => // NOOP
      case FileChooser.Result.Error => // Mostly means that dialog was dismissed, also NOOP
      case FileChooser.Result.Approve => {
        config.update(DataLoaders.LastFileKey, chooser.selectedFile.getAbsolutePath)
        worldFreezingIFuture("Saving data...") {
          callback(chooser.selectedFile)
        }
      }
    }
  }

  def showUsersDialog(): Unit = {
    val userList = new DaoList({ dao =>
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

  def showSelectDatasetsToMergeDialog(remote: Boolean): Unit = {
    checkEdt()
    if (loadedDaos.isEmpty) {
      showWarning("Load a database first!")
    } else if (!loadedDaos.exists(_._1.isMutable)) {
      showWarning("You'll need an editable database first. Save the one you want to use as base.")
    } else if (loadedDaos.keys.flatMap(_.datasets).size == 1) {
      showWarning("Only one dataset is loaded - nothing to merge.")
    } else {
      val selectDsDialog = new SelectMergeDatasetDialog(loadedDaos.keys.toSeq, remote)
      selectDsDialog.visible = true
      selectDsDialog.selection foreach {
        case ((masterDao, masterDs), (slaveDao, slaveDs)) =>
          val storagePath = masterDao.storagePath
          Dialog.showInput(
            title   = "Merge datasets",
            message = "Choose a name for a new database",
            initial = storagePath.getName
          ) foreach { newDbName =>
            val newDbPath = new JFile(storagePath.getParentFile, newDbName)
            if (newDbPath.exists && newDbPath.list().nonEmpty) {
              showError(s"Database directory ${newDbPath.getAbsolutePath} exists and is not empty")
            } else {
              val selectChatsDialog = new SelectMergeChatsDialog(masterDao, masterDs, slaveDao, slaveDs)
              selectChatsDialog.visible = true
              selectChatsDialog.selection foreach { chatsToMerge =>
                val merger = if (!remote) {
                  new DatasetMergerLocal(masterDao, masterDs, slaveDao, slaveDs, DataLoaders.createH2)
                } else {
                  new DatasetMergerRemote(
                    grpcHolder.channel,
                    masterDao.asInstanceOf[GrpcChatHistoryDao], masterDs,
                    slaveDao.asInstanceOf[GrpcChatHistoryDao], slaveDs
                  )
                }
                val sw = new StopWatch
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
            ChatHistoryDao.ensureDatasetsAreEqual(slaveDao, masterDao, slaveDs.uuid, masterDs.uuid)
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
      merger: DatasetMerger,
      masterDao: ChatHistoryDao,
      slaveDao: ChatHistoryDao,
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
          chatList.append(dao)
          loadedDaos = loadedDaos + (dao -> Map.empty) // TODO: Reverse?
      }
    }
    daoListChanged()
    unfreezeTheWorld()
  }

  def daoListChanged(): Unit = {
    def saveAs(dao: ChatHistoryDao): Unit = {
      showPickDirDialog { file =>
        val dstDao = DataLoaders.saveAsH2(dao, file)
        Swing.onEDTWait {
          loadDaoInEDT(dstDao, Some(dao))
        }
      }
    }

    def saveAsRemote(dao: GrpcChatHistoryDao): Unit = {
      showPickDirDialog { file =>
        val dstDao = dao.saveAsRemote(file)
        Swing.onEDTWait {
          loadDaoInEDT(dstDao, Some(dao))
        }
      }
    }

    dbEmbeddedMenu.clear()
    for (dao <- loadedDaos.keys) {
      val daoMenu = new Menu(dao.name) {
        contents += menuItem("Save As...")(saveAs(dao))
        dao match {
          case dao: GrpcChatHistoryDao =>
            contents += menuItem("Save As (remote)...")(saveAsRemote(dao))
            if (dao.key.endsWith(H2DataManager.DefaultExt)) {
              contents += menuItem("Verify")(verifyRemoteDao(dao))
            }
          case _ => // NOOP
        }
        contents += new Separator()
        contents += menuItem("Close")(closeDb(dao))
      }
      dbEmbeddedMenu.append(daoMenu)
    }
    chatsOuterPanel.revalidate()
    chatsOuterPanel.repaint()
  }

  def renameDataset(dao: ChatHistoryDao, dsUuid: PbUuid, newName: String): Unit = {
    checkEdt()
    require(dao.isMutable, "DAO is immutable!")
    freezeTheWorld("Renaming...")
    Swing.onEDT { // To release UI lock
      try {
        MutationLock.synchronized {
          dao match { case dao: MutableChatHistoryDao => dao.renameDataset(dsUuid, newName) }
          chatList.replaceWith(loadedDaos.keys.toSeq)
        }
        chatsOuterPanel.revalidate()
        chatsOuterPanel.repaint()
      } finally {
        unfreezeTheWorld()
      }
    }
  }

  def deleteDataset(dao: ChatHistoryDao, dsUuid: PbUuid): Unit = {
    checkEdt()
    require(dao.isMutable, "DAO is immutable!")
    freezeTheWorld("Deleting...")
    Swing.onEDT { // To release UI lock
      try {
        MutationLock.synchronized {
          dao match { case dao: MutableChatHistoryDao => dao.deleteDataset(dsUuid) }
          chatList.replaceWith(loadedDaos.keys.toSeq)
        }
        chatsOuterPanel.revalidate()
        chatsOuterPanel.repaint()
      } finally {
        unfreezeTheWorld()
      }
    }
  }

  def shiftDatasetTime(dao: ChatHistoryDao, dsUuid: PbUuid, hrs: Int): Unit = {
    checkEdt()
    require(dao.isMutable || dao.isInstanceOf[EagerChatHistoryDao], "DAO is immutable!")
    freezeTheWorld("Shifting time...")
    Swing.onEDT { // To release UI lock
      try {
        MutationLock.synchronized {
          dao match {
            case dao: MutableChatHistoryDao =>
              dao.shiftDatasetTime(dsUuid, hrs)
            case dao: EagerChatHistoryDao =>
              val newDao = dao.copyWithShiftedDatasetTime(dsUuid, hrs)
              loadDaoInEDT(newDao, Some(dao))
          }
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

  override def userEdited(user: User, dao: ChatHistoryDao): Unit = {
    checkEdt()
    require(dao.isMutable, "DAO is immutable!")
    freezeTheWorld("Modifying...")
    asyncChangeUsers(dao, {
      dao match { case dao: MutableChatHistoryDao => dao.updateUser(user) }
      Seq(user.id)
    })
  }

  override def usersMerged(baseUser: User, absorbedUser: User, dao: ChatHistoryDao): Unit = {
    checkEdt()
    require(dao.isMutable, "DAO is immutable!")
    require(baseUser.dsUuid == absorbedUser.dsUuid, "Users are from different datasets!")
    freezeTheWorld("Modifying...")
    asyncChangeUsers(dao, {
      dao match { case dao: MutableChatHistoryDao => dao.mergeUsers(baseUser, absorbedUser) }
      Seq(baseUser.id, absorbedUser.id)
    })
  }

  override def deleteChat(dao: ChatHistoryDao, chat: Chat): Unit = {
    freezeTheWorld("Deleting...")
    Swing.onEDT {
      try {
        MutationLock.synchronized {
          dao match { case dao: MutableChatHistoryDao => dao.deleteChat(chat) }
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

  override def selectChat(dao: ChatHistoryDao, cwd: ChatWithDetails): Unit = {
    checkEdt()
    MutationLock.synchronized {
      currentChatOption = None
      msgRenderer.renderPleaseWait()
      if (!loadedDaos(dao).contains(cwd.chat)) {
        updateCache(dao, cwd.chat, ChatCache(None, None))
      }
      freezeTheWorld("Loading chat...")
    }
    futureHandlingExceptions {
      MutationLock.synchronized {
        currentChatOption = Some(dao -> cwd)
        loadMessagesInProgress = true
      }
      // If the chat has been already rendered, restore previous document as-is
      if (loadedDaos(dao)(cwd.chat).msgDocOption.isEmpty) {
        loadLastMessagesAndUpdateCache(dao, cwd)
      }
      Swing.onEDTWait(MutationLock.synchronized {
        val doc = loadedDaos(dao)(cwd.chat).msgDocOption.get
        msgRenderer.render(doc, false)
        loadMessagesInProgress = false
        unfreezeTheWorld()
      })
    }
  }

  override def navigateToBeginning(): Unit = {
    checkEdt()
    freezeTheWorld("Navigating...")
    futureHandlingExceptions {
      currentChatOption match {
        case Some((dao, cwd)) =>
          val cache = loadedDaos(dao)(cwd.chat)
          cache.loadStatusOption match {
            case Some(ls) if ls.beginReached =>
              // Just scroll
              Swing.onEDTWait(msgRenderer.render(cache.msgDocOption.get, true))
            case _ =>
              MutationLock.synchronized {
                loadMessagesInProgress = true
              }
              loadFirstMessagesAndUpdateCache(dao, cwd)
          }
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
        case Some((dao, cwd)) =>
          val cache = loadedDaos(dao)(cwd.chat)
          cache.loadStatusOption match {
            case Some(ls) if ls.endReached =>
              // Just scroll
              Swing.onEDTWait(msgRenderer.render(cache.msgDocOption.get, false))
            case _ =>
              MutationLock.synchronized {
                loadMessagesInProgress = true
              }
              loadLastMessagesAndUpdateCache(dao, cwd)
          }
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
    checkEdt()
    freezeTheWorld("Navigating...")
    futureHandlingExceptions {
      currentChatOption match {
        case Some((dao, cwd)) =>
          // TODO: Don't replace a document if currently cached document already contains message?
          MutationLock.synchronized {
            loadMessagesInProgress = true
          }
          loadDateMessagesAndUpdateCache(dao, cwd, date)
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

  def tryLoadPreviousMessages(): Unit = {
    log.debug("Trying to load previous messages")
    tryLoadMessages(
      ls => !ls.beginReached,
      (dao, cwd, ls) => {
        val newMsgs = dao.messagesBefore(cwd.chat, ls.firstOption.get.internalIdTyped, MsgBatchLoadSize + 1).dropRight(1)
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
        val newMsgs = dao.messagesAfter(cwd.chat, ls.lastOption.get.internalIdTyped, MsgBatchLoadSize + 1).drop(1)
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
    val chatInfoOption = MutationLock.synchronized {
      currentChatOption match {
        case _ if loadMessagesInProgress =>
          log.debug("Loading messages: Already in progress")
          None
        case None =>
          log.debug("Loading messages: No chat selected")
          None
        case Some((dao, cwd)) =>
          val cache      = loadedDaos(dao)(cwd.chat)
          val loadStatus = cache.loadStatusOption.get
          log.debug(s"Loading messages: loadStatus = ${loadStatus}")
          if (!shouldLoad(loadStatus)) {
            None
          } else {
            assert(loadStatus.firstOption.isDefined)
            assert(loadStatus.lastOption.isDefined)
            loadMessagesInProgress = true
            freezeTheWorld("Loading messages...")
            Some((loadStatus, dao, cwd))
          }
      }
    }
    chatInfoOption match {
      case Some((loadStatus, dao, cwd)) =>
        msgRenderer.updateStarted()
        val f = futureHandlingExceptions {
          Swing.onEDTWait(msgRenderer.prependLoading())
          val (addedMessages, loadStatus2) = load(dao, cwd, loadStatus)
          log.debug(s"Loading messages: Loaded ${addedMessages.size} messages")
          Swing.onEDTWait(MutationLock.synchronized {
            val md = addToRender(dao, cwd, addedMessages, loadStatus2)
            log.debug("Loading messages: Reloaded message container")
            updateCache(dao, cwd.chat, ChatCache(Some(md), Some(loadStatus2)))

            msgRenderer.updateFinished()
            loadMessagesInProgress = false
            unfreezeTheWorld()
          })
        }
      case None => /* NOOP */
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
            if userIds.toSet.intersect(chat.memberIds.toSet).nonEmpty
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
          Swing.onEDT { unfreezeTheWorld() }
          handleException(ex)
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

  private class DaoChatItem(dao: ChatHistoryDao)
      extends DaoItem(
        dao             = dao,
        getInnerItems = { ds =>
          dao.chats(ds.uuid) map (cwd => new ChatListItem(dao, cwd, Some(chatSelGroup), Some(this)))
        },
        popupEnabled                   = true,
        renameDatasetCallbackOption    = if (dao.isMutable) Some(renameDataset) else None,
        deleteDatasetCallbackOption    = if (dao.isMutable) Some(deleteDataset) else None,
        shiftDatasetTimeCallbackOption = Some(shiftDatasetTime)
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
    private val gts5610  = new GTS5610DataLoader

    /** Initializes DAOs to speed up subsequent calls */
    def preload(): Seq[Future[_]] = {
      h2.preload()
    }

    private val sqliteFf = easyFileFilter(
      s"${BuildInfo.name} database (sqlite)"
    ) { f => f.getName == "data.sqlite" }

    private val h2ff = easyFileFilter(
      s"${BuildInfo.name} database (*.${H2DataManager.DefaultExt})"
    )(_.getName endsWith ("." + H2DataManager.DefaultExt))

    private val tgFf = easyFileFilter(
      "Telegram export JSON database (result.json)"
    )(_.getName == "result.json")

    private val gts5610Ff = easyFileFilter(
      s"Samsung GT-S5610 export vMessage files [choose any in folder] (*.${GTS5610DataLoader.DefaultExt})"
    ) { _.getName endsWith ("." + GTS5610DataLoader.DefaultExt) }

    private val androidFf = easyFileFilter(
      s"Supported app's Android database"
    ) { _.getName.endsWith(".db") }

    private val waTextFf = easyFileFilter(
      s"WhatsApp text export"
    ) { f => f.getName.startsWith("WhatsApp Chat with ") && f.getName.endsWith(".txt") }

    def openChooser(remote: Boolean): FileChooser = new FileChooser(null) {
      title = "Select a database to open"
      peer.addChoosableFileFilter(sqliteFf)
      if (!remote) {
        peer.addChoosableFileFilter(h2ff)
      }
      peer.addChoosableFileFilter(tgFf)
      peer.addChoosableFileFilter(androidFf)
      peer.addChoosableFileFilter(waTextFf)
      if (!remote) {
        peer.addChoosableFileFilter(gts5610Ff)
      }
    }

    def load(file: JFile, remote: Boolean): ChatHistoryDao = {
      val f = file.getParentFile
      if (!remote && h2ff.accept(file)) {
        h2.loadData(f)
      } else if (sqliteFf.accept(file) || tgFf.accept(file) || androidFf.accept(file) || waTextFf.accept(file)) {
        val loader = if (remote) grpcHolder.remoteLoader else grpcHolder.eagerLoader
        loader.loadData(file)
      } else if (!remote && gts5610Ff.accept(file)) {
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

    def createH2(dir: JFile): MutableChatHistoryDao = {
      h2.create(dir)
    }

    def saveAsH2(srcDao: ChatHistoryDao, dir: JFile): MutableChatHistoryDao = {
      val dstDao = h2.create(dir)
      dstDao.copyAllFrom(srcDao)
      dstDao
    }
  }
}
