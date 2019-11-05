package org.fs.chm.ui.swing

import java.awt.Desktop
import java.awt.event.AdjustmentEvent
import java.awt.event.InputEvent
import java.awt.event.KeyEvent
import java.awt.{ Container => AwtContainer }
import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.swing.BorderPanel.Position._
import scala.swing._
import scala.swing.event.ButtonClicked

import com.github.nscala_time.time.Imports._
import javax.swing.JComponent
import javax.swing.JPanel
import javax.swing.KeyStroke
import javax.swing.border.EmptyBorder
import javax.swing.event.HyperlinkEvent
import org.fs.chm.dao._
import org.fs.chm.ui.swing.MessagesService._
import org.fs.chm.ui.swing.general.ExtendedHtmlEditorKit
import org.fs.chm.ui.swing.webp.Webp
import org.fs.utility.Imports._

class MainFrameApp(dao: ChatHistoryDao) extends SimpleSwingApplication {

  val MsgBatchLoadSize = 100

  /** Lock that should be taken to perform operations on any of the variables below */
  val Lock = new Object

  var documentsCache:  Map[Chat, MessageDocument] = Map.empty
  var loadStatusCache: Map[Chat, LoadStatus]      = Map.empty

  var currentContextOption:   Option[Context] = None
  var loadMessagesInProgress: AtomicBoolean   = new AtomicBoolean(false)

  val desktopOption = if (Desktop.isDesktopSupported) Some(Desktop.getDesktop) else None
  val htmlKit       = new ExtendedHtmlEditorKit(desktopOption)
  val msgService    = new MessagesService(dao, htmlKit)

  val searchTextField = new TextField

  // TODO:
  // reply-to (make clickable)
  // word-wrap and narrower width
  // search
  // jump to date
  // better pictures rendering
  // emoji and fonts
  // better chat switching

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
    layout(chatsPanel) = West
    layout(new BorderPanel {
      layout(msgAreaContainer.panel) = Center
      layout(searchPanel) = South
    }) = Center

    addHotkey(peer, "search", InputEvent.CTRL_MASK, KeyEvent.VK_F, searchOpened())
  }

  lazy val chatsPanel = new BorderPanel {
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

  lazy val searchPanel = new BorderPanel {
    visible = false;

    val prevBtn  = new Button("Previous")
    val nextBtn  = new Button("Next")
    val closeBtn = new Button("Close")

    layout(new BorderPanel {
      layout(searchTextField) = Center
      border = new EmptyBorder(5, 5, 5, 5)
    }) = Center
    layout(new FlowPanel(prevBtn, nextBtn, closeBtn)) = East

    def closeClicked(): Unit = {
      visible = false;
    }
    listenTo(prevBtn, nextBtn, closeBtn)
    reactions += {
      case ButtonClicked(`prevBtn`)  => searchClicked(false)
      case ButtonClicked(`nextBtn`)  => searchClicked(true)
      case ButtonClicked(`closeBtn`) => closeClicked()
    }
    addHotkey(peer, "searchPrev", 0 /* mod */, KeyEvent.VK_ENTER, searchClicked(false))
    addHotkey(peer, "searchClose", 0 /* mod */, KeyEvent.VK_ESCAPE, closeClicked())
  }

  def changeUiClickable(enabled: Boolean): Unit = {
    def changeClickableRecursive(c: AwtContainer): Unit = {
      c.setEnabled(enabled)
      c.getComponents foreach {
        case c: AwtContainer => changeClickableRecursive(c)
        case _               => //NOOP
      }
    }
    changeClickableRecursive(ui.peer)
  }

  //
  // Events
  //

  def chatSelected(c: Chat): Unit = {
    Lock.synchronized {
      currentContextOption = None
      msgAreaContainer.document = msgService.pleaseWaitDoc
      changeUiClickable(false)
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
          firstId = messages.headOption map (_.id) getOrElse (-1),
          lastId = messages.lastOption map (_.id) getOrElse (-1),
          beginReached = messages.size < MsgBatchLoadSize,
          endReached = true
        )
        Lock.synchronized {
          loadStatusCache = loadStatusCache + ((c, loadStatus))
          documentsCache = documentsCache + ((c, md))
        }
      }
    }
    f foreach { _ =>
      Swing.onEDTWait(Lock.synchronized {
        currentContextOption = Some(Context(c))
        val doc = documentsCache(c).doc
        msgAreaContainer.document = doc
        msgAreaContainer.scroll.toEnd()
        changeUiClickable(true)
      })
    }
  }

  def searchOpened(): Unit = currentContextOption foreach { c =>
    searchPanel.visible = true
    searchTextField.requestFocus()
  }

  def searchClicked(forward: Boolean): Unit =
    // Everything is done with a lock since we're constantly altering vars.
    // This can possibly be optimized though.
    Lock.synchronized {
      import scala.collection.Searching._
      val ctx = currentContextOption.get

      try {
        val text = searchTextField.text.trim

        if (text.nonEmpty) {
          // If search isn't performed yet, or if the text changed - reset the search
          ctx.searchStateOption match {
            case Some(s) if s.text == text => // NOOP
            case _                         => ctx.searchStateOption = Some(SearchState(text))
          }

          val searchState = ctx.searchStateOption.get
          if (searchState.resultsOption.isEmpty) {
            searchState.resultsOption = Some(dao.search(ctx.chat, text, true))
          }

          val msgsFound = searchState.resultsOption.get
          println(msgsFound.size)
          if (msgsFound.isEmpty) {
            // NOOP
            println("Empty!")
          } else {
            val msgToShow = searchState.lastHighlightedMessageOption match {
              case Some(msg) =>
                val Found(idx) = msgsFound.search(msg)(MessagesOrdering)
                val newIdx     = ((if (forward) idx + 1 else idx - 1) + msgsFound.length) % msgsFound.length
                msgsFound(newIdx)
              case _ =>
                msgsFound.last
            }
            val loadStatus = loadStatusCache(ctx.chat)
            val distance1  = dao.distance(ctx.chat, msgToShow.id, loadStatus.firstId)
            val distance2  = dao.distance(ctx.chat, msgToShow.id, loadStatus.lastId)
            //    if ((distance1 min distance2) < MsgBatchLoadSize) {
            //      // TODO
            //    }
            val messages = dao.messagesAround(ctx.chat, msgToShow.id, MsgBatchLoadSize)
            println(messages.size)
            // TODO: Render
            ???
            searchState.lastHighlightedMessageOption = Some(msgToShow)
          }
        }
      } catch {
        case ex: Exception =>
          ctx.searchStateOption = None
          throw ex
      }
    }

  //
  // Additional logic
  //

  def tryLoadPreviousMessages(): Unit =
    currentContextOption match {
      case _ if loadMessagesInProgress.get => // NOOP
      case None                            => // NOOP
      case Some(ctx) =>
        Lock.synchronized {
          msgAreaContainer.caretUpdatesEnabled = false
          val loadStatus = loadStatusCache(ctx.chat)
          if (!loadStatus.beginReached) {
            changeUiClickable(false)
            loadMessagesInProgress set true
            val md = documentsCache(ctx.chat)
            val f = Future {
              Lock.synchronized {
                val (viewPos1, viewSize1) = msgAreaContainer.view.posAndSize
                md.insert("<div id=\"loading\"><hr><p> Loading... </p><hr></div>", MessageInsertPosition.Leading)
                val addedMessages = dao.messagesBefore(ctx.chat, loadStatus.firstId, MsgBatchLoadSize)
                loadStatusCache = loadStatusCache.updated(
                  ctx.chat,
                  loadStatusCache(ctx.chat).copy(
                    firstId = addedMessages.headOption map (_.id) getOrElse (-1),
                    beginReached = addedMessages.size < MsgBatchLoadSize
                  )
                )
                // We're still holding the lock
                Swing.onEDTWait {
                  // TODO: Prevent flickering
                  // TODO: Preserve selection
                  md.removeFirst()
                  for (m <- addedMessages.reverse) {
                    md.insert(msgService.renderMessageHtml(ctx.chat, m), MessageInsertPosition.Leading)
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
                changeUiClickable(true)
                msgAreaContainer.caretUpdatesEnabled = true
              }))
          }
        }
    }

  def addHotkey(peer: JComponent, key: String, mod: Int, event: Int, f: => Unit): Unit = {
    peer
      .getInputMap(JComponent.WHEN_IN_FOCUSED_WINDOW)
      .put(KeyStroke.getKeyStroke(event, mod), key)
    peer.getActionMap.put(key, new javax.swing.AbstractAction {
      def actionPerformed(arg: java.awt.event.ActionEvent): Unit = f
    })
  }

  case class Context(chat: Chat) {
    var searchStateOption: Option[SearchState] = None
  }

  case class SearchState(text: String) {
    var resultsOption:                Option[IndexedSeq[Message]] = None
    var lastHighlightedMessageOption: Option[Message]             = None
  }

  case class LoadStatus(
      firstId: Long,
      lastId: Long,
      beginReached: Boolean,
      endReached: Boolean
  )

  object MessagesOrdering extends Ordering[Message] {
    override def compare(x: Message, y: Message): Int = {
      x.date compare y.date match {
        case 0       => x.id compare y.id
        case nonZero => nonZero
      }
    }
  }
}
