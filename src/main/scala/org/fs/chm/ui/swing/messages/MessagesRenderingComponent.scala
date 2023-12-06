package org.fs.chm.ui.swing.messages

import scala.swing.Component

import org.fs.chm.dao.ChatHistoryDao
import org.fs.chm.dao.Entities.CombinedChat
import org.fs.chm.protobuf.Message

trait MessagesRenderingComponent[MD] {

  def component: Component

  /** Replace current content with a doc reading "please, wait" style message */
  def renderPleaseWait(): Unit

  /** Replace current content with a doc rendering messages */
  def render(dao: ChatHistoryDao, cc: CombinedChat, msgs: IndexedSeq[Message], beginReached: Boolean, showTop: Boolean): MD

  /** Replace current content with a given doc */
  def render(msgDoc: MD, showTop: Boolean): Unit

  /** Prepend current content with "loading" section */
  def prependLoading(): MD

  /** Append current content with "loading" section */
  def appendLoading(): MD

  /** Prepend current content given messages. Removes "loading" section, if any. */
  def prepend(dao: ChatHistoryDao, cc: CombinedChat, msgs: IndexedSeq[Message], beginReached: Boolean): MD

  /** Append current content given messages. Removes "loading" section, if any. */
  def append(dao: ChatHistoryDao, cc: CombinedChat, msgs: IndexedSeq[Message], endReached: Boolean): MD

  /** Signal update started. Either append or prepend might happen in the meantime, not both! */
  def updateStarted(): Unit

  def updateFinished(): Unit
}

object MessagesRenderingComponent {
  trait MessageDocument
}
