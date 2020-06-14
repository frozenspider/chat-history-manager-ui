package org.fs.chm.ui.swing.messages

import scala.swing.Component

import org.fs.chm.dao._
import org.fs.chm.ui.swing.general.ChatWithDao

trait MessagesRenderingComponent[MD] {

  def component: Component

  /** Replace current content with a doc reading "please, wait" style message */
  def renderPleaseWait(): Unit

  /** Replace current content with a doc rendering messages */
  def render(cwd: ChatWithDao, msgs: IndexedSeq[Message], beginReached: Boolean, showTop: Boolean): MD

  /** Replace current content with a given doc */
  def render(msgDoc: MD, showTop: Boolean): Unit

  /** Prepend current content with "loading" section */
  def prependLoading(): MD

  /** Prepend current content given messages. Removes "loading" section, if any. */
  def prepend(cwd: ChatWithDao, msgs: IndexedSeq[Message], beginReached: Boolean): MD

  def updateStarted(): Unit

  def updateFinished(): Unit
}

object MessagesRenderingComponent {
  trait MessageDocument
}
