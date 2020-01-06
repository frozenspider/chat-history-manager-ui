package org.fs.chm.ui.swing.chatlist

import org.fs.chm.ui.swing.general.ChatWithDao

trait ChatListSelectionCallbacks {
  def chatSelected(cc: ChatWithDao): Unit
}
