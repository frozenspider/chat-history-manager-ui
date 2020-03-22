package org.fs.chm.ui.swing.list.chat

import org.fs.chm.ui.swing.general.ChatWithDao

trait ChatListSelectionCallbacks {
  def chatSelected(cc: ChatWithDao): Unit
}
