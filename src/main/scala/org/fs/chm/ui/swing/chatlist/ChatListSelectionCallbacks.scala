package org.fs.chm.ui.swing.chatlist

import org.fs.chm.dao.Chat

trait ChatListSelectionCallbacks {
  def chatSelected(c: Chat): Unit
}
