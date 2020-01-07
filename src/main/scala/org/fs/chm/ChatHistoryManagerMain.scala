package org.fs.chm

import org.fs.chm.ui.swing.MainFrameApp
import org.slf4s.Logging

object ChatHistoryManagerMain extends App with Logging {
  new MainFrameApp().startup(args)
}
