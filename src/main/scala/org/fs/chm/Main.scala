package org.fs.chm

import org.fs.chm.ui.swing.MainFrameApp
import org.slf4s.Logging

object Main extends App with Logging {
  val grpcPort = 50051
  val app = new MainFrameApp(grpcPort)
  app.startup(args)
  app.synchronized {
    app.wait()
  }
}
