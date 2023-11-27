package org.fs.chm

import org.fs.chm.loader.GrpcDataLoaderHolder
import org.fs.chm.ui.swing.MainFrameApp
import org.slf4s.Logging

object Main extends App with Logging {
  val grpcPort = 50051
  new MainFrameApp(grpcPort).startup(args)
}
