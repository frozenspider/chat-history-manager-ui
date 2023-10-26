package org.fs.chm

import org.fs.chm.loader.telegram.GrpcDataLoader
import org.fs.chm.ui.swing.MainFrameApp
import org.slf4s.Logging

object Main extends App with Logging {
  val grpcPort = 50051
  val grpcDataLoader = new GrpcDataLoader(grpcPort)
  new MainFrameApp(grpcDataLoader).startup(args)
}
