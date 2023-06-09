package org.fs.chm

import org.fs.chm.loader.telegram.TelegramGrpcDataLoader
import org.fs.chm.ui.swing.MainFrameApp
import org.slf4s.Logging

object ChatHistoryManagerMain extends App with Logging {
  val grpcDataLoader = new TelegramGrpcDataLoader(50051)
  new MainFrameApp(grpcDataLoader).startup(args)
}
