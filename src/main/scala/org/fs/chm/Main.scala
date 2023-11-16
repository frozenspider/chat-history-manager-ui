package org.fs.chm

import org.fs.chm.loader.GrpcDataLoaderHolder
import org.fs.chm.ui.swing.MainFrameApp
import org.slf4s.Logging

object Main extends App with Logging {
  val grpcPort = 50051
  val grpcDataLoaderHolder = new GrpcDataLoaderHolder(grpcPort)
  new MainFrameApp(grpcDataLoaderHolder.eagerLoader, grpcDataLoaderHolder.remoteLoader).startup(args)
}
