package org.fs.chm

import java.io.File

import org.fs.chm.loader.DataLoader
import org.fs.chm.loader.TelegramDataLoader

object ChatHistoryManagerMain extends App {
  val dataLoaders: Seq[DataLoader] = Seq(
    new TelegramDataLoader
  )
  val loaded = dataLoaders.head.loadData(new File("_data/DataExport_2019-10-04_04-06_json"))
  println(loaded)
}
