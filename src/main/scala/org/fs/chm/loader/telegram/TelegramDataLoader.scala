package org.fs.chm.loader.telegram

import java.io.File
import org.fs.chm.dao.EagerChatHistoryDao
import org.fs.chm.loader.DataLoader
import org.json4s.JNothing
import org.json4s.jackson.JsonMethods

trait TelegramDataLoader extends DataLoader[EagerChatHistoryDao] {

  /** Check whether the given file looks to have a right format, show what's wrong if it isn't */
  def doesLookRight(rootFile: File): Option[String]

  protected def checkFormatLooksRight(rootFile: File, expectedFields: Seq[String]): Option[String] = {
    val resultJsonFile: File = new File(rootFile, "result.json").getAbsoluteFile
    if (!resultJsonFile.exists()) {
      Some("result.json not found in " + rootFile.getAbsolutePath)
    } else {
      val parsed = JsonMethods.parse(resultJsonFile)
      expectedFields.foldLeft(Option.empty[String]) {
        case (s: Some[String], _) => s
        case (None, fieldName) =>
          val res = parsed \ fieldName
          if (res == JNothing) {
            Some(s"Incompatible format! Field '$fieldName' not found")
          } else {
            None
          }
      }
    }
  }
}
