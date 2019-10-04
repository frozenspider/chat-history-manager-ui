package org.fs.chm.loader

import java.io.File
import java.io.FileNotFoundException
import java.time.format.DateTimeFormatter

import com.github.nscala_time.time.Imports.DateTime
import org.fs.chm.dao.ChatHistoryDao
import org.fs.chm.dao.Contact
import org.fs.chm.dao.EagerChatHistoryDao
import org.joda.time.format.DateTimeFormat
import org.json4s._
import org.json4s.jackson.JsonMethods
import org.json4s.prefs.EmptyValueStrategy

class TelegramDataLoader extends DataLoader {
  implicit private val formats: Formats = DefaultFormats

  override def loadDataInner(path: File): ChatHistoryDao = {
    val resultJsonFile: File = new File(path, "result.json")
    if (!resultJsonFile.exists()) throw new FileNotFoundException("result.json not found in " + path.getAbsolutePath)
    val parsed = JsonMethods.parse(resultJsonFile)
    val contacts = for {
      contact <- checkField(parsed \ "contacts" \ "list").extract[Seq[JValue]]
    } yield
      Contact(
        id = checkField(contact \ "user_id").extract[Int],
        firstNameOption = stringToOpt(checkField(contact \ "first_name").extract[String]),
        lastNameOption = stringToOpt(checkField(contact \ "last_name").extract[String]),
        phoneNumberOption = stringToOpt(checkField(contact \ "phone_number").extract[String]),
        // TODO: timezone?
        lastSeenDateOption = stringToOpt(checkField(contact \ "date").extract[String]).map(DateTime.parse)
      )

    new EagerChatHistoryDao(contacts = contacts)
  }

  private def stringToOpt(s: String): Option[String] = {
    if (s.isEmpty) None else Some(s)
  }

  private def checkField(jv: JValue): JValue = {
    require(jv != JNothing, "Incompatible format!")
    jv
  }
}
