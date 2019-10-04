package org.fs.chm.loader

import java.io.File
import java.io.FileNotFoundException
import java.time.format.DateTimeFormatter

import scala.collection.immutable.ListMap

import com.github.nscala_time.time.Imports.DateTime
import org.fs.chm.dao._
import org.joda.time.format.DateTimeFormat
import org.json4s._
import org.json4s.jackson.JsonMethods
import org.json4s.prefs.EmptyValueStrategy

class TelegramDataLoader extends DataLoader {
  implicit private val formats: Formats = DefaultFormats.withLong

  override def loadDataInner(path: File): ChatHistoryDao = {
    val resultJsonFile: File = new File(path, "result.json")
    if (!resultJsonFile.exists()) throw new FileNotFoundException("result.json not found in " + path.getAbsolutePath)
    val parsed = JsonMethods.parse(resultJsonFile)
    val contacts = for {
      contact <- getCheckedField(parsed, "contacts", "list").extract[Seq[JValue]]
    } yield
      Contact(
        id                = getCheckedField(contact, "user_id").extract[Long],
        firstNameOption   = stringToOpt(getCheckedField(contact, "first_name").extract[String]),
        lastNameOption    = stringToOpt(getCheckedField(contact, "last_name").extract[String]),
        phoneNumberOption = stringToOpt(getCheckedField(contact, "phone_number").extract[String]),
        // TODO: timezone?
        lastSeenDateOption = stringToDateTimeOpt(getCheckedField(contact, "date").extract[String])
      )

    val chatsWithMessages = for {
      chat      <- getCheckedField(parsed, "chats", "list").extract[Seq[JValue]]
      tpeString = getCheckedField(chat, "type").extract[String]
      if (tpeString != "saved_messages")
    } yield {
      val messagesRes = for {
        messages <- getCheckedField(chat, "messages").extract[IndexedSeq[JValue]]
        if getCheckedField(messages, "type").extract[String] == "message"
        // FIXME: Service messages, phone calls
      } yield {
        Message.Regular(
          id                     = getCheckedField(messages, "id").extract[Long],
          date                   = stringToDateTimeOpt(getCheckedField(messages, "date").extract[String]).get,
          editDateOption         = stringToDateTimeOpt(getCheckedField(messages, "edited").extract[String]),
          fromName               = getCheckedField(messages, "from").extract[String],
          fromId                 = getCheckedField(messages, "from_id").extract[Long],
          forwardFromNameOption  = (messages \ "forwarded_from").extractOpt[String],
          replyToMessageIdOption = (messages \ "reply_to_message_id").extractOpt[Long],
          textOption             = stringToOpt(getCheckedField(messages, "text").toString), // FIXME
          contentOption          = None // FIXME
        )
      }
      val chatRes = Chat(
        id         = getCheckedField(chat, "id").extract[Long],
        nameOption = getCheckedField(chat, "name").extractOpt[String],
        tpe = tpeString match {
          case "personal_chat" => ChatType.Personal
          case "private_group" => ChatType.PrivateGroup
          case s               => throw new IllegalArgumentException("Illegal format, unknown chat type '$s'")
        },
        msgNum = messagesRes.size
      )
      (chatRes, messagesRes)
    }
    val chatsWithMessagesLM = ListMap(chatsWithMessages: _*)

    new EagerChatHistoryDao(contacts = contacts, chatsWithMessages = chatsWithMessagesLM)
  }

  //{
  //"id": 5165,
  //"type": "message",
  //"date": "2016-11-18T20:09:04",
  //"edited": "1970-01-01T05:00:00",
  //"from": "Vadim Lazarenko",
  //"from_id": 182120723,
  //"file": "chats/chat_01/stickers/sticker (14).webp",
  //"thumbnail": "chats/chat_01/stickers/sticker (14).webp_thumb.jpg",
  //"media_type": "sticker",
  //"sticker_emoji": "💪",
  //"width": 438,
  //"height": 512,
  //"text": ""
  //},
  //{
  //"id": 5167,
  //"type": "message",
  //"date": "2016-11-19T15:31:59",
  //"edited": "1970-01-01T05:00:00",
  //"from": "Alex Abdugafarov",
  //"from_id": 92139334,
  //"text": "Кошка заходит в кафе, заказывает кофе и пирожное. Официант стоит с открытым ртом.\nКошка:\n— Что?\n— Эээ... вы кошка!\n— Да.\n— Вы разговариваете!\n— Какая новость. Вы принесете мой заказ или нет?\n— Ооо, простите, пожалуйста, конечно, принесу. Я просто никогда раньше не видел...\n— А я тут раньше и не бывала. Я ищу работу, была на собеседовании, решила вот выпить кофе.\nОфициант возвращается с заказом, видит кошку, строчащую что-то на клавиатуре ноутбука.\n— Ваш кофе. Эээ... я тут подумал... Вы ведь ищете работу, да? Просто мой дядя — директор цирка, и он с удовольствием взял бы вас на отличную зарплату!\n— Цирк? — говорит кошка. — Это где арена, купол, оркестр?\n— Да!\n— Клоуны, акробаты, слоны?\n— Да!\n— Сахарная вата, попкорн, леденцы на палочке?\n— Да-да-да!\n— Звучит заманчиво! А нахрена им программист?"
  //},

  private def stringToOpt(s: String): Option[String] = {
    if (s.isEmpty) None else Some(s)
  }

  private def stringToDateTimeOpt(s: String): Option[DateTime] = {
    stringToOpt(s).map(DateTime.parse) match {
      case Some(dt) if dt.year.get == 1970 => None // TG puts minimum timestamp in place of absent
      case other                           => other
    }
  }

  private def getCheckedField(jv: JValue, fn: String): JValue = {
    val res = jv \ fn
    require(res != JNothing, s"Incompatible format! Field '$fn' not found in $jv")
    res
  }

  private def getCheckedField(jv: JValue, fn1: String, fn2: String): JValue = {
    val res = jv \ fn1 \ fn2
    require(res != JNothing, s"Incompatible format! Path '$fn1 \\ $fn2' not found in $jv")
    res
  }
}
