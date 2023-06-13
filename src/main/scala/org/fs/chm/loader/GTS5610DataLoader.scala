package org.fs.chm.loader

import java.io.File
import java.nio.file.Files

import scala.jdk.CollectionConverters._
import scala.collection.immutable.ListMap

import com.github.nscala_time.time.Imports._
import org.apache.commons.codec.net.QuotedPrintableCodec
import org.fs.chm.dao._

/** Loads messages exported from Samsung GT-S5610 */
class GTS5610DataLoader extends DataLoader[EagerChatHistoryDao] {
  import org.fs.chm.loader.GTS5610DataLoader._

  private val dtf = DateTimeFormat.forPattern("dd.MM.yyyy HH:mm:ss")

  /**
   * Messages exported have some REALLY WEIRD encoding for people names (at least in cyrillic).
   * Fortunately, they also embed contact name in the file name.
   */
  private val filenameRegex = ("\\d{14}_(.+)\\." + DefaultExt).r

  /** Path should point to the folder with a bunch of `*.vmg` files */
  override protected def loadDataInner(path: File, createNew: Boolean): EagerChatHistoryDao = {
    require(!createNew, "Creating new dataset is not supported for GT-S5610 (as it makes no sense)")
    val vmsgFiles = path.listFiles((f, n) => n.endsWith("." + DefaultExt)).toSeq
    require(vmsgFiles.nonEmpty, s"No $DefaultExt files to import!")

    val nameWithSrcs = vmsgFiles.map { f =>
      val filenameRegex(contactName) = f.getName
      contactName -> Files.readAllLines(f.toPath).asScala.mkString("\n")
    }
    val vmsgs = nameWithSrcs map { case (name, src) => parseVmessage(name, src) }

    val dataset = Dataset.createDefault("GT-S5610", "samsung-gt-s5610")

    val myself = User(
      dsUuid             = dataset.uuid,
      id                 = 1,
      firstNameOption    = Some("Me"),
      lastNameOption     = None,
      usernameOption     = None,
      phoneNumberOption  = None
    )

    val userToChatWithMsgsMap: Map[User, (Chat, IndexedSeq[Message])] =
      vmsgs
        .groupBy(_.name)
        .view.mapValues(_.sortBy(_.dateTime)).toMap
        .map {
          case (_, vmsgs) =>
            val head = vmsgs.head
            val userId = (head.name + head.phoneOption.getOrElse("")).hashCode.abs

            // TODO: Special treatment for myself
            val user = User(
              dsUuid            = dataset.uuid,
              id                = userId,
              firstNameOption   = Some(head.name),
              lastNameOption    = None,
              usernameOption    = None,
              phoneNumberOption = head.phoneOption
            )

            val msgs: IndexedSeq[Message] = vmsgs.toIndexedSeq map { vmsg =>
              Message.Regular(
                internalId             = Message.NoInternalId,
                sourceIdOption         = None,
                time                   = vmsg.dateTime,
                editTimeOption         = None,
                fromId                 = userId,
                forwardFromNameOption  = None,
                replyToMessageIdOption = None,
                textOption             = Some(RichText.fromPlainString(vmsg.text)),
                contentOption          = None
              )
            }
            val chat = Chat(
              dsUuid        = dataset.uuid,
              id            = userId,
              nameOption    = user.firstNameOption,
              tpe           = ChatType.Personal,
              imgPathOption = None,
              memberIds     = Set(myself, user).map(_.id),
              msgCount      = msgs.size
            )
            user -> (chat, msgs)
        }

    // Ordering by descending last message
    val chatsWithMessages = ListMap(userToChatWithMsgsMap.values.toSeq.sortBy(_._2.last.time).reverse.toSeq: _*)

    val users = (userToChatWithMsgsMap.keys.toSet + myself).toSeq sortBy (u => (u.id, u.prettyName))

    new EagerChatHistoryDao(
      name               = "GT-S5610 export data from " + path.getName,
      _dataRootFile       = path,
      dataset            = dataset,
      myself1            = myself,
      users1             = users,
      _chatsWithMessages = chatsWithMessages
    )
  }

  /** For the "name" argument, see comment for filenameRegex */
  private def parseVmessage(name: String, src: String): VMessage = {
    val lines = src.split("\n").toSeq
    val vtags = parseVTagsRecursive(lines, Seq.empty)

    val phoneOption = vtags("TEL").valueOption
    val date        = dtf.parseDateTime(vtags("DATE").value)
    val text        = decodeVText(vtags("TEXT"))
    VMessage(name, phoneOption, date, text)
  }

  private def parseVTagsRecursive(lines: Seq[String], acc: Seq[VTag]): Seq[VTag] =
    if (lines.isEmpty) acc
    else {
      require(!lines.head.startsWith("="), "Broken parser!")
      // According to RFC 2045 spec, encoded lines might span across multiple lines (ending with =)
      val softBreakLines = lines.takeWhile(_.endsWith("="))
      val line           = (softBreakLines.map(_.dropRight(1)) :+ lines.drop(softBreakLines.size).head).mkString
      val tag            = parseVTag(line)
      val rest           = lines.drop(softBreakLines.length + 1)
      parseVTagsRecursive(rest, acc :+ tag)
    }

  private def parseVTag(concatenatedLine: String): VTag = {
    val (prefix, value) = splitOn2(concatenatedLine, ":")
    val (tag, metas)    = splitOn2(prefix, ";")
    val meta = metas
      .split(";")
      .map(splitOn2(_, "="))
      .toMap
      .map { case (k, v) => (k.toUpperCase, v.toUpperCase) }
      .view.filterKeys(_.nonEmpty).toMap
    VTag(tag.toUpperCase, meta, value)
  }

  private def splitOn2(s: String, split: String): (String, String) = {
    val t = s.split(split, 2)
    (t(0), if (t.size > 1) t(1) else "")
  }

  private def decodeVText(tag: VTag): String = {
    if (tag.meta.isEmpty) {
      tag.value
    } else {
      require(tag.meta("ENCODING") == "QUOTED-PRINTABLE", s"VTag unknown encoding ${tag.meta("ENCODING")}!")
      require(tag.meta.contains("CHARSET"), "VTag has no charset!")
      val codec = new QuotedPrintableCodec(tag.meta("CHARSET"))
      codec.decode(tag.value)
    }
  }

  /** Tag and meta are upper-cased */
  private case class VTag(tag: String, meta: Map[String, String], value: String) {
    def valueOption = if (value.isEmpty) None else Some(value)
  }

  private implicit class RichVTagSeq(s: Seq[VTag]) {
    def apply(tag: String): VTag = {
      s.find(_.tag == tag).getOrElse(throw new IllegalStateException(s"Tag '$tag' is not present!"))
    }
  }

  private case class VMessage(name: String, phoneOption: Option[String], dateTime: DateTime, text: String)
}

object GTS5610DataLoader {
  val DefaultExt = "vmg"
}
