package org.fs.chm.dao

import java.io.File

import scala.collection.immutable.ListMap

import com.github.nscala_time.time.Imports._
import org.fs.chm.dao.Entities._
import org.fs.chm.protobuf.Chat
import org.fs.chm.protobuf.Dataset
import org.fs.chm.protobuf.Message
import org.fs.chm.protobuf.PbUuid
import org.fs.chm.protobuf.User
import org.fs.chm.utility.LangUtils._
import org.fs.utility.Imports._

/**
 * Serves as in-memory ChatHistoryDao.
 * Guarantees `internalId` ordering to match order in  `_chatsWithMessages`.
 */
class EagerChatHistoryDao(
    override val name: String,
    _dataRootFile: File,
    dataset: Dataset,
    myself1: User,
    users1: Seq[User],
    _chatsWithMessages: ListMap[Chat, IndexedSeq[Message]]
) extends ChatHistoryDao {
  require(users1 contains myself1)

  private val chatsWithMessages: ListMap[Chat, IndexedSeq[Message]] = {
    var internalId = 0L
    // We can't use mapValues because it's lazy
    _chatsWithMessages map {
      case (c, ms) =>
        (c, ms map { (m: Message) =>
          internalId += 1
          m.withInternalId(internalId)
        })
    }
  }

  // Sanity check: all chat members should have users.
  chatsWithMessages.keys.foreach(c => chatMembers(c))

  override def storagePath: File = _dataRootFile

  override def datasets: Seq[Dataset] = Seq(dataset)

  override def datasetRoot(dsUuid: PbUuid): DatasetRoot = _dataRootFile.getAbsoluteFile.asInstanceOf[DatasetRoot]

  override def datasetFiles(dsUuid: PbUuid): Set[File] = {
    val dsRoot       = datasetRoot(dsUuid)
    val cwds         = chats(dsUuid)
    val chatImgFiles = cwds.map(_.chat.imgPathOption.map(_.toFile(dsRoot))).yieldDefined.toSet
    val msgFiles = for {
      cwd <- cwds
      m   <- firstMessages(cwd.chat, Int.MaxValue)
    } yield m.files(dsRoot)
    chatImgFiles ++ msgFiles.toSet.flatten
  }

  override def myself(dsUuid: PbUuid): User = myself1

  override def users(dsUuid: PbUuid): Seq[User] = myself1 +: users1.filter(_ != myself1)

  override def userOption(dsUuid: PbUuid, id: Long): Option[User] = users1.find(_.id == id)

  private val chats1: Seq[Chat] = chatsWithMessages.keys.toSeq

  override def chats(dsUuid: PbUuid): Seq[ChatWithDetails] = {
    chatsWithMessages.toSeq.map {
      case (c, msgs) => ChatWithDetails(c, msgs.lastOption, chatMembers(c))
    }.sortBy(_.lastMsgOption.map(- _.timestamp).getOrElse(Long.MaxValue)) // Minus used to reverse order
  }

  override def chatOption(dsUuid: PbUuid, id: Long): Option[ChatWithDetails] = chatsWithMessages find (_._1.id == id) map {
    case (c, msgs) => ChatWithDetails(c, msgs.lastOption, chatMembers(c))
  }

  private def chatMembers(chat: Chat): Seq[User] = {
    val me = myself(chat.dsUuid)
    me +: (chat.memberIds
      .filter(_ != me.id)
      .map(mId => users1.find(_.id == mId).getOrElse(throw new IllegalStateException(s"No member with id ${mId} found for chat ${chat.qualifiedName}")))
      .toSeq
      .sortBy(_.id))
  }

  override def messagesBeforeImpl(chat: Chat, msg: Message, limit: Int): IndexedSeq[Message] = {
    val messages = chatsWithMessages(chat)
    val idx      = messages.lastIndexWhere(_.internalId <= msg.internalId)
    require(idx > -1, "Message not found!")
    val lowerLimit = (idx - limit + 1) max 0
    messages.slice(lowerLimit, idx + 1)
  }

  override def messagesAfterImpl(chat: Chat, msg: Message, limit: Int): IndexedSeq[Message] = {
    val messages = chatsWithMessages(chat)
    val idx      = messages.indexWhere(_.internalId >= msg.internalId)
    require(idx > -1, "Message not found!")
    val upperLimit = (idx + limit) min messages.size
    messages.slice(idx, upperLimit)
  }

  override def messagesBetweenImpl(chat: Chat, msg1: Message, msg2: Message): IndexedSeq[Message] = {
    require(msg1.internalId <= msg2.internalId)
    val messages = chatsWithMessages(chat)
    val idx1     = messages.indexWhere(_.internalId >= msg1.internalId)
    val idx2     = messages.lastIndexWhere(_.internalId <= msg2.internalId)
    require(idx1 > -1, "Message 1 not found!")
    require(idx2 > -1, "Message 2 not found!")
    assert(idx2 >= idx1)
    messages.slice(idx1, idx2 + 1)
  }

  override def countMessagesBetween(chat: Chat, msg1: Message, msg2: Message): Int = {
    require(msg1.internalId <= msg2.internalId)
    val between = messagesBetween(chat, msg1, msg2)
    var size    = between.size
    if (size == 0) {
      0
    } else {
      if (between.head.internalId == msg1.internalId) size -= 1
      if (between.last.internalId == msg2.internalId) size -= 1
      math.max(size, 0)
    }
  }

  def messagesAroundDate(chat: Chat, date: DateTime, limit: Int): (IndexedSeq[Message], IndexedSeq[Message]) = {
    val dateTs    = date.unixTimestamp
    val messages  = chatsWithMessages(chat)
    val idx       = messages.indexWhere(m => m.timestamp >= dateTs)
    if (idx == -1) {
      // Not found
      (lastMessages(chat, limit), IndexedSeq.empty)
    } else {
      val msgsBefore = messages.slice(idx - limit, idx)
      val msgsAfter  = messages.slice(idx, idx + limit)
      (msgsBefore, msgsAfter)
    }
  }

  override def scrollMessages(chat: Chat, offset: Int, limit: Int): IndexedSeq[Message] = {
    chatsWithMessages.get(chat) map (_.slice(offset, offset + limit)) getOrElse IndexedSeq.empty
  }

  override def lastMessages(chat: Chat, limit: Int): IndexedSeq[Message] = {
    chatsWithMessages.get(chat) map (_.takeRight(limit)) getOrElse IndexedSeq.empty
  }

  override def messageOption(chat: Chat, id: MessageSourceId): Option[Message] =
    chatsWithMessages.get(chat) flatMap (_ find (_.sourceIdOption contains id))

  override def messageOptionByInternalId(chat: Chat, id: MessageInternalId): Option[Message] =
    chatsWithMessages.get(chat) flatMap (_ find (_.internalId == id))

  /** Get a copy of this DAO with shifted time of all timestamps in the dataset to accommodate timezone differences */
  def copyWithShiftedDatasetTime(dsUuid: PbUuid, hrs: Int): EagerChatHistoryDao = {
    val tsShift = hrs * 3600L
    new EagerChatHistoryDao(
      name               = name,
      _dataRootFile      = _dataRootFile,
      dataset            = dataset,
      myself1            = myself1,
      users1             = users1,
      _chatsWithMessages = chatsWithMessages.map { case (c, msgs) =>
        (c, msgs.map(m => {
          val typed: Message.Typed = m.typed match {
            case Message.Typed.Regular(regular) if regular.editTimestampOption.isDefined =>
              Message.Typed.Regular(regular.copy(editTimestampOption = regular.editTimestampOption.map(_ + tsShift)))
            case _ =>
              m.typed
          }
          m.copy(timestamp = m.timestamp + tsShift, typed = typed)
        }))
      }
    )
  }

  override def toString: String = {
    Seq(
      "EagerChatHistoryDao(",
      "  myself:",
      "    " + myself1.toString + "\n",
      "  users:",
      users1.mkString("    ", "\n    ", "\n"),
      "  chats:",
      chats1.mkString("    ", "\n    ", "\n"),
      ")"
    ).mkString("\n")
  }

  override def isLoaded(storagePath: File): Boolean = {
    storagePath != null && this._dataRootFile == storagePath
  }

  override def equals(that: Any): Boolean = that match {
    case that: EagerChatHistoryDao => this.name == that.name && that.isLoaded(this._dataRootFile)
    case _                         => false
  }

  override def hashCode(): Int = this.name.hashCode + 17 * this._dataRootFile.hashCode
}
