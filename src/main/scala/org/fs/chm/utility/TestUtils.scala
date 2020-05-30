package org.fs.chm.utility

import java.io.File
import java.nio.file.Files
import java.util.UUID

import scala.collection.immutable.ListMap
import scala.util.Random

import com.github.nscala_time.time.Imports._
import org.fs.chm.dao._
import org.fs.chm.dao.merge.DatasetMerger.TaggedMessage

/**
 * Utility stuff used for testing, both automatically and manually
 */
object TestUtils {

  val noUuid   = UUID.fromString("00000000-0000-0000-0000-000000000000")
  val baseDate = DateTime.parse("2019-01-02T11:15:21")
  val rnd      = new Random()

  def createUser(dsUuid: UUID, idx: Int): User =
    User(
      dsUuid             = dsUuid,
      id                 = idx,
      firstNameOption    = Some("User"),
      lastNameOption     = Some(idx.toString),
      usernameOption     = Some("user" + idx),
      phoneNumberOption  = Some("xxx xx xx".replaceAll("x", idx.toString)),
      lastSeenTimeOption = Some(baseDate.plusMinutes(idx))
    )

  def createChat(dsUuid: UUID, idx: Int, nameSuffix: String, messagesSize: Int): Chat =
    Chat(
      dsUuid        = dsUuid,
      id            = idx,
      nameOption    = Some("Chat " + nameSuffix),
      tpe           = ChatType.Personal,
      imgPathOption = None,
      msgCount      = messagesSize
    )

  def createRegularMessage(idx: Int, userId: Int): Message = {
    // Any previous message
    val replyToMessageIdOption =
      if (idx > 0) Some(rnd.nextInt(idx).toLong.asInstanceOf[Message.SourceId]) else None

    Message.Regular(
      internalId             = Message.NoInternalId,
      sourceIdOption         = Some(idx.toLong.asInstanceOf[Message.SourceId]),
      time                   = baseDate.plusMinutes(idx),
      editTimeOption         = Some(baseDate.plusMinutes(idx).plusSeconds(5)),
      fromId                 = userId,
      forwardFromNameOption  = Some("u" + userId),
      replyToMessageIdOption = replyToMessageIdOption,
      textOption             = Some(RichText(Seq(RichText.Plain(s"Hello there, ${idx}!")))),
      contentOption          = Some(Content.Poll(s"Hey, ${idx}!"))
    )
  }

  def createSimpleDao(nameSuffix: String, msgs: Seq[Message], numUsers: Int): MutableChatHistoryDao = {
    val chat  = createChat(noUuid, 1, "One", msgs.size)
    val users = (1 to numUsers).map(i => createUser(noUuid, i))
    createDao(nameSuffix, users, ListMap(chat -> msgs.toIndexedSeq))
  }

  def createDao(
      nameSuffix: String,
      users: Seq[User],
      chatsWithMsgs: ListMap[Chat, Seq[Message]],
      amendMessage: ((File, Message) => Message) = ((_, m) => m)
  ): MutableChatHistoryDao = {
    require({
      val userIds = users.map(_.id).toSet
      chatsWithMsgs.values.flatten.forall(userIds contains _.fromId)
    }, "All messages should have valid user IDs!")
    val ds = Dataset(
      uuid       = UUID.randomUUID(),
      alias      = "Dataset " + nameSuffix,
      sourceType = "test source"
    )
    val users1       = users map (_ copy (dsUuid = ds.uuid))
    val dataPathRoot = Files.createTempDirectory(null).toFile
    dataPathRoot.deleteOnExit()
    val amend2 = amendMessage.curried(dataPathRoot)
    new EagerChatHistoryDao(
      name         = "Dao " + nameSuffix,
      _dataRootFile = dataPathRoot,
      dataset      = ds,
      myself1      = users1.head,
      users1       = users1,
      _chatsWithMessages = chatsWithMsgs.map {
        case (c, ms) => (c.copy(dsUuid = ds.uuid) -> ms.map(amend2).toIndexedSeq)
      }
    ) with EagerMutableDaoTrait
  }

  def getSimpleDaoEntities(dao: ChatHistoryDao): (Dataset, Seq[User], Chat, Seq[Message]) = {
    val ds    = dao.datasets.head
    val users = dao.users(ds.uuid)
    val chat  = dao.chats(ds.uuid).head
    val msgs  = dao.firstMessages(chat, Int.MaxValue)
    (ds, users, chat, msgs)
  }

  implicit class RichUserSeq(users: Seq[User]) {
    def byId(id: Long): User =
      users.find(_.id == id).get
  }

  implicit class RichMsgSeq(msgs: Seq[Message]) {
    def bySrcId[TM <: Message with TaggedMessage](id: Long): TM =
      tag(msgs.find(_.sourceIdOption.get == id).get)
  }

  def tag[TM <: Message with TaggedMessage](m: Message): TM = m.asInstanceOf[TM]

  def tag[TM <: Message with TaggedMessage](id: Int)(implicit msgs: Seq[Message]): TM =
    msgs.bySrcId(id)

  trait EagerMutableDaoTrait extends MutableChatHistoryDao {
    override def insertDataset(ds: Dataset): Unit = ???

    override def renameDataset(dsUuid: UUID, newName: String): Dataset = ???

    override def deleteDataset(dsUuid: UUID): Unit = ???

    override def insertUser(user: User, isMyself: Boolean): Unit = ???

    override def updateUser(user: User): Unit = ???

    override def mergeUsers(baseUser: User, absorbedUser: User): Unit = ???

    override def insertChat(dsRoot: File, chat: Chat): Unit = ???

    override def deleteChat(chat: Chat): Unit = ???

    override def insertMessages(dsRoot: File, chat: Chat, msgs: Seq[Message]): Unit = ???

    override def disableBackups(): Unit = ???

    override def enableBackups(): Unit = ???

    override protected def backup(): Unit = ???
  }
}
