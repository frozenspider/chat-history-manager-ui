package org.fs.chm.utility

import java.io.File
import java.nio.file.Files

import scala.collection.immutable.ListMap
import scala.util.Random

import com.github.nscala_time.time.Imports._
import org.fs.chm.dao._
import org.fs.chm.dao.Entities._
import org.fs.chm.dao.merge.DatasetMerger.TaggedMessage
import org.fs.chm.protobuf.Chat
import org.fs.chm.protobuf.ChatType
import org.fs.chm.protobuf.Content
import org.fs.chm.protobuf.ContentPoll
import org.fs.chm.protobuf.Dataset
import org.fs.chm.protobuf.Message
import org.fs.chm.protobuf.MessageRegular
import org.fs.chm.protobuf.PbUuid
import org.fs.chm.protobuf.User

/**
 * Utility stuff used for testing, both automatically and manually
 */
object TestUtils {

  val noUuid   = PbUuid("00000000-0000-0000-0000-000000000000")
  val baseDate = DateTime.parse("2019-01-02T11:15:21")
  val rnd      = new Random()

  def createUser(dsUuid: PbUuid, idx: Int): User =
    User(
      dsUuid            = dsUuid,
      id                = idx,
      firstNameOption   = Some("User"),
      lastNameOption    = Some(idx.toString),
      usernameOption    = Some("user" + idx),
      phoneNumberOption = Some("xxx xx xx".replaceAll("x", idx.toString))
    )

  def createGroupChat(dsUuid: PbUuid, idx: Int, nameSuffix: String, memberIds: Iterable[Long], messagesSize: Int): Chat = {
    require(memberIds.size >= 2)
    Chat(
      dsUuid        = dsUuid,
      id            = idx,
      nameOption    = Some("Chat " + nameSuffix),
      tpe           = ChatType.PrivateGroup,
      imgPathOption = None,
      memberIds     = memberIds.toSeq,
      msgCount      = messagesSize
    )
  }

  def createPersonalChat(dsUuid: PbUuid, idx: Int, user: User, memberIds: Iterable[Long], messagesSize: Int): Chat = {
    require(memberIds.size == 2)
    Chat(
      dsUuid        = dsUuid,
      id            = idx,
      nameOption    = user.prettyNameOption,
      tpe           = ChatType.Personal,
      imgPathOption = None,
      memberIds     = memberIds.toSeq,
      msgCount      = messagesSize
    )
  }

  def createRegularMessage(idx: Int, userId: Int): Message = {
    // Any previous message
    val replyToMessageIdOption =
      if (idx > 0) Some(rnd.nextInt(idx).toLong.asInstanceOf[MessageSourceId]) else None

    val typed = Message.Typed.Regular(MessageRegular(
      editTimestampOption    = Some(baseDate.plusMinutes(idx).plusSeconds(5).getMillis),
      replyToMessageIdOption = replyToMessageIdOption,
      forwardFromNameOption  = Some("u" + userId),
      contentOption          = Some(Content(Content.Val.Poll(ContentPoll(question = s"Hey, ${idx}!"))))
    ))
    val text = Seq(RichText.makePlain(s"Hello there, ${idx}!"))
    Message(
      internalId       = NoInternalId,
      sourceIdOption   = Some(idx.toLong.asInstanceOf[MessageSourceId]),
      timestamp        = baseDate.plusMinutes(idx).getMillis,
      fromId           = userId,
      searchableString = Some(makeSearchableString(text, typed)),
      text             = text,
      typed            = typed
    )
  }

  def createSimpleDao(nameSuffix: String, msgs: Seq[Message], numUsers: Int): MutableChatHistoryDao = {
    val chat  = createGroupChat(noUuid, 1, "One", (1L to numUsers).toSet, msgs.size)
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
      uuid       = randomUuid,
      alias      = "Dataset " + nameSuffix,
      sourceType = "test source"
    )
    val users1       = users map (_ copy (dsUuid = ds.uuid))
    val dataPathRoot = Files.createTempDirectory(null).toFile
    dataPathRoot.deleteOnExit()
    val amend2 = amendMessage.curried(dataPathRoot)
    new EagerChatHistoryDao(
      name               = "Dao " + nameSuffix,
      _dataRootFile      = dataPathRoot,
      dataset            = ds,
      myself1            = users1.head,
      users1             = users1,
      _chatsWithMessages = chatsWithMsgs.map {
        case (c, ms) => (c.copy(dsUuid = ds.uuid) -> ms.map(amend2).toIndexedSeq)
      }
    ) with EagerMutableDaoTrait
  }

  def getSimpleDaoEntities(dao: ChatHistoryDao): (Dataset, DatasetRoot, Seq[User], ChatWithDetails, Seq[Message]) = {
    val ds     = dao.datasets.head
    val dsRoot = dao.datasetRoot(ds.uuid)
    val users  = dao.users(ds.uuid)
    val cwd    = dao.chats(ds.uuid).head
    val msgs   = dao.firstMessages(cwd.chat, Int.MaxValue)
    (ds, dsRoot, users, cwd, msgs)
  }

  implicit class RichUserSeq(users: Seq[User]) {
    def byId(id: Long): User =
      users.find(_.id == id).get
  }

  implicit class RichMsgSeq(msgs: Seq[Message]) {
    def bySrcId[TM <: Message with TaggedMessage](id: Long): TM =
      tag(msgs.find(_.sourceIdTypedOption.get == id).get)
  }

  def tag[TM <: Message with TaggedMessage](m: Message): TM = m.asInstanceOf[TM]

  def tag[TM <: Message with TaggedMessage](id: Int)(implicit msgs: Seq[Message]): TM =
    msgs.bySrcId(id)

  trait EagerMutableDaoTrait extends MutableChatHistoryDao {
    override def insertDataset(ds: Dataset): Unit = ???

    override def renameDataset(dsUuid: PbUuid, newName: String): Dataset = ???

    override def deleteDataset(dsUuid: PbUuid): Unit = ???

    override def shiftDatasetTime(dsUuid: PbUuid, hrs: Int): Unit = ???

    override def insertUser(user: User, isMyself: Boolean): Unit = ???

    override def updateUser(user: User): Unit = ???

    override def mergeUsers(baseUser: User, absorbedUser: User): Unit = ???

    override def insertChat(dsRoot: File, chat: Chat): Unit = ???

    override def deleteChat(chat: Chat): Unit = ???

    override def insertMessages(dsRoot: File, chat: Chat, msgs: Seq[Message]): Unit = ???

    override def disableBackups(): Unit = ???

    override def enableBackups(): Unit = ???

    override def backup(): Unit = ???
  }
}
