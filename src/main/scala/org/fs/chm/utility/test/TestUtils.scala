package org.fs.chm.utility.test

import java.io.File
import java.nio.file.Files

import scala.collection.immutable.ListMap
import scala.util.Random

import com.github.nscala_time.time.Imports._
import org.fs.chm.dao.Entities._
import org.fs.chm.dao._
import org.fs.chm.dao.merge.DatasetMerger.TaggedMessage
import org.fs.chm.dao.merge.DatasetMerger.TaggedMessageId
import org.fs.chm.protobuf._
import org.fs.chm.utility.LangUtils._

/**
 * Utility stuff used for testing, both automatically and manually
 */
object TestUtils {

  val noUuid      = PbUuid("00000000-0000-0000-0000-000000000000")
  val baseDate    = DateTime.parse("2019-01-02T11:15:21")
  val rnd         = new Random()
  val tmpFileName = "whatever"

  def makeTempDir(suffix: String = "tmp"): File = {
    val dir = Files.createTempDirectory(s"java_chm-${suffix}_").toFile
    dir.deleteOnExit()
    dir
  }

  def createUser(dsUuid: PbUuid, idx: Int): User =
    User(
      dsUuid            = dsUuid,
      id                = idx,
      firstNameOption   = Some("User"),
      lastNameOption    = Some(idx.toString),
      usernameOption    = Some("user" + idx),
      phoneNumberOption = Some("xxx xx xx".replaceAll("x", idx.toString))
    )

  def createGroupChat(dsUuid: PbUuid, id: Int, nameSuffix: String, memberIds: Iterable[Long], messagesSize: Int): Chat = {
    require(memberIds.size >= 2)
    Chat(
      dsUuid        = dsUuid,
      id            = id,
      nameOption    = Some("Chat " + nameSuffix),
      sourceType    = SourceType.Telegram,
      tpe           = ChatType.PrivateGroup,
      imgPathOption = None,
      memberIds     = memberIds.toSeq,
      msgCount      = messagesSize,
      mainChatId    = None,
    )
  }

  def createPersonalChat(dsUuid: PbUuid, idx: Int, user: User, memberIds: Iterable[Long], messagesSize: Int): Chat = {
    require(memberIds.size == 2)
    Chat(
      dsUuid        = dsUuid,
      id            = idx,
      nameOption    = user.prettyNameOption,
      sourceType    = SourceType.Telegram,
      tpe           = ChatType.Personal,
      imgPathOption = None,
      memberIds     = memberIds.toSeq,
      msgCount      = messagesSize,
      mainChatId    = None,
    )
  }

  def createRegularMessage(idx: Int, userId: Int): Message = {
    // Any previous message
    val replyToMessageIdOption =
      if (idx > 0) Some(rnd.nextInt(idx).toLong.asInstanceOf[MessageSourceId]) else None

    val typed = Message.Typed.Regular(MessageRegular(
      editTimestampOption    = Some(baseDate.plusMinutes(idx).plusSeconds(5).unixTimestamp),
      isDeleted              = false,
      replyToMessageIdOption = replyToMessageIdOption,
      forwardFromNameOption  = Some("u" + userId),
      contentOption          = Some(ContentPoll(question = s"Hey, ${idx}!"))
    ))
    val text = Seq(RichText.makePlain(s"Hello there, ${idx}!"))
    Message(
      internalId       = NoInternalId,
      sourceIdOption   = Some(idx.toLong.asInstanceOf[MessageSourceId]),
      timestamp        = baseDate.plusMinutes(idx).unixTimestamp,
      fromId           = userId,
      searchableString = "",
      text             = text,
      typed            = typed
    )
  }

  def createSimpleDao(
      isMaster: Boolean,
      nameSuffix: String,
      msgs: Seq[Message],
      numUsers: Int,
      amendMessage: ((Boolean, DatasetRoot, Message) => Message) = ((_, _, m) => m)
  ): MutableChatHistoryDao = {
    val chat = createGroupChat(noUuid, 1, "One", (1L to numUsers).toSet, msgs.size)
    val users = (1 to numUsers).map(i => createUser(noUuid, i))
    createDao(isMaster, nameSuffix, users, ListMap(chat -> msgs.toIndexedSeq), amendMessage)
  }

  def createDao(
      isMaster: Boolean,
      nameSuffix: String,
      users: Seq[User],
      chatsWithMsgs: ListMap[Chat, Seq[Message]],
      amendMessage: ((Boolean, DatasetRoot, Message) => Message) = ((_, _, m) => m)
  ): MutableChatHistoryDao = {
    require({
      val userIds = users.map(_.id).toSet
      chatsWithMsgs.values.flatten.forall(userIds contains _.fromId)
    }, "All messages should have valid user IDs!")
    val ds = Dataset(
      uuid  = randomUuid,
      alias = "Dataset " + nameSuffix,
    )
    val users1       = users map (_ copy (dsUuid = ds.uuid))
    val dataPathRoot = makeTempDir("eager").asInstanceOf[DatasetRoot]
    val amend2       = amendMessage.curried(isMaster)(dataPathRoot)
    Files.createFile(new File(dataPathRoot, tmpFileName).toPath)
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

  def createRandomTempFile(parent: File): File = {
    val file = new File(parent, rnd.alphanumeric.take(30).mkString("", "", ".bin"))
    Files.write(file.toPath, rnd.alphanumeric.take(256).mkString.getBytes)
    file.deleteOnExit()
    file
  }

  def getSimpleDaoEntities(dao: ChatHistoryDao): (Dataset, DatasetRoot, Seq[User], ChatWithDetails, IndexedSeq[Message]) = {
    val ds     = dao.datasets.head
    val dsRoot = dao.datasetRoot(ds.uuid)
    val users  = dao.users(ds.uuid)
    val cwd    = dao.chats(ds.uuid).head
    val msgs   = dao.firstMessages(cwd.chat, Int.MaxValue)
    (ds, dsRoot, users, cwd, msgs)
  }

  implicit class RichMsgSeq(msgs: Seq[Message]) {
    def internalIdBySrcId[TMId <: MessageInternalId with TaggedMessageId](id: Long): TMId =
      msgs.find(_.sourceIdTypedOption.get == id).get.internalId.asInstanceOf[TMId]
  }

  object RichText {
    def makePlain(text: String): RichTextElement =
      make(RichTextElement.Val.Plain(RtePlain(text)), text)

    private def make(rteVal: RichTextElement.Val, searchableString: String): RichTextElement = {
      RichTextElement(rteVal, searchableString)
    }
  }

  trait EagerMutableDaoTrait extends MutableChatHistoryDao {
    override def renameDataset(dsUuid: PbUuid, newName: String): Dataset = ???

    override def deleteDataset(dsUuid: PbUuid): Unit = ???

    override def shiftDatasetTime(dsUuid: PbUuid, hrs: Int): Unit = ???

    override def updateUser(user: User): Unit = ???

    override def updateChatId(dsUuid: PbUuid, oldId: Long, newId: Long): Chat = ???

    override def deleteChat(chat: Chat): Unit = ???

    override def combineChats(masterChat: Chat, slaveChat: Chat): Unit = ???

    override def backup(): Unit = ???
  }
}
