package org.fs.chm.utility

import java.nio.file.Files
import java.util.UUID

import scala.collection.immutable.ListMap
import scala.util.Random

import com.github.nscala_time.time.Imports._
import org.fs.chm.dao.MutableChatHistoryDao
import org.fs.chm.dao._

/**
 * Utility stuff used for testing, both automatically and manually
 */
object TestUtils {

  val baseDate = DateTime.parse("2019-01-02T11:15:21")
  val rnd      = new Random()

  def createUser(ds: Dataset, idx: Int): User =
    User(
      dsUuid             = ds.uuid,
      id                 = idx,
      firstNameOption    = Some("User"),
      lastNameOption     = Some(idx.toString),
      usernameOption     = Some("user" + idx),
      phoneNumberOption  = Some("xxx xx xx".replaceAll("x", idx.toString)),
      lastSeenTimeOption = Some(DateTime.now())
    )

  def createChat(ds: Dataset, idx: Int, nameSuffix: String, messagesSize: Int): Chat =
    Chat(
      dsUuid        = ds.uuid,
      id            = idx,
      nameOption    = Some("Chat " + nameSuffix),
      tpe           = ChatType.Personal,
      imgPathOption = None,
      msgCount      = messagesSize
    )

  def createRegularMessage(idx: Int, userId: Int): Message = {
    Message.Regular(
      id                     = idx,
      time                   = baseDate.plusMinutes(idx),
      editTimeOption         = Some(baseDate.plusMinutes(idx).plusSeconds(5)),
      fromId                 = userId,
      forwardFromNameOption  = Some("u" + userId),
      replyToMessageIdOption = Some(rnd.nextInt(idx)), // Any previous message
      textOption             = Some(RichText(Seq(RichText.Plain(s"Hello there, ${idx}!")))),
      contentOption          = Some(Content.Poll(s"Hey, ${idx}!"))
    )
  }

  def createSimpleDao(nameSuffix: String, msgs: Seq[Message], numUsers: Int): MutableChatHistoryDao = {
    val ds = Dataset(
      uuid       = UUID.randomUUID(),
      alias      = "Dataset " + nameSuffix,
      sourceType = "test source"
    )
    val chat         = createChat(ds, 1, "One", msgs.size)
    val users        = (1 to numUsers).map(i => createUser(ds, i))
    val dataPathRoot = Files.createTempDirectory(null).toFile
    dataPathRoot.deleteOnExit()
    new EagerChatHistoryDao(
      name              = "Dao " + nameSuffix,
      dataPathRoot      = dataPathRoot,
      dataset           = ds,
      myself1           = users.head,
      users1            = users,
      chatsWithMessages = ListMap(chat -> msgs.toIndexedSeq)
    ) with EagerMutableDaoTrait
  }

  def getSimpleDaoEntities(dao: ChatHistoryDao): (Dataset, Seq[User], Chat) = {
    val ds    = dao.datasets.head
    val users = dao.users(ds.uuid)
    val chat  = dao.chats(ds.uuid).head
    (ds, users, chat)
  }

  private trait EagerMutableDaoTrait extends MutableChatHistoryDao {
    override def renameDataset(dsUuid: UUID, newName: String): Dataset = ???

    override def updateUser(user: User): Unit = ???

    override def mergeUsers(baseUser: User, absorbedUser: User): Unit = ???

    override def delete(chat: Chat): Unit = ???

    override protected def backup(): Unit = ???
  }
}
