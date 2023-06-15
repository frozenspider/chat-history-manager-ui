package org.fs.chm.dao

import java.io.{File => JFile}
import java.nio.file.Files
import java.nio.file.attribute.FileTime
import java.sql.Timestamp

import scala.collection.parallel.CollectionConverters._

import cats.effect._
import cats.effect.unsafe.implicits.global
import cats.free.Free
import cats.implicits._
import com.github.nscala_time.time.Imports._
import doobie.ConnectionIO
import doobie._
import doobie.free.connection
import doobie.free.connection.ConnectionOp
import doobie.h2.implicits._
import doobie.implicits._
import org.fs.chm.dao.Entities._
import org.fs.chm.protobuf.Chat
import org.fs.chm.protobuf.ChatType
import org.fs.chm.protobuf.Content
import org.fs.chm.protobuf.ContentAnimation
import org.fs.chm.protobuf.ContentFile
import org.fs.chm.protobuf.ContentLocation
import org.fs.chm.protobuf.ContentPhoto
import org.fs.chm.protobuf.ContentPoll
import org.fs.chm.protobuf.ContentSharedContact
import org.fs.chm.protobuf.ContentSticker
import org.fs.chm.protobuf.ContentVideoMsg
import org.fs.chm.protobuf.ContentVoiceMsg
import org.fs.chm.protobuf.Message
import org.fs.chm.protobuf.MessageRegular
import org.fs.chm.protobuf.MessageService
import org.fs.chm.protobuf.MessageServiceClearHistory
import org.fs.chm.protobuf.MessageServiceGroupCall
import org.fs.chm.protobuf.MessageServiceGroupCreate
import org.fs.chm.protobuf.MessageServiceGroupEditPhoto
import org.fs.chm.protobuf.MessageServiceGroupEditTitle
import org.fs.chm.protobuf.MessageServiceGroupInviteMembers
import org.fs.chm.protobuf.MessageServiceGroupMigrateFrom
import org.fs.chm.protobuf.MessageServiceGroupMigrateTo
import org.fs.chm.protobuf.MessageServiceGroupRemoveMembers
import org.fs.chm.protobuf.MessageServicePhoneCall
import org.fs.chm.protobuf.MessageServicePinMessage
import org.fs.chm.protobuf.PbUuid
import org.fs.chm.protobuf.RichTextElement
import org.fs.chm.protobuf.RteBold
import org.fs.chm.protobuf.RteItalic
import org.fs.chm.protobuf.RteLink
import org.fs.chm.protobuf.RtePlain
import org.fs.chm.protobuf.RtePrefmtBlock
import org.fs.chm.protobuf.RtePrefmtInline
import org.fs.chm.protobuf.RteStrikethrough
import org.fs.chm.protobuf.RteUnderline
import org.fs.chm.utility.IoUtils
import org.fs.chm.utility.LangUtils._
import org.fs.chm.utility.Logging
import org.fs.chm.utility.PerfUtils._
import org.fs.utility.StopWatch

class H2ChatHistoryDao(
    dataPathRoot: JFile,
    txctr: Transactor.Aux[IO, _],
    closeTransactor: () => Unit
) extends MutableChatHistoryDao
    with Logging {

  import org.fs.chm.dao.H2ChatHistoryDao._

  private val Lock = new Object

  private var _usersCacheOption: Option[Map[PbUuid, (Option[User], Seq[User])]] = None
  private var _backupsEnabled = true
  private var _closed = false

  override def name: String = s"${dataPathRoot.getName} database"

  def preload(): Unit = {
    assert(queries.noop.transact(txctr).unsafeRunSync() == 1)
  }

  override def datasets: Seq[Dataset] = {
    queries.datasets.selectAll.transact(txctr).unsafeRunSync()
  }

  override def datasetRoot(dsUuid: PbUuid): DatasetRoot = {
    new JFile(dataPathRoot, dsUuid.value.toLowerCase).getAbsoluteFile.asInstanceOf[DatasetRoot]
  }

  override def datasetFiles(dsUuid: PbUuid): Set[JFile] = {
    queries.selectAllPaths(dsUuid).transact(txctr).unsafeRunSync() map (_.toFile(datasetRoot(dsUuid)))
  }

  private def usersCache: Map[PbUuid, (Option[User], Seq[User])] =
    Lock.synchronized {
      if (_usersCacheOption.isEmpty) {
        _usersCacheOption = Some((for {
          ds <- datasets.par
        } yield {
          val meOption = queries.users.selectMyself(ds.uuid).transact(txctr).unsafeRunSync()
          val users1 = queries.users.selectAll(ds.uuid).transact(txctr).unsafeRunSync()
          meOption match {
            case None =>
              // Any query to this dataset's users would trigger an error.
              // This is an expected intermediate state.
              (ds.uuid, (None, Seq.empty))
            case Some(me) =>
              val others = users1.filter(u => !meOption.contains(u))
              (ds.uuid, (meOption, me +: others.sortBy(u => (u.id, u.prettyName))))
          }
        }).seq.toMap)
      }
      _usersCacheOption.get
    }

  override def myself(dsUuid: PbUuid): User = {
    usersCache(dsUuid)._1.getOrElse(throw new IllegalStateException(s"Dataset ${dsUuid} has no self user!"))
  }

  /** Contains myself as the first element */
  override def users(dsUuid: PbUuid): Seq[User] = {
    usersCache(dsUuid)._2
  } ensuring (us => us.head == myself(dsUuid))

  def userOption(dsUuid: PbUuid, id: Long): Option[User] = {
    usersCache(dsUuid)._2.find(_.id == id)
  }

  private def chatMembers(chat: Chat): Seq[User] = {
    val allUsers = users(chat.dsUuid.get)
    val me = myself(chat.dsUuid.get)
    me +: (chat.memberIds
      .filter(_ != me.id)
      .map(mId => allUsers.find(_.id == mId).get)
      .toSeq
      .sortBy(_.id))
  }

  private def addChatsDetails(dsUuid: PbUuid, chatQuery: ConnectionIO[Seq[Chat]]):  ConnectionIO[Seq[ChatWithDetails]] = {
    for {
      chats <- chatQuery
      lasts <- materializeMessagesQuery(dsUuid, queries.rawMessages.selectLastForChats(dsUuid, chats))
    } yield chats map (c => ChatWithDetails(c, lasts.find(_._1 == c.id).map(_._2), chatMembers(c)))
  }

  override def chats(dsUuid: PbUuid): Seq[ChatWithDetails] = {
    // FIXME: Double-call when loading a DB!
    logPerformance {
      addChatsDetails(dsUuid, queries.chats.selectAll(dsUuid)).transact(txctr).unsafeRunSync().sortBy(_.lastMsgOption.map(_.timestamp)).reverse
    }((res, ms) => s"${res.size} chats fetched in ${ms} ms")
  }

  override def chatOption(dsUuid: PbUuid, id: ChatId): Option[ChatWithDetails] = {
    addChatsDetails(dsUuid, queries.chats.select(dsUuid, id).map(_.toSeq)).transact(txctr).unsafeRunSync().headOption
  }

  /**
   * Materialize a query from `IndexedSeq[RawMessage]` to `IndexedSeq[Message]`.
   * Tries to do so efficiently.
   */
  private def materializeMessagesQuery(
      dsUuid: PbUuid,
      msgsQuery: ConnectionIO[IndexedSeq[RawMessage]]
  ): ConnectionIO[IndexedSeq[(ChatId, Message)]] = {
    for {
      rawMsgs <- msgsQuery
      rawMsgIds = rawMsgs.map(_.internalId)
      rawTextElements <- queries.rawRichTextElements.selectAllForMultiple(dsUuid, rawMsgIds)
      rawContents <- (if (rawMsgs.exists(_.canHaveContent)) queries.rawContent.selectMultiple(dsUuid, rawMsgIds)
                      else queries.pure(Seq.empty))
    } yield {
      val richTextsMap = rawTextElements
        .groupBy(_.messageInternalId)
        .view.mapValues(_ map Raws.toRichTextElement).toMap
      val contentMap = rawContents
        .map(rc => (rc.messageInternalId -> Raws.toContent(dsUuid, rc)))
        .toMap
      rawMsgs map (rm => (rm.chatId, Raws.toMessage(rm, richTextsMap, contentMap)))
    }
  }

  override def scrollMessages(chat: Chat, offset: Int, limit: Int): IndexedSeq[Message] = {
    logPerformance {
      materializeMessagesQuery(chat.dsUuid.get, queries.rawMessages.selectSlice(chat, offset, limit))
        .transact(txctr)
        .unsafeRunSync()
        .map(_._2)
    }((res, ms) => s"${res.size} messages fetched in ${ms} ms [scrollMessages]")
  }

  override def lastMessages(chat: Chat, limit: Int): IndexedSeq[Message] = {
    logPerformance {
      materializeMessagesQuery(chat.dsUuid.get, queries.rawMessages.selectLastInversed(chat, limit))
        .transact(txctr)
        .unsafeRunSync()
        .reverse
        .map(_._2)
    }((res, ms) => s"${res.size} messages fetched in ${ms} ms [lastMessages]")
  }

  override def messagesBeforeImpl(chat: Chat, msg: Message, limit: Int): IndexedSeq[Message] = {
    logPerformance {
      materializeMessagesQuery(chat.dsUuid.get, queries.rawMessages.selectBeforeInversedInc(chat, msg, limit))
        .transact(txctr)
        .unsafeRunSync()
        .reverse
        .map(_._2)
    }((res, ms) => s"${res.size} messages fetched in ${ms} ms [messagesBeforeImpl]")
  } ensuring (seq => seq.nonEmpty && {
    val root = datasetRoot(chat.dsUuid.get)
    (seq.last, root) =~= (msg, root)
  })

  override def messagesAfterImpl(chat: Chat, msg: Message, limit: Int): IndexedSeq[Message] = {
    logPerformance {
      materializeMessagesQuery(chat.dsUuid.get, queries.rawMessages.selectAfterInc(chat, msg, limit))
        .transact(txctr)
        .unsafeRunSync()
        .map(_._2)
    }((res, ms) => s"${res.size} messages fetched in ${ms} ms [messagesAfterImpl]")
  }

  override def messagesBetweenImpl(chat: Chat, msg1: Message, msg2: Message): IndexedSeq[Message] = {
    logPerformance {
      materializeMessagesQuery(chat.dsUuid.get, queries.rawMessages.selectBetweenInc(chat, msg1, msg2))
        .transact(txctr)
        .unsafeRunSync()
        .map(_._2)
    }((res, ms) => s"${res.size} messages fetched in ${ms} ms [messagesBetweenImpl]")
  }

  override def countMessagesBetween(chat: Chat, msg1: Message, msg2: Message): Int = {
    logPerformance {
      require(msg1.timestamp <= msg2.timestamp)
      queries.rawMessages.countBetweenExc(chat, msg1, msg2).transact(txctr).unsafeRunSync()
    }((res, ms) => s"${res} messages counted in ${ms} ms [countMessagesBetween]")
  }

  def messagesAroundDate(chat: Chat, date: DateTime, limit: Int): (IndexedSeq[Message], IndexedSeq[Message]) = {
    ???
  }

  override def messageOption(chat: Chat, id: MessageSourceId): Option[Message] = {
    logPerformance {
      materializeMessagesQuery(chat.dsUuid.get, queries.rawMessages.selectOptionBySourceId(chat, id).map(_.toIndexedSeq))
        .transact(txctr)
        .unsafeRunSync()
        .headOption
        .map(_._2)
    }((res, ms) => s"Message fetched in ${ms} ms [messageOption]")
  }

  override def messageOptionByInternalId(chat: Chat, id: MessageInternalId): Option[Message] =
    logPerformance {
      materializeMessagesQuery(chat.dsUuid.get, queries.rawMessages.selectOption(chat, id).map(_.toIndexedSeq))
        .transact(txctr)
        .unsafeRunSync()
        .headOption
        .map(_._2)
    }((res, ms) => s"Message fetched in ${ms} ms [messageOptionByInternalId]")

  def copyAllFrom(dao: ChatHistoryDao): Unit = {
    StopWatch.measureAndCall {
      log.info("Starting insertAll")
      for (ds <- dao.datasets) {
        val myself1 = dao.myself(ds.uuid)
        val dsRoot = dao.datasetRoot(ds.uuid)

        StopWatch.measureAndCall {
          log.info(s"Inserting $ds")
          var query: ConnectionIO[_] = queries.datasets.insert(ds)

          for (u <- dao.users(ds.uuid)) {
            require(u.id > 0, "IDs should be positive!")
            query = query flatMap (_ => queries.users.insert(u, u == myself1))
          }

          for (c <- dao.chats(ds.uuid).map(_.chat)) {
            require(c.id > 0, "IDs should be positive!")
            query = query flatMap (_ => queries.chats.insert(dsRoot, c))
            for (memberId <- c.memberIds) {
              query = query flatMap (_ => queries.chatMembers.insert(ds.uuid, c.id, memberId))
            }

            val batchSize = 1000
            def batchStream(idx: Int): Stream[IndexedSeq[Message]] = {
              val batch = dao.scrollMessages(c, idx * batchSize, batchSize)
              if (batch.isEmpty) {
                Stream.empty
              } else {
                batch #:: batchStream(idx + 1)
              }
            }
            for {
              batch <- batchStream(0)
              msg   <- batch
            } {
              query = query flatMap (_ => queries.messages.insert(ds.uuid, dsRoot, c.id, msg))
            }
          }

          query.transact(txctr).unsafeRunSync()
        }((_, t) => log.info(s"Dataset inserted in $t ms"))

        // Copying JFiles
        val fromRoot      = dao.datasetRoot(ds.uuid)
        val fromFiles     = dao.datasetFiles(ds.uuid)
        val fromPrefixLen = dsRoot.getAbsolutePath.length
        val toRoot        = datasetRoot(ds.uuid)
        val filesMap      = fromFiles.map { fromFile =>
          val relPath     = fromFile.getAbsolutePath.drop(fromPrefixLen)
          (fromFile, new JFile(toRoot, relPath))
        }.toMap
        val (notFound, _) = StopWatch.measureAndCall(IoUtils.copyAll(filesMap))((_, t) => log.info(s"Copied in $t ms"))
        notFound.foreach(nf => log.info(s"Not found: ${nf.getAbsolutePath}"))

        // Sanity checks
        StopWatch.measureAndCall {
          log.info(s"Running sanity checks on $ds")
          val ds2Option = datasets.find(_.uuid == ds.uuid)
          assert(
            ds2Option.isDefined && ds == ds2Option.get,
            s"dataset differs:\nWas    $ds\nBecame ${ds2Option getOrElse "<none>"}")
          assert(myself1 == myself(ds.uuid), s"'myself' differs:\nWas    $myself1\nBecame ${myself(ds.uuid)}")
          assert(
            dao.users(ds.uuid) == users(ds.uuid),
            s"Users differ:\nWas    ${dao.users(ds.uuid)}\nBecame ${users(ds.uuid)}")
          val chats1 = dao.chats(ds.uuid).map(_.chat)
          val chats2 = chats(ds.uuid).map(_.chat)
          assert(chats1.size == chats2.size, s"Chat size differs:\nWas    ${chats1.size}\nBecame ${chats2.size}")
          for (((c1, c2), i) <- chats1.zip(chats2).zipWithIndex) {
            StopWatch.measureAndCall {
              log.info(s"Checking chat '${c1.name.getOrElse("")}' with ${c1.msgCount} messages")
              assert(c1 == c2, s"Chat #$i differs:\nWas    $c1\nBecame $c2")
              val messages1 = dao.lastMessages(c1, c1.msgCount + 1)
              val messages2 = lastMessages(c2, c2.msgCount + 1)
              assert(
                messages1.size == messages1.size,
                s"Messages size for chat $c1 (#$i) differs:\nWas    ${messages1.size}\nBecame ${messages2.size}")
              for (((m1, m2), j) <- messages1.zip(messages2).zipWithIndex) {
                assert((m1, fromRoot) =~= (m2, toRoot), s"Message #$j for chat $c1 (#$i) differs:\nWas    $m1\nBecame $m2")
              }
            }((_, t) => log.info(s"Chat checked in $t ms"))
          }
        }((_, t) => log.info(s"Dataset checked in $t ms"))
      }
    }((_, t) => log.info(s"All done in $t ms"))
  }

  override def isMutable: Boolean = true

  override def insertDataset(ds: Dataset): Unit = {
    backup()
    queries.datasets.insert(ds).transact(txctr).unsafeRunSync()
    Lock.synchronized {
      _usersCacheOption = None
    }
  }

  override def renameDataset(dsUuid: PbUuid, newName: String): Dataset = {
    backup()
    queries.datasets.rename(dsUuid, newName).transact(txctr).unsafeRunSync()
    datasets.find(_.uuid == dsUuid).get
  }

  override def deleteDataset(dsUuid: PbUuid): Unit = {
    backup()
    StopWatch.measureAndCall {
      val query = for {
        q1 <- queries.rawContent.deleteByDataset(dsUuid)
        q2 <- queries.rawRichTextElements.deleteByDataset(dsUuid)
        q3 <- queries.rawMessages.deleteByDataset(dsUuid)
        q4 <- queries.chatMembers.deleteByDataset(dsUuid)
        q5 <- queries.chats.deleteByDataset(dsUuid)
        q6 <- queries.users.deleteByDataset(dsUuid)
        qL <- queries.datasets.delete(dsUuid)
      } yield q1 + q2 + q3 + q4 + q5+ q6 + qL
      query.transact(txctr).unsafeRunSync()
      val srcDataPath = datasetRoot(dsUuid)
      if (srcDataPath.exists()) {
        val dstDataPath = new JFile(getBackupPath(), srcDataPath.getName)
        Files.move(srcDataPath.toPath, dstDataPath.toPath)
      }
    }((_, t) => log.info(s"Dataset ${dsUuid} deleted in $t ms"))
  }

  override def shiftDatasetTime(dsUuid: PbUuid, hrs: Int): Unit = {
    backup()
    require(hrs >= -12 && hrs <= 12, "Hours are out of bounds! Expected [-12, 12]")
    StopWatch.measureAndCall {
      queries.shiftDatasetTime(dsUuid, hrs).transact(txctr).unsafeRunSync()
    }((_, t) => log.info(s"Dataset ${dsUuid} time shifted in $t ms"))
  }

  override def insertUser(user: User, isMyself: Boolean): Unit = {
    require(user.id > 0, "ID should be positive!")
    if (isMyself) {
      val oldMyselfOption = queries.users.selectMyself(user.dsUuid).transact(txctr).unsafeRunSync()
      require(oldMyselfOption.isEmpty, "Myself is already defined for this dataset!")
    }
    backup()
    queries.users.insert(user, isMyself).transact(txctr).unsafeRunSync()
    Lock.synchronized {
      _usersCacheOption = None
    }
  }

  override def updateUser(user: User): Unit = {
    backup()
    val isMyself = myself(user.dsUuid).id == user.id
    val rowsNum  = queries.users.update(user, isMyself).transact(txctr).unsafeRunSync()
    require(rowsNum == 1, s"Updating user affected ${rowsNum} rows!")
    queries.chats.updateRenameUser(user).transact(txctr).unsafeRunSync()
    Lock.synchronized {
      _usersCacheOption = None
    }
  }

  override def mergeUsers(baseUser: User, absorbedUser: User): Unit = {
    backup()
    val dsUuid   = baseUser.dsUuid
    val me       = myself(baseUser.dsUuid)
    val isMyself = me.id == baseUser.id || me.id == absorbedUser.id
    val newUser = baseUser.copy(
      firstNameOption    = absorbedUser.firstNameOption,
      lastNameOption     = absorbedUser.lastNameOption,
      usernameOption     = absorbedUser.usernameOption,
      phoneNumberOption  = absorbedUser.phoneNumberOption,
    )

    val query = for {
      v1 <- queries.users.update(newUser, isMyself)
      // Change ownership of old user's stuff
      v2 <- queries.chatMembers.updateUser(dsUuid, absorbedUser.id, newUser.id)
      v3 <- queries.rawMessages.updateUser(dsUuid, absorbedUser.id, newUser.id)
      v4 <- queries.users.delete(dsUuid, absorbedUser.id)
      v5 <- queries.mergePersonalChats(dsUuid, newUser.id)
      v6 <- queries.chats.updateRenameUser(newUser)
    } yield v1 + v2 + v3 + v4 + v5 + v6
    val rowsNum = query.transact(txctr).unsafeRunSync()
    assert(rowsNum > 0, "Nothing was updated!")
    Lock.synchronized {
      _usersCacheOption = None
    }
  }

  override def insertChat(dsRoot: JFile, chat: Chat): Unit = {
    require(chat.id > 0, "ID should be positive!")
    require(chat.memberIds.nonEmpty, "Chat should have more than one member!")
    val me = myself(chat.dsUuid.get)
    require(chat.memberIds contains me.id, "Chat members list should contain self!")
    backup()
    val query = for {
      _ <- queries.chats.insert(dsRoot, chat)
      r <- chat.memberIds.foldLeft(queries.pure0) { (q, uId) =>
        q flatMap (_ => queries.chatMembers.insert(chat.dsUuid.get, chat.id, uId))
      }
    } yield r
    query.transact(txctr).unsafeRunSync()
  }

  override def deleteChat(chat: Chat): Unit = {
    backup()
    StopWatch.measureAndCall {
      val query = for {
        _ <- queries.rawContent.deleteByChat(chat.dsUuid.get, chat.id)
        _ <- queries.rawRichTextElements.deleteByChat(chat.dsUuid.get, chat.id)
        _ <- queries.rawMessages.deleteByChat(chat.dsUuid.get, chat.id)
        _ <- queries.chatMembers.deleteByChat(chat.dsUuid.get, chat.id)
        _ <- queries.chats.delete(chat.dsUuid.get, chat.id)
        u <- queries.users.deleteOrphans(chat.dsUuid.get)
      } yield u
      val deletedUsersNum = query.transact(txctr).unsafeRunSync()
      log.info(s"Deleted ${deletedUsersNum} orphaned users")
      Lock.synchronized {
        _usersCacheOption = None
      }
    }((_, t) => log.info(s"Chat ${chat.name.getOrElse("#" + chat.id)} deleted in $t ms"))
  }

  override def insertMessages(dsRoot: JFile, chat: Chat, msgs: Seq[Message]): Unit = {
    if (msgs.nonEmpty) {
      val query = msgs
        .map(msg => queries.messages.insert(chat.dsUuid.get, dsRoot, chat.id, msg))
        .reduce((q1, q2) => q1 flatMap (_ => q2))
      query.transact(txctr).unsafeRunSync()
      backup()
    }
  }

  override def disableBackups(): Unit = {
    Lock.synchronized {
      _backupsEnabled = false
    }
  }

  override def enableBackups(): Unit = {
    Lock.synchronized {
      _backupsEnabled = true
    }
  }

  protected[dao] def getBackupPath(): JFile = {
    val backupDir = new JFile(dataPathRoot, H2ChatHistoryDao.BackupsDir)
    backupDir.mkdir()
    backupDir
  }

  override def backup(): Unit = {
    val backupsEnabled = Lock.synchronized { _backupsEnabled }
    if (backupsEnabled) {
      val backupDir = getBackupPath()
      val backups =
        backupDir
          .listFiles((dir, name) => name.matches("""backup_(\d\d\d\d)-(\d\d)-(\d\d)_(\d\d)-(\d\d)-(\d\d).zip"""))
          .sortBy(f => Files.getAttribute(f.toPath, "creationTime").asInstanceOf[FileTime].toMillis)
      val newBackupName = "backup_" + DateTime.now.toString("yyyy-MM-dd_HH-mm-ss") + ".zip"
      StopWatch.measureAndCall {
        queries
          .backup(new JFile(backupDir, newBackupName).getAbsolutePath.replace("\\", "/"))
          .transact(txctr)
          .unsafeRunSync()
      }((_, t) => log.info(s"Backup ${newBackupName} done in $t ms"))
      for (oldBackup <- backups.dropRight(H2ChatHistoryDao.MaxBackups - 1)) {
        oldBackup.delete()
      }
    }
  }

  override def close(): Unit =
    Lock.synchronized {
      if (!_closed) {
        _closed = true;
        closeTransactor()
      }
    }

  override def isLoaded(dataPathRoot: JFile): Boolean = {
    dataPathRoot != null && this.dataPathRoot == dataPathRoot
  }

  object queries {
    val pure0: Free[ConnectionOp, Int] = pure[Int](0)

    def pure[T](v: T): Free[ConnectionOp, T] = cats.free.Free.pure[ConnectionOp, T](v)

    val noop: doobie.ConnectionIO[Int] = sql"SELECT 1".query[Int].unique

    def setRefIntegrity(enabled: Boolean): ConnectionIO[Int] =
      Fragment.const(s"SET REFERENTIAL_INTEGRITY ${if (enabled) "TRUE" else "FALSE"}").update.run

    def withLimit(limit: Int) =
      fr"LIMIT ${limit}"

    def backup(path: String): ConnectionIO[Int] =
      sql"BACKUP TO $path".update.run

    object datasets {
      private val colsFr = fr"uuid, alias, source_type"

      lazy val selectAll: ConnectionIO[Seq[Dataset]] =
        (fr"SELECT" ++ colsFr ++ fr"FROM datasets").query[Dataset].to[Seq]

      def insert(ds: Dataset): ConnectionIO[Int] =
        (fr"INSERT INTO datasets (" ++ colsFr ++ fr") VALUES (${ds.uuid}, ${ds.alias}, ${ds.sourceType})").update.run

      def rename(dsUuid: PbUuid, newName: String): ConnectionIO[Int] =
        sql"UPDATE datasets SET alias = ${newName} WHERE uuid = ${dsUuid}".update.run

      def delete(dsUuid: PbUuid): ConnectionIO[Int] =
        sql"DELETE FROM datasets WHERE uuid = ${dsUuid}".update.run
    }

    object users {
      private val colsFr                   = fr"ds_uuid, id, first_name, last_name, username, phone_number"
      private val selectAllFr              = fr"SELECT" ++ colsFr ++ fr"FROM users"
      private def selectFr(dsUuid: PbUuid) = selectAllFr ++ fr"WHERE ds_uuid = $dsUuid"
      private val defaultOrder             = fr"ORDER BY id, first_name, last_name, username, phone_number"

      def selectAll(dsUuid: PbUuid) =
        (selectFr(dsUuid) ++ defaultOrder).query[User].to[Seq]

      def selectMyself(dsUuid: PbUuid) =
        (selectFr(dsUuid) ++ fr"AND is_myself = true").query[User].option

      def select(dsUuid: PbUuid, id: Long) =
        (selectFr(dsUuid) ++ fr"AND id = $id").query[User].option

      def insert(u: User, isMyself: Boolean) =
        (fr"INSERT INTO users (" ++ colsFr ++ fr", is_myself) VALUES ("
          ++ fr"${u.dsUuid}, ${u.id}, ${u.firstNameOption}, ${u.lastNameOption}, ${u.usernameOption},"
          ++ fr"${u.phoneNumberOption}, ${isMyself}"
          ++ fr")").update.run

      def update(u: User, isMyself: Boolean) =
        sql"""
            UPDATE users SET
              first_name    = ${u.firstNameOption},
              last_name     = ${u.lastNameOption},
              username      = ${u.usernameOption},
              phone_number  = ${u.phoneNumberOption},
              is_myself     = ${isMyself}
            WHERE ds_uuid = ${u.dsUuid} AND id = ${u.id}
           """.update.run

      /** Delete all users from the given dataset (other than self) that have no messages in any chat */
      def delete(dsUuid: PbUuid, id: Long): ConnectionIO[Int] =
        sql"DELETE FROM users u WHERE u.id = ${id}".update.run

      /** Delete all users from the given dataset (other than self) that aren't associated any chat */
      def deleteOrphans(dsUuid: PbUuid): ConnectionIO[Int] =
        sql"""
            DELETE FROM users u
            WHERE u.ds_uuid = $dsUuid AND u.id IN (
              SELECT id FROM (
                SELECT
                  u2.id,
                  u2.first_name,
                  (SELECT COUNT(*) FROM chat_members cm WHERE cm.ds_uuid = u2.ds_uuid AND cm.user_id = u2.id) AS chats_count
                FROM users u2
                WHERE u2.ds_uuid = $dsUuid
                AND u2.is_myself = false
              ) WHERE chats_count = 0
            )
           """.update.run

      def deleteByDataset(dsUuid: PbUuid): ConnectionIO[Int] =
        sql"DELETE FROM users WHERE ds_uuid = ${dsUuid}".update.run
    }

    object chats {
      private val colsFr     = fr"ds_uuid, id, name, type, img_path"

      private def selectFr(dsUuid: PbUuid) =
        (fr"SELECT" ++ colsFr ++ fr","
          ++ fr"(SELECT ARRAY_AGG(u.id) FROM users u"
          ++ fr"  INNER JOIN chat_members cm ON cm.ds_uuid = c.ds_uuid AND cm.user_id = u.id"
          ++ fr"  WHERE u.ds_uuid = c.ds_uuid AND cm.chat_id = c.id"
          ++ fr") AS member_ids,"
          // m.ds_uuid = c.ds_uuid here serves so that H2 could discover an index
          ++ fr"(SELECT COUNT(*) FROM messages m WHERE m.ds_uuid = c.ds_uuid AND m.chat_id = c.id) AS msg_count"
          ++ fr"FROM chats c WHERE c.ds_uuid = $dsUuid")

      private def hasMessagesFromUser(userId: Long) =
        (fr"EXISTS (SELECT m.from_id FROM messages m"
          ++ fr"WHERE m.ds_uuid = c.ds_uuid AND m.chat_id = c.id AND m.from_id = ${userId})")

      def selectAll(dsUuid: PbUuid): ConnectionIO[Seq[Chat]] =
        selectFr(dsUuid).query[RawChat].to[Seq].map(_ map Raws.toChat)

      def selectAllPersonalChats(dsUuid: PbUuid, userId: Long): ConnectionIO[Seq[Chat]] =
        if (myself(dsUuid).id == userId) {
          cats.free.Free.pure(Seq.empty)
        } else {
          (selectFr(dsUuid) ++ fr"""
                AND c.type = ${ChatType.Personal: ChatType}
                AND """ ++ hasMessagesFromUser(userId) ++ fr"""
             """).query[RawChat].to[Seq].map(_ map Raws.toChat)
        }

      def select(dsUuid: PbUuid, id: ChatId): ConnectionIO[Option[Chat]] =
        (selectFr(dsUuid) ++ fr"AND c.id = ${id}").query[RawChat].option.map(_ map Raws.toChat)

      def insert(dsRoot: JFile, c: Chat) = {
        val rc = Raws.fromChat(dsRoot, c)
        (fr"INSERT INTO chats (" ++ colsFr ++ fr") VALUES ("
          ++ fr"${rc.dsUuid}, ${rc.id}, ${rc.nameOption}, ${rc.tpe}, ${rc.imgPathOption}"
          ++ fr")").update.run
      }

      /**
       * After changing user, rename private chat(s) with him accordingly.
       * If user is self, do nothing.
       */
      def updateRenameUser(u: User): ConnectionIO[Int] = {
        if (myself(u.dsUuid).id == u.id) {
          pure0
        } else {
          (fr"""
              UPDATE chats c SET
                c.name = ${u.prettyNameOption}
              WHERE c.ds_uuid = ${u.dsUuid}
                AND c.type = ${ChatType.Personal: ChatType}
                AND """ ++ hasMessagesFromUser(u.id) ++ fr"""
             """).update.run
        }
      }

      def delete(dsUuid: PbUuid, id: ChatId): ConnectionIO[Int] =
        sql"DELETE FROM chats WHERE ds_uuid = ${dsUuid} AND id = ${id}".update.run

      def deleteByDataset(dsUuid: PbUuid): ConnectionIO[Int] =
        sql"DELETE FROM chats WHERE ds_uuid = ${dsUuid}".update.run
    }

    object chatMembers {
      def insert(dsUuid: PbUuid, chatId: ChatId, userId: Long) =
        sql"INSERT INTO chat_members (ds_uuid, chat_id, user_id) VALUES ($dsUuid, $chatId, $userId)".update.run

      def updateUser(dsUuid: PbUuid, fromUserId: Long, toUserId: Long): ConnectionIO[Int] =
        sql"UPDATE chat_members SET user_id = $toUserId WHERE ds_uuid = $dsUuid AND user_id = $fromUserId".update.run

      def deleteByChat(dsUuid: PbUuid, chatId: ChatId): ConnectionIO[Int] =
        sql"DELETE FROM chat_members WHERE ds_uuid = ${dsUuid} AND chat_id = ${chatId}".update.run

      def deleteByDataset(dsUuid: PbUuid): ConnectionIO[Int] =
        sql"DELETE FROM chat_members WHERE ds_uuid = ${dsUuid}".update.run
    }

    object messages {
      /** Inserts a message, overriding it's internal ID. */
      def insert(
          dsUuid: PbUuid,
          dsRoot: JFile,
          chatId: Long,
          msg: Message,
      ): ConnectionIO[MessageInternalId] = {
        val (rm, rcOption, rrtEls) = Raws.fromMessage(dsUuid, dsRoot, chatId, msg)

        for {
          msgInternalId <- rawMessages.insert(rm)

          // Content
          _ <- rcOption map { rc =>
            rawContent.insert(rc.copy(messageInternalId = msgInternalId), dsUuid)
          } getOrElse pure0

          // RichText
          _ <- rrtEls.foldLeft(pure0) { (q, rrtEl) =>
            q flatMap (_ => rawRichTextElements.insert(rrtEl.copy(messageInternalId = msgInternalId), dsUuid) map (_ => 0))
          }
        } yield msgInternalId
      }
    }

    object rawMessages {
      private val cols = Seq(
        "ds_uuid",
        "chat_id",
        "internal_id",
        "source_id",
        "message_type",
        "time",
        "edit_time",
        "from_id",
        "forward_from_name",
        "reply_to_message_id",
        "title",
        "members",
        "duration_sec",
        "discard_reason",
        "pinned_message_id",
        "path",
        "width",
        "height"
      )

      private val colsPureFr  = Fragment.const(cols.mkString(", "))
      private val colsFr      = Fragment.const(cols.map(c => "m." + c).mkString(", "))
      private val selectAllFr = fr"SELECT" ++ colsFr ++ fr"FROM messages m"
      private val orderAsc    = fr"ORDER BY m.time, m.source_id, m.internal_id"
      private val orderDesc   = fr"ORDER BY m.time DESC, m.source_id DESC, m.internal_id DESC"

      private def whereDsAndChatFr(dsUuid: PbUuid, chatId: Long): Fragment =
        fr"WHERE m.ds_uuid = $dsUuid AND m.chat_id = $chatId"

      private def selectAllByChatFr(dsUuid: PbUuid, chatId: Long): Fragment =
        selectAllFr ++ whereDsAndChatFr(dsUuid, chatId)


      def selectOption(chat: Chat, id: MessageInternalId): ConnectionIO[Option[RawMessage]] =
        (selectAllByChatFr(chat.dsUuid.get, chat.id) ++ fr"AND m.internal_id = ${id}").query[RawMessage].option

      def selectOptionBySourceId(chat: Chat, id: MessageSourceId): ConnectionIO[Option[RawMessage]] =
        (selectAllByChatFr(chat.dsUuid.get, chat.id) ++ fr"AND m.source_id = ${id}").query[RawMessage].option

      def selectSlice(chat: Chat, offset: Int, limit: Int): ConnectionIO[IndexedSeq[RawMessage]] =
        (selectAllByChatFr(chat.dsUuid.get, chat.id)
          ++ orderAsc ++ withLimit(limit)
          ++ fr"OFFSET $offset").query[RawMessage].to[IndexedSeq]

      def selectLastForChats(dsUuid: PbUuid, chats: Seq[Chat]): ConnectionIO[IndexedSeq[RawMessage]] =
        (selectAllFr
          ++ fr"WHERE m.ds_uuid = $dsUuid"
          ++ fr"AND m.internal_id IN (SELECT MAX(m2.internal_id) FROM messages m2 WHERE m2.ds_uuid = $dsUuid AND m2.chat_id IN ("
          ++ Fragment.const(chats.map(_.id).mkString(","))
          ++ fr") GROUP BY m2.chat_id)").query[RawMessage].to[IndexedSeq]

      def selectLastInversed(chat: Chat, limit: Int): ConnectionIO[IndexedSeq[RawMessage]] =
        (selectAllByChatFr(chat.dsUuid.get, chat.id) ++ orderDesc ++ withLimit(limit)).query[RawMessage].to[IndexedSeq]

      def selectBeforeInversedInc(chat: Chat, msg: Message, limit: Int): ConnectionIO[IndexedSeq[RawMessage]] =
        (selectAllByChatFr(chat.dsUuid.get, chat.id)
          ++ fr"AND (m.time < ${msg.time} OR (m.time = ${msg.time} AND m.internal_id <= ${msg.internalId}))"
          ++ orderDesc ++ withLimit(limit)).query[RawMessage].to[IndexedSeq]

      def selectAfterInc(chat: Chat, msg: Message, limit: Int): ConnectionIO[IndexedSeq[RawMessage]] =
        (selectAllByChatFr(chat.dsUuid.get, chat.id)
          ++ fr"AND (m.time > ${msg.time} OR (m.time = ${msg.time} AND m.internal_id >= ${msg.internalId}))"
          ++ orderAsc ++ withLimit(limit)).query[RawMessage].to[IndexedSeq]

      def selectBetweenInc(chat: Chat, msg1: Message, msg2: Message): ConnectionIO[IndexedSeq[RawMessage]] =
        (selectAllByChatFr(chat.dsUuid.get, chat.id)
          ++ fr"AND (m.time > ${msg1.time} OR (m.time = ${msg1.time} AND m.internal_id >= ${msg1.internalId}))"
          ++ fr"AND (m.time < ${msg2.time} OR (m.time = ${msg2.time} AND m.internal_id <= ${msg2.internalId}))"
          ++ orderAsc).query[RawMessage].to[IndexedSeq]

      def countBetweenExc(chat: Chat, msg1: Message, msg2: Message): ConnectionIO[Int] =
        (fr"""
          SELECT COUNT(*) FROM messages m
          """ ++ whereDsAndChatFr(chat.dsUuid.get, chat.id) ++ fr"""
          AND (m.time > ${msg1.time} OR (m.time = ${msg1.time} AND m.internal_id > ${msg1.internalId}))
          AND (m.time < ${msg2.time} OR (m.time = ${msg2.time} AND m.internal_id < ${msg2.internalId}))
        """).query[Int].unique

      def insert(m: RawMessage): ConnectionIO[MessageInternalId] =
        (fr"INSERT INTO messages (" ++ colsPureFr ++ fr") VALUES ("
          ++ fr"${m.dsUuid}, ${m.chatId}, default, ${m.sourceIdOption}, ${m.messageType},"
          ++ fr"${m.time}, ${m.editTimeOption},"
          ++ fr"${m.fromId}, ${m.forwardFromNameOption}, ${m.replyToMessageIdOption},"
          ++ fr"${m.titleOption}, ${m.members}, ${m.durationSecOption}, ${m.discardReasonOption},"
          ++ fr"${m.pinnedMessageIdOption}, ${m.pathOption}, ${m.widthOption}, ${m.heightOption}"
          ++ fr")").update.withUniqueGeneratedKeys[MessageInternalId]("internal_id")

      def removeSourceIds(dsUuid: PbUuid, fromChatId: Long): ConnectionIO[Int] =
        sql"""
            UPDATE messages m SET m.source_id = NULL
            WHERE m.chat_id = ${fromChatId} AND m.ds_uuid = ${dsUuid}
           """.update.run

      def updateUser(dsUuid: PbUuid, fromUserId: Long, toUserId: Long): ConnectionIO[Int] =
        sql"""
            UPDATE messages m SET m.from_id = ${toUserId}
            WHERE m.from_id = ${fromUserId} AND m.ds_uuid = ${dsUuid}
           """.update.run

      def updateChat(dsUuid: PbUuid, fromChatId: Long, toChatId: Long): ConnectionIO[Int] =
        sql"""
            UPDATE messages m SET m.chat_id = ${toChatId}
            WHERE m.chat_id = ${fromChatId} AND m.ds_uuid = ${dsUuid}
           """.update.run

      def deleteByChat(dsUuid: PbUuid, chatId: ChatId): ConnectionIO[Int] =
        (fr"DELETE FROM messages m WHERE m.ds_uuid = ${dsUuid} AND m.chat_id = ${chatId}").update.run

      def deleteByDataset(dsUuid: PbUuid): ConnectionIO[Int] =
        sql"DELETE FROM messages m WHERE m.ds_uuid = ${dsUuid}".update.run
    }

    object rawRichTextElements {
      private val colsNoKeysFr = fr"message_internal_id, element_type, text, href, hidden, language"

      def selectAllForMultiple(dsUuid: PbUuid, msgIds: Seq[MessageInternalId]): ConnectionIO[Seq[RawRichTextElement]] =
        if (msgIds.isEmpty)
          pure(Seq.empty)
        else
          (fr"SELECT" ++ colsNoKeysFr ++ fr"FROM messages_text_elements"
            ++ fr"WHERE ds_uuid = ${dsUuid} AND message_internal_id IN (" ++ Fragment.const(msgIds.mkString(",")) ++ fr")"
            ++ fr"ORDER BY id").query[RawRichTextElement].to[Seq]

      def insert(rrte: RawRichTextElement, dsUuid: PbUuid) =
        (fr"INSERT INTO messages_text_elements(ds_uuid, " ++ colsNoKeysFr ++ fr") VALUES ("
          ++ fr"${dsUuid}, ${rrte.messageInternalId}, ${rrte.elementType}, ${rrte.text},"
          ++ fr"${rrte.hrefOption}, ${rrte.hiddenOption}, ${rrte.languageOption}"
          ++ fr")").update.withUniqueGeneratedKeys[Long]("id")

      def deleteByChat(dsUuid: PbUuid, chatId: ChatId): ConnectionIO[Int] =
        sql"""
            DELETE FROM messages_text_elements
            WHERE ds_uuid = ${dsUuid} AND message_internal_id IN (
              SELECT m.internal_id FROM messages m
              WHERE m.ds_uuid = ${dsUuid} AND m.chat_id = ${chatId}
            )
           """.update.run

      def deleteByDataset(dsUuid: PbUuid): ConnectionIO[Int] =
        sql"DELETE FROM messages_text_elements WHERE ds_uuid = ${dsUuid}".update.run
    }

    object rawContent {
      private val colsNoIdentityFr =
        Fragment.const(
          s"""|message_internal_id, element_type, path, thumbnail_path, emoji, width, height, mime_type, title, performer, address,
              |lat, lon, duration_sec, poll_question, first_name, last_name, phone_number, vcard_path""".stripMargin)

      def selectMultiple(dsUuid: PbUuid, msgIds: Seq[MessageInternalId]): ConnectionIO[Seq[RawContent]] =
        if (msgIds.isEmpty)
          pure(Seq.empty)
        else
          (fr"SELECT" ++ colsNoIdentityFr ++ fr"FROM messages_content"
            ++ fr"WHERE ds_uuid = ${dsUuid} AND message_internal_id IN (" ++ Fragment.const(msgIds.mkString(",")) ++ fr")")
            .query[RawContent]
            .to[Seq]

      def insert(rc: RawContent, dsUuid: PbUuid): ConnectionIO[Long] =
        (fr"INSERT INTO messages_content(ds_uuid, " ++ colsNoIdentityFr ++ fr") VALUES ("
          ++ fr"${dsUuid}, ${rc.messageInternalId},"
          ++ fr"${rc.elementType}, ${rc.pathOption}, ${rc.thumbnailPathOption}, ${rc.emojiOption}, ${rc.widthOption}, "
          ++ fr"${rc.heightOption}, ${rc.mimeTypeOption}, ${rc.titleOption}, ${rc.performerOption}, ${rc.addressOption}, "
          ++ fr"${rc.latOption}, ${rc.lonOption}, ${rc.durationSecOption}, ${rc.pollQuestionOption}, ${rc.firstNameOption}, "
          ++ fr" ${rc.lastNameOption}, ${rc.phoneNumberOption}, ${rc.vcardPathOption}"
          ++ fr")").update.withUniqueGeneratedKeys[Long]("id")

      def deleteByChat(dsUuid: PbUuid, chatId: ChatId): ConnectionIO[Int] =
        sql"""
            DELETE FROM messages_content
            WHERE ds_uuid = ${dsUuid} AND message_internal_id IN (
              SELECT m.internal_id FROM messages m
              WHERE m.ds_uuid = ${dsUuid} AND m.chat_id = ${chatId}
            )
           """.update.run

      def deleteByDataset(dsUuid: PbUuid): ConnectionIO[Int] =
        sql"DELETE FROM messages_content WHERE ds_uuid = ${dsUuid}".update.run
    }

    /** Merge all messages from all personal chats with this user into the first one and delete the rest */
    def mergePersonalChats(dsUuid: PbUuid, userId: Long): ConnectionIO[Int] = {
      // TODO: This looks really inefficient
      for {
        pcs <- queries.chats.selectAllPersonalChats(dsUuid, userId)

        v0 <- if (pcs.isEmpty || pcs.tail.isEmpty) {
          pure0
        } else {
          val mainChat = pcs.head
          var query: ConnectionIO[Int] = pure0
          for (otherChat <- pcs.tail) {
            val query2 = for {
              i1 <- rawMessages.removeSourceIds(dsUuid, otherChat.id)
              i2 <- rawMessages.updateChat(dsUuid, otherChat.id, mainChat.id)
              i3 <- chatMembers.deleteByChat(dsUuid, otherChat.id)
              i4 <- chats.delete(dsUuid, otherChat.id)
            } yield i1 + i2 + i3 + i4
            query = query flatMap (_ => query2)
          }
          query
        }

      } yield v0
    }

    /** Select all non-null JFilesystem paths from all tables for the specific dataset */
    def selectAllPaths(dsUuid: PbUuid): ConnectionIO[Set[String]] = {
      def q(col: String, table: String) = {
        val whereFr = Fragments.whereAnd(
          fr"ds_uuid = ${dsUuid}",
          (Fragment.const(col) ++ fr"IS NOT NULL")
        )
        (fr"SELECT" ++ Fragment.const(col) ++ fr"FROM" ++ Fragment.const(table) ++ whereFr).query[String].to[Seq]
      }
      for {
        chatImgPaths   <- q("img_path", "chats")
        msgPaths       <- q("path", "messages")
        msgCPaths      <- q("path", "messages_content")
        msgCThumbPaths <- q("thumbnail_path", "messages_content")
        msgCVcardPaths <- q("vcard_path", "messages_content")
      } yield chatImgPaths ++ msgPaths ++ msgCPaths ++ msgCThumbPaths ++ msgCVcardPaths
    } map (_.toSet)

    def shiftDatasetTime(dsUuid: PbUuid, hrs: Int): ConnectionIO[Int] = {
      sql"""
        UPDATE messages SET
          time      = DATEADD(HOUR, $hrs, time),
          edit_time = DATEADD(HOUR, $hrs, edit_time)
        WHERE ds_uuid = ${dsUuid}
      """.update.run
    }
  }

  object Raws {
    def toChat(rc: RawChat): Chat = {
      Chat(
        dsUuid    = Some(rc.dsUuid),
        id        = rc.id,
        name      = rc.nameOption,
        tpe       = rc.tpe,
        imgPath   = rc.imgPathOption map (_.makeRelativePath),
        memberIds = rc.memberIdsOption map (_.toSeq) getOrElse Seq.empty,
        msgCount  = rc.msgCount
      )
    }

    def toMessage(rm: RawMessage,
                  richTexts: Map[MessageInternalId, Seq[RichTextElement]],
                  contents: Map[MessageInternalId, Content]): Message = {
      val typed: Message.Typed = rm.messageType match {
        case "regular" =>
          Message.Typed.Regular(MessageRegular(
            editTimestamp    = rm.editTimeOption map (_.getMillis),
            forwardFromName  = rm.forwardFromNameOption,
            replyToMessageId = rm.replyToMessageIdOption,
            content          = contents.get(rm.internalId)
          ))
        case "service_phone_call" =>
          Message.Typed.Service(MessageService(MessageService.Val.PhoneCall(MessageServicePhoneCall(
            durationSec   = rm.durationSecOption,
            discardReason = rm.discardReasonOption
          ))))
        case "service_pin_message" =>
          Message.Typed.Service(MessageService(MessageService.Val.PinMessage(MessageServicePinMessage(
            messageId = rm.pinnedMessageIdOption.get
          ))))
        case "service_clear_history" =>
          Message.Typed.Service(MessageService(MessageService.Val.ClearHistory(MessageServiceClearHistory())))
        case "service_group_create" =>
          Message.Typed.Service(MessageService(MessageService.Val.GroupCreate(MessageServiceGroupCreate(
            title   = rm.titleOption.get,
            members = rm.members
          ))))
        case "service_edit_title" =>
          Message.Typed.Service(MessageService(MessageService.Val.GroupEditTitle(MessageServiceGroupEditTitle(
            title = rm.titleOption.get
          ))))
        case "service_edit_photo" =>
          Message.Typed.Service(MessageService(MessageService.Val.GroupEditPhoto(MessageServiceGroupEditPhoto(
            Some(ContentPhoto(
              path   = rm.pathOption,
              width  = rm.widthOption.get,
              height = rm.heightOption.get
            ))
          ))))
        case "service_invite_group_members" =>
          Message.Typed.Service(MessageService(MessageService.Val.GroupInviteMembers(MessageServiceGroupInviteMembers(
            members = rm.members
          ))))
        case "service_group_remove_members" =>
          Message.Typed.Service(MessageService(MessageService.Val.GroupRemoveMembers(MessageServiceGroupRemoveMembers(
            members = rm.members
          ))))
        case "service_group_migrate_from" =>
          Message.Typed.Service(MessageService(MessageService.Val.GroupMigrateFrom(MessageServiceGroupMigrateFrom(
            title = rm.titleOption.get
          ))))
        case "service_group_migrate_to" =>
          Message.Typed.Service(MessageService(MessageService.Val.GroupMigrateTo(MessageServiceGroupMigrateTo())))
        case "service_group_call" =>
          Message.Typed.Service(MessageService(MessageService.Val.GroupCall(MessageServiceGroupCall(
            members = rm.members
          ))))
      }
      val text = richTexts.getOrElse(rm.internalId, Seq.empty)
      Message(
        internalId             = rm.internalId,
        sourceId               = rm.sourceIdOption,
        timestamp              = rm.time.getMillis,
        fromId                 = rm.fromId,
        text                   = text,
        searchableString       = Some(makeSearchableString(text, typed)),
        typed                  = typed,
      )
    }

    def toRichTextElement(r: RawRichTextElement): RichTextElement = {
      r.elementType match {
        case "plain" =>
          RichText.makePlain(r.text)
        case "bold" =>
          RichText.makeBold(r.text)
        case "italic" =>
          RichText.makeItalic(text = r.text)
        case "underline" =>
          RichText.makeUnderline(text = r.text)
        case "strikethrough" =>
          RichText.makeStrikethrough(text = r.text)
        case "link" =>
          RichText.makeLink(
            textOption = r.textOption,
            href       = r.hrefOption.get,
            hidden     = r.hiddenOption.get
          )
        case "prefmt_inline" =>
          RichText.makePrefmtInline(text = r.text)
        case "prefmt_block" =>
          RichText.makePrefmtBlock(text = r.text, languageOption = r.languageOption)
      }
    }

    def toContent(dsUuid: PbUuid, rc: RawContent): Content = {
      Content(rc.elementType match {
        case "sticker" =>
          Content.Val.Sticker(ContentSticker(
            path          = rc.pathOption map (_.makeRelativePath),
            thumbnailPath = rc.thumbnailPathOption map (_.makeRelativePath),
            emoji         = rc.emojiOption,
            width         = rc.widthOption.get,
            height        = rc.heightOption.get
          ))
        case "photo" =>
          Content.Val.Photo(ContentPhoto(
            path   = rc.pathOption map (_.makeRelativePath),
            width  = rc.widthOption.get,
            height = rc.heightOption.get,
          ))
        case "voice_message" =>
          Content.Val.VoiceMsg(ContentVoiceMsg(
            path        = rc.pathOption map (_.makeRelativePath),
            mimeType    = rc.mimeTypeOption.get,
            durationSec = rc.durationSecOption
          ))
        case "video_message" =>
          Content.Val.VideoMsg(ContentVideoMsg(
            path          = rc.pathOption map (_.makeRelativePath),
            thumbnailPath = rc.thumbnailPathOption map (_.makeRelativePath),
            mimeType      = rc.mimeTypeOption.get,
            durationSec   = rc.durationSecOption,
            width         = rc.widthOption.get,
            height        = rc.heightOption.get
          ))
        case "animation" =>
          Content.Val.Animation(ContentAnimation(
            path          = rc.pathOption map (_.makeRelativePath),
            thumbnailPath = rc.thumbnailPathOption map (_.makeRelativePath),
            mimeType      = rc.mimeTypeOption.get,
            durationSec   = rc.durationSecOption,
            width         = rc.widthOption.get,
            height        = rc.heightOption.get
          ))
        case "file" =>
          Content.Val.File(ContentFile(
            path          = rc.pathOption map (_.makeRelativePath),
            thumbnailPath = rc.thumbnailPathOption map (_.makeRelativePath),
            mimeType      = rc.mimeTypeOption,
            title         = rc.titleOption.getOrElse("<File>"),
            performer     = rc.performerOption,
            durationSec   = rc.durationSecOption,
            width         = rc.widthOption,
            height        = rc.heightOption
          ))
        case "location" =>
          Content.Val.Location(ContentLocation(
            title       = rc.titleOption,
            address     = rc.addressOption,
            latStr      = rc.latOption.get.toString,
            lonStr      = rc.lonOption.get.toString,
            durationSec = rc.durationSecOption
          ))
        case "poll" =>
          Content.Val.Poll(ContentPoll(
            question = rc.pollQuestionOption.get
          ))
        case "shared_contact" =>
          Content.Val.SharedContact(ContentSharedContact(
            firstName   = rc.firstNameOption.get,
            lastName    = rc.lastNameOption,
            phoneNumber = rc.phoneNumberOption,
            vcardPath   = rc.vcardPathOption map (_.makeRelativePath),
          ))
      })
    }

    def fromChat(dsRoot: JFile, c: Chat): RawChat = {
      RawChat(
        dsUuid          = c.dsUuid.get,
        id              = c.id,
        nameOption      = c.name,
        tpe             = c.tpe,
        imgPathOption   = c.imgPath map (_.makeRelativePath),
        memberIdsOption = Some(c.memberIds.toList),
        msgCount        = c.msgCount
      )
    }

    def fromMessage(
        dsUuid: PbUuid,
        dsRoot: JFile,
        chatId: Long,
        msg: Message
    ): (RawMessage, Option[RawContent], Seq[RawRichTextElement]) = {
      val rawRichTextEls: Seq[RawRichTextElement] =
        msg.text map fromRichText(msg.internalIdTyped)
      val template = RawMessage(
        dsUuid                 = dsUuid,
        chatId                 = chatId,
        internalId             = msg.internalIdTyped,
        sourceIdOption         = msg.sourceIdTyped,
        messageType            = "",
        time                   = msg.time,
        editTimeOption         = None,
        fromId                 = msg.fromId,
        forwardFromNameOption  = None,
        replyToMessageIdOption = None,
        titleOption            = None,
        members                = Seq.empty,
        durationSecOption      = None,
        discardReasonOption    = None,
        pinnedMessageIdOption  = None,
        pathOption             = None,
        widthOption            = None,
        heightOption           = None
      )

      val (rawMessage: RawMessage, rawContentOption: Option[RawContent]) = msg.typed.value match {
        case m: MessageRegular =>
          val rawContentOption = m.content map fromContent(dsUuid, dsRoot, msg.internalIdTyped)
          template.copy(
            messageType            = "regular",
            editTimeOption         = m.editTimeOption,
            forwardFromNameOption  = m.forwardFromName,
            replyToMessageIdOption = m.replyToMessageIdTypedOption
          ) -> rawContentOption
        case MessageService(MessageService.Val.PhoneCall(m), _) =>
          template.copy(
            messageType         = "service_phone_call",
            durationSecOption   = m.durationSec,
            discardReasonOption = m.discardReason
          ) -> None
        case MessageService(MessageService.Val.PinMessage(m), _) =>
          template.copy(
            messageType           = "service_pin_message",
            pinnedMessageIdOption = Some(m.messageIdTyped),
          ) -> None
        case MessageService(MessageService.Val.ClearHistory(_), _) =>
          template.copy(
            messageType = "service_clear_history",
          ) -> None
        case MessageService(MessageService.Val.GroupCreate(m), _) =>
          template.copy(
            messageType = "service_group_create",
            titleOption = Some(m.title),
            members     = m.members
          ) -> None
        case MessageService(MessageService.Val.GroupEditTitle(m), _) =>
          template.copy(
            messageType = "service_edit_title",
            titleOption = Some(m.title)
          ) -> None
        case MessageService(MessageService.Val.GroupEditPhoto(m), _) =>
          val photo = m.photo.get
          template.copy(
            messageType  = "service_edit_photo",
            pathOption   = photo.path map (_.makeRelativePath),
            widthOption  = Some(photo.width),
            heightOption = Some(photo.height)
          ) -> None
        case MessageService(MessageService.Val.GroupInviteMembers(m), _) =>
          template.copy(
            messageType = "service_invite_group_members",
            members     = m.members
          ) -> None
        case MessageService(MessageService.Val.GroupRemoveMembers(m), _) =>
          template.copy(
            messageType = "service_group_remove_members",
            members     = m.members
          ) -> None
        case MessageService(MessageService.Val.GroupMigrateFrom(m), _) =>
          template.copy(
            messageType = "service_group_migrate_from",
            titleOption = Some(m.title)
          ) -> None
        case MessageService(MessageService.Val.GroupMigrateTo(_), _) =>
          template.copy(
            messageType = "service_group_migrate_to",
          ) -> None
        case MessageService(MessageService.Val.GroupCall(m), _) =>
          template.copy(
            messageType = "service_group_call",
            members     = m.members
          ) -> None
      }
      (rawMessage, rawContentOption, rawRichTextEls)
    }

    def fromRichText(msgId: MessageInternalId)(rte: RichTextElement): RawRichTextElement = {
      val template = RawRichTextElement(
        messageInternalId = msgId,
        elementType       = "",
        text              = rte.textOrEmptyString,
        hrefOption        = None,
        hiddenOption      = None,
        languageOption    = None
      )
      if (rte.`val`.isEmpty) {
        template.copy(elementType = "empty")
      } else rte.`val`.value match {
        case _: RtePlain         => template.copy(elementType = "plain")
        case _: RteBold          => template.copy(elementType = "bold")
        case _: RteItalic        => template.copy(elementType = "italic")
        case _: RteUnderline     => template.copy(elementType = "underline")
        case _: RteStrikethrough => template.copy(elementType = "strikethrough")
        case link: RteLink =>
          template.copy(
            elementType  = "link",
            hrefOption   = Some(link.href),
            hiddenOption = Some(link.hidden)
          )
        case _: RtePrefmtInline => template.copy(elementType = "prefmt_inline")
        case el: RtePrefmtBlock =>
          template.copy(
            elementType    = "prefmt_block",
            languageOption = el.language
          )
      }
    }

    def fromContent(dsUuid: PbUuid, dsRoot: JFile, msgId: MessageInternalId)(c: Content): RawContent = {
      val template = RawContent(
        messageInternalId   = msgId,
        elementType         = "",
        pathOption          = None,
        thumbnailPathOption = None,
        emojiOption         = None,
        widthOption         = None,
        heightOption        = None,
        mimeTypeOption      = None,
        titleOption         = None,
        performerOption     = None,
        addressOption       = None,
        latOption           = None,
        lonOption           = None,
        durationSecOption   = None,
        pollQuestionOption  = None,
        firstNameOption     = None,
        lastNameOption      = None,
        phoneNumberOption   = None,
        vcardPathOption     = None
      )
      c.`val` match {
        case Content.Val.Sticker(c) =>
          template.copy(
            elementType         = "sticker",
            pathOption          = c.path map (_.makeRelativePath),
            thumbnailPathOption = c.thumbnailPath map (_.makeRelativePath),
            emojiOption         = c.emoji,
            widthOption         = Some(c.width),
            heightOption        = Some(c.height)
          )
        case Content.Val.Photo(c) =>
          template.copy(
            elementType  = "photo",
            pathOption   = c.path map (_.makeRelativePath),
            widthOption  = Some(c.width),
            heightOption = Some(c.height)
          )
        case Content.Val.VoiceMsg(c) =>
          template.copy(
            elementType       = "voice_message",
            pathOption        = c.path map (_.makeRelativePath),
            mimeTypeOption    = Some(c.mimeType),
            durationSecOption = c.durationSec
          )
        case Content.Val.VideoMsg(c) =>
          template.copy(
            elementType         = "video_message",
            pathOption          = c.path map (_.makeRelativePath),
            thumbnailPathOption = c.thumbnailPath map (_.makeRelativePath),
            mimeTypeOption      = Some(c.mimeType),
            durationSecOption   = c.durationSec,
            widthOption         = Some(c.width),
            heightOption        = Some(c.height)
          )
        case Content.Val.Animation(c) =>
          template.copy(
            elementType         = "animation",
            pathOption          = c.path map (_.makeRelativePath),
            thumbnailPathOption = c.thumbnailPath map (_.makeRelativePath),
            mimeTypeOption      = Some(c.mimeType),
            durationSecOption   = c.durationSec,
            widthOption         = Some(c.width),
            heightOption        = Some(c.height)
          )
        case Content.Val.File(c) =>
          template.copy(
            elementType         = "file",
            pathOption          = c.path map (_.makeRelativePath),
            thumbnailPathOption = c.thumbnailPath map (_.makeRelativePath),
            mimeTypeOption      = c.mimeType,
            titleOption         = Some(c.title),
            performerOption     = c.performer,
            durationSecOption   = c.durationSec,
            widthOption         = c.width,
            heightOption        = c.height
          )
        case Content.Val.Location(c) =>
          template.copy(
            elementType       = "location",
            titleOption       = c.title,
            addressOption     = c.address,
            latOption         = Some(c.lat),
            lonOption         = Some(c.lon),
            durationSecOption = c.durationSec
          )
        case Content.Val.Poll(c) =>
          template.copy(
            elementType        = "poll",
            pollQuestionOption = Some(c.question)
          )
        case Content.Val.SharedContact(c) =>
          template.copy(
            elementType       = "shared_contact",
            firstNameOption   = Some(c.firstName),
            lastNameOption    = c.lastName,
            phoneNumberOption = c.phoneNumber,
            vcardPathOption   = c.vcardPath map (_.makeRelativePath)
          )
        case Content.Val.Empty =>
          throw new IllegalArgumentException("Empty content!")
      }
    }
  }

  override def equals(that: Any): Boolean = that match {
    case that: H2ChatHistoryDao => this.name == that.name && that.isLoaded(this.dataPathRoot)
    case _                      => false
  }

  override def hashCode: Int = this.name.hashCode + 17 * this.dataPathRoot.hashCode
}

object H2ChatHistoryDao {

  type ChatId = Long

  val BackupsDir = "_backups"
  val MaxBackups = 3

  //
  // "Raw" case classes, more closely matching DB structure
  //

  case class RawChat(
      dsUuid: PbUuid,
      id: Long,
      nameOption: Option[String],
      tpe: ChatType,
      imgPathOption: Option[String],
      memberIdsOption: Option[List[Long]],
      msgCount: Int
  )

  /** [[Message]] class with all the inlined fields, whose type is determined by `messageType` */
  case class RawMessage(
      dsUuid: PbUuid,
      chatId: ChatId,
      /** Not assigned (set to -1) before insert! */
      internalId: MessageInternalId,
      sourceIdOption: Option[MessageSourceId],
      messageType: String,
      time: DateTime,
      editTimeOption: Option[DateTime],
      fromId: Long,
      forwardFromNameOption: Option[String],
      replyToMessageIdOption: Option[MessageSourceId],
      titleOption: Option[String],
      members: Seq[String],
      durationSecOption: Option[Int],
      discardReasonOption: Option[String],
      pinnedMessageIdOption: Option[MessageSourceId],
      pathOption: Option[String],
      widthOption: Option[Int],
      heightOption: Option[Int]
  ) {
    val canHaveContent: Boolean =
      messageType == "regular"
  }

  case class RawRichTextElement(
      messageInternalId: MessageInternalId,
      elementType: String,
      // Can be empty string!
      text: String,
      hrefOption: Option[String],
      hiddenOption: Option[Boolean],
      languageOption: Option[String]
  ) {
    def textOption: Option[String] = text.toOption
  }

  /** Could hold any kind of content, distinguished by elementType. */
  case class RawContent(
      messageInternalId: MessageInternalId,
      elementType: String,
      pathOption: Option[String],
      thumbnailPathOption: Option[String],
      emojiOption: Option[String],
      widthOption: Option[Int],
      heightOption: Option[Int],
      mimeTypeOption: Option[String],
      titleOption: Option[String],
      performerOption: Option[String],
      addressOption: Option[String],
      latOption: Option[BigDecimal],
      lonOption: Option[BigDecimal],
      durationSecOption: Option[Int],
      pollQuestionOption: Option[String],
      firstNameOption: Option[String],
      lastNameOption: Option[String],
      phoneNumberOption: Option[String],
      vcardPathOption: Option[String]
  )

  //
  // Doobie serialization helpers
  //

  import javasql._
  implicit protected val dateTimeMeta: Meta[DateTime] =
    Meta[Timestamp].imap(ts => new DateTime(ts.getTime))(dt => new Timestamp(dt.getMillis))

  implicit protected val msgInternalIdMeta: Meta[MessageInternalId] =
    implicitly[Meta[Long]].asInstanceOf[Meta[MessageInternalId]]

  implicit protected val msgSourceIdMeta: Meta[MessageSourceId] =
    implicitly[Meta[Long]].asInstanceOf[Meta[MessageSourceId]]

  implicit val chatTypeFromString: Get[ChatType] = Get[String].tmap {
    case "personal"      => ChatType.Personal
    case "private_group" => ChatType.PrivateGroup
  }

  implicit val chatTypeToString: Put[ChatType] = Put[String].tcontramap {
    case ChatType.Personal => "personal"
    case ChatType.PrivateGroup => "private_group"
  }

  implicit val pbUuidFromString: Get[PbUuid] = Get[String].tmap(s => fromJavaUuid(java.util.UUID.fromString(s)))

  implicit val pbUuidToString: Put[PbUuid] = Put[String].tcontramap(pb => pb.value.toLowerCase)

  private val ArraySeparator = ";;;"
  implicit val stringSeqFromString: Get[Seq[String]] = Get[String].tmap(s => s.split(ArraySeparator))
  implicit val stringSeqToString:   Put[Seq[String]] = Put[String].tcontramap(ss => ss.mkString(ArraySeparator))
}
