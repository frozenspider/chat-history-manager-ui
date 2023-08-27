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
import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.io.FilenameUtils
import org.fs.chm.dao.Entities._
import org.fs.chm.protobuf._
import org.fs.chm.utility.LangUtils._
import org.fs.chm.utility.Logging
import org.fs.chm.utility.PerfUtils._
import org.fs.utility.StopWatch

class H2ChatHistoryDao(
    override val storagePath: JFile,
    txctr: Transactor.Aux[IO, _],
    closeTransactor: () => Unit
) extends MutableChatHistoryDao
    with Logging {

  import org.fs.chm.dao.H2ChatHistoryDao._

  private val Lock = new Object

  private var _usersCacheOption: Option[Map[PbUuid, (Option[User], Seq[User])]] = None
  private var _backupsEnabled = true
  private var _closed = false

  override def name: String = s"${storagePath.getName} database"

  def preload(): Unit = {
    assert(queries.noop.transact(txctr).unsafeRunSync() == 1)
  }

  override def datasets: Seq[Dataset] = {
    queries.datasets.selectAll.transact(txctr).unsafeRunSync()
  }

  override def datasetRoot(dsUuid: PbUuid): DatasetRoot = {
    new JFile(storagePath, dsUuid.value.toLowerCase).getAbsoluteFile.asInstanceOf[DatasetRoot]
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
    val allUsers = users(chat.dsUuid)
    val me = myself(chat.dsUuid)
    me +: chat.memberIds
      .filter(_ != me.id)
      .map(mId => allUsers.find(_.id == mId).get)
      .sortBy(_.id)
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
      addChatsDetails(dsUuid, queries.chats.selectAll(dsUuid)).transact(txctr).unsafeRunSync()
        .sortBy(_.lastMsgOption.map(-_.timestamp).getOrElse(Long.MaxValue))
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
      materializeMessagesQuery(chat.dsUuid, queries.rawMessages.selectSlice(chat, offset, limit))
        .transact(txctr)
        .unsafeRunSync()
        .map(_._2)
    }((res, ms) => s"${res.size} messages fetched in ${ms} ms [scrollMessages]")
  }

  override def lastMessages(chat: Chat, limit: Int): IndexedSeq[Message] = {
    logPerformance {
      materializeMessagesQuery(chat.dsUuid, queries.rawMessages.selectLastInversed(chat, limit))
        .transact(txctr)
        .unsafeRunSync()
        .reverse
        .map(_._2)
    }((res, ms) => s"${res.size} messages fetched in ${ms} ms [lastMessages]")
  }

  override def messagesBeforeImpl(chat: Chat, msg: Message, limit: Int): IndexedSeq[Message] = {
    logPerformance {
      materializeMessagesQuery(chat.dsUuid, queries.rawMessages.selectBeforeInversedInc(chat, msg, limit))
        .transact(txctr)
        .unsafeRunSync()
        .reverse
        .map(_._2)
    }((res, ms) => s"${res.size} messages fetched in ${ms} ms [messagesBeforeImpl]")
  } ensuring (seq => seq.nonEmpty && seq.last.internalId == msg.internalId)

  override def messagesAfterImpl(chat: Chat, msg: Message, limit: Int): IndexedSeq[Message] = {
    logPerformance {
      materializeMessagesQuery(chat.dsUuid, queries.rawMessages.selectAfterInc(chat, msg, limit))
        .transact(txctr)
        .unsafeRunSync()
        .map(_._2)
    }((res, ms) => s"${res.size} messages fetched in ${ms} ms [messagesAfterImpl]")
  }

  override def messagesBetweenImpl(chat: Chat, msg1: Message, msg2: Message): IndexedSeq[Message] = {
    logPerformance {
      materializeMessagesQuery(chat.dsUuid, queries.rawMessages.selectBetweenInc(chat, msg1, msg2))
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
      materializeMessagesQuery(chat.dsUuid, queries.rawMessages.selectOptionBySourceId(chat, id).map(_.toIndexedSeq))
        .transact(txctr)
        .unsafeRunSync()
        .headOption
        .map(_._2)
    }((res, ms) => s"Message fetched in ${ms} ms [messageOption]")
  }

  override def messageOptionByInternalId(chat: Chat, id: MessageInternalId): Option[Message] =
    logPerformance {
      materializeMessagesQuery(chat.dsUuid, queries.rawMessages.selectOption(chat, id).map(_.toIndexedSeq))
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
        val srcDsRoot = dao.datasetRoot(ds.uuid)
        val dstDsRoot = datasetRoot(ds.uuid)
        require(srcDsRoot != dstDsRoot, "Soruce and destination dataset root is the same!")

        StopWatch.measureAndCall {
          log.info(s"Inserting $ds")
          var query: ConnectionIO[_] = queries.datasets.insert(ds)

          for (u <- dao.users(ds.uuid)) {
            require(u.id > 0, "IDs should be positive!")
            query = query flatMap (_ => queries.users.insert(u, u == myself1))
          }

          for (c <- dao.chats(ds.uuid).map(_.chat)) {
            require(c.id > 0, "IDs should be positive!")
            query = query flatMap (_ => queries.chats.insert(srcDsRoot, dstDsRoot, c))
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
              // Also copies files
              query = query flatMap (_ => queries.messages.insert(ds.uuid, srcDsRoot, dstDsRoot, c.id, msg))
            }
          }

          query.transact(txctr).unsafeRunSync()
        }((_, t) => log.info(s"Dataset inserted in $t ms"))

        // Sanity checks
        StopWatch.measureAndCall {
          log.info(s"Running sanity checks on dataset ${ds.uuid.value}")
          val ds2Option = datasets.find(_.uuid == ds.uuid)
          assert(
            ds2Option.isDefined && ds == ds2Option.get,
            s"dataset differs:\nWas    $ds\nBecame ${ds2Option getOrElse "<none>"}")
          assert(myself1 == myself(ds.uuid), s"'myself' differs:\nWas    $myself1\nBecame ${myself(ds.uuid)}")
          assert(
            dao.users(ds.uuid) == users(ds.uuid),
            s"Users differ:\nWas    ${dao.users(ds.uuid)}\nBecame ${users(ds.uuid)}")
          val chats1 = dao.chats(ds.uuid)
          val chats2 = chats(ds.uuid)
          assert(chats1.size == chats2.size, s"Chat size differs:\nWas    ${chats1.size}\nBecame ${chats2.size}")
          for (((cwd1, cwd2), i) <- chats1.zip(chats2).zipWithIndex) {
            StopWatch.measureAndCall {
              log.info(s"Checking chat '${cwd1.chat.nameOption.getOrElse("")}' with ${cwd1.chat.msgCount} messages")
              assert(cwd1.chat == cwd2.chat, s"Chat #$i differs:\nWas    ${cwd1.chat}\nBecame ${cwd2.chat}")
              val messages1 = dao.lastMessages(cwd1.chat, cwd1.chat.msgCount + 1)
              val messages2 = lastMessages(cwd2.chat, cwd2.chat.msgCount + 1)
              assert(
                messages1.size == messages1.size,
                s"Messages size for chat ${cwd1.chat} (#$i) differs:\nWas    ${messages1.size}\nBecame ${messages2.size}")
              for (((m1, m2), j) <- messages1.zip(messages2).zipWithIndex) {
                assert((m1, srcDsRoot, cwd1) =~= (m2, dstDsRoot, cwd2),
                       s"Message #$j for chat ${cwd1.chat} (#$i) differs:\nWas    $m1\nBecame $m2")
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

  override def insertChat(srcDsRoot: JFile, chat: Chat): Unit = {
    require(chat.id > 0, "ID should be positive!")
    require(chat.memberIds.nonEmpty, "Chat should have more than one member!")
    val me = myself(chat.dsUuid)
    require(chat.memberIds contains me.id, "Chat members list should contain self!")
    val dsRoot = datasetRoot(chat.dsUuid)
    require(dsRoot.exists || dsRoot.mkdirs(), s"Can't create dataset root path ${dsRoot.getAbsolutePath}")
    backup()
    val query = for {
      _ <- queries.chats.insert(srcDsRoot, dsRoot, chat)
      r <- chat.memberIds.foldLeft(queries.pure0) { (q, uId) =>
        q flatMap (_ => queries.chatMembers.insert(chat.dsUuid, chat.id, uId))
      }
    } yield r
    query.transact(txctr).unsafeRunSync()
  }

  override def deleteChat(chat: Chat): Unit = {
    backup()
    StopWatch.measureAndCall {
      val query = for {
        _ <- queries.rawContent.deleteByChat(chat.dsUuid, chat.id)
        _ <- queries.rawRichTextElements.deleteByChat(chat.dsUuid, chat.id)
        _ <- queries.rawMessages.deleteByChat(chat.dsUuid, chat.id)
        _ <- queries.chatMembers.deleteByChat(chat.dsUuid, chat.id)
        _ <- queries.chats.delete(chat.dsUuid, chat.id)
        u <- queries.users.deleteOrphans(chat.dsUuid)
      } yield u
      val deletedUsersNum = query.transact(txctr).unsafeRunSync()
      log.info(s"Deleted ${deletedUsersNum} orphaned users")
      Lock.synchronized {
        _usersCacheOption = None
      }
    }((_, t) => log.info(s"Chat ${chat.nameOption.getOrElse("#" + chat.id)} deleted in $t ms"))
  }

  override def insertMessages(srcDsRoot: JFile, chat: Chat, msgs: Seq[Message]): Unit = {
    if (msgs.nonEmpty) {
      val dsRoot = datasetRoot(chat.dsUuid)
      require(dsRoot.exists, s"Dataset root path ${dsRoot.getAbsolutePath} does not exist!")
      val query = msgs
        .map(msg => queries.messages.insert(chat.dsUuid, srcDsRoot, dsRoot, chat.id, msg))
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
    val backupDir = new JFile(storagePath, BackupsDir)
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
      for (oldBackup <- backups.dropRight(MaxBackups - 1)) {
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

  override def isLoaded(storagePath: JFile): Boolean = {
    storagePath != null && this.storagePath == storagePath
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

      def insert(srcDsRoot: JFile, dstDsRoot: JFile, c: Chat) = {
        val rc = Raws.fromChat(srcDsRoot, dstDsRoot, c)
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
          srcDsRoot: JFile,
          dstDsRoot: JFile,
          chatId: Long,
          msg: Message,
      ): ConnectionIO[MessageInternalId] = {
        val (rm, rcOption, rrtEls) = Raws.fromMessage(dsUuid, srcDsRoot, dstDsRoot, chatId, msg)

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
        (selectAllByChatFr(chat.dsUuid, chat.id) ++ fr"AND m.internal_id = ${id}").query[RawMessage].option

      def selectOptionBySourceId(chat: Chat, id: MessageSourceId): ConnectionIO[Option[RawMessage]] =
        (selectAllByChatFr(chat.dsUuid, chat.id) ++ fr"AND m.source_id = ${id}").query[RawMessage].option

      def selectSlice(chat: Chat, offset: Int, limit: Int): ConnectionIO[IndexedSeq[RawMessage]] =
        (selectAllByChatFr(chat.dsUuid, chat.id)
          ++ orderAsc ++ withLimit(limit)
          ++ fr"OFFSET $offset").query[RawMessage].to[IndexedSeq]

      def selectLastForChats(dsUuid: PbUuid, chats: Seq[Chat]): ConnectionIO[IndexedSeq[RawMessage]] =
        (selectAllFr
          ++ fr"WHERE m.ds_uuid = $dsUuid"
          ++ fr"AND m.internal_id IN (SELECT MAX(m2.internal_id) FROM messages m2 WHERE m2.ds_uuid = $dsUuid AND m2.chat_id IN ("
          ++ Fragment.const(chats.map(_.id).mkString(","))
          ++ fr") GROUP BY m2.chat_id)").query[RawMessage].to[IndexedSeq]

      def selectLastInversed(chat: Chat, limit: Int): ConnectionIO[IndexedSeq[RawMessage]] =
        (selectAllByChatFr(chat.dsUuid, chat.id) ++ orderDesc ++ withLimit(limit)).query[RawMessage].to[IndexedSeq]

      def selectBeforeInversedInc(chat: Chat, msg: Message, limit: Int): ConnectionIO[IndexedSeq[RawMessage]] =
        (selectAllByChatFr(chat.dsUuid, chat.id)
          ++ fr"AND (m.time < ${msg.time} OR (m.time = ${msg.time} AND m.internal_id <= ${msg.internalId}))"
          ++ orderDesc ++ withLimit(limit)).query[RawMessage].to[IndexedSeq]

      def selectAfterInc(chat: Chat, msg: Message, limit: Int): ConnectionIO[IndexedSeq[RawMessage]] =
        (selectAllByChatFr(chat.dsUuid, chat.id)
          ++ fr"AND (m.time > ${msg.time} OR (m.time = ${msg.time} AND m.internal_id >= ${msg.internalId}))"
          ++ orderAsc ++ withLimit(limit)).query[RawMessage].to[IndexedSeq]

      def selectBetweenInc(chat: Chat, msg1: Message, msg2: Message): ConnectionIO[IndexedSeq[RawMessage]] =
        (selectAllByChatFr(chat.dsUuid, chat.id)
          ++ fr"AND (m.time > ${msg1.time} OR (m.time = ${msg1.time} AND m.internal_id >= ${msg1.internalId}))"
          ++ fr"AND (m.time < ${msg2.time} OR (m.time = ${msg2.time} AND m.internal_id <= ${msg2.internalId}))"
          ++ orderAsc).query[RawMessage].to[IndexedSeq]

      def countBetweenExc(chat: Chat, msg1: Message, msg2: Message): ConnectionIO[Int] =
        (fr"""
          SELECT COUNT(*) FROM messages m
          """ ++ whereDsAndChatFr(chat.dsUuid, chat.id) ++ fr"""
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
        dsUuid        = rc.dsUuid,
        id            = rc.id,
        nameOption     = rc.nameOption,
        tpe           = rc.tpe,
        imgPathOption = rc.imgPathOption map (_.makeRelativePath),
        memberIds     = rc.memberIdsOption map (_.toSeq) getOrElse Seq.empty,
        msgCount      = rc.msgCount
      )
    }

    def toMessage(rm: RawMessage,
                  richTexts: Map[MessageInternalId, Seq[RichTextElement]],
                  contents: Map[MessageInternalId, Content]): Message = {
      val typed: Message.Typed = rm.messageType match {
        case "regular" =>
          Message.Typed.Regular(MessageRegular(
            editTimestampOption    = rm.editTimeOption map (_.unixTimestamp),
            forwardFromNameOption  = rm.forwardFromNameOption,
            replyToMessageIdOption = rm.replyToMessageIdOption,
            contentOption          = contents.get(rm.internalId)
          ))
        case "service_phone_call" =>
          Message.Typed.Service(Some(MessageServicePhoneCall(
            durationSecOption   = rm.durationSecOption,
            discardReasonOption = rm.discardReasonOption
          )))
        case "service_pin_message" =>
          Message.Typed.Service(Some(MessageServicePinMessage(
            messageId = rm.pinnedMessageIdOption.get
          )))
        case "service_clear_history" =>
          Message.Typed.Service(Some(MessageServiceClearHistory()))
        case "service_group_create" =>
          Message.Typed.Service(Some(MessageServiceGroupCreate(
            title   = rm.titleOption.get,
            members = rm.members
          )))
        case "service_edit_title" =>
          Message.Typed.Service(Some(MessageServiceGroupEditTitle(
            title = rm.titleOption.get
          )))
        case "service_edit_photo" =>
          Message.Typed.Service(Some(MessageServiceGroupEditPhoto(
            ContentPhoto(
              pathOption = rm.pathOption,
              width      = rm.widthOption.get,
              height     = rm.heightOption.get
            )
          )))
        case "service_delete_photo" =>
          Message.Typed.Service(Some(MessageServiceGroupDeletePhoto()))
        case "service_invite_group_members" =>
          Message.Typed.Service(Some(MessageServiceGroupInviteMembers(
            members = rm.members
          )))
        case "service_group_remove_members" =>
          Message.Typed.Service(Some(MessageServiceGroupRemoveMembers(
            members = rm.members
          )))
        case "service_group_migrate_from" =>
          Message.Typed.Service(Some(MessageServiceGroupMigrateFrom(
            title = rm.titleOption.get
          )))
        case "service_group_migrate_to" =>
          Message.Typed.Service(Some(MessageServiceGroupMigrateTo()))
        case "service_group_call" =>
          Message.Typed.Service(Some(MessageServiceGroupCall(
            members = rm.members
          )))
      }
      val text = richTexts.getOrElse(rm.internalId, Seq.empty)
      Message(
        internalId             = rm.internalId,
        sourceIdOption         = rm.sourceIdOption,
        timestamp              = rm.time.unixTimestamp,
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
        case "spoiler" =>
          RichText.makeSpoiler(text = r.text)
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
      rc.elementType match {
        case "sticker" =>
          assert(rc.widthOption.isDefined && rc.heightOption.isDefined, rc)
          ContentSticker(
            pathOption          = rc.pathOption map (_.makeRelativePath),
            thumbnailPathOption = rc.thumbnailPathOption map (_.makeRelativePath),
            emojiOption         = rc.emojiOption,
            width               = rc.widthOption.get,
            height              = rc.heightOption.get
          )
        case "photo" =>
          assert(rc.widthOption.isDefined && rc.heightOption.isDefined, rc)
          ContentPhoto(
            pathOption = rc.pathOption map (_.makeRelativePath),
            width      = rc.widthOption.get,
            height     = rc.heightOption.get,
          )
        case "voice_message" =>
          assert(rc.mimeTypeOption.isDefined, rc)
          ContentVoiceMsg(
            pathOption        = rc.pathOption map (_.makeRelativePath),
            mimeType          = rc.mimeTypeOption.get,
            durationSecOption = rc.durationSecOption
          )
        case "video_message" =>
          assert(rc.mimeTypeOption.isDefined && rc.widthOption.isDefined && rc.heightOption.isDefined, rc)
          ContentVideoMsg(
            pathOption          = rc.pathOption map (_.makeRelativePath),
            thumbnailPathOption = rc.thumbnailPathOption map (_.makeRelativePath),
            mimeType            = rc.mimeTypeOption.get,
            durationSecOption   = rc.durationSecOption,
            width               = rc.widthOption.get,
            height              = rc.heightOption.get
          )
        case "animation" =>
          assert(rc.mimeTypeOption.isDefined && rc.widthOption.isDefined && rc.heightOption.isDefined, rc)
          ContentAnimation(
            pathOption          = rc.pathOption map (_.makeRelativePath),
            thumbnailPathOption = rc.thumbnailPathOption map (_.makeRelativePath),
            mimeType            = rc.mimeTypeOption.get,
            durationSecOption   = rc.durationSecOption,
            width               = rc.widthOption.get,
            height              = rc.heightOption.get
          )
        case "file" =>
          ContentFile(
            pathOption          = rc.pathOption map (_.makeRelativePath),
            thumbnailPathOption = rc.thumbnailPathOption map (_.makeRelativePath),
            mimeTypeOption      = rc.mimeTypeOption,
            title               = rc.titleOption.getOrElse("<File>"),
            performerOption     = rc.performerOption,
            durationSecOption   = rc.durationSecOption,
            widthOption         = rc.widthOption,
            heightOption        = rc.heightOption
          )
        case "location" =>
          assert(rc.latOption.isDefined && rc.lonOption.isDefined, rc)
          ContentLocation(
            titleOption       = rc.titleOption,
            addressOption     = rc.addressOption,
            latStr            = rc.latOption.get.toString,
            lonStr            = rc.lonOption.get.toString,
            durationSecOption = rc.durationSecOption
          )
        case "poll" =>
          assert(rc.pollQuestionOption.isDefined, rc)
          ContentPoll(
            question = rc.pollQuestionOption.get
          )
        case "shared_contact" =>
          ContentSharedContact(
            firstNameOption   = rc.firstNameOption,
            lastNameOption    = rc.lastNameOption,
            phoneNumberOption = rc.phoneNumberOption,
            vcardPathOption   = rc.vcardPathOption map (_.makeRelativePath),
          )
      }
    }

    def copyFileFrom(srcDsRoot: JFile,
                     dstDsRoot: JFile,
                     chatId: ChatId)(subpath: Subpath, thumbnailForOption: Option[String])(srcRelPath: String): Option[String] = {
      val srcFile = new JFile(srcDsRoot, srcRelPath)
      if (!srcFile.exists) {
        log.info(s"Not found: ${srcFile.getAbsolutePath}")
        None
      } else Some {
        require(srcFile.isFile, s"Not a file: ${srcFile.getAbsolutePath}")
        val srcExtSuffix = FilenameUtils.getExtension(srcFile.getName) match {
          case "" => ""
          case x  => "." + x
        }
        val name = thumbnailForOption match {
          case Some(mainFile) =>
            val baseName = FilenameUtils.getBaseName(mainFile)
            require(baseName.nonEmpty)
            baseName + "_thumb" + srcExtSuffix
          case _ if subpath.useHashing =>
            val hash = Hash.digestAsHex(srcFile)
            hash + srcExtSuffix
          case _ =>
            srcFile.getName
        }
        require(name.nonEmpty, s"Filename empty: ${srcFile.getAbsolutePath}")

        val dstRelPath = s"chats/chat_${chatId}/${subpath.fragment}${name}"
        val dstFile = new JFile(dstDsRoot, dstRelPath)
        require(dstFile.getParentFile.exists || dstFile.getParentFile.mkdirs(),
          s"Can't create dataset root path ${dstFile.getParentFile.getAbsolutePath}")
        if (dstFile.exists) {
          // Assume collisions don't exist
          require(subpath.useHashing || (srcFile.bytes sameElements dstFile.bytes),
            s"File already exists: ${dstFile.getAbsolutePath}, and it doesn't match source ${srcFile.getAbsolutePath}")
        } else {
          Files.copy(srcFile.toPath, dstFile.toPath)
        }

        dstRelPath
      }
    }

    def fromChat(srcDsRoot: JFile, dstDsRoot: JFile, c: Chat): RawChat = {
      RawChat(
        dsUuid          = c.dsUuid,
        id              = c.id,
        nameOption      = c.nameOption,
        tpe             = c.tpe,
        imgPathOption   = c.imgPathOption flatMap copyFileFrom(srcDsRoot, dstDsRoot, c.id)(Subpath.Root, None),
        memberIdsOption = Some(c.memberIds.toList),
        msgCount        = c.msgCount
      )
    }

    def fromMessage(
        dsUuid: PbUuid,
        srcDsRoot: JFile,
        dstDsRoot: JFile,
        chatId: ChatId,
        msg: Message
    ): (RawMessage, Option[RawContent], Seq[RawRichTextElement]) = {
      val rawRichTextEls: Seq[RawRichTextElement] =
        msg.text map fromRichText(msg.internalIdTyped)
      val template = RawMessage(
        dsUuid                 = dsUuid,
        chatId                 = chatId,
        internalId             = msg.internalIdTyped,
        sourceIdOption         = msg.sourceIdTypedOption,
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

      val (rawMessage: RawMessage, rawContentOption: Option[RawContent]) = msg.typed match {
        case Message.Typed.Regular(m) =>
          val rawContentOption = m.contentOption map fromContent(dsUuid, srcDsRoot, dstDsRoot, chatId, msg.internalIdTyped)
          template.copy(
            messageType            = "regular",
            editTimeOption         = m.editTimeOption,
            forwardFromNameOption  = m.forwardFromNameOption,
            replyToMessageIdOption = m.replyToMessageIdTypedOption
          ) -> rawContentOption
        case Message.Typed.Service(Some(m: MessageServicePhoneCall)) =>
          template.copy(
            messageType         = "service_phone_call",
            durationSecOption   = m.durationSecOption,
            discardReasonOption = m.discardReasonOption
          ) -> None
        case Message.Typed.Service(Some(m: MessageServicePinMessage)) =>
          template.copy(
            messageType           = "service_pin_message",
            pinnedMessageIdOption = Some(m.messageIdTyped),
          ) -> None
        case Message.Typed.Service(Some(_: MessageServiceClearHistory)) =>
          template.copy(
            messageType = "service_clear_history",
          ) -> None
        case Message.Typed.Service(Some(m: MessageServiceGroupCreate)) =>
          template.copy(
            messageType = "service_group_create",
            titleOption = Some(m.title),
            members     = m.members
          ) -> None
        case Message.Typed.Service(Some(m: MessageServiceGroupEditTitle)) =>
          template.copy(
            messageType = "service_edit_title",
            titleOption = Some(m.title)
          ) -> None
        case Message.Typed.Service(Some(m: MessageServiceGroupEditPhoto)) =>
          val photo = m.photo
          template.copy(
            messageType  = "service_edit_photo",
            pathOption   = photo.pathOption flatMap copyFileFrom(srcDsRoot, dstDsRoot, chatId)(Subpath.Photos, None),
            widthOption  = Some(photo.width),
            heightOption = Some(photo.height)
          ) -> None
        case Message.Typed.Service(Some(_: MessageServiceGroupDeletePhoto)) =>
          template.copy(
            messageType  = "service_delete_photo",
          ) -> None
        case Message.Typed.Service(Some(m: MessageServiceGroupInviteMembers)) =>
          template.copy(
            messageType = "service_invite_group_members",
            members     = m.members
          ) -> None
        case Message.Typed.Service(Some(m: MessageServiceGroupRemoveMembers)) =>
          template.copy(
            messageType = "service_group_remove_members",
            members     = m.members
          ) -> None
        case Message.Typed.Service(Some(m: MessageServiceGroupMigrateFrom)) =>
          template.copy(
            messageType = "service_group_migrate_from",
            titleOption = Some(m.title)
          ) -> None
        case Message.Typed.Service(Some(_: MessageServiceGroupMigrateTo)) =>
          template.copy(
            messageType = "service_group_migrate_to",
          ) -> None
        case Message.Typed.Service(Some(m: MessageServiceGroupCall)) =>
          template.copy(
            messageType = "service_group_call",
            members     = m.members
          ) -> None
        case Message.Typed.Empty | Message.Typed.Service(None) =>
          unexpectedCase(msg)
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
        case _: RteSpoiler       => template.copy(elementType = "spoiler")
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
            languageOption = el.languageOption
          )
      }
    }

    def fromContent(dsUuid: PbUuid,
                    srcDsRoot: JFile,
                    dstDsRoot: JFile,
                    chatId: ChatId,
                    msgId: MessageInternalId)(c: Content): RawContent = {
      val copy = copyFileFrom(srcDsRoot, dstDsRoot, chatId) _
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
      c match {
        case c: ContentSticker =>
          val newPathOption = c.pathOption flatMap copy(Subpath.Stickers, None)
          template.copy(
            elementType         = "sticker",
            pathOption          = newPathOption,
            thumbnailPathOption = c.thumbnailPathOption flatMap copy(Subpath.Stickers, newPathOption),
            emojiOption         = c.emojiOption,
            widthOption         = Some(c.width),
            heightOption        = Some(c.height)
          )
        case c: ContentPhoto =>
          template.copy(
            elementType  = "photo",
            pathOption   = c.pathOption flatMap copy(Subpath.Photos, None),
            widthOption  = Some(c.width),
            heightOption = Some(c.height)
          )
        case c: ContentVoiceMsg =>
          template.copy(
            elementType       = "voice_message",
            pathOption        = c.pathOption flatMap copy(Subpath.Voices, None),
            mimeTypeOption    = Some(c.mimeType),
            durationSecOption = c.durationSecOption
          )
        case c: ContentVideoMsg =>
          val newPathOption = c.pathOption flatMap copy(Subpath.Videos, None)
          template.copy(
            elementType         = "video_message",
            pathOption          = newPathOption,
            thumbnailPathOption = c.thumbnailPathOption flatMap copy(Subpath.Videos, newPathOption),
            mimeTypeOption      = Some(c.mimeType),
            durationSecOption   = c.durationSecOption,
            widthOption         = Some(c.width),
            heightOption        = Some(c.height)
          )
        case c: ContentAnimation =>
          val newPathOption = c.pathOption flatMap copy(Subpath.Videos, None)
          template.copy(
            elementType         = "animation",
            pathOption          = newPathOption,
            thumbnailPathOption = c.thumbnailPathOption flatMap copy(Subpath.Videos, newPathOption),
            mimeTypeOption      = Some(c.mimeType),
            durationSecOption   = c.durationSecOption,
            widthOption         = Some(c.width),
            heightOption        = Some(c.height)
          )
        case c: ContentFile =>
          val newPathOption = c.pathOption flatMap copy(Subpath.Files, None)
          template.copy(
            elementType         = "file",
            pathOption          = newPathOption,
            thumbnailPathOption = c.thumbnailPathOption flatMap copy(Subpath.Files, newPathOption),
            mimeTypeOption      = c.mimeTypeOption,
            titleOption         = Some(c.title),
            performerOption     = c.performerOption,
            durationSecOption   = c.durationSecOption,
            widthOption         = c.widthOption,
            heightOption        = c.heightOption
          )
        case c: ContentLocation =>
          template.copy(
            elementType       = "location",
            titleOption       = c.titleOption,
            addressOption     = c.addressOption,
            latOption         = Some(c.lat),
            lonOption         = Some(c.lon),
            durationSecOption = c.durationSecOption
          )
        case c: ContentPoll =>
          template.copy(
            elementType        = "poll",
            pollQuestionOption = Some(c.question)
          )
        case c: ContentSharedContact =>
          template.copy(
            elementType       = "shared_contact",
            firstNameOption   = c.firstNameOption,
            lastNameOption    = c.lastNameOption,
            phoneNumberOption = c.phoneNumberOption,
            vcardPathOption   = c.vcardPathOption flatMap copy(Subpath.Files, None),
          )
      }
    }
  }

  override def equals(that: Any): Boolean = that match {
    case that: H2ChatHistoryDao => this.name == that.name && that.isLoaded(this.storagePath)
    case _                      => false
  }

  override def hashCode: Int = this.name.hashCode + 17 * this.storagePath.hashCode
}

object H2ChatHistoryDao {

  type ChatId = Long

  val BackupsDir = "_backups"
  val MaxBackups = 3

  /** Subpath inside a directory, suffixed by "/" to be concatenated. */
  sealed abstract class Subpath(val fragment: String, val useHashing: Boolean = false)

  object Subpath {
    case object Root extends Subpath("")
    case object Photos extends Subpath("photos/", useHashing = true)
    case object Stickers extends Subpath("stickers/", useHashing = true)
    case object Voices extends Subpath("voice_messages/")
    case object Videos extends Subpath("videos/", useHashing = true)
    case object Files extends Subpath("files/")
  }

  lazy val Hash: DigestUtils =
    new DigestUtils("SHA3-256")

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
    case ChatType.Personal     => "personal"
    case ChatType.PrivateGroup => "private_group"
    case x                     => unexpectedCase(x)
  }

  implicit val pbUuidFromString: Get[PbUuid] = Get[String].tmap(s => fromJavaUuid(java.util.UUID.fromString(s)))

  implicit val pbUuidToString: Put[PbUuid] = Put[String].tcontramap(pb => pb.value.toLowerCase)

  private case class UserMapping(
    dsUuid           : PbUuid,
    id               : Long,
    firstNameOption  : Option[String],
    lastNameOption   : Option[String],
    usernameOption   : Option[String],
    phoneNumberOption: Option[String]
  )

  implicit val userRead: Read[User] = Read[UserMapping].map { um =>
    User(
      dsUuid            = um.dsUuid,
      id                = um.id,
      firstNameOption   = um.firstNameOption,
      lastNameOption    = um.lastNameOption,
      usernameOption    = um.usernameOption,
      phoneNumberOption = um.phoneNumberOption
    )
  }

  implicit val userWrite: Write[User] = Write[UserMapping].contramap { u =>
    UserMapping(
      dsUuid            = u.dsUuid,
      id                = u.id,
      firstNameOption   = u.firstNameOption,
      lastNameOption    = u.lastNameOption,
      usernameOption    = u.usernameOption,
      phoneNumberOption = u.phoneNumberOption
    )
  }

  private case class DatasetMapping(
    uuid      : PbUuid,
    alias     : String,
    sourceType: String
  )

  implicit val datasetRead: Read[Dataset] = Read[DatasetMapping].map { dm =>
    Dataset(
      uuid       = dm.uuid,
      alias      = dm.alias,
      sourceType = dm.sourceType,
    )
  }

  implicit val datasetWrite: Write[Dataset] = Write[DatasetMapping].contramap { d =>
    DatasetMapping(
      uuid       = d.uuid,
      alias      = d.alias,
      sourceType = d.sourceType,
    )
  }

  private val ArraySeparator = ";;;"
  implicit val stringSeqFromString: Get[Seq[String]] = Get[String].tmap(s => s.split(ArraySeparator))
  implicit val stringSeqToString:   Put[Seq[String]] = Put[String].tcontramap(ss => ss.mkString(ArraySeparator))
}
