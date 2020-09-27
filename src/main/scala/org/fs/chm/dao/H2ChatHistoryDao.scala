package org.fs.chm.dao

import java.io.File
import java.nio.file.Files
import java.nio.file.attribute.FileTime
import java.sql.Timestamp
import java.util.UUID

import scala.annotation.tailrec

import cats.effect._
import cats.implicits._
import com.github.nscala_time.time.Imports._
import doobie.ConnectionIO
import doobie._
import doobie.free.connection
import doobie.free.connection.ConnectionOp
import doobie.h2.implicits._
import doobie.implicits._
import org.fs.chm.utility.EntityUtils._
import org.fs.chm.utility.IoUtils
import org.fs.utility.Imports._
import org.fs.utility.StopWatch
import org.slf4s.Logging

class H2ChatHistoryDao(
    dataPathRoot: File,
    txctr: Transactor.Aux[IO, _],
    closeTransactor: () => Unit
) extends MutableChatHistoryDao
    with Logging {

  import org.fs.chm.dao.H2ChatHistoryDao._

  private val Lock = new Object

  private var _interlocutorsCacheOption: Option[Map[UUID, Map[ChatId, Seq[User]]]] = None
  private var _backupsEnabled = true

  override def name: String = s"${dataPathRoot.getName} database"

  override def datasets: Seq[Dataset] = {
    queries.datasets.selectAll.transact(txctr).unsafeRunSync()
  }

  override def datasetRoot(dsUuid: UUID): File = {
    new File(dataPathRoot, dsUuid.toString.toLowerCase).getAbsoluteFile
  }

  override def datasetFiles(dsUuid: UUID): Set[File] = {
    queries.selectAllPaths(dsUuid).transact(txctr).unsafeRunSync() map (_.toFile(dsUuid))
  }

  override def myself(dsUuid: UUID): User = {
    queries.users
      .selectMyself(dsUuid)
      .transact(txctr)
      .unsafeRunSync()
      .getOrElse(throw new IllegalStateException(s"Dataset $dsUuid has no self user!"))
  }

  /** Contains myself as well */
  override def users(dsUuid: UUID): Seq[User] = {
    queries.users.selectAll(dsUuid).transact(txctr).unsafeRunSync()
  }

  def userOption(dsUuid: UUID, id: Long): Option[User] = {
    queries.users.select(dsUuid, id).transact(txctr).unsafeRunSync()
  }

  override def chats(dsUuid: UUID): Seq[Chat] = {
    // FIXME: Double-call when loading a DB!
    logPerformance {
      queries.chats.selectAll(dsUuid).transact(txctr).unsafeRunSync()
    }((res, ms) => s"${res.size} chats fetched in ${ms} ms")
  }

  override def chatOption(dsUuid: UUID, id: ChatId): Option[Chat] = {
    queries.chats.select(dsUuid, id).transact(txctr).unsafeRunSync()
  }

  private def interlocutorsCache: Map[UUID, Map[ChatId, Seq[User]]] = Lock.synchronized {
    if (_interlocutorsCacheOption.isEmpty) {
      _interlocutorsCacheOption = Some((for {
        ds      <- datasets.par
        myself1 = myself(ds.uuid)
        users1  = users(ds.uuid)
      } yield {
        val chatInterlocutors = chats(ds.uuid).map { c =>
          val ids: Seq[Long] = queries.users.selectInterlocutorIds(ds.uuid, c.id).transact(txctr).unsafeRunSync()
          val usersWithoutMe = users1.filter(u => u != myself1 && ids.contains(u.id))
          (c.id, myself1 +: usersWithoutMe.sortBy(u => (u.id, u.prettyName)))
        }.toMap
        (ds.uuid, chatInterlocutors)
      }).seq.toMap)
    }
    _interlocutorsCacheOption.get
  }

  override def interlocutors(chat: Chat): Seq[User] =
    (for {
      c1 <- interlocutorsCache.get(chat.dsUuid)
      c2 <- c1.get(chat.id)
    } yield c2) getOrElse Seq.empty

  override def scrollMessages(chat: Chat, offset: Int, limit: Int): IndexedSeq[Message] = {
    logPerformance {
      val raws = queries.rawMessages.selectSlice(chat, offset, limit).transact(txctr).unsafeRunSync()
      raws map Raws.toMessage
    }((res, ms) => s"${res.size} messages fetched in ${ms} ms [scrollMessages]")
  }

  override def lastMessages(chat: Chat, limit: Int): IndexedSeq[Message] = {
    logPerformance {
      val raws = queries.rawMessages.selectLastInversed(chat, limit).transact(txctr).unsafeRunSync().reverse
      raws map Raws.toMessage
    }((res, ms) => s"${res.size} messages fetched in ${ms} ms [lastMessages]")
  }

  override def messagesBeforeImpl(chat: Chat, msg: Message, limit: Int): IndexedSeq[Message] = {
    logPerformance {
      val raws = queries.rawMessages.selectBeforeInversedInc(chat, msg, limit).transact(txctr).unsafeRunSync().reverse
      raws map Raws.toMessage
    }((res, ms) => s"${res.size} messages fetched in ${ms} ms [messagesBeforeImpl]")
  } ensuring (seq => seq.nonEmpty && seq.last =~= msg)

  override def messagesAfterImpl(chat: Chat, msg: Message, limit: Int): IndexedSeq[Message] = {
    logPerformance {
      val raws = queries.rawMessages.selectAfterInc(chat, msg, limit).transact(txctr).unsafeRunSync()
      raws map Raws.toMessage
    }((res, ms) => s"${res.size} messages fetched in ${ms} ms [messagesAfterImpl]")
  }

  override def messagesBetweenImpl(chat: Chat, msg1: Message, msg2: Message): IndexedSeq[Message] = {
    logPerformance {
      val raws = queries.rawMessages.selectBetweenInc(chat, msg1, msg2).transact(txctr).unsafeRunSync()
      raws map Raws.toMessage
    }((res, ms) => s"${res.size} messages fetched in ${ms} ms [messagesBetweenImpl]")
  }

  override def countMessagesBetween(chat: Chat, msg1: Message, msg2: Message): Int = {
    logPerformance {
      require(msg1.time <= msg2.time)
      queries.rawMessages.countBetweenExc(chat, msg1, msg2).transact(txctr).unsafeRunSync()
    }((res, ms) => s"${res} messages counted in ${ms} ms [countMessagesBetween]")
  }

  override def messageOption(chat: Chat, id: Message.SourceId): Option[Message] = {
    queries.rawMessages.selectOptionBySourceId(chat, id).transact(txctr).unsafeRunSync().map(Raws.toMessage)
  }

  override def messageOptionByInternalId(chat: Chat, id: Message.InternalId): Option[Message] =
    queries.rawMessages.selectOption(chat, id).transact(txctr).unsafeRunSync().map(Raws.toMessage)

  def createTables(): Unit = {
    queries.createDdl.transact(txctr).unsafeRunSync()
  }

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

          for (c <- dao.chats(ds.uuid)) {
            require(c.id > 0, "IDs should be positive!")
            query = query flatMap (_ => queries.chats.insert(dsRoot, c))

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

        // Copying files
        val fromFiles     = dao.datasetFiles(ds.uuid)
        val fromPrefixLen = dsRoot.getAbsolutePath.length
        val toRoot        = datasetRoot(ds.uuid)
        val filesMap = fromFiles.map { fromFile =>
          val relPath  = fromFile.getAbsolutePath.drop(fromPrefixLen)
          (fromFile, new File(toRoot, relPath))
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
          val chats1 = dao.chats(ds.uuid)
          val chats2 = chats(ds.uuid)
          assert(chats1.size == chats2.size, s"Chat size differs:\nWas    ${chats1.size}\nBecame ${chats2.size}")
          for (((c1, c2), i) <- chats1.zip(chats2).zipWithIndex) {
            StopWatch.measureAndCall {
              log.info(s"Checking chat '${c1.nameOption.getOrElse("")}' with ${c1.msgCount} messages")
              assert(c1 == c2, s"Chat #$i differs:\nWas    $c1\nBecame $c2")
              val messages1 = dao.lastMessages(c1, c1.msgCount + 1)
              val messages2 = lastMessages(c2, c2.msgCount + 1)
              assert(
                messages1.size == messages1.size,
                s"Messages size for chat $c1 (#$i) differs:\nWas    ${messages1.size}\nBecame ${messages2.size}")
              for (((m1, m2), j) <- messages1.zip(messages2).zipWithIndex) {
                assert(m1 =~= m2, s"Message #$j for chat $c1 (#$i) differs:\nWas    $m1\nBecame $m2")
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
      _interlocutorsCacheOption = None
    }
  }

  override def renameDataset(dsUuid: UUID, newName: String): Dataset = {
    backup()
    queries.datasets.rename(dsUuid, newName).transact(txctr).unsafeRunSync()
    datasets.find(_.uuid == dsUuid).get
  }

  override def deleteDataset(dsUuid: UUID): Unit = {
    backup()
    val query = for {
      q1 <- queries.rawContent.deleteByDataset(dsUuid)
      q2 <- queries.rawRichTextElements.deleteByDataset(dsUuid)
      q3 <- queries.rawMessages.deleteByDataset(dsUuid)
      q4 <- queries.chats.deleteByDataset(dsUuid)
      q5 <- queries.users.deleteByDataset(dsUuid)
      qL <- queries.datasets.delete(dsUuid)
    } yield q1 + q2 + q3 + q4 + q5 + qL
    query.transact(txctr).unsafeRunSync()
    val srcDataPath = datasetRoot(dsUuid)
    if (srcDataPath.exists()) {
      val dstDataPath = new File(getBackupPath(), srcDataPath.getName)
      Files.move(srcDataPath.toPath, dstDataPath.toPath)
    }
    Lock.synchronized {
      _interlocutorsCacheOption = None
    }
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
      _interlocutorsCacheOption = None
    }
  }

  override def updateUser(user: User): Unit = {
    backup()
    val isMyself = myself(user.dsUuid).id == user.id
    val rowsNum  = queries.users.update(user, isMyself).transact(txctr).unsafeRunSync()
    require(rowsNum == 1, s"Updating user affected ${rowsNum} rows!")
    queries.chats.updateRenameUser(user).transact(txctr).unsafeRunSync()
    Lock.synchronized {
      _interlocutorsCacheOption = None
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
      lastSeenTimeOption = latest(baseUser.lastSeenTimeOption, absorbedUser.lastSeenTimeOption)
    )

    val query = for {
      v1 <- queries.users.update(newUser, isMyself)
      // Change ownership of old user's stuff
      v2 <- queries.rawMessages.updateUser(dsUuid, absorbedUser.id, newUser.id)
      v3 <- queries.users.delete(dsUuid, absorbedUser.id)
      v4 <- queries.mergePersonalChats(dsUuid, newUser.id)
      v5 <- queries.chats.updateRenameUser(newUser)
    } yield v1 + v2 + v3 + v4 + v5
    val rowsNum = query.transact(txctr).unsafeRunSync()
    assert(rowsNum > 0, "Nothing was updated!")
    Lock.synchronized {
      _interlocutorsCacheOption = None
    }
  }

  override def insertChat(dsRoot: File, chat: Chat): Unit = {
    require(chat.id > 0, "ID should be positive!")
    backup()
    queries.chats.insert(dsRoot, chat).transact(txctr).unsafeRunSync()
    Lock.synchronized {
      _interlocutorsCacheOption = None
    }
  }

  override def deleteChat(chat: Chat): Unit = {
    backup()
    val query = for {
      _ <- queries.rawContent.deleteByChat(chat.dsUuid, chat.id)
      _ <- queries.rawRichTextElements.deleteByChat(chat.dsUuid, chat.id)
      _ <- queries.rawMessages.deleteByChat(chat.dsUuid, chat.id)
      _ <- queries.chats.delete(chat.dsUuid, chat.id)
      u <- queries.users.deleteOrphans(chat.dsUuid)
    } yield u
    val deletedUsersNum = query.transact(txctr).unsafeRunSync()
    log.info(s"Deleted ${deletedUsersNum} orphaned users")
    Lock.synchronized {
      _interlocutorsCacheOption = None
    }
  }

  override def insertMessages(dsRoot: File, chat: Chat, msgs: Seq[Message]): Unit = {
    if (msgs.nonEmpty) {
      val query = msgs
        .map(msg => queries.messages.insert(chat.dsUuid, dsRoot, chat.id, msg))
        .reduce((q1, q2) => q1 flatMap (_ => q2))
      query.transact(txctr).unsafeRunSync()
      backup()
      Lock.synchronized {
        _interlocutorsCacheOption = None
      }
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

  protected[dao] def getBackupPath(): File = {
    val backupDir = new File(dataPathRoot, H2ChatHistoryDao.BackupsDir)
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
          .backup(new File(backupDir, newBackupName).getAbsolutePath.replace("\\", "/"))
          .transact(txctr)
          .unsafeRunSync()
      }((_, t) => log.info(s"Backup ${newBackupName} done in $t ms"))
      for (oldBackup <- backups.dropRight(H2ChatHistoryDao.MaxBackups - 1)) {
        oldBackup.delete()
      }
    }
  }

  override def close(): Unit = {
    closeTransactor()
  }

  override def isLoaded(dataPathRoot: File): Boolean = {
    dataPathRoot != null && this.dataPathRoot == dataPathRoot
  }

  object queries {
    lazy val createDdl: ConnectionIO[Int] = {
      val createQueries = Seq(
        sql"""
        CREATE TABLE datasets (
          uuid                UUID NOT NULL PRIMARY KEY,
          alias               VARCHAR(255),
          source_type         VARCHAR(255)
        );
        """,
        sql"""
        CREATE TABLE users (
          ds_uuid             UUID NOT NULL,
          id                  BIGINT NOT NULL,
          first_name          VARCHAR(255),
          last_name           VARCHAR(255),
          username            VARCHAR(255),
          phone_number        VARCHAR(20),
          last_seen_time      TIMESTAMP,
          -- Not in the entity
          is_myself           BOOLEAN NOT NULL,
          PRIMARY KEY (ds_uuid, id),
          FOREIGN KEY (ds_uuid) REFERENCES datasets (uuid)
        );
        """,
        sql"""
        CREATE TABLE chats (
          ds_uuid             UUID NOT NULL,
          id                  BIGINT NOT NULL,
          name                VARCHAR(255),
          type                VARCHAR(255) NOT NULL,
          img_path            VARCHAR(4095),
          PRIMARY KEY (ds_uuid, id),
          FOREIGN KEY (ds_uuid) REFERENCES datasets (uuid)
        );
        """,
        sql"""
        CREATE TABLE messages (
          ds_uuid             UUID NOT NULL,
          chat_id             BIGINT NOT NULL,
          internal_id         IDENTITY NOT NULL PRIMARY KEY,
          source_id           BIGINT,
          message_type        VARCHAR(255) NOT NULL,
          time                TIMESTAMP NOT NULL,
          edit_time           TIMESTAMP,
          from_id             BIGINT NOT NULL,
          forward_from_name   VARCHAR(255),
          reply_to_message_id BIGINT, -- not a reference
          title               VARCHAR(255),
          members             VARCHAR, -- serialized
          duration_sec        INT,
          discard_reason      VARCHAR(255),
          pinned_message_id   BIGINT,
          path                VARCHAR(4095),
          width               INT,
          height              INT,
          FOREIGN KEY (ds_uuid) REFERENCES datasets (uuid),
          FOREIGN KEY (ds_uuid, chat_id) REFERENCES chats (ds_uuid, id),
          FOREIGN KEY (ds_uuid, from_id) REFERENCES users (ds_uuid, id)
        );
        """,
        sql"""
        CREATE UNIQUE INDEX messages_internal_id ON messages(ds_uuid, chat_id, source_id); -- allows duplicate NULLs
        """,
        sql"""
        CREATE TABLE messages_text_elements (
          id                  IDENTITY NOT NULL,
          ds_uuid             UUID NOT NULL, -- not necessary, but is there for efficiency
          message_internal_id BIGINT NOT NULL,
          element_type        VARCHAR(255) NOT NULL,
          text                VARCHAR,
          href                VARCHAR(4095),
          hidden              BOOLEAN,
          language            VARCHAR(4095),
          PRIMARY KEY (id),
          FOREIGN KEY (ds_uuid) REFERENCES datasets (uuid),
          FOREIGN KEY (message_internal_id) REFERENCES messages (internal_id)
        );
        """,
        sql"""
        CREATE INDEX messages_text_elements_idx ON messages_text_elements(ds_uuid, message_internal_id);
        """,
        sql"""
        CREATE TABLE messages_content (
          id                  IDENTITY NOT NULL,
          ds_uuid             UUID NOT NULL, -- not necessary, but is there for efficiency
          message_internal_id BIGINT NOT NULL,
          element_type        VARCHAR(255) NOT NULL,
          path                VARCHAR(4095),
          thumbnail_path      VARCHAR(4095),
          emoji               VARCHAR(255),
          width               INT,
          height              INT,
          mime_type           VARCHAR(255),
          title               VARCHAR(255),
          performer           VARCHAR(255),
          lat                 DECIMAL(9,6),
          lon                 DECIMAL(9,6),
          duration_sec        INT,
          poll_question       VARCHAR,
          first_name          VARCHAR(255),
          last_name           VARCHAR(255),
          phone_number        VARCHAR(20),
          vcard_path          VARCHAR(4095),
          PRIMARY KEY (id),
          FOREIGN KEY (ds_uuid) REFERENCES datasets (uuid),
          FOREIGN KEY (message_internal_id) REFERENCES messages (internal_id)
        );
      """,
      sql"""
        CREATE INDEX messages_content_idx ON messages_content(ds_uuid, message_internal_id);
        """
      ) map (_.update.run)
      createQueries.reduce((a, b) => a flatMap (_ => b))
    }

    val pure0 = cats.free.Free.pure[ConnectionOp, Int](0)

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

      def rename(dsUuid: UUID, newName: String): ConnectionIO[Int] =
        sql"UPDATE datasets SET alias = ${newName} WHERE uuid = ${dsUuid}".update.run

      def delete(dsUuid: UUID): ConnectionIO[Int] =
        sql"DELETE FROM datasets WHERE uuid = ${dsUuid}".update.run
    }

    object users {
      private val colsFr                 = fr"ds_uuid, id, first_name, last_name, username, phone_number, last_seen_time"
      private val selectAllFr            = fr"SELECT" ++ colsFr ++ fr"FROM users"
      private def selectFr(dsUuid: UUID) = selectAllFr ++ fr"WHERE ds_uuid = $dsUuid"
      private val defaultOrder           = fr"ORDER BY id, first_name, last_name, username, phone_number"

      def selectAll(dsUuid: UUID) =
        (selectFr(dsUuid) ++ defaultOrder).query[User].to[Seq]

      def selectMyself(dsUuid: UUID) =
        (selectFr(dsUuid) ++ fr"AND is_myself = true").query[User].option

      def select(dsUuid: UUID, id: Long) =
        (selectFr(dsUuid) ++ fr"AND id = $id").query[User].option

      def selectInterlocutorIds(dsUuid: UUID, chatId: Long) =
        sql"SELECT DISTINCT from_id FROM messages WHERE ds_uuid = $dsUuid AND chat_id = $chatId ORDER BY from_id"
          .query[Long]
          .to[Seq]

      def insert(u: User, isMyself: Boolean) =
        (fr"INSERT INTO users (" ++ colsFr ++ fr", is_myself) VALUES ("
          ++ fr"${u.dsUuid}, ${u.id}, ${u.firstNameOption}, ${u.lastNameOption}, ${u.usernameOption},"
          ++ fr"${u.phoneNumberOption}, ${u.lastSeenTimeOption}, ${isMyself}"
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
      def delete(dsUuid: UUID, id: Long): ConnectionIO[Int] =
        sql"DELETE FROM users u WHERE u.id = ${id}".update.run

      /** Delete all users from the given dataset (other than self) that have no messages in any chat */
      def deleteOrphans(dsUuid: UUID): ConnectionIO[Int] =
        sql"""
            DELETE FROM users u
            WHERE u.id IN (
              SELECT id FROM (
                SELECT
                  u.id,
                  u.first_name,
                  (SELECT COUNT(*) FROM messages m WHERE m.ds_uuid = $dsUuid AND m.from_id = u.id) AS msgs_count
                FROM users u
                WHERE u.ds_uuid = $dsUuid
                AND u.is_myself = false
              ) WHERE msgs_count = 0
            )
           """.update.run

      def deleteByDataset(dsUuid: UUID): ConnectionIO[Int] =
        sql"DELETE FROM users WHERE ds_uuid = ${dsUuid}".update.run
    }

    object chats {
      private val colsFr     = fr"ds_uuid, id, name, type, img_path"

      private def selectFr(dsUuid: UUID) =
        (fr"SELECT" ++ colsFr ++ fr","
          // m.ds_uuid = c.ds_uuid here serves so that H2 could discover an index
          ++ fr"(SELECT COUNT(*) FROM messages m WHERE m.ds_uuid = c.ds_uuid AND m.chat_id = c.id) AS msg_count,"
          ++ fr"FROM chats c WHERE c.ds_uuid = $dsUuid")

      private def hasMessagesFromUser(userId: Long) =
        (fr"EXISTS (SELECT m.from_id FROM messages m"
          ++ fr"WHERE m.ds_uuid = c.ds_uuid AND m.chat_id = c.id AND m.from_id = ${userId})")

      def selectAll(dsUuid: UUID): ConnectionIO[Seq[Chat]] =
        selectFr(dsUuid).query[RawChat].to[Seq].map(_ map Raws.toChat)

      def selectAllPersonalChats(dsUuid: UUID, userId: Long): ConnectionIO[Seq[Chat]] =
        if (myself(dsUuid).id == userId) {
          cats.free.Free.pure(Seq.empty)
        } else {
          (selectFr(dsUuid) ++ fr"""
                AND c.type = ${ChatType.Personal: ChatType}
                AND """ ++ hasMessagesFromUser(userId) ++ fr"""
             """).query[RawChat].to[Seq].map(_ map Raws.toChat)
        }

      def select(dsUuid: UUID, id: ChatId): ConnectionIO[Option[Chat]] =
        (selectFr(dsUuid) ++ fr"AND c.id = ${id}").query[RawChat].option.map(_ map Raws.toChat)

      def insert(dsRoot: File, c: Chat) = {
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

      def delete(dsUuid: UUID, id: ChatId): ConnectionIO[Int] =
        (fr"DELETE FROM chats WHERE ds_uuid = ${dsUuid} AND id = ${id}").update.run

      def deleteByDataset(dsUuid: UUID): ConnectionIO[Int] =
        sql"DELETE FROM chats WHERE ds_uuid = ${dsUuid}".update.run
    }

    object messages {
      def insert(
          dsUuid: UUID,
          dsRoot: File,
          chatId: Long,
          msg: Message,
      ): ConnectionIO[Unit] = {
        msg.sourceIdOption foreach (id => require(id > 0, "Source IDs should be positive!"))
        val (rm, rcOption, rrtEls) = Raws.fromMessage(dsUuid, dsRoot, chatId, msg)

        for {
          msgInternalId <- rawMessages.insert(rm)

          // Content
          _ <- rcOption map { rc =>
            rawContent.insert(rc, dsUuid, msgInternalId)
          } getOrElse pure0

          // RichText
          _ <- rrtEls.foldLeft(pure0) { (q, rrtEl) =>
            q flatMap (_ => rawRichTextElements.insert(rrtEl, dsUuid, msgInternalId) map (_ => 0))
          }
        } yield ()
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

      private def whereDsAndChatFr(dsUuid: UUID, chatId: Long) =
        fr"WHERE m.ds_uuid = $dsUuid AND m.chat_id = $chatId"

      private def selectAllByChatFr(dsUuid: UUID, chatId: Long) =
        selectAllFr ++ whereDsAndChatFr(dsUuid, chatId)

      def selectMaxId(chat: Chat): ConnectionIO[Int] =
        (fr"SELECT MAX(m.id) FROM messages m" ++ whereDsAndChatFr(chat.dsUuid, chat.id))
          .query[Int]
          .option
          .map(_ getOrElse 0)

      def selectOption(chat: Chat, id: Message.InternalId): ConnectionIO[Option[RawMessage]] =
        (selectAllByChatFr(chat.dsUuid, chat.id) ++ fr"AND m.internal_id = ${id}").query[RawMessage].option

      def selectOptionBySourceId(chat: Chat, id: Message.SourceId): ConnectionIO[Option[RawMessage]] =
        (selectAllByChatFr(chat.dsUuid, chat.id) ++ fr"AND m.source_id = ${id}").query[RawMessage].option

      def selectSlice(chat: Chat, offset: Int, limit: Int): ConnectionIO[IndexedSeq[RawMessage]] =
        (selectAllByChatFr(chat.dsUuid, chat.id)
          ++ orderAsc ++ withLimit(limit)
          ++ fr"OFFSET $offset").query[RawMessage].to[IndexedSeq]

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

      def insert(m: RawMessage): ConnectionIO[Message.InternalId] =
        (fr"INSERT INTO messages (" ++ colsPureFr ++ fr") VALUES ("
          ++ fr"${m.dsUuid}, ${m.chatId}, default, ${m.sourceIdOption}, ${m.messageType},"
          ++ fr"${m.time}, ${m.editTimeOption},"
          ++ fr"${m.fromId}, ${m.forwardFromNameOption}, ${m.replyToMessageIdOption},"
          ++ fr"${m.titleOption}, ${m.members}, ${m.durationSecOption}, ${m.discardReasonOption},"
          ++ fr"${m.pinnedMessageIdOption}, ${m.pathOption}, ${m.widthOption}, ${m.heightOption}"
          ++ fr")").update.withUniqueGeneratedKeys[Message.InternalId]("internal_id")

      def removeSourceIds(dsUuid: UUID, fromChatId: Long): ConnectionIO[Int] =
        sql"""
            UPDATE messages m SET m.source_id = NULL
            WHERE m.chat_id = ${fromChatId} AND m.ds_uuid = ${dsUuid}
           """.update.run

      def updateUser(dsUuid: UUID, fromUserId: Long, toUserId: Long): ConnectionIO[Int] =
        sql"""
            UPDATE messages m SET m.from_id = ${toUserId}
            WHERE m.from_id = ${fromUserId} AND m.ds_uuid = ${dsUuid}
           """.update.run

      def updateChat(dsUuid: UUID, fromChatId: Long, toChatId: Long): ConnectionIO[Int] =
        sql"""
            UPDATE messages m SET m.chat_id = ${toChatId}
            WHERE m.chat_id = ${fromChatId} AND m.ds_uuid = ${dsUuid}
           """.update.run

      def deleteByChat(dsUuid: UUID, chatId: ChatId): ConnectionIO[Int] =
        (fr"DELETE FROM messages m WHERE m.ds_uuid = ${dsUuid} AND m.chat_id = ${chatId}").update.run

      def deleteByDataset(dsUuid: UUID): ConnectionIO[Int] =
        sql"DELETE FROM messages m WHERE m.ds_uuid = ${dsUuid}".update.run
    }

    object rawRichTextElements {
      private val colsNoKeysFr = fr"element_type, text, href, hidden, language"

      def selectAll(dsUuid: UUID, msgId: Message.InternalId) =
        (fr"SELECT" ++ colsNoKeysFr ++ fr"FROM messages_text_elements"
          ++ fr"WHERE ds_uuid = ${dsUuid} AND message_internal_id = ${msgId}"
          ++ fr"ORDER BY id").query[RawRichTextElement].to[Seq]

      def insert(rrte: RawRichTextElement, dsUuid: UUID, msgId: Message.InternalId) =
        (fr"INSERT INTO messages_text_elements(ds_uuid, message_internal_id, " ++ colsNoKeysFr ++ fr") VALUES ("
          ++ fr"${dsUuid}, ${msgId}, ${rrte.elementType}, ${rrte.text},"
          ++ fr"${rrte.hrefOption}, ${rrte.hiddenOption}, ${rrte.languageOption}"
          ++ fr")").update.withUniqueGeneratedKeys[Long]("id")

      def deleteByChat(dsUuid: UUID, chatId: ChatId): ConnectionIO[Int] =
        sql"""
            DELETE FROM messages_text_elements
            WHERE ds_uuid = ${dsUuid} AND message_internal_id IN (
              SELECT m.internal_id FROM messages m
              WHERE m.ds_uuid = ${dsUuid} AND m.chat_id = ${chatId}
            )
           """.update.run

      def deleteByDataset(dsUuid: UUID): ConnectionIO[Int] =
        sql"DELETE FROM messages_text_elements WHERE ds_uuid = ${dsUuid}".update.run
    }

    object rawContent {
      private val colsNoIdentityFr =
        Fragment.const(
          s"""|element_type, path, thumbnail_path, emoji, width, height, mime_type, title, performer, lat, lon,
              |duration_sec, poll_question, first_name, last_name, phone_number, vcard_path""".stripMargin)

      def select(dsUuid: UUID, msgId: Message.InternalId): ConnectionIO[Option[RawContent]] =
        (fr"SELECT" ++ colsNoIdentityFr ++ fr"FROM messages_content"
          ++ fr"WHERE ds_uuid = ${dsUuid} AND  message_internal_id = ${msgId}")
          .query[RawContent]
          .option

      def insert(rc: RawContent, dsUuid: UUID, msgId: Message.InternalId): ConnectionIO[Long] =
        (fr"INSERT INTO messages_content(ds_uuid, message_internal_id, " ++ colsNoIdentityFr ++ fr") VALUES ("
          ++ fr"${dsUuid}, ${msgId},"
          ++ fr"${rc.elementType}, ${rc.pathOption}, ${rc.thumbnailPathOption}, ${rc.emojiOption}, ${rc.widthOption},"
          ++ fr"${rc.heightOption}, ${rc.mimeTypeOption}, ${rc.titleOption}, ${rc.performerOption}, ${rc.latOption},"
          ++ fr"${rc.lonOption}, ${rc.durationSecOption}, ${rc.pollQuestionOption}, ${rc.firstNameOption},"
          ++ fr" ${rc.lastNameOption}, ${rc.phoneNumberOption}, ${rc.vcardPathOption}"
          ++ fr")").update.withUniqueGeneratedKeys[Long]("id")

      def deleteByChat(dsUuid: UUID, chatId: ChatId): ConnectionIO[Int] =
        sql"""
            DELETE FROM messages_content
            WHERE ds_uuid = ${dsUuid} AND message_internal_id IN (
              SELECT m.internal_id FROM messages m
              WHERE m.ds_uuid = ${dsUuid} AND m.chat_id = ${chatId}
            )
           """.update.run

      def deleteByDataset(dsUuid: UUID): ConnectionIO[Int] =
        sql"DELETE FROM messages_content WHERE ds_uuid = ${dsUuid}".update.run
    }

    /** Merge all messages from all personal chats with this user into the first one and delete the rest */
    def mergePersonalChats(dsUuid: UUID, userId: Long): ConnectionIO[Int] = {
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
              i3 <- chats.delete(dsUuid, otherChat.id)
            } yield i1 + i2 + i3
            query = query flatMap (_ => query2)
          }
          query
        }

      } yield v0
    }

    /** Select all non-null filesystem paths from all tables for the specific dataset */
    def selectAllPaths(dsUuid: UUID): ConnectionIO[Set[String]] = {
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
  }

  object Raws {
    def toChat(rc: RawChat): Chat = {
      Chat(
        dsUuid        = rc.dsUuid,
        id            = rc.id,
        nameOption    = rc.nameOption,
        tpe           = rc.tpe,
        imgPathOption = rc.imgPathOption map (_.toFile(rc.dsUuid)),
        msgCount      = rc.msgCount
      )
    }

    def toMessage(rm: RawMessage): Message = {
      val textOption: Option[RichText] =
        queries.rawRichTextElements
          .selectAll(rm.dsUuid, rm.internalId)
          .map {
            case els if els.nonEmpty => Some(RichText(components = els map toRichTextElement))
            case _                   => None
          }
          .transact(txctr)
          .unsafeRunSync()
      rm.messageType match {
        case "regular" =>
          val contentOption: Option[Content] =
            queries.rawContent
              .select(rm.dsUuid, rm.internalId)
              .transact(txctr)
              .unsafeRunSync()
              .map(rc => toContent(rm.dsUuid, rc))
          Message.Regular(
            internalId             = rm.internalId,
            sourceIdOption         = rm.sourceIdOption,
            time                   = rm.time,
            editTimeOption         = rm.editTimeOption,
            fromId                 = rm.fromId,
            forwardFromNameOption  = rm.forwardFromNameOption,
            replyToMessageIdOption = rm.replyToMessageIdOption,
            textOption             = textOption,
            contentOption          = contentOption
          )
        case "service_phone_call" =>
          Message.Service.PhoneCall(
            internalId          = rm.internalId,
            sourceIdOption      = rm.sourceIdOption,
            time                = rm.time,
            fromId              = rm.fromId,
            textOption          = textOption,
            durationSecOption   = rm.durationSecOption,
            discardReasonOption = rm.discardReasonOption
          )
        case "service_pin_message" =>
          Message.Service.PinMessage(
            internalId     = rm.internalId,
            sourceIdOption = rm.sourceIdOption,
            time           = rm.time,
            fromId         = rm.fromId,
            textOption     = textOption,
            messageId      = rm.pinnedMessageIdOption.get
          )
        case "service_clear_history" =>
          Message.Service.ClearHistory(
            internalId     = rm.internalId,
            sourceIdOption = rm.sourceIdOption,
            time           = rm.time,
            fromId         = rm.fromId,
            textOption     = textOption,
          )
        case "service_group_create" =>
          Message.Service.Group.Create(
            internalId     = rm.internalId,
            sourceIdOption = rm.sourceIdOption,
            time           = rm.time,
            fromId         = rm.fromId,
            textOption     = textOption,
            title          = rm.titleOption.get,
            members        = rm.members
          )
        case "service_edit_title" =>
          Message.Service.Group.EditTitle(
            internalId     = rm.internalId,
            sourceIdOption = rm.sourceIdOption,
            time           = rm.time,
            fromId         = rm.fromId,
            textOption     = textOption,
            title          = rm.titleOption.get
          )
        case "service_edit_photo" =>
          Message.Service.Group.EditPhoto(
            internalId     = rm.internalId,
            sourceIdOption = rm.sourceIdOption,
            time           = rm.time,
            fromId         = rm.fromId,
            textOption     = textOption,
            pathOption     = rm.pathOption map (_.toFile(rm.dsUuid)),
            widthOption    = rm.widthOption,
            heightOption   = rm.heightOption
          )
        case "service_invite_group_members" =>
          Message.Service.Group.InviteMembers(
            internalId     = rm.internalId,
            sourceIdOption = rm.sourceIdOption,
            time           = rm.time,
            fromId         = rm.fromId,
            textOption     = textOption,
            members        = rm.members
          )
        case "service_group_remove_members" =>
          Message.Service.Group.RemoveMembers(
            internalId     = rm.internalId,
            sourceIdOption = rm.sourceIdOption,
            time           = rm.time,
            fromId         = rm.fromId,
            textOption     = textOption,
            members        = rm.members
          )
        case "service_group_migrate_from" =>
          Message.Service.Group.MigrateFrom(
            internalId     = rm.internalId,
            sourceIdOption = rm.sourceIdOption,
            time           = rm.time,
            fromId         = rm.fromId,
            titleOption    = rm.titleOption,
            textOption     = textOption
          )
        case "service_group_migrate_to" =>
          Message.Service.Group.MigrateTo(
            internalId     = rm.internalId,
            sourceIdOption = rm.sourceIdOption,
            time           = rm.time,
            fromId         = rm.fromId,
            textOption     = textOption
          )
      }
    }

    def toRichTextElement(r: RawRichTextElement): RichText.Element = {
      r.elementType match {
        case "plain" =>
          RichText.Plain(text = r.text)
        case "bold" =>
          RichText.Bold(text = r.text)
        case "italic" =>
          RichText.Italic(text = r.text)
        case "underline" =>
          RichText.Underline(text = r.text)
        case "strikethrough" =>
          RichText.Strikethrough(text = r.text)
        case "link" =>
          RichText.Link(text = r.text, href = r.hrefOption.get, hidden = r.hiddenOption.get)
        case "prefmt_inline" =>
          RichText.PrefmtInline(text = r.text)
        case "prefmt_block" =>
          RichText.PrefmtBlock(text = r.text, languageOption = r.languageOption)
      }
    }

    def toContent(dsUuid: UUID, rc: RawContent): Content = {
      rc.elementType match {
        case "sticker" =>
          Content.Sticker(
            pathOption          = rc.pathOption map (_.toFile(dsUuid)),
            thumbnailPathOption = rc.thumbnailPathOption map (_.toFile(dsUuid)),
            emojiOption         = rc.emojiOption,
            widthOption         = rc.widthOption,
            heightOption        = rc.heightOption
          )
        case "photo" =>
          Content.Photo(
            pathOption = rc.pathOption map (_.toFile(dsUuid)),
            width      = rc.widthOption.get,
            height     = rc.heightOption.get,
          )
        case "voice_message" =>
          Content.VoiceMsg(
            pathOption        = rc.pathOption map (_.toFile(dsUuid)),
            mimeTypeOption    = rc.mimeTypeOption,
            durationSecOption = rc.durationSecOption
          )
        case "video_message" =>
          Content.VideoMsg(
            pathOption          = rc.pathOption map (_.toFile(dsUuid)),
            thumbnailPathOption = rc.thumbnailPathOption map (_.toFile(dsUuid)),
            mimeTypeOption      = rc.mimeTypeOption,
            durationSecOption   = rc.durationSecOption,
            width               = rc.widthOption.get,
            height              = rc.heightOption.get
          )
        case "animation" =>
          Content.Animation(
            pathOption          = rc.pathOption map (_.toFile(dsUuid)),
            thumbnailPathOption = rc.thumbnailPathOption map (_.toFile(dsUuid)),
            mimeTypeOption      = rc.mimeTypeOption,
            durationSecOption   = rc.durationSecOption,
            width               = rc.widthOption.get,
            height              = rc.heightOption.get
          )
        case "file" =>
          Content.File(
            pathOption          = rc.pathOption map (_.toFile(dsUuid)),
            thumbnailPathOption = rc.thumbnailPathOption map (_.toFile(dsUuid)),
            mimeTypeOption      = rc.mimeTypeOption,
            titleOption         = rc.titleOption,
            performerOption     = rc.performerOption,
            durationSecOption   = rc.durationSecOption,
            widthOption         = rc.widthOption,
            heightOption        = rc.heightOption
          )
        case "location" =>
          Content.Location(
            lat               = rc.latOption.get,
            lon               = rc.lonOption.get,
            durationSecOption = rc.durationSecOption
          )
        case "poll" =>
          Content.Poll(
            question = rc.pollQuestionOption.get
          )
        case "shared_contact" =>
          Content.SharedContact(
            firstNameOption   = rc.firstNameOption,
            lastNameOption    = rc.lastNameOption,
            phoneNumberOption = rc.phoneNumberOption,
            vcardPathOption   = rc.vcardPathOption map (_.toFile(dsUuid))
          )
      }
    }

    def fromChat(dsRoot: File, c: Chat): RawChat = {
      RawChat(
        dsUuid        = c.dsUuid,
        id            = c.id,
        nameOption    = c.nameOption,
        tpe           = c.tpe,
        imgPathOption = c.imgPathOption map (_.toRelativePath(dsRoot)),
        msgCount      = c.msgCount
      )
    }

    def fromMessage(
        dsUuid: UUID,
        dsRoot: File,
        chatId: Long,
        msg: Message
    ): (RawMessage, Option[RawContent], Seq[RawRichTextElement]) = {
      val rawRichTextEls = msg.textOption map fromRichText getOrElse Seq.empty
      val template = RawMessage(
        dsUuid                 = dsUuid,
        chatId                 = chatId,
        internalId             = msg.internalId,
        sourceIdOption         = msg.sourceIdOption,
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

      val (rawMessage: RawMessage, rawContentOption: Option[RawContent]) = msg match {
        case msg: Message.Regular =>
          val rawContentOption = msg.contentOption map (c => fromContent(dsUuid, dsRoot, c))
          template.copy(
            messageType            = "regular",
            editTimeOption         = msg.editTimeOption,
            forwardFromNameOption  = msg.forwardFromNameOption,
            replyToMessageIdOption = msg.replyToMessageIdOption
          ) -> rawContentOption
        case msg: Message.Service.PhoneCall =>
          template.copy(
            messageType         = "service_phone_call",
            durationSecOption   = msg.durationSecOption,
            discardReasonOption = msg.discardReasonOption
          ) -> None
        case msg: Message.Service.PinMessage =>
          template.copy(
            messageType           = "service_pin_message",
            pinnedMessageIdOption = Some(msg.messageId),
          ) -> None
        case msg: Message.Service.ClearHistory =>
          template.copy(
            messageType = "service_clear_history",
          ) -> None
        case msg: Message.Service.Group.Create =>
          template.copy(
            messageType = "service_group_create",
            titleOption = Some(msg.title),
            members     = msg.members
          ) -> None
        case msg: Message.Service.Group.EditTitle =>
          template.copy(
            messageType = "service_edit_title",
            titleOption = Some(msg.title)
          ) -> None
        case msg: Message.Service.Group.EditPhoto =>
          template.copy(
            messageType  = "service_edit_photo",
            pathOption   = msg.pathOption map (_.toRelativePath(dsRoot)),
            widthOption  = msg.widthOption,
            heightOption = msg.heightOption
          ) -> None
        case msg: Message.Service.Group.InviteMembers =>
          template.copy(
            messageType = "service_invite_group_members",
            members     = msg.members
          ) -> None
        case msg: Message.Service.Group.RemoveMembers =>
          template.copy(
            messageType = "service_group_remove_members",
            members     = msg.members
          ) -> None
        case msg: Message.Service.Group.MigrateFrom =>
          template.copy(
            messageType = "service_group_migrate_from",
            titleOption = msg.titleOption
          ) -> None
        case msg: Message.Service.Group.MigrateTo =>
          template.copy(
            messageType = "service_group_migrate_to",
          ) -> None
      }
      (rawMessage, rawContentOption, rawRichTextEls)
    }

    def fromRichText(t: RichText): Seq[RawRichTextElement] = {
      val rawEls: Seq[RawRichTextElement] = t.components.map { el =>
        val template = RawRichTextElement(
          elementType    = "",
          text           = el.text,
          hrefOption     = None,
          hiddenOption   = None,
          languageOption = None
        )
        el match {
          case el: RichText.Plain         => template.copy(elementType = "plain")
          case el: RichText.Bold          => template.copy(elementType = "bold")
          case el: RichText.Italic        => template.copy(elementType = "italic")
          case el: RichText.Underline     => template.copy(elementType = "underline")
          case el: RichText.Strikethrough => template.copy(elementType = "strikethrough")
          case el: RichText.Link =>
            template.copy(
              elementType  = "link",
              hrefOption   = Some(el.href),
              hiddenOption = Some(el.hidden)
            )
          case el: RichText.PrefmtInline => template.copy(elementType = "prefmt_inline")
          case el: RichText.PrefmtBlock =>
            template.copy(
              elementType    = "prefmt_block",
              languageOption = el.languageOption
            )
        }
      }
      rawEls
    }

    def fromContent(dsUuid: UUID, dsRoot: File, c: Content): RawContent = {
      val template = RawContent(
        elementType         = "",
        pathOption          = None,
        thumbnailPathOption = None,
        emojiOption         = None,
        widthOption         = None,
        heightOption        = None,
        mimeTypeOption      = None,
        titleOption         = None,
        performerOption     = None,
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
        case c: Content.Sticker =>
          template.copy(
            elementType         = "sticker",
            pathOption          = c.pathOption map (_.toRelativePath(dsRoot)),
            thumbnailPathOption = c.thumbnailPathOption map (_.toRelativePath(dsRoot)),
            emojiOption         = c.emojiOption,
            widthOption         = c.widthOption,
            heightOption        = c.heightOption
          )
        case c: Content.Photo =>
          template.copy(
            elementType  = "photo",
            pathOption   = c.pathOption map (_.toRelativePath(dsRoot)),
            widthOption  = Some(c.width),
            heightOption = Some(c.height)
          )
        case c: Content.VoiceMsg =>
          template.copy(
            elementType       = "voice_message",
            pathOption        = c.pathOption map (_.toRelativePath(dsRoot)),
            mimeTypeOption    = c.mimeTypeOption,
            durationSecOption = c.durationSecOption
          )
        case c: Content.VideoMsg =>
          template.copy(
            elementType         = "video_message",
            pathOption          = c.pathOption map (_.toRelativePath(dsRoot)),
            thumbnailPathOption = c.thumbnailPathOption map (_.toRelativePath(dsRoot)),
            mimeTypeOption      = c.mimeTypeOption,
            durationSecOption   = c.durationSecOption,
            widthOption         = Some(c.width),
            heightOption        = Some(c.height)
          )
        case c: Content.Animation =>
          template.copy(
            elementType         = "animation",
            pathOption          = c.pathOption map (_.toRelativePath(dsRoot)),
            thumbnailPathOption = c.thumbnailPathOption map (_.toRelativePath(dsRoot)),
            mimeTypeOption      = c.mimeTypeOption,
            durationSecOption   = c.durationSecOption,
            widthOption         = Some(c.width),
            heightOption        = Some(c.height)
          )
        case c: Content.File =>
          template.copy(
            elementType         = "file",
            pathOption          = c.pathOption map (_.toRelativePath(dsRoot)),
            thumbnailPathOption = c.thumbnailPathOption map (_.toRelativePath(dsRoot)),
            mimeTypeOption      = c.mimeTypeOption,
            titleOption         = c.titleOption,
            performerOption     = c.performerOption,
            durationSecOption   = c.durationSecOption,
            widthOption         = c.widthOption,
            heightOption        = c.heightOption
          )
        case c: Content.Location =>
          template.copy(
            elementType       = "location",
            latOption         = Some(c.lat),
            lonOption         = Some(c.lon),
            durationSecOption = c.durationSecOption
          )
        case c: Content.Poll =>
          template.copy(
            elementType        = "poll",
            pollQuestionOption = Some(c.question)
          )
        case c: Content.SharedContact =>
          template.copy(
            elementType       = "shared_contact",
            firstNameOption   = c.firstNameOption,
            lastNameOption    = c.lastNameOption,
            phoneNumberOption = c.phoneNumberOption,
            vcardPathOption   = c.vcardPathOption map (_.toRelativePath(dsRoot))
          )
      }
    }
  }

  override def equals(that: Any): Boolean = that match {
    case that: H2ChatHistoryDao => this.name == that.name && that.isLoaded(this.dataPathRoot)
    case _                      => false
  }

  override def hashCode: Int = this.name.hashCode + 17 * this.dataPathRoot.hashCode

  private implicit class RichString(s: String) {
    def toFile(dsUuid: UUID): File = new File(datasetRoot(dsUuid), s).getAbsoluteFile
  }

  private implicit class RichFile(f: File) {
    def toRelativePath(rootFile: File): String = {
      val dpp = rootFile.getAbsolutePath
      val fp = f.getAbsolutePath
      assert(fp startsWith dpp, s"Expected ${fp} to start with ${dpp}")
      fp.drop(dpp.length)
    }

    @tailrec
    protected final def isChildOf(parent: File): Boolean = {
      f.getParentFile match {
        case null               => false
        case f2 if f2 == parent => true
        case f2                 => f2.isChildOf(parent)
      }
    }
  }

  private def logPerformance[R](cb: => R)(msg: (R, Long) => String): R = {
    StopWatch.measureAndCall(cb)((res, ms) => if (ms > 50) log.debug(msg(res, ms)))
  }
}

object H2ChatHistoryDao {

  type ChatId = Long

  val BackupsDir = "_backups"
  val MaxBackups = 3

  //
  // "Raw" case classes, more closely matching DB structure
  //

  case class RawChat(
      dsUuid: UUID,
      id: Long,
      nameOption: Option[String],
      tpe: ChatType,
      imgPathOption: Option[String],
      msgCount: Int
  )

  /** [[Message]] class with all the inlined fields, whose type is determined by `messageType` */
  case class RawMessage(
      dsUuid: UUID,
      chatId: Long,
      /** Not assigned (set to -1) before insert! */
      internalId: Message.InternalId,
      sourceIdOption: Option[Message.SourceId],
      messageType: String,
      time: DateTime,
      editTimeOption: Option[DateTime],
      fromId: Long,
      forwardFromNameOption: Option[String],
      replyToMessageIdOption: Option[Message.SourceId],
      titleOption: Option[String],
      members: Seq[String],
      durationSecOption: Option[Int],
      discardReasonOption: Option[String],
      pinnedMessageIdOption: Option[Message.SourceId],
      pathOption: Option[String],
      widthOption: Option[Int],
      heightOption: Option[Int]
  )

  case class RawRichTextElement(
      elementType: String,
      text: String,
      hrefOption: Option[String],
      hiddenOption: Option[Boolean],
      languageOption: Option[String]
  )

  case class RawContent(
      elementType: String,
      pathOption: Option[String],
      thumbnailPathOption: Option[String],
      emojiOption: Option[String],
      widthOption: Option[Int],
      heightOption: Option[Int],
      mimeTypeOption: Option[String],
      titleOption: Option[String],
      performerOption: Option[String],
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

  implicit protected val msgInternalIdMeta: Meta[Message.InternalId] =
    implicitly[Meta[Long]].asInstanceOf[Meta[Message.InternalId]]

  implicit protected val msgSourceIdMeta: Meta[Message.SourceId] =
    implicitly[Meta[Long]].asInstanceOf[Meta[Message.SourceId]]

  implicit val chatTypeFromString: Get[ChatType] = Get[String].tmap {
    case x if x == ChatType.Personal.name     => ChatType.Personal
    case x if x == ChatType.PrivateGroup.name => ChatType.PrivateGroup
  }
  implicit val chatTypeToString: Put[ChatType] = Put[String].tcontramap(_.name)

  private val ArraySeparator = ";;;"
  implicit val stringSeqFromString: Get[Seq[String]] = Get[String].tmap(s => s.split(ArraySeparator))
  implicit val stringSeqToString:   Put[Seq[String]] = Put[String].tcontramap(ss => ss.mkString(ArraySeparator))
}
