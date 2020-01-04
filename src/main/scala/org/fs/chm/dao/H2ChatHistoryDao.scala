package org.fs.chm.dao

import java.io.File
import java.util.UUID

import cats.effect._
import cats.implicits._
import com.github.nscala_time.time.Imports._
import doobie._
import doobie.implicits._
import doobie.h2.implicits._

class H2ChatHistoryDao(
    override val dataPathRoot: File,
//    /** Connection becomes managed by this Dao */
//    conn: Connection,
    txctr: Transactor.Aux[IO, Unit]
) extends ChatHistoryDao {

  import org.fs.chm.dao.H2ChatHistoryDao._

  /*
    Tables:

    users(id, first_name, ...)
    chats(id, name, ...)
    messages(id, chat_id, user_id, time, ...)

    messages_text(id, message_id, plaintext, ...)
    messages_text_element(id, text_id, ...)

    content(id, path, .........)
   */

  override def datasets: Seq[Dataset] = {
    queries.datasets.selectAll.transact(txctr).unsafeRunSync()
  }

  override def myself(dsUuid: UUID): User = {
    queries.users.selectMyself(dsUuid).transact(txctr).unsafeRunSync()
  }

  /** Contains myself as well */
  override def users(dsUuid: UUID): Seq[User] = {
    queries.users.selectAll(dsUuid).transact(txctr).unsafeRunSync()
  }

  override def chats(dsUuid: UUID): Seq[Chat] = {
    queries.chats.selectAll(dsUuid).transact(txctr).unsafeRunSync()
  }

  private lazy val interlocutorsCache: Map[UUID, Map[Chat, Seq[User]]] = {
    (for {
      ds <- datasets.par
      myself1 = myself(ds.uuid)
      users1 = users(ds.uuid)
    } yield {
      val chatInterlocutors = chats(ds.uuid).map { c =>
        val ids: Seq[Long] = queries.users.selectInterlocutorIds(c.id).transact(txctr).unsafeRunSync()
        val usersWithoutMe = users1.filter(u => u != myself1 && ids.contains(u.id))
        (c, myself1 +: usersWithoutMe.sortBy(u => (u.id, u.prettyName)))
      }.toMap
      (ds.uuid, chatInterlocutors)
    }).seq.toMap
  }

  override def interlocutors(chat: Chat): Seq[User] =
    (for {
      c1 <- interlocutorsCache.get(chat.dsUuid)
      c2 <- c1.get(chat)
    } yield c2) getOrElse Seq.empty

  override def lastMessages(chat: Chat, limit: Int): IndexedSeq[Message] = {
    val raws = queries.rawMessages.selectLastInversed(chat, limit).transact(txctr).unsafeRunSync().reverse
    raws map Raws.toMessage
  }

  override def messagesBefore(chat: Chat, msgId: Long, limit: Int): Option[IndexedSeq[Message]] = {
    messageOption(chat, msgId) map { m =>
      val raws = queries.rawMessages.selectBeforeInversed(chat, m, limit).transact(txctr).unsafeRunSync().reverse
      raws map Raws.toMessage
    }
  }

  override def messageOption(chat: Chat, id: Long): Option[Message] = {
    queries.rawMessages.selectOption(chat, id).transact(txctr).unsafeRunSync().map(Raws.toMessage)
  }

  def createTables(): Unit = {
    /*
  Tables:

  users(id, first_name, ...)
  chats(id, name, ...)
  messages(id, chat_id, user_id, time, ...)

  messages_text(id, message_id, plaintext, ...)
  messages_text_element(id, text_id, ...)

  content(id, path, .........)
     */
    val createQueries = Seq(
      sql"""
        CREATE TABLE IF NOT EXISTS datasets (
          uuid           UUID NOT NULL PRIMARY KEY,
          alias          VARCHAR(255),
          source_type    VARCHAR(255)
        );
      """,
      sql"""
        CREATE TABLE IF NOT EXISTS users (
          ds_uuid        UUID NOT NULL,
          id             BIGINT NOT NULL,
          first_name     VARCHAR(255),
          last_name      VARCHAR(255),
          username       VARCHAR(255),
          phone_number   VARCHAR(20),
          last_seen_time TIMESTAMP,
          -- Not in the entity
          is_myself      BOOLEAN NOT NULL,
          PRIMARY KEY (ds_uuid, id),
          FOREIGN KEY (ds_uuid) REFERENCES datasets (uuid)
        );
      """,
      sql"""
        CREATE TABLE IF NOT EXISTS chats (
          ds_uuid  UUID NOT NULL,
          id       BIGINT NOT NULL,
          name     VARCHAR(255) NOT NULL,
          type     VARCHAR(255) NOT NULL,
          img_path VARCHAR(4095),
          PRIMARY KEY (ds_uuid, id),
          FOREIGN KEY (ds_uuid) REFERENCES datasets (uuid)
        );
      """,
      sql"""
        CREATE TABLE IF NOT EXISTS messages (
          ds_uuid             UUID NOT NULL,
          chat_id             BIGINT NOT NULL,
          id                  BIGINT NOT NULL,
          message_type        VARCHAR(255) NOT NULL,
          time                TIMESTAMP NOT NULL,
          edit_time           TIMESTAMP,
          from_name           VARCHAR(255),
          from_id             BIGINT NOT NULL,
          forward_from_name   VARCHAR(255),
          reply_to_message_id BIGINT, -- not a reference
          members             VARCHAR, -- serialized
          duration_sec        INT,
          discard_reason      VARCHAR(255),
          pinned_message_id   BIGINT,
          group_title         VARCHAR(255),
          path                VARCHAR(4095),
          width               INT,
          height              INT,
          PRIMARY KEY (ds_uuid, chat_id, id),
          FOREIGN KEY (ds_uuid) REFERENCES datasets (uuid),
          FOREIGN KEY (chat_id) REFERENCES chats (id),
          FOREIGN KEY (from_id) REFERENCES users (id)
        );
      """,
      sql"""
        CREATE TABLE IF NOT EXISTS messages_text (
          id                  IDENTITY NOT NULL,
          ds_uuid             UUID NOT NULL,
          chat_id             BIGINT NOT NULL,
          message_id          BIGINT NOT NULL,
          plaintext           VARCHAR NOT NULL,
          PRIMARY KEY (id),
          FOREIGN KEY (ds_uuid) REFERENCES datasets (uuid),
          FOREIGN KEY (chat_id) REFERENCES chats (id),
          FOREIGN KEY (message_id) REFERENCES messages (id)
        );
      """,
      sql"""
        CREATE TABLE IF NOT EXISTS messages_text_element (
          id                  IDENTITY NOT NULL,
          text_id             BIGINT NOT NULL,
          element_type        VARCHAR(255) NOT NULL,
          text                VARCHAR,
          href                VARCHAR(4095),
          hidden              BOOLEAN,
          language            VARCHAR(4095),
          PRIMARY KEY (id),
          FOREIGN KEY (text_id) REFERENCES messages_text (id)
        );
      """
    ) map (_.update.run)
    createQueries.reduce((a, b) => a flatMap (_ => b)).transact(txctr).unsafeRunSync()
  }

  def insertAll(loader: ChatHistoryDao): Unit = {
    for (ds <- loader.datasets) {
      var query: ConnectionIO[_] = queries.datasets.insert(ds)

      val myself = loader.myself(ds.uuid)
      for (u <- loader.users(ds.uuid)) {
        query = query flatMap (_ => queries.users.insert(u, u == myself))
      }

      for (c <- loader.chats(ds.uuid)) {
        query = query flatMap (_ => queries.chats.insert(c))

        // TODO: Iterate over smaller subset?
        for (m <- loader.lastMessages(c, c.msgCount + 1)) {
          val (rm, rcOption, rrtOption) = Raws.fromMessage(ds.uuid, c.id, m)
          query = query flatMap (_ => queries.rawMessages.insert(rm))

          for ((rrt, rrtEls) <- rrtOption) {
            val rrtQuery = for {
              rrtId <- queries.rawRichText.insert(rrt, ds.uuid, c.id, m.id)
              combined <- (for (rrtEl <- rrtEls) yield {
                queries.rawRichTextElements.insert(rrtEl, rrtId)
              }) reduce ((a, b) => a flatMap (_ => b))
            } yield combined
            query = query flatMap (_ => rrtQuery)
          }
        }
      }

      query.transact(txctr).unsafeRunSync()
    }
  }

  override def close(): Unit = {
//    IoUtils.closeWithoutThrowing(conn)
  }

  private object queries {
    object datasets {
      private val colsFr = fr"uuid, alias, source_type"

      lazy val selectAll =
        (fr"SELECT" ++ colsFr ++ fr"FROM datasets").query[Dataset].to[Seq]

      def insert(ds: Dataset) =
        (fr"INSERT INTO datasets (" ++ colsFr ++ fr") VALUES (${ds.uuid}, ${ds.alias}, ${ds.sourceType})").update.run
    }

    object users {
      private val colsFr = fr"ds_uuid, id, first_name, last_name, username, phone_number, last_seen_time"
      private val selectAllFr = fr"SELECT" ++ colsFr ++ fr"FROM users"
      private def selectFr(dsUuid: UUID) = selectAllFr ++ fr"WHERE ds_uuid = $dsUuid"

      def selectAll(dsUuid: UUID) =
        selectFr(dsUuid).query[User].to[Seq]

      def selectMyself(dsUuid: UUID) =
        (selectFr(dsUuid) ++ fr"AND is_myself = true").query[User].unique

      def selectInterlocutorIds(chatId: Long) =
        sql"SELECT DISTINCT user_id FROM messages WHERE chat_id = $chatId".query[Long].to[Seq]

      def insert(u: User, isMyself: Boolean) =
        (fr"INSERT INTO users (" ++ colsFr ++ fr", is_myself) VALUES ("
          ++ fr"${u.dsUuid}, ${u.id}, ${u.firstNameOption}, ${u.lastNameOption}, ${u.usernameOption},"
          ++ fr"${u.phoneNumberOption}, ${u.lastSeenTimeOption}, ${isMyself}"
          ++ fr")").update.run
    }

    object chats {
      private val colsFr = fr"ds_uuid, id, name, type, img_path"
      private val msgCountFr = fr"(SELECT COUNT(*) FROM messages m WHERE m.chat_id = c.id) AS msg_count"

      def selectAll(dsUuid: UUID) =
        (fr"SELECT" ++ colsFr ++ fr"," ++ msgCountFr ++ fr"FROM chats c WHERE c.ds_uuid = $dsUuid").query[Chat].to[Seq]

      def insert(c: Chat) =
        (fr"INSERT INTO chats (" ++ colsFr ++ fr") VALUES ("
          ++ fr"${c.dsUuid}, ${c.id}, ${c.nameOption}, ${c.tpe}, ${c.imgPathOption}"
          ++ fr")").update.run
    }

    object rawMessages {
      private val colsFr = Fragment.const(s"""|ds_uuid, chat_id, id, message_type, time, edit_time, from_name,
                                              |from_id, forward_from_name, reply_to_message_id, members, duration_sec,
                                              |discard_reason, pinned_message_id, group_title,
                                              |path, width, height""".stripMargin)
      private val selectAllFr = fr"SELECT" ++ colsFr ++ fr"FROM messages"

      private def selectAllByChatFr(dsUuid: UUID, chatId: Long) =
        selectAllFr ++ fr"WHERE ds_uuid = $dsUuid AND chat_id = $chatId"

      private def orderByTimeDescLimit(limit: Int) = fr"ORDER BY time DESC LIMIT ${limit}"

      def selectOption(chat: Chat, id: Long) =
        (selectAllByChatFr(chat.dsUuid, chat.id) ++ fr"AND id = ${id}").query[RawMessage].option

      def selectLastInversed(chat: Chat, limit: Int) =
        (selectAllByChatFr(chat.dsUuid, chat.id) ++ orderByTimeDescLimit(limit)).query[RawMessage].to[IndexedSeq]

      def selectBeforeInversed(chat: Chat, m: Message, limit: Int) =
        (selectAllByChatFr(chat.dsUuid, chat.id)
          ++ fr"AND time < ${m.time} AND id < ${m.id}"
          ++ orderByTimeDescLimit(limit)).query[RawMessage].to[IndexedSeq]

      def insert(m: RawMessage) =
        (fr"INSERT INTO messages (" ++ colsFr ++ fr") VALUES ("
          ++ fr"${m.dsUuid}, ${m.chatId}, ${m.id}, ${m.messageType}, ${m.time}, ${m.editTimeOption}, ${m.fromName},"
          ++ fr"${m.fromId}, ${m.forwardFromNameOption}, ${m.replyToMessageIdOption}, ${m.members},"
          ++ fr"${m.durationSecOption}, ${m.discardReasonOption}, ${m.pinnedMessageIdOption}, ${m.groupTitleOption},"
          ++ fr"${m.pathOption}, ${m.widthOption}, ${m.heightOption}"
          ++ fr")").update.run
    }

    object rawRichText {
      def insert(rrt: RawRichText, dsUuid: UUID, chatId: Long, messageId: Long) =
        (fr"INSERT INTO messages_text(ds_uuid, chat_id, message_id, plaintext) VALUES ("
          ++ fr"${dsUuid}, ${chatId}, ${messageId}, ${rrt.plaintext}"
          ++ fr")").update.withUniqueGeneratedKeys[Long]("id")
    }

    object rawRichTextElements {
      private val colsNoIdentityFr = fr"element_type, text, href, hidden, language"

      // TODO: Select ordered by ID

      def insert(rrte: RawRichTextElement, textId: Long) =
        (fr"INSERT INTO messages_text_element(text_id," ++ colsNoIdentityFr ++ fr") VALUES ("
          ++ fr"${textId}, ${rrte.elementType}, ${rrte.text},"
          ++ fr"${rrte.hrefOption}, ${rrte.hiddenOption}, ${rrte.languageOption}"
          ++ fr")").update.withUniqueGeneratedKeys[Long]("id")
    }
  }

  object Raws {
    def toMessage(rm: RawMessage): Message = {
      // TODO: Select inner entities
      rm.messageType match {
        case "regular" =>
          Message.Regular(
            id                     = rm.id,
            time                   = rm.time,
            editTimeOption         = rm.editTimeOption,
            fromName               = rm.fromName,
            fromId                 = rm.fromId,
            forwardFromNameOption  = rm.forwardFromNameOption,
            replyToMessageIdOption = rm.replyToMessageIdOption,
            textOption             = None, // FIXME
            contentOption          = None // FIXME
          )
      }
    }

    def fromMessage(
        dsUuid: UUID,
        chatId: Long,
        m: Message
    ): (RawMessage, Option[RawContent], Option[(RawRichText, Seq[RawRichTextElement])]) = {
      val rawRichTextOption: Option[(RawRichText, Seq[RawRichTextElement])] =
        m.textOption map { t =>
          val elements: Seq[RawRichTextElement] = t.components.map { c =>
            val template = RawRichTextElement(
              elementType    = "",
              text           = c.text,
              hrefOption     = None,
              hiddenOption   = None,
              languageOption = None
            )
            c match {
              case c: RichText.Plain  => template.copy(elementType = "plain")
              case c: RichText.Bold   => template.copy(elementType = "bold")
              case c: RichText.Italic => template.copy(elementType = "italic")
              case c: RichText.Link =>
                template.copy(
                  elementType  = "link",
                  hrefOption   = Some(c.href),
                  hiddenOption = Some(c.hidden)
                )
              case c: RichText.PrefmtInline => template.copy(elementType = "prefmt_inline")
              case c: RichText.PrefmtBlock =>
                template.copy(
                  elementType    = "prefmt_block",
                  languageOption = c.languageOption
                )
            }
          }
          (RawRichText(t.plainSearchableString), elements)
        }
      val template = RawMessage(
        dsUuid                 = dsUuid,
        chatId                 = chatId,
        id                     = m.id,
        messageType            = "",
        time                   = m.time,
        editTimeOption         = None,
        fromName               = m.fromName,
        fromId                 = m.fromId,
        forwardFromNameOption  = None,
        replyToMessageIdOption = None,
        members                = Seq.empty,
        durationSecOption      = None,
        discardReasonOption    = None,
        pinnedMessageIdOption  = None,
        groupTitleOption       = None,
        pathOption             = None,
        widthOption            = None,
        heightOption           = None,
      )

      val (rawMessage: RawMessage, rawContentOption: Option[RawContent]) = m match {
        case m: Message.Regular =>
          val rawContentOption = None // FIXME
          template.copy(
            messageType    = "regular",
            editTimeOption = m.editTimeOption
          ) -> rawContentOption
        case m: Message.Service.PhoneCall =>
          template.copy(
            messageType         = "service_phone_call",
            durationSecOption   = m.durationSecOption,
            discardReasonOption = m.discardReasonOption
          ) -> None
        case m: Message.Service.PinMessage =>
          template.copy(
            messageType           = "service_pin_message",
            pinnedMessageIdOption = Some(m.messageId),
          ) -> None
        case m: Message.Service.ClearHistory =>
          template.copy(
            messageType = "service_clear_history",
          ) -> None
        case m: Message.Service.EditPhoto =>
          template.copy(
            messageType  = "service_edit_photo",
            pathOption   = m.pathOption,
            widthOption  = m.widthOption,
            heightOption = m.heightOption
          ) -> None
        case m: Message.Service.Group.Create =>
          template.copy(
            messageType      = "service_group_create",
            groupTitleOption = Some(m.title),
            members          = m.members
          ) -> None
        case m: Message.Service.Group.InviteMembers =>
          template.copy(
            messageType = "service_invite_group_members",
            members     = m.members
          ) -> None
        case m: Message.Service.Group.RemoveMembers =>
          template.copy(
            messageType = "service_group_remove_members",
            members     = m.members
          ) -> None
      }
      (rawMessage, rawContentOption, rawRichTextOption)
    }
  }
}

object H2ChatHistoryDao {
  //
  // Additional case classes
  //

  /** [[Message]] class with all the inlined fields, whose type is determined by `messageType` */
  case class RawMessage(
      dsUuid: UUID,
      chatId: Long,
      id: Long,
      messageType: String,
      time: DateTime,
      editTimeOption: Option[DateTime],
      fromName: String,
      fromId: Long,
      forwardFromNameOption: Option[String],
      replyToMessageIdOption: Option[Long],
      members: Seq[String],
      durationSecOption: Option[Int],
      discardReasonOption: Option[String],
      pinnedMessageIdOption: Option[Long],
      groupTitleOption: Option[String],
      pathOption: Option[String],
      widthOption: Option[Int],
      heightOption: Option[Int]
  )

  case class RawRichText(
      plaintext: String
  )

  case class RawRichTextElement(
      elementType: String,
      text: String,
      hrefOption: Option[String],
      hiddenOption: Option[Boolean],
      languageOption: Option[String]
  )

  case class RawContent(
      pathOption: Option[String],
      thumbnailPathOption: Option[String],
      emojiOption: Option[String],
      widthOption: Option[Int],
      heightOption: Option[Int],
      mimeTypeOption: Option[String],
      titleOption: Option[String],
      performerOption: Option[String],
      durationSecOption: Option[Int],
      latOption: Option[BigDecimal],
      lonOption: Option[BigDecimal],
      liveDurationSecOption: Option[Int],
      pollQuestionOption: Option[String],
      firstNameOption: Option[String],
      lastNameOption: Option[String],
      phoneNumberOption: Option[String],
      vcardPathOption: Option[String]
  )

  import java.util.{ Date => JUDate }
  implicit val dateTimeFromTimestamp: Get[DateTime] = Get[JUDate].tmap(ts => new DateTime(ts.getTime))
  implicit val dateTimeToTimestamp:   Put[DateTime] = Put[JUDate].tcontramap(dt => new JUDate(dt.getMillis))

  implicit val chatTypeFromString: Get[ChatType] = Get[String].tmap {
    case x if x == ChatType.Personal.name     => ChatType.Personal
    case x if x == ChatType.PrivateGroup.name => ChatType.PrivateGroup
  }
  implicit val chatTypeToString: Put[ChatType] = Put[String].tcontramap(_.name)

  private val ArraySeparator = ";;;"
  implicit val stringSeqFromString: Get[Seq[String]] = Get[String].tmap(s => s.split(ArraySeparator))
  implicit val stringSeqToString:   Put[Seq[String]] = Put[String].tcontramap(ss => ss.mkString(ArraySeparator))
}
