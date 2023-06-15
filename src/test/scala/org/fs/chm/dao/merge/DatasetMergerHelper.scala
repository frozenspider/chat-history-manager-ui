package org.fs.chm.dao.merge

import org.fs.chm.dao.Entities._
import org.fs.chm.protobuf.Message
import org.fs.chm.protobuf.User
import org.fs.chm.utility.TestUtils._

object DatasetMergerHelper {

  val maxId     = (DatasetMerger.BatchSize * 3)
  val maxUserId = 3
  def rndUserId = 1 + rnd.nextInt(maxUserId)

  def changedMessages(msgs: Seq[Message], idCondition: Long => Boolean): Seq[Message] = {
    msgs.collect {
      case m if m.typed.isRegular && idCondition(m.sourceId.get) =>
        val text2 = Seq(RichText.makePlain("Different message " + m.sourceId.getOrElse("<no src id>")))
        m.copy(
          text = text2,
          searchableString = Some(makeSearchableString(text2, m.typed))
        )
      case m =>
        m
    }
  }

  def changedUsers(users: Seq[User], idCondition: Long => Boolean): Seq[User] = {
    users.collect {
      case u if idCondition(u.id) =>
        u.copy(
          firstNameOption   = Some("AnotherUserFN"),
          lastNameOption    = Some("AnotherUserLN"),
          usernameOption    = Some("AnotherUserUN"),
          phoneNumberOption = Some(123000 + u.id.toInt).map(_.toString)
        )
      case u =>
        u
    }
  }
}
