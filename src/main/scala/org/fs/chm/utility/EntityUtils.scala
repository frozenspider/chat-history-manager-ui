package org.fs.chm.utility

import org.fs.chm.dao.Entities._
import org.fs.chm.dao._

object EntityUtils {
  def groupById[T: WithId](cs: Seq[T]): Map[Long, T] = {
    val withId = implicitly[WithId[T]]
    cs groupBy (withId.id) map { case (k, v) => (k, v.head) }
  }

  sealed trait WithId[T] {
    def id(t: T): Long
  }

  implicit object CwmWithId extends WithId[ChatWithDetails] {
    override def id(cwm: ChatWithDetails): Long = cwm.chat.id
  }

  implicit object UserWithId extends WithId[User] {
    override def id(user: User): Long = user.id
  }

  def getOrUnnamed(so: Option[String]): String =
    so getOrElse ChatHistoryDao.Unnamed
}
