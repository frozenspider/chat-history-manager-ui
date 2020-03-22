package org.fs.chm.utility

import org.fs.chm.dao._

object EntityUtils {
  def groupById[E <: WithId](cs: Seq[E]): Map[Long, E] = {
    cs groupBy (_.id) map { case (k, v) => (k, v.head) }
  }

  def getOrUnnamed(so: Option[String]): String =
    so getOrElse ChatHistoryDao.Unnamed
}
