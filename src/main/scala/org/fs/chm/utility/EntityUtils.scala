package org.fs.chm.utility

import java.io.{ File => JFile }

import org.fs.chm.dao._
import com.github.nscala_time.time.Imports._

object EntityUtils {
  private val startOfTime = new DateTime(0L)

  def groupById[E <: WithId](cs: Seq[E]): Map[Long, E] = {
    cs groupBy (_.id) map { case (k, v) => (k, v.head) }
  }

  def getOrUnnamed(so: Option[String]): String =
    so getOrElse ChatHistoryDao.Unnamed

  def latest(timeOptions: Option[DateTime]*): Option[DateTime] = {
    timeOptions.maxBy(_ getOrElse startOfTime)
  }

  private implicit class RichString(s: String) {
    def toFile(datasetRoot: JFile): JFile = new JFile(datasetRoot, s.replace('\\', '/')).getAbsoluteFile
  }
}
