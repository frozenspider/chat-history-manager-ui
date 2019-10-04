package org.fs.chm.dao

import com.github.nscala_time.time.Imports._

trait ChatHistoryDao {
  def contacts: Seq[Contact]
}

case class Contact(
    /** Not guaranteed to be unique -- or even meaningful*/
    id: Int,
    firstNameOption: Option[String],
    lastNameOption: Option[String],
    phoneNumberOption: Option[String],
    lastSeenDateOption: Option[DateTime]
) {
  lazy val prettyName: String = {
    Seq(firstNameOption getOrElse "", lastNameOption getOrElse "").mkString(" ").trim
  }
}
