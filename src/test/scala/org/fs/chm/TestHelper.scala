package org.fs.chm

import java.io.File

import com.github.nscala_time.time.Imports._
import org.fs.chm.dao.Entities._
import org.fs.chm.protobuf.Message
import org.fs.chm.utility.LangUtils._
import org.joda.time.format.DateTimeFormatter

trait TestHelper {
  val resourcesFolder: File  = new File("src/test/resources")
  val dtf: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")

  def dt(s: String): DateTime = {
    DateTime.parse(s, dtf)
  }

  implicit object MessageSeqPracticallyEquals extends PracticallyEquals[(Seq[Message], DatasetRoot)] {
    override def practicallyEquals(v1: (Seq[Message], DatasetRoot), v2: (Seq[Message], DatasetRoot)): Boolean =
      v1._1.size == v2._1.size && (v1._1 zip v2._1).forall { case (m1, m2) => (m1, v1._2) =~= (m2, v2._2) }
  }

  implicit object MessageIndexedSeqPracticallyEquals extends PracticallyEquals[(IndexedSeq[Message], DatasetRoot)] {
    override def practicallyEquals(v1: (IndexedSeq[Message], DatasetRoot), v2: (IndexedSeq[Message], DatasetRoot)): Boolean =
      v1._1.size == v2._1.size && (v1._1 zip v2._1).forall { case (m1, m2) => (m1, v1._2) =~= (m2, v2._2) }
  }
}
