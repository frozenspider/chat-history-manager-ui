package org.fs.chm

import java.io.File

import com.github.nscala_time.time.Imports._
import org.fs.chm.dao.Message

trait TestHelper {
  val resourcesFolder = new File("src/test/resources")
  val dtf             = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")

  def dt(s: String): DateTime = {
    DateTime.parse(s, dtf)
  }

  implicit class RichMessageIterable(seq1: Seq[Message]) {
    def =~=(seq2: Seq[Message]): Boolean =
      seq1.size == seq2.size && (seq1 zip seq2).forall { case (m1, m2) => m1 =~= m2 }
  }
}
