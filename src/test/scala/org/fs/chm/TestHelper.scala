package org.fs.chm

import java.io.File

import com.github.nscala_time.time.Imports._
import org.fs.chm.dao.Entities._
import org.fs.chm.protobuf.Message
import org.fs.chm.utility.LangUtils._
import org.joda.time.format.DateTimeFormatter
import org.scalatest.TestSuite

trait TestHelper { self: TestSuite =>
  val resourcesFolder: File  = new File("src/test/resources")
  val dtf: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")

  def dt(s: String): DateTime = {
    DateTime.parse(s, dtf)
  }

  def assertFiles(expectedFiles: Iterable[File], actualFiles: Iterable[File], canBeMissing: Boolean): Unit = {
    def withFetchedContent(f: File): (File, Array[Byte]) = {
      (f, if (canBeMissing && !f.exists) Array.empty else f.bytes)
    }

    // Sorting by content.mkString is not particularly smart or efficient, but it does its job
    val expectedFilesWithContent = expectedFiles.toSeq map (withFetchedContent) sortBy (_._2.mkString)
    val actualFilesWithContent   = actualFiles  .toSeq map (withFetchedContent) sortBy (_._2.mkString)

    assert(actualFilesWithContent.size === expectedFilesWithContent.size)

    (expectedFilesWithContent zip actualFilesWithContent).foreach {
      case ((src, srcBytes), (dst, dstBytes)) =>
        assert(src.exists, s"File ${src} (source) isn't found! Bug in test?")
        assert(dst.exists, s"File ${dst} wasn't copied from ${src}!")
        assert(!srcBytes.isEmpty, s"Source file ${src} was empty! Bug in test?")
        val contentEquals = srcBytes === dstBytes
        assert(contentEquals, s"Content of ${dst} didn't match its source ${src}!")
    }

    assert(actualFilesWithContent.map(_._1) =~= expectedFilesWithContent.map(_._1))
  }

  implicit object MessageSeqPracticallyEquals extends PracticallyEquals[(Seq[Message], DatasetRoot, ChatWithDetails)] {
    override def practicallyEquals(v1: (Seq[Message], DatasetRoot, ChatWithDetails),
                                   v2: (Seq[Message], DatasetRoot, ChatWithDetails)): Boolean =
      v1._1.size == v2._1.size && (v1._1 zip v2._1).forall { case (m1, m2) => (m1, v1._2, v1._3) =~= (m2, v2._2, v2._3) }
  }

  implicit object MessageIndexedSeqPracticallyEquals extends PracticallyEquals[(IndexedSeq[Message], DatasetRoot, ChatWithDetails)] {
    override def practicallyEquals(v1: (IndexedSeq[Message], DatasetRoot, ChatWithDetails),
                                   v2: (IndexedSeq[Message], DatasetRoot, ChatWithDetails)): Boolean =
      v1._1.size == v2._1.size && (v1._1 zip v2._1).forall { case (m1, m2) => (m1, v1._2, v1._3) =~= (m2, v2._2, v2._3) }
  }
}
