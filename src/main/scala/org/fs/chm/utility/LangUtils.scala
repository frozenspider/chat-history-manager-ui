package org.fs.chm.utility

import java.io.{File => JFile}
import java.nio.file.Files

import scala.annotation.tailrec

import com.github.nscala_time.time.Imports._

object LangUtils extends Logging {
  private val startOfTime = new DateTime(0L)

  def latest(timeOptions: Option[DateTime]*): Option[DateTime] = {
    timeOptions.maxBy(_ getOrElse startOfTime)
  }

  def tryWith[A <: AutoCloseable, B](alloc: => A)(code: A => B): B = {
    val a = alloc
    try {
      code(a)
    } finally {
      closeWithoutThrowing(a);
    }
  }

  def closeWithoutThrowing(c: AutoCloseable): Unit = {
    try {
      c.close()
    } catch {
      case ex: Throwable => log.warn("Failed to close resource", ex)
    }
  }

  implicit class RichString(s: String) {
    def toOption: Option[String] =
      if (s.isEmpty) None else Some(s)

    def toFile(datasetRoot: JFile): JFile =
      new JFile(datasetRoot, s.replace('\\', '/')).getAbsoluteFile

    def makeRelativePath: String = {
      require(!s.contains(":/") &&
        !s.contains(":\\") &&
        !s.startsWith("/home/") &&
        !s.startsWith("/Users/") &&
        !s.startsWith("/var/"),
        s"$s is not a relative path!")
      (if (s.startsWith("/") || s.startsWith("\\")) {
        s.drop(1)
      } else {
        s
      }).replace('\\', '/')
    }
  }

  implicit class RichJFile(f: JFile) {
    @tailrec
    final def existingDir: JFile = {
      if (f.exists && f.isDirectory) f
      else f.getParentFile.existingDir
    }

    def bytes: Array[Byte] =
      Files.readAllBytes(f.toPath)

    def toRelativePath(rootFile: JFile): String = {
      val dpp = rootFile.getAbsolutePath
      val fp = f.getAbsolutePath
      assert(fp startsWith dpp, s"Expected ${fp} to start with ${dpp}")
      fp.drop(dpp.length)
    }

    @tailrec
    protected final def isChildOf(parent: JFile): Boolean = {
      f.getParentFile match {
        case null => false
        case f2 if f2 == parent => true
        case f2 => f2.isChildOf(parent)
      }
    }
  }

  implicit class RichDateTime(dt: DateTime) {
    def unixTimestamp: Long = {
      dt.getMillis / 1000
    }
  }

  //
  // PracticallyEquals
  //

  trait PracticallyEquals[T] {
    def practicallyEquals(v1: T, v2: T): Boolean
  }

  implicit class PracticalEqualityWrapper[T: PracticallyEquals](self: T) {
    def =~=(that: T): Boolean = {
      implicitly[PracticallyEquals[T]].practicallyEquals(self, that)
    }

    def !=~=(that: T): Boolean =
      !(this =~= that)
  }

  class OptionPracticallyEquals[T: PracticallyEquals] extends PracticallyEquals[Option[T]] {
    override def practicallyEquals(thisOption: Option[T], thatOption: Option[T]): Boolean =
      (thisOption, thatOption) match {
        case (None, None)       => true
        case (Some(a), Some(b)) => a =~= b
        case _                  => false
      }
  }

  implicit def x[T: PracticallyEquals]: PracticallyEquals[Option[T]] =
    new OptionPracticallyEquals[T]

  implicit object JFilePracticallyEquals extends PracticallyEquals[JFile] {
    override def practicallyEquals(f1: JFile, f2: JFile): Boolean = {
      if (!f1.exists()) {
        !f2.exists()
      } else {
        f2.exists() && (f1.bytes sameElements f2.bytes)
      }
    }
  }

  implicit object JFileSeqPracticallyEquals extends PracticallyEquals[Seq[JFile]] {
    override def practicallyEquals(seq1: Seq[JFile], seq2: Seq[JFile]): Boolean =
      seq1.size == seq2.size && (seq1 zip seq2).forall { case (f1, f2) => f1 =~= f2 }
  }
}
