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

  def unexpectedCase(clue: Any): Nothing = {
    throw new IllegalStateException(s"Unexpected case! $clue")
  }

  implicit class RichString(s: String) {
    def toOption: Option[String] =
      if (s.isEmpty) None else Some(s)

    def toFile(datasetRoot: JFile): JFile =
      new JFile(datasetRoot, s.replace('\\', '/')).getAbsoluteFile
  }

  implicit class RichJFile(f: JFile) {
    @tailrec
    final def nearestExistingDir: JFile = {
      if (f.exists && f.isDirectory) f
      else f.getParentFile.nearestExistingDir
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
}
