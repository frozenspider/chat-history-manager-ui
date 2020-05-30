package org.fs.chm.utility

import java.io.File
import java.nio.file.Files

import scala.annotation.tailrec

import org.slf4s.Logging

object IoUtils extends Logging {
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

  /**
   * Given a files map, copy key files to value files, not overwriting them.
   * @return `(NotFound, AlreadyExist)`
   */
  def copyAll(filesMap: Map[File, File]): (IndexedSeq[File], IndexedSeq[File]) = {
    log.info(s"Copying ${filesMap.size} files")
    var notFound      = IndexedSeq.empty[File]
    var alreadyExists = IndexedSeq.empty[File]
    filesMap.values.toSet.map((f: File) => f.getParentFile).foreach(_.mkdirs())
    for ((from, to) <- filesMap.par) {
      if (from.exists()) {
        if (!to.exists()) {
          Files.copy(from.toPath, to.toPath)
        } else {
          alreadyExists = alreadyExists :+ to
        }
      } else {
        notFound = notFound :+ from
      }
    }
    (notFound, alreadyExists)
  }

  implicit class RichFileOption(fo1: Option[File]) {
    def =~=(fo2: Option[File]): Boolean = fo1 match {
      case None     => fo1.isEmpty
      case Some(f1) => fo2.nonEmpty && f1 =~= fo2.get
    }
  }

  implicit class RichFile(f1: File) {
    @tailrec
    final def existingDir: File = {
      if (f1.exists && f1.isDirectory) f1
      else f1.getParentFile.existingDir
    }

    def bytes: Array[Byte] =
      Files.readAllBytes(f1.toPath)

    def =~=(f2: File): Boolean = {
      if (!f1.exists()) {
        !f2.exists()
      } else {
        f2.exists() && (f1.bytes sameElements f2.bytes)
      }
    }
  }

  implicit class RichFileSeq(seq1: Seq[File]) {
    def =~=(seq2: Seq[File]): Boolean =
      seq1.size == seq2.size && (seq1 zip seq2).forall { case (f1, f2) => f1 =~= f2 }
  }
}
