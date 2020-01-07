package org.fs.chm.utility

import java.io.File

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

  implicit class RichFile(f: File) {
    @tailrec
    final def existingDir: File = {
      if (f.exists && f.isDirectory) f
      else f.getParentFile.existingDir
    }
  }
}
