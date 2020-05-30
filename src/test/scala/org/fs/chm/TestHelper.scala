package org.fs.chm

import java.io.File
import java.nio.file.Files

import com.github.nscala_time.time.Imports._

trait TestHelper {
  val resourcesFolder = new File("src/test/resources")
  val dtf             = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")

  def dt(s: String): DateTime = {
    DateTime.parse(s, dtf)
  }

  def bytesOf(f: File): Array[Byte] = Files.readAllBytes(f.toPath)
}
