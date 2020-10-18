package org.fs.chm.utility

/** Very simple utility class for dealing with command-line args */
object CliUtils {

  /** Search for a flag (prefixed with `--` and return its value, if any. */
  def parse(args: Array[String], flag: String, hasValue: Boolean): Option[String] = {
    val idx = args.indexOf("--" + flag)
    if (idx == -1) {
      None
    } else if (!hasValue) {
      Some("")
    } else if ((idx + 1) >= args.length) {
      throw new IllegalArgumentException(s"Flag $flag should have a value")
    } else {
      Some(args(idx + 1))
    }
  }
}
