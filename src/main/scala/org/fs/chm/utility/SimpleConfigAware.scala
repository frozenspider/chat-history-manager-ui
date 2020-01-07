package org.fs.chm.utility

import java.io.File
import java.io.FileOutputStream
import java.io.OutputStreamWriter

import scala.io.Codec

import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigParseOptions

trait SimpleConfigAware {
  object config {
    def contains(key: String): Boolean =
      SimpleConfigAware.config.contains(key)

    def get(key: String): Option[String] =
      SimpleConfigAware.config.get(key)

    def apply(key: String): String =
      SimpleConfigAware.config(key)

    def update(key: String, value: String) = SimpleConfigAware.Lock.synchronized {
      SimpleConfigAware.config = SimpleConfigAware.config.updated(key, value)
    }
  }
}

object SimpleConfigAware {
  private val FileName = "application.conf"
  private val Lock     = new Object

  private var _config: Map[String, String] = Lock.synchronized {
    if (!new File(FileName).exists()) {
      saveConfigFile(FileName, Map.empty)
    }
    loadConfigFile(FileName)
  }

  private[SimpleConfigAware] def config: Map[String, String] = Lock.synchronized {
    _config
  }

  private[SimpleConfigAware] def config_=(cfg: Map[String, String]): Unit = Lock.synchronized {
    _config = cfg
    saveConfigFile(FileName, _config)
  }

  private def loadConfigFile(fileName: String): Map[String, String] = {
    import scala.collection.JavaConverters._
    val config =
      ConfigFactory.parseFileAnySyntax(new File(fileName), ConfigParseOptions.defaults.setAllowMissing(false))
    val configContent = config.root.unwrapped.asScala.toMap map {
      case (k, v: String) => (k -> v)
      case etc            => throw new MatchError(etc) // To suppress warning
    }
    configContent
  }

  private def saveConfigFile(fileName: String, data: Map[String, String]): Unit = {
    val file = new File(fileName)
    val content = data map {
      case (k, v) => s"""  "$k" : "${v.replace("\\", "\\\\")}""""
    } mkString ("{\n", "\n", "\n}")
    writeTextFile(file, content)
  }

  private def writeTextFile(f: File, content: String): Unit = {
    val osw = new OutputStreamWriter(new FileOutputStream(f), Codec.UTF8.charSet)
    try {
      osw.write(content)
      osw.flush()
    } finally {
      osw.close
    }
  }
}
