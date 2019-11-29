package org.fs.chm.ui.swing.audio

import java.awt.Component
import java.awt.Desktop
import java.io.File
import java.net.URL

import javax.swing.text.ComponentView
import javax.swing.text.Element
import javax.swing.text.html.HTML

/** View for {@code <audio>} tag */
class AudioView(el: Element, desktopOption: Option[Desktop]) extends ComponentView(el) {

  private val filesWithMimeTypes: Seq[(File, Option[String])] = {
    val sources = for {
      i      <- 0 until el.getElementCount
      soucre = el.getElement(i)
      if soucre.getName == "source"
    } yield soucre
    sources map { source =>
      val src = source.getAttributes.getAttribute(HTML.Attribute.SRC).asInstanceOf[String]
      require(src != null, "Audio source should be specified")
      val srcUrl = new URL(src)
      require(srcUrl.getProtocol == "file", "AudioView is made to support local clips only!")
      val typeAttr = el.getAttributes.getAttribute(HTML.Attribute.TYPE)
      val tpe = typeAttr match {
        case s: String if !s.isEmpty => Some(s)
        case _                       => None
      }
      (new File(srcUrl.getFile), tpe)
    }
  }

  private val durationAttrValueOption = Option(el.getAttributes.getAttribute("duration")) match {
    case Some(s: String) if !s.isEmpty && s.forall(_.isDigit) => Some(s.toInt)
    case _                                                    => None
  }

  override def createComponent(): Component =
    (new AudioMessageComponent(filesWithMimeTypes, durationAttrValueOption, desktopOption)).peer

  override def getMinimumSpan(axis: Int): Float = getPreferredSpan(axis)

  override def getMaximumSpan(axis: Int): Float = getPreferredSpan(axis)
}
