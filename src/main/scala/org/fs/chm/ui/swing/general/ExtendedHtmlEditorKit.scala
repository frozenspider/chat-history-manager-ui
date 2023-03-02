package org.fs.chm.ui.swing.general

import java.awt.Desktop
import java.io.Reader

import javax.swing.text.Document
import javax.swing.text.Element
import javax.swing.text.StyleConstants
import javax.swing.text.View
import javax.swing.text.ViewFactory
import javax.swing.text.html.HTML
import javax.swing.text.html.HTMLDocument
import javax.swing.text.html.HTMLEditorKit
import javax.swing.text.html.StyleSheet
import javax.swing.text.html.parser.AttributeList
import javax.swing.text.html.parser.ContentModel
import javax.swing.text.html.parser.DTD
import javax.swing.text.html.parser.DTDConstants
import javax.swing.text.html.parser.ParserDelegator
import org.fs.chm.ui.swing.audio.AudioView

class ExtendedHtmlEditorKit(desktopOption: Option[Desktop]) extends HTMLEditorKit {

  protected def defineCustomTags(dtd: DTD): Unit = {
    // I haven't actually read SGML/DTD specs to know exact meaning of each constants.
    // Instead, tags are modelled using values in existing elements resembling target ones.

    // <source> tag
    val sourceAttrs: AttributeList =
      new AttributeList("src", DTDConstants.CDATA, DTDConstants.REQUIRED, null, null, {
        new AttributeList("type", DTDConstants.CDATA, 0, "", null, null)
      })
    val sourceTag = dtd.defineElement("source", DTDConstants.EMPTY, false, true, null, null, null, sourceAttrs)

    // <audio> tag
    val audioAttrs: AttributeList =
      new AttributeList("duration", DTDConstants.NUMBER, 0, "-1", null, {
        new AttributeList("controls", DTDConstants.EMPTY, 0, null, null, null)
      })
    val audioContentModel: ContentModel = {
      // Modelled after <div>
      new ContentModel('*', new ContentModel(sourceTag), null)
    }
    dtd.defineElement("audio", DTDConstants.MODEL, false, false, audioContentModel, null, null, audioAttrs)
  }

  override lazy val getViewFactory: ViewFactory =
    new ExtendedViewFactory(desktopOption)

  /** Doing the same as `super.createDefaultDocument()` but returning ExtendedHtmlDocument. */
  override def createDefaultDocument(): ExtendedHtmlDocument = {
    val styles = getStyleSheet
    val ss = new StyleSheet()

    ss.addStyleSheet(styles);

    val doc = new ExtendedHtmlDocument(ss);
    doc.setParser(getParser)
    doc.setAsynchronousLoadPriority(4)
    doc.setTokenThreshold(100)
    doc
  }
}

class ExtendedViewFactory(desktopOption: Option[Desktop]) extends HTMLEditorKit.HTMLFactory {
  val goodBreakWeight: Int = 100

  override def create(el: Element): View = {
    val attrs         = el.getAttributes
    val tagNameOption = Option(attrs.getAttribute(StyleConstants.NameAttribute))
    val isEndTag      = Option(attrs.getAttribute(HTML.Attribute.ENDTAG)).contains("true")
    tagNameOption match {
      case Some(HTML.Tag.IMG)     => new ExtendedImageView(el)
      case Some(HtmlTag("audio")) => new AudioView(el, desktopOption)
      case _                      => super.create(el)
    }
    // Source: http://java-sl.com/tip_html_letter_wrap.html
    /*super.create(e) match {
      case v: InlineView =>
        new InlineView(e) {
          override def getBreakWeight(axis: Int, pos: Float, len: Float): Int =
            goodBreakWeight

          override def breakView(axis: Int, p0: Int, pos: Float, len: Float): View = {
            if (axis == View.X_AXIS) {
              checkPainter()
              val p1 = getGlyphPainter().getBoundedPosition(this, p0, pos, len)
              if (p0 == getStartOffset() && p1 == getEndOffset()) {
                this
              } else {
                createFragment(p0, p1)
              }
            }
            this
          }
        }

      case v: ParagraphView =>
        new ParagraphView(e) {
          override def calculateMinorAxisRequirements(axis: Int, r: SizeRequirements): SizeRequirements = {
            val r2   = Option(r) getOrElse (new SizeRequirements)
            val pref = layoutPool.getPreferredSpan(axis)
            val min  = layoutPool.getMinimumSpan(axis)
            // Don't include insets, Box.getXXXSpan will include them.
            r2.minimum = min.toInt
            r2.preferred = r2.minimum max pref.toInt
            r2.maximum = Integer.MAX_VALUE
            r2.alignment = 0.5f
            r2
          }
        }

      case v => v
    }*/
  }

  /** Syntactic sugar for pattern matching */
  protected object HtmlTag {
    def unapply(arg: HTML.Tag): Option[String] = Some(arg.toString)
  }
}
