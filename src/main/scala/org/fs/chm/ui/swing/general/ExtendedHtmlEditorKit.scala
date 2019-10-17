package org.fs.chm.ui.swing.general

import javax.swing.text.AbstractDocument
import javax.swing.text.Element
import javax.swing.text.StyleConstants
import javax.swing.text.View
import javax.swing.text.ViewFactory
import javax.swing.text.html.HTML
import javax.swing.text.html.HTMLEditorKit

class ExtendedHtmlEditorKit extends HTMLEditorKit {

  val goodBreakWeight: Int = 100

  // Source: http://java-sl.com/tip_html_letter_wrap.html
  override lazy val getViewFactory: ViewFactory = {
    new HTMLEditorKit.HTMLFactory() {
      override def create(el: Element): View = {
        val attrs         = el.getAttributes
        val tagNameOption = Option(attrs.getAttribute(StyleConstants.NameAttribute))
        tagNameOption match {
          case Some(HTML.Tag.IMG) => new ExtendedImageView(el)
          case _                  => super.create(el)
        }
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
    }
  }
}
