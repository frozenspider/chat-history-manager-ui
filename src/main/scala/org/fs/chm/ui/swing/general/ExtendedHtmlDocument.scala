package org.fs.chm.ui.swing.general

import java.net.URL

import javax.swing.text.Document
import javax.swing.text.html.HTML
import javax.swing.text.html.HTMLDocument
import javax.swing.text.html.HTMLEditorKit
import javax.swing.text.html.StyleSheet

class ExtendedHtmlDocument(ss: StyleSheet) extends HTMLDocument(ss) {

  override def getReader(pos: Int): HTMLEditorKit.ParserCallback = {
    getReader(pos, 0, 0, null)
  }

  override def getReader(
      pos: Int,
      popDepth: Int,
      pushDepth: Int,
      insertTag: HTML.Tag
  ): HTMLEditorKit.ParserCallback = {
    val desc = getProperty(Document.StreamDescriptionProperty)
    desc match {
      case desc: URL => setBase(desc)
      case _         => // NOOP
    }
    new ExtendedHtmlReader(pos, popDepth, pushDepth, insertTag);
  }

  // There's one more non-overridable version of getReader but looks like it's unused.

  /**
   * HTMLReader extended to support custom tags.
   */
  class ExtendedHtmlReader(
      offset: Int,
      popDepth: Int,
      pushDepth: Int,
      insertTag: HTML.Tag
  ) extends HTMLReader(offset, popDepth, pushDepth, insertTag) {
    {
      val ba = new BlockAction()
      registerTag(new HTML.UnknownTag("audio"), ba)
      registerTag(new HTML.UnknownTag("source"), ba)
    }
  }
}
