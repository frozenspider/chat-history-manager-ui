package org.fs.chm.ui.swing.general.field

import scala.swing.TextArea

import org.fs.chm.ui.swing.general.SwingUtils._

class TextComponent(
    initialValue: String,
    override val mutable: Boolean,
) extends ValueComponent[String] {

  def this(mutable: Boolean) = this("", mutable)

  private val tc = new TextArea

  {
    import scala.swing.BorderPanel.Position._
    tc.text     = initialValue
    tc.fontSize = 15

    if (!mutable) {
      tc.editable   = false
      tc.background = null
      tc.border     = null
    }

    layout(tc) = Center
  }

  override def value: String =
    tc.text.trim

  override def value_=(v: String): Unit =
    tc.text = v

  override def innerComponent: TextArea = tc
}
