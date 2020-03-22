package org.fs.chm.ui.swing.general.field

import scala.swing.TextArea

import org.fs.chm.ui.swing.general.SwingUtils._

class TextComponent(
    initialValue: String,
    override val mutable: Boolean
) extends ValueComponent[String] {

  def this(mutable: Boolean) = this("", mutable)

  private val ta = new TextArea

  {
    import scala.swing.BorderPanel.Position._
    ta.text     = initialValue
    ta.fontSize = 15

    if (!mutable) {
      ta.editable   = false
      ta.background = null
      ta.border     = null
    }

    layout(ta) = Center
  }

  override def value_=(v: String): Unit =
    ta.text = v

  override def value: String =
    ta.text.trim
}
