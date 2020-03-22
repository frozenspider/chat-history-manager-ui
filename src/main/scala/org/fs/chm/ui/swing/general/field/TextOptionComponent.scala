package org.fs.chm.ui.swing.general.field

import scala.swing.TextArea

import org.fs.chm.ui.swing.general.SwingUtils._

class TextOptionComponent(
    initialValue: Option[String],
    override val mutable: Boolean
) extends ValueComponent[Option[String]] {

  def this(mutable: Boolean) = this(None, mutable)

  private val ta = new TextArea

  {
    import scala.swing.BorderPanel.Position._
    ta.text     = initialValue getOrElse ""
    ta.fontSize = 15

    if (!mutable) {
      ta.editable   = false
      ta.background = null
      ta.border     = null
    }

    layout(ta) = Center
  }

  override def value_=(v: Option[String]): Unit =
    ta.text = v getOrElse ""

  override def value: Option[String] = {
    val v = ta.text.trim
    if (v.isEmpty) None else Some(v)
  }
}
