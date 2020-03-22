package org.fs.chm.ui.swing.general.field

import scala.swing.TextField

import org.fs.chm.ui.swing.general.SwingUtils._

class TextOptionComponent(
    initialValue: Option[String],
    override val mutable: Boolean
) extends ValueComponent[Option[String]] {

  def this(mutable: Boolean) = this(None, mutable)

  private val tc = new TextField

  {
    import scala.swing.BorderPanel.Position._
    tc.text     = initialValue getOrElse ""
    tc.fontSize = 15

    if (!mutable) {
      tc.editable   = false
      tc.background = null
      tc.border     = null
    }

    layout(tc) = Center
  }

  override def value_=(v: Option[String]): Unit =
    tc.text = v getOrElse ""

  override def value: Option[String] = {
    val v = tc.text.trim
    if (v.isEmpty) None else Some(v)
  }
}
