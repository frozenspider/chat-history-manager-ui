package org.fs.chm.ui.swing.general.field

import scala.swing.Component
import scala.swing.TextArea
import scala.swing.TextField

import org.fs.chm.ui.swing.general.SwingUtils._

class TextOptionComponent(
    initialValue: Option[String],
    multilineCsv: Boolean,
    override val mutable: Boolean
) extends ValueComponent[Option[String]] {

  def this(multilineCsv: Boolean,mutable: Boolean) = this(None, multilineCsv, mutable)

  private val tc = if (multilineCsv) new TextArea() else new TextField()

  {
    import scala.swing.BorderPanel.Position._
    tc.text     = csvToMultilineIfNeeded(initialValue getOrElse "")
    tc.fontSize = 15

    if (!mutable) {
      tc.editable   = false
      tc.background = null
      tc.border     = null
    }

    layout(tc) = Center
  }

  override def value: Option[String] = {
    val v = multilineToCsvIfNeeded(tc.text.trim)
    if (v.isEmpty) None else Some(v)
  }

  override def value_=(v: Option[String]): Unit =
    tc.text = csvToMultilineIfNeeded(v getOrElse "")

  override def innerComponent: Component = tc

  private def csvToMultilineIfNeeded(v: String): String = if (multilineCsv) csvToMultiline(v) else v

  private def multilineToCsvIfNeeded(v: String): String = if (multilineCsv) multilineToCsv(v) else v

  private def csvToMultiline(v: String): String = v.split(",").mkString("\n")

  private def multilineToCsv(v: String): String = v.split("\n").mkString(",")
}
