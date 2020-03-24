package org.fs.chm.ui.swing.general.field

import scala.swing._

/**
 * Component for rendering (and possibly editing) a value of the given type
 */
abstract class ValueComponent[T] extends BorderPanel {
  def mutable: Boolean
  def value: T
  def value_=(v: T): Unit

  /** Can e.g. be used for styling, if necessary */
  def innerComponent: Component
}
