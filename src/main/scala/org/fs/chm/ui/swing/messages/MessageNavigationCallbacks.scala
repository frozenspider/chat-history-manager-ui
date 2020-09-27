package org.fs.chm.ui.swing.messages

import org.joda.time.DateTime

/** Callbacks for navigating around messages history */
trait MessageNavigationCallbacks {

  def navigateToBeginning(): Unit

  def navigateToEnd(): Unit

  /**
   * Go to a first message at the given date.
   * If none found, go to the first one after.
   * If none found again, go to the last message.
   */
  def navigateToDate(date: DateTime): Unit
}
