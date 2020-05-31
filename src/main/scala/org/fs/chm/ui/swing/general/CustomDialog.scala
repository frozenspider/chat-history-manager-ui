package org.fs.chm.ui.swing.general

import scala.swing._
import scala.swing.event.ButtonClicked
import org.fs.chm.ui.swing.general.SwingUtils._

import javax.swing.WindowConstants

/**
 * Custom dialog that has an OK button which performs validation of user choices.
 * OK button will close the dialog once user selection passes validation.
 *
 * Usage:
 *
 * ```
 * val dialog = new MyCustomDialog()
 * dialog.visible = true
 * dialog.selection foreach (result => ???)
 * ```
 */
abstract class CustomDialog[A] extends Dialog {

  private var _selected: Option[A] = None

  protected def okButtonText = "OK"

  {
    val okBtn = new Button(okButtonText)
    val component = dialogComponent()

    contents = new BorderPanel {
      import scala.swing.BorderPanel.Position._
      layout(component)            = Center
      layout(new FlowPanel(okBtn)) = South
    }

    modal         = true
    defaultButton = okBtn

    peer.setLocationRelativeTo(null)
    peer.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE)

    listenTo(okBtn)
    reactions += {
      case ButtonClicked(`okBtn`) =>
        _selected = validateChoices()
        if (_selected.isDefined) {
          dispose()
        }
    }
  }

  def selection: Option[A] = _selected

  /** Construct a component to be placed in a dialog. Will only be called once. */
  protected def dialogComponent(): Component

  /**
   * Perform a validation on user choices inside a dialog content.
   * Implementation should show an error if any, otherwise - return a result to set.
   */
  protected def validateChoices(): Option[A]
}
