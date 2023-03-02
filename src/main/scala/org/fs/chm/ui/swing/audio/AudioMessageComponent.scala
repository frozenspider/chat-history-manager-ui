package org.fs.chm.ui.swing.audio

import java.awt.CardLayout
import java.awt.Desktop
import java.io.File

import scala.swing._
import scala.swing.event.ButtonClicked

import javax.swing.border.EmptyBorder
import org.fs.chm.utility.Logging

/** Panel-based component that renders audio messages */
class AudioMessageComponent(
    filesWithMimeTypes: Seq[(File, Option[String])],
    providedDurationOption: Option[Int],
    desktopOption: Option[Desktop]
) extends Panel with Logging {
  private val width  = 200
  private val height = 50

  private val chosenFileTypeOption: Option[(File, Option[String])] =
    filesWithMimeTypes find {
      case (f, t) => f.exists()
    }

  private val playerOption: Option[AudioPlayer] =
    chosenFileTypeOption map (ft => new AudioPlayer(ft._1, ft._2, providedDurationOption))

  if (playerOption.isEmpty) {
    log.debug(s"Voice message not found: ${filesWithMimeTypes.map(_._1).map(_.getAbsolutePath)}")
  }

  private val durationOption = for {
    p <- playerOption
    d <- p.durationOption
  } yield d

  private val playBtn = new Button("Play") {
    enabled = durationOption.isDefined
  }
  private val slider = new Slider {
    min     = 0
    max     = math.round(durationOption map (_ * 100) getOrElse 0.0).toInt
    value   = 0
    enabled = false // For now, we use external player
  }
  private val durationLabel = new Label(durationOption map (_.toString) getOrElse "")

  private val ActiveCard   = "Active"
  private val InactiveCard = "Inactive"

  private val inactiveCardLabel = new Label {
    if (desktopOption.isEmpty) {
      text = "Voice messages are not supported on this platform"
    } else if (playerOption.isEmpty) {
      text = "Voice message not found"
    } else {
      text = "Something went wrong!"
    }
  }

  private val activeCard = new BorderPanel {
    import scala.swing.BorderPanel.Position._
    layout(playBtn)       = West
    layout(slider)        = Center
    layout(durationLabel) = East

    layoutManager.setHgap(10)
    border        = new EmptyBorder(10, 10, 10, 10)
    preferredSize = new Dimension(width, height)
  }

  // No scala.swing wrapper for CardLayout, unfortunately
  val cardLayout = new CardLayout()
  peer.setLayout(cardLayout)

  peer.add(ActiveCard, activeCard.peer)
  peer.add(InactiveCard, inactiveCardLabel.peer)

  xLayoutAlignment = 0
  yLayoutAlignment = 0.5

  if (desktopOption.isDefined && playerOption.isDefined) {
    listenTo(playBtn)
    reactions += {
      case ButtonClicked(`playBtn`) => desktopOption.get.open(playerOption.get.file)
    }
  } else {
    cardLayout.show(peer, InactiveCard)
  }
}
