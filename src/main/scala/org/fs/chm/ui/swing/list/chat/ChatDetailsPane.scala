package org.fs.chm.ui.swing.list.chat

import javax.swing.ImageIcon

import scala.swing.BorderPanel.Position._
import scala.swing.GridBagPanel.Anchor
import scala.swing.GridBagPanel.Fill
import scala.swing._

import org.fs.chm.dao.ChatHistoryDao
import org.fs.chm.dao.Entities._
import org.fs.chm.protobuf.ChatType
import org.fs.chm.protobuf.SourceType
import org.fs.chm.ui.swing.general.SwingUtils._
import org.fs.chm.ui.swing.general.field.TextComponent

class ChatDetailsPane(
    dao: ChatHistoryDao,
    cwd: ChatWithDetails,
    full: Boolean,
) extends GridBagPanel {

  private def tc(s: String) = new TextComponent(s, mutable = false)

  private val nameC = tc(cwd.chat.nameOrUnnamed)

  {
    val data: Seq[(String, BorderPanel)] = Seq(
      ("ID:", if (!full) Some(tc(cwd.chat.id.toReadableId)) else None),
      ("Name:", Some(nameC)),
      ("Type:", if (!full) None else Some(tc(cwd.chat.tpe match {
        case ChatType.Personal     =>
          val userId = cwd.chat.memberIds.find(_ != dao.myself(cwd.dsUuid).id).map(_.toReadableId).getOrElse("[unknown]")
          s"Personal (User ID #$userId)"
        case ChatType.PrivateGroup =>
          "Private Group"
      }))),
      ("Members:", if (!full) None else cwd.chat.tpe match {
        case ChatType.PrivateGroup =>
          Some(tc(cwd.members.filter(_.prettyName != cwd.chat.nameOption.get).map(_.prettyName).mkString("\n")))
        case ChatType.Personal     =>
          None
      }),
      ("Image:", if (!full) None else Some {
        resolveImage(cwd.chat.imgPathOption, dao.datasetRoot(cwd.chat.dsUuid)) match {
          case Some(image) =>
            new BorderPanel {
              // Chat image
              val label = new Label
              label.icon = new ImageIcon(image)
              layout(label) = Center
            }
          case None =>
            tc("(None)")
        }
      }),
      ("Messages:", Some(tc(cwd.chat.msgCount.toString))),
      ("Source Type:", Some(tc(cwd.chat.sourceType match {
        case SourceType.TextImport => "Text import"
        case SourceType.Telegram   => "Telegram"
        case SourceType.WhatsappDb => "WhatsApp"
        case SourceType.TinderDb   => "Tinder"
      }))),
      ("", if (!full) None else Some(tc(""))),
      ("ID:", if (!full) None else Some(tc(cwd.chat.id.toReadableId))),
      ("Dataset ID:", if (!full) None else Some(tc(cwd.chat.dsUuid.value))),
      ("Dataset:", if (!full) None else Some(tc(dao.datasets.find(_.uuid == cwd.chat.dsUuid).get.alias))),
      ("Database:", if (!full) None else Some(tc(dao.name)))
    ).collect {
      case (x, Some(y)) => (x, y)
    }

    val c = new Constraints
    c.fill  = Fill.None
    c.ipadx = 10
    c.ipady = 3

    // Labels column
    c.gridx  = 0
    c.anchor = Anchor.NorthEast
    for (((t, _), idx) <- data.zipWithIndex) {
      c.gridy = idx
      add(new Label(t) {
        this.fontSize = 15
      }, c)
    }

    // Data column
    c.gridx  = 1
    c.anchor = Anchor.NorthWest
    for (((_, tc), idx) <- data.zipWithIndex) {
      c.gridy = idx
      add(tc, c)
    }
  }

  def stylizeName(color: Color): Unit = {
    for (el <- Seq(nameC)) {
      el.innerComponent.foreground = color
      el.innerComponent.fontStyle = Font.Style.Bold
    }
  }
}
