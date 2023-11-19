package org.fs.chm.ui.swing.list.chat

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
    cwd: ChatWithDetails
) extends GridBagPanel {
  {
    val data: Seq[(String, String)] = Seq(
      ("Name:", Some(cwd.chat.nameOrUnnamed)),
      ("Type:", Some(cwd.chat.tpe match {
        case ChatType.Personal     =>
          val userId = cwd.chat.memberIds.find(_ != dao.myself(cwd.dsUuid).id).map(_.toReadableId).getOrElse(Unknown)
          s"Personal (User ID #$userId)"
        case ChatType.PrivateGroup =>
          "Private Group"
      })),
      ("Members:", cwd.chat.tpe match {
        case ChatType.PrivateGroup =>
          Some(cwd.members.filter(_.prettyName != cwd.chat.nameOption.get).map(_.prettyName).mkString("\n"))
        case ChatType.Personal     =>
          None
      }),
      ("Image:", Some(if (cwd.chat.imgPathOption.isDefined) "(Yes)" else "(None)")),
      ("Messages:", Some(cwd.chat.msgCount.toString)),
      ("Source Type:", Some(cwd.chat.sourceType match {
        case SourceType.TextImport => "Text import"
        case SourceType.Telegram   => "Telegram"
        case SourceType.WhatsappDb => "WhatsApp"
        case SourceType.TinderDb   => "Tinder"
      })),
      ("", Some("")),
      ("ID:", Some(cwd.chat.id.toReadableId)),
      ("Dataset ID:", Some(cwd.chat.dsUuid.value)),
      ("Dataset:", Some(dao.datasets.find(_.uuid == cwd.chat.dsUuid).get.alias)),
      ("Database:", Some(dao.name))
    ).collect {
      case ((x, Some(y))) => (x, y)
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
    for (((_, v), idx) <- data.zipWithIndex) {
      c.gridy = idx
      add(new TextComponent(v, false), c)
    }
  }
}
