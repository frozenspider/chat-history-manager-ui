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
    cc: CombinedChat,
    full: Boolean,
) extends GridBagPanel {

  private def tc(s: String) = new TextComponent(s, mutable = false)

  private val mainChat = cc.mainCwd.chat
  private val dsUuid = mainChat.dsUuid

  private val nameC = tc(mainChat.nameOrUnnamed)

  {
    val data: Seq[(String, BorderPanel)] = Seq(
      ("ID:", if (!full) Some(tc(cc.cwds.map(_.chat.id.toReadableId).mkString("\n"))) else None),
      ("Name:", Some(nameC)),
      ("Type:", if (!full) None else Some(tc(mainChat.tpe match {
        case ChatType.Personal     =>
          val userId = mainChat.memberIds.find(_ != dao.myself(dsUuid).id).map(_.toReadableId).getOrElse("[unknown]")
          s"Personal (User ID #$userId)"
        case ChatType.PrivateGroup =>
          "Private Group"
      }))),
      ("Members:", if (!full) None else {
        val isGroup = mainChat.tpe == ChatType.PrivateGroup
        val showMembers = isGroup || cc.slaveCwds.nonEmpty
        if (showMembers) {
          val users = cc.members.filter(u => !isGroup || u.prettyName != mainChat.nameOption.get)
          Some(tc(users.map(_.prettyName).mkString("\n")))
        } else {
          None
        }
      }),
      ("Image:", if (!full) None else Some {
        resolveImage(mainChat.imgPathOption, dao.datasetRoot(dsUuid)) match {
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
      ("Messages:", Some(tc(cc.cwds.map(_.chat.msgCount).sum.toString))),
      ("Source Type:", Some(tc(cc.cwds.map(_.chat.sourceType.prettyString).distinct.mkString(", ")))),
      ("", if (!full) None else Some(tc(""))),
      ("ID:", if (full) Some(tc(cc.cwds.map(_.chat.id.toReadableId).mkString("\n"))) else None),
      ("Dataset ID:", if (!full) None else Some(tc(dsUuid.value))),
      ("Dataset:", if (!full) None else Some(tc(dao.datasets.find(_.uuid == dsUuid).get.alias))),
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
