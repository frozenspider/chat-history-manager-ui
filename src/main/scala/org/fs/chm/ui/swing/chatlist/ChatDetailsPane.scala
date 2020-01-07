package org.fs.chm.ui.swing.chatlist

import scala.swing.GridBagPanel
import scala.swing.GridBagPanel.Anchor
import scala.swing.GridBagPanel.Fill
import scala.swing.Label
import scala.swing.TextArea
import scala.swing.TextField

import org.fs.chm.dao.ChatType
import org.fs.chm.ui.swing.general.ChatWithDao
import org.fs.chm.ui.swing.general.SwingUtils._
import org.fs.utility.Imports._

class ChatDetailsPane(
    cc: ChatWithDao
) extends GridBagPanel {
  {
    val data: Seq[(String, String)] = Seq(
      ("Name:", Some(cc.chat.nameOption getOrElse "<Unnamed>")),
      ("Type:", Some(cc.chat.tpe match {
        case ChatType.Personal     => "Personal"
        case ChatType.PrivateGroup => "Private Group"
      })),
      ("Members:", cc.chat.tpe match {
        case ChatType.PrivateGroup => Some(cc.dao.interlocutors(cc.chat).map(_.prettyName).mkString("\n"))
        case ChatType.Personal     => None
      }),
      ("Image:", Some(if (cc.chat.imgPathOption.isDefined) "(Yes)" else "(None)")),
      ("Messages:", Some(cc.chat.msgCount.toString)),
      ("", Some("")),
      ("ID:", Some(cc.chat.id.toString)),
      ("Dataset ID:", Some(cc.chat.dsUuid.toString.toLowerCase)),
      ("Dataset:", Some(cc.dao.datasets.find(_.uuid == cc.chat.dsUuid).get.alias)),
      ("Database:", Some(cc.dao.name))
    ).collect {
      case ((x, Some(y))) => (x, y)
    }

    val c = new Constraints
    c.fill = Fill.None
    c.ipadx = 10
    c.ipady = 3

    // Labels column
    c.gridx = 0
    c.anchor = Anchor.NorthEast
    for (((t, _), idx) <- data.zipWithIndex) {
      c.gridy = idx
      add(new Label(t) {
        this.fontSize = 15
      }, c)
    }

    // Data column
    c.gridx = 1
    c.anchor = Anchor.NorthWest
    for (((_, v), idx) <- data.zipWithIndex) {
      c.gridy = idx
      add(new TextArea(v) {
        this.fontSize = 15
        this.editable = false
        this.background = null
        this.border = null
      }, c)
    }
  }
}
