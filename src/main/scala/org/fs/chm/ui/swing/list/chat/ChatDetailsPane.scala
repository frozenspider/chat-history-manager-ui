package org.fs.chm.ui.swing.list.chat

import java.awt.Color
import java.awt.font.TextAttribute

import javax.swing.BorderFactory
import javax.swing.ImageIcon
import javax.swing.SwingUtilities

import scala.swing.BorderPanel.Position._
import scala.swing.Dialog.Options
import scala.swing.Dialog.Result
import scala.swing.GridBagPanel.Anchor
import scala.swing.GridBagPanel.Fill
import scala.swing._
import scala.swing.event.MouseReleased
import scala.util.Try

import org.fs.chm.dao.ChatHistoryDao
import org.fs.chm.dao.Entities._
import org.fs.chm.protobuf.ChatType
import org.fs.chm.ui.swing.Callbacks
import org.fs.chm.ui.swing.general.SwingUtils._
import org.fs.chm.ui.swing.general.field.TextComponent

class ChatDetailsPane(
    dao: ChatHistoryDao,
    var cc: CombinedChat,
    full: Boolean,
    editCallbacksOption: Option[Callbacks.ChatCb]
) extends GridBagPanel {

  private def tc(s: String) = new TextComponent(s, mutable = false)

  private val mainChat = cc.mainCwd.chat
  private val dsUuid = mainChat.dsUuid

  private val idC   = tc(cc.cwds.map(_.chat.id.toReadableId).mkString("\n"))
  private val nameC = tc(mainChat.nameOrUnnamed)

  {
    val data: Seq[(String, BorderPanel)] = Seq(
      ("ID:", if (!full) Some(idC) else None),
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
      ("ID:", if (full) Some(idC) else None),
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

    editCallbacksOption foreach { _ =>
      idC.innerComponent.peer.setFont({
        val font = idC.innerComponent.peer.getFont
        val fontAttributes = new java.util.HashMap[TextAttribute, Integer]();
        fontAttributes.put(TextAttribute.UNDERLINE, TextAttribute.UNDERLINE_ON);
        font.deriveFont(fontAttributes)
      })
      idC.innerComponent.foreground = Color.BLUE

      // Reactions
      listenTo(idC.innerComponent, idC.innerComponent.mouse.clicks)
      reactions += {
        case e @ MouseReleased(_, _, _, _, _) if SwingUtilities.isLeftMouseButton(e.peer) && enabled =>
          editId()
      }
    }
  }

  def stylizeName(color: Color): Unit = {
    for (el <- Seq(nameC)) {
      el.innerComponent.foreground = color
      el.innerComponent.fontStyle = Font.Style.Bold
    }
  }

  private def editId(): Unit = {
    val ta = new TextArea(idC.value) {
      this.preferredWidth = 250
      this.border = BorderFactory.createLineBorder(Color.black)
    }
    val conf = Dialog.showConfirmation(
      title      = "Edit relevant IDs for a chat",
      message    = ta.peer,
      optionType = Options.OkCancel
    )
    if (conf == Result.Ok) {
      try {
        val idString = ta.text
        val idStrings = idString.trim.linesIterator.toIndexedSeq
        require(idStrings.length == cc.cwds.length, "Wrong number of IDs")
        val ids = idStrings.map(s => {
          val tryId = Try(s.replace(" ", "").toLong)
          require(tryId.isSuccess && tryId.get > 0, s"$s is not a valid positive integer")
          tryId.get
        })
        require(ids.toSet.size == ids.size, "Duplicate IDs found")
        val zipped = cc.cwds.zip(ids)
        val newCwds = for ((cwd, newId) <- zipped) yield {
          require(cwd.chat.id == newId || dao.chatOption(cwd.dsUuid, newId).isEmpty, s"ID ${cwd.chat.id} is already taken")
          cwd.copy(chat = cwd.chat.copy(id = newId))
        }
        editCallbacksOption.get.updateChatIds(dao, zipped.map(p => p._1.chat -> p._2))
        cc = CombinedChat(newCwds.head, newCwds.tail)
        idC.value = cc.cwds.map(_.chat.id.toReadableId).mkString("\n")
      } catch {
        case ex: IllegalArgumentException =>
          showError(ex.getMessage)
      }
    }
  }
}
