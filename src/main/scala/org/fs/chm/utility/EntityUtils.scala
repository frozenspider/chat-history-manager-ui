package org.fs.chm.utility

import scala.swing.Dialog
import scala.swing.Swing
import scala.swing.Swing.EmptyIcon
import javax.swing.JOptionPane

import org.fs.chm.dao.Entities._
import org.fs.chm.protobuf.User

object EntityUtils {
  def groupById[T: WithId](cs: Seq[T]): Map[Long, T] = {
    val withId = implicitly[WithId[T]]
    cs groupBy (withId.id) map { case (k, v) => (k, v.head) }
  }

  sealed trait WithId[T] {
    def id(t: T): Long
  }

  implicit object CwmWithId extends WithId[ChatWithDetails] {
    override def id(cwm: ChatWithDetails): Long = cwm.chat.id
  }

  implicit object UserWithId extends WithId[User] {
    override def id(user: User): Long = user.id
  }

  def chooseMyself(users: Seq[User]): Int = {
    val options = users map (_.prettyName)
    val res = JOptionPane.showOptionDialog(
      null,
      "Which one of them is you?",
      "Choose yourself",
      Dialog.Options.Default.id,
      Dialog.Message.Question.id,
      Swing.wrapIcon(EmptyIcon),
      (options map (_.asInstanceOf[AnyRef])).toArray,
      options.head
    )
    if (res == JOptionPane.CLOSED_OPTION) {
      throw new IllegalArgumentException("Well, tough luck")
    } else {
      res
    }
  }

  def askForUserInput(prompt: String): String = {
    val res = JOptionPane.showInputDialog(
      null,
      prompt,
      "Input requested value",
      Dialog.Message.Question.id
    )
    if (res == null) {
      throw new IllegalArgumentException("Well, tough luck")
    } else {
      res
    }
  }
}
