package org.fs.chm.dao.merge

import org.fs.chm.dao.Message
import org.fs.chm.dao.RichText
import org.fs.chm.utility.TestUtils._

object DatasetMergerHelper {

  val maxId     = (DatasetMerger.BatchSize * 3)
  val maxUserId = 3
  def rndUserId = 1 + rnd.nextInt(maxUserId)

  def changedMessages(msgs: Seq[Message], idCondition: Long => Boolean): Seq[Message] = {
    msgs.collect {
      case m: Message.Regular if idCondition(m.sourceIdOption.get) =>
        m.copy(textOption = Some(RichText(Seq(RichText.Plain("Different message")))))
      case m =>
        m
    }
  }
}
