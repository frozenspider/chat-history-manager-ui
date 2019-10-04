package org.fs.chm.dao

class EagerChatHistoryDao(
    override val contacts: Seq[Contact]
) extends ChatHistoryDao {
  override def toString: String = {
    Seq(
      "EagerChatHistoryDao(",
      "  contacts: ",
      contacts.mkString("    ", "\n    ", "\n"),
      ")"
    ).mkString("\n")
  }
}
