package org.fs.chm.utility

import org.fs.chm.dao._

object EntityUtils {
  def groupChatsById(cs: Seq[Chat]): Map[Long, Chat] = {
    cs groupBy (_.id) map { case (k, v) => (k, v.head) }
  }
}
