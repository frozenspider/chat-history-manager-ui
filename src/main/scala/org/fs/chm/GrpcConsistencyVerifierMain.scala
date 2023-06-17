package org.fs.chm

import java.io.{File => JFile}

import scala.util.Random

import org.fs.chm.dao.Entities.ChatWithDetails
import org.fs.chm.loader.telegram.TelegramFullDataLoader
import org.fs.chm.loader.telegram.TelegramGRPCDataLoader

object GrpcConsistencyVerifierMain extends App {
  val grpcDataLoader = new TelegramGRPCDataLoader(50051)
  val localDataLoader = new TelegramFullDataLoader()

  val rnd = new Random()
  val pageSize = rnd.between(100, 1000)
  println(s"Page size: ${pageSize}")

  val file = new JFile("???")
  assert(file.exists())

  val grpcMgr = grpcDataLoader.loadData(file)
  val localMgr = localDataLoader.loadData(file)

  assert(grpcMgr.datasets.length == 1)
  assert(localMgr.datasets.length == 1)

  for ((gDs, lDs) <- grpcMgr.datasets zip localMgr.datasets) {
    assert(gDs.alias == lDs.alias, s"${gDs.alias} == ${lDs.alias}")
    assert(gDs.sourceType == lDs.sourceType, s"${gDs.sourceType} == ${lDs.sourceType}")

    val gDsUuid = gDs.uuid
    val lDsUuid = lDs.uuid
    assert(gDsUuid != lDsUuid, s"$gDsUuid != $lDsUuid")

    assert(grpcMgr.datasetRoot(gDsUuid) == localMgr.datasetRoot(lDsUuid),
      s"${grpcMgr.datasetRoot(gDsUuid)} == ${localMgr.datasetRoot(lDsUuid)}")
    val gSelf = grpcMgr.myself(gDsUuid)
    val lSelf = localMgr.myself(lDsUuid).copy(dsUuid = gDsUuid)
    assert(gSelf == lSelf, s"$gSelf == $lSelf")

    val gUsers = grpcMgr.users(gDsUuid).sortBy(_.id)
    val lUsers = localMgr.users(lDsUuid).sortBy(_.id)
    assert(gUsers.length == lUsers.length, s"${gUsers}.length == ${lUsers}.length")
    for ((gu, luRaw) <- gUsers zip lUsers) {
      val lu = luRaw.copy(dsUuid = gDsUuid)
      assert(gu == lu, s"$gu == $lu")
    }

    val gCwds = grpcMgr.chats(gDsUuid).sortBy(_.chat.id)
    val lCwds = localMgr.chats(lDsUuid).sortBy(_.chat.id)
    assert(gCwds.length == lCwds.length, s"${gCwds.map(_.chat)}.length == ${lCwds.map(_.chat)}.length")
    for ((gCwd, lCwd) <- gCwds zip lCwds) {
      val ChatWithDetails(gc, gLmo, gMembers) = gCwd
      val ChatWithDetails(lc, lLmo, lMembersRaw) = lCwd
      assert(gc.copy(memberIds = gc.memberIds.sorted) == lc.copy(dsUuid = gDsUuid, memberIds = lc.memberIds.sorted),
        s"$gc == ${lc.copy(dsUuid = gDsUuid)}")

      assert(gLmo == lLmo, s"${gLmo} == ${lLmo}")

      val lMembers = lMembersRaw.map(_.copy(dsUuid = gDsUuid))
      assert(gMembers == lMembers, s"$gMembers == $lMembers")

      var nProcessedMessages = 0
      while (nProcessedMessages < gc.msgCount) {
        val gMsgs = grpcMgr.scrollMessages(gc, nProcessedMessages, pageSize)
        val lMsgs = localMgr.scrollMessages(lc, nProcessedMessages, pageSize)
        assert(gMsgs.length == lMsgs.length, s"For chat ${gc.nameOption.getOrElse(gc.id)}, nProcessedMessages = ${nProcessedMessages}, pageSize = ${pageSize}, mismatching # messages!")
        nProcessedMessages += gMsgs.length

        for ((gm, lm) <- gMsgs zip lMsgs) {
          assert(gm == lm, s"Chat ${gc.nameOption.getOrElse(gc.id)}: ${gm} == ${lm}")
        }
      }
    }

    {
      val gFiles = grpcMgr.datasetFiles(gDsUuid).toSeq.sortBy(_.getAbsolutePath)
      val lFiles = localMgr.datasetFiles(lDsUuid).toSeq.sortBy(_.getAbsolutePath)
      assert(gFiles == lFiles, s"${gFiles} == ${lFiles}")
    }
  }

  println("Checks successful!")
}
