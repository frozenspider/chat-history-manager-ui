package org.fs.chm.dao.merge

import scala.language.implicitConversions

import io.grpc.ManagedChannel
import org.fs.chm.dao.Entities._
import org.fs.chm.dao.GrpcChatHistoryDao
import org.fs.chm.dao.MutableChatHistoryDao
import org.fs.chm.dao.merge.DatasetMerger._
import org.fs.chm.protobuf._
import org.fs.chm.utility.RpcUtils._
import org.fs.utility.StopWatch

class DatasetMergerRemote(channel: ManagedChannel,
                          val masterDao: GrpcChatHistoryDao,
                          val masterDs: Dataset,
                          val slaveDao: GrpcChatHistoryDao,
                          val slaveDs: Dataset) extends DatasetMerger {

  private val rpcStub = MergeServiceGrpc.blockingStub(channel)

  override def analyze(masterCwd: ChatWithDetails, slaveCwd: ChatWithDetails, title: String): IndexedSeq[MessagesMergeDiff] = {
    implicit def toInternal[I <: TaggedMessageId](l: Long): I = l.asInstanceOf[I]

    require(masterCwd.chat.id == slaveCwd.chat.id)
    StopWatch.measureAndCall {
      sendRequest(AnalyzeRequest(masterDao.key, masterDs.uuid, slaveDao.key, slaveDs.uuid, chatIds = Seq(masterCwd.chat.id)))(req => {
        val analysis = rpcStub.analyze(req).analysis
        require(analysis.length == 1)
        require(analysis.head.chatId == masterCwd.chat.id)
        analysis.head.sections.map(v => {
          v.tpe match {
            case AnalysisSectionType.Match =>
              MessagesMergeDiff.Match(v.firstMasterMsgId, v.lastMasterMsgId, v.firstSlaveMsgId, v.lastSlaveMsgId)
            case AnalysisSectionType.Retention =>
              MessagesMergeDiff.Retain(v.firstMasterMsgId, v.lastMasterMsgId)
            case AnalysisSectionType.Addition =>
              MessagesMergeDiff.Add(v.firstSlaveMsgId, v.lastSlaveMsgId)
            case AnalysisSectionType.Conflict =>
              MessagesMergeDiff.Replace(v.firstMasterMsgId, v.lastMasterMsgId, v.firstSlaveMsgId, v.lastSlaveMsgId)
          }
        }).toIndexedSeq
      })
    }((_, t) => log.info(s"Chat $title analyzed in $t ms (remotely)"))
  }

  override def merge(explicitUsersToMerge: Seq[UserMergeOption],
                     chatsToMerge: Seq[ResolvedChatMergeOption],
                     newDao: MutableChatHistoryDao): Dataset = {
    ???
  }
}
