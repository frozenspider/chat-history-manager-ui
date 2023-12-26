package org.fs.chm.dao.merge

import java.io.File

import scala.language.implicitConversions

import io.grpc.ManagedChannel
import org.fs.chm.dao.ChatHistoryDao
import org.fs.chm.dao.Entities._
import org.fs.chm.dao.GrpcChatHistoryDao
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

  override def analyze(masterChat: Chat, slaveChat: Chat, title: String): IndexedSeq[MessagesMergeDiff] = {
    implicit def toInternal[I <: TaggedMessageId](l: Long): I = l.asInstanceOf[I]

    val chatIdPair = ChatIdPair(masterChat.id, slaveChat.id)
    StopWatch.measureAndCall {
      sendRequest(AnalyzeRequest(
        masterDao.key,
        masterDs.uuid,
        slaveDao.key,
        slaveDs.uuid,
        chatIdPairs = Seq(chatIdPair)
      ))(req => {
        val analysis = rpcStub.analyze(req).analysis
        require(analysis.length == 1)
        require(analysis.head.chatIds == chatIdPair)
        analysis.head.sections.map(v => {
          v.tpe match {
            case AnalysisSectionType.Match =>
              MessagesMergeDiff.Match(v.range.firstMasterMsgId, v.range.lastMasterMsgId, v.range.firstSlaveMsgId, v.range.lastSlaveMsgId)
            case AnalysisSectionType.Retention =>
              MessagesMergeDiff.Retain(v.range.firstMasterMsgId, v.range.lastMasterMsgId)
            case AnalysisSectionType.Addition =>
              MessagesMergeDiff.Add(v.range.firstSlaveMsgId, v.range.lastSlaveMsgId)
            case AnalysisSectionType.Conflict =>
              MessagesMergeDiff.Replace(v.range.firstMasterMsgId, v.range.lastMasterMsgId, v.range.firstSlaveMsgId, v.range.lastSlaveMsgId)
          }
        }).toIndexedSeq
      })
    }((_, t) => log.info(s"Chat $title analyzed in $t ms (remotely)"))
  }

  override def merge(usersToMerge: Seq[UserMergeOption],
                     chatsToMerge: Seq[ResolvedChatMergeOption],
                     newDbPath: File): (GrpcChatHistoryDao, Dataset) = {
    StopWatch.measureAndCall {
      val userMerges = usersToMerge.map {
        case UserMergeOption.Retain(masterUser) => UserMerge(UserMergeType.Retain, masterUser.id)
        case UserMergeOption.Add(slaveUser) => UserMerge(UserMergeType.Add, slaveUser.id)
        case UserMergeOption.DontAdd(slaveUser) => UserMerge(UserMergeType.DontAdd, slaveUser.id)
        case UserMergeOption.Replace(masterUser, _) => UserMerge(UserMergeType.Replace, masterUser.id)
        case UserMergeOption.MatchOrDontReplace(masterUser, _) => UserMerge(UserMergeType.MatchOrDontReplace, masterUser.id)
      }

      val chatMerges = chatsToMerge.map {
        case ChatMergeOption.Keep(masterCwd) => ChatMerge(ChatMergeType.Retain, masterCwd.chat.id, Seq.empty)
        case ChatMergeOption.Add(slaveCwd) => ChatMerge(ChatMergeType.Add, slaveCwd.chat.id, Seq.empty)
        case ChatMergeOption.DontAdd(slaveCwd) => ChatMerge(ChatMergeType.DontAdd, slaveCwd.chat.id, Seq.empty)
        case ChatMergeOption.DontCombine(masterCwd, _) => ChatMerge(ChatMergeType.DontMerge, masterCwd.chat.id, Seq.empty)
        case ChatMergeOption.ResolvedCombine(masterCwd, _, resoluion) =>
          val messageMerges = resoluion.map {
            case MessagesMergeDiff.Retain(firstMasterMsgId, lastMasterMsgId) =>
              MessageMerge(MessageMergeType.Retain,
                Some(MessageMergeSectionRange(firstMasterMsgId, lastMasterMsgId, -1, -1)))
            case MessagesMergeDiff.Add(firstSlaveMsgId, lastSlaveMsgId) =>
              MessageMerge(MessageMergeType.Add,
                Some(MessageMergeSectionRange(-1, -1, firstSlaveMsgId, lastSlaveMsgId)))
            case MessagesMergeDiff.DontAdd(firstSlaveMsgId, lastSlaveMsgId) =>
              MessageMerge(MessageMergeType.DontAdd,
                Some(MessageMergeSectionRange(-1, -1, firstSlaveMsgId, lastSlaveMsgId)))
            case MessagesMergeDiff.Replace(firstMasterMsgId, lastMasterMsgId, firstSlaveMsgId, lastSlaveMsgId) =>
              MessageMerge(MessageMergeType.Replace,
                Some(MessageMergeSectionRange(firstMasterMsgId, lastMasterMsgId, firstSlaveMsgId, lastSlaveMsgId)))
            case MessagesMergeDiff.DontReplace(firstMasterMsgId, lastMasterMsgId, firstSlaveMsgId, lastSlaveMsgId) =>
              MessageMerge(MessageMergeType.DontReplace,
                Some(MessageMergeSectionRange(firstMasterMsgId, lastMasterMsgId, firstSlaveMsgId, lastSlaveMsgId)))
            case MessagesMergeDiff.Match(firstMasterMsgId, lastMasterMsgId, firstSlaveMsgId, lastSlaveMsgId) =>
              MessageMerge(MessageMergeType.Match,
                Some(MessageMergeSectionRange(firstMasterMsgId, lastMasterMsgId, firstSlaveMsgId, lastSlaveMsgId)))
          }
          ChatMerge(ChatMergeType.Merge, masterCwd.chat.id, messageMerges)
      }
      val req = MergeRequest(masterDao.key, masterDs.uuid, slaveDao.key, slaveDs.uuid, newDbPath.getAbsolutePath, userMerges, chatMerges)
      val result = sendRequest(req)(req => rpcStub.merge(req))
      val daoRpcStub = HistoryDaoServiceGrpc.blockingStub(channel)
      val loaderRpcStub = HistoryLoaderServiceGrpc.blockingStub(channel)
      val dao = new GrpcChatHistoryDao(result.newFile.key, result.newFile.name, daoRpcStub, loaderRpcStub)
      val ds = dao.datasets.find(_.uuid == result.newDsUuid).get
      (dao, ds)
    }((_, t) => log.info(s"Datasets merged in ${t} ms"))
  }
}
