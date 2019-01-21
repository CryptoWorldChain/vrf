package org.csc.vrfblk.action

import org.apache.felix.ipojo.annotations.Instantiate
import org.apache.felix.ipojo.annotations.Provides
import onight.tfw.ntrans.api.ActorService
import onight.tfw.proxy.IActor
import onight.tfw.otransio.api.session.CMDService
import onight.osgi.annotation.NActorProvider
import org.csc.dposblk.PSMDPoSNet
import org.csc.p22p.utils.LogHelper
import onight.oapi.scala.commons.PBUtils
import onight.oapi.scala.commons.LService
import org.csc.p22p.action.PMNodeHelper
import onight.tfw.otransio.api.beans.FramePacket
import onight.tfw.async.CompleteHandler
import org.csc.bcapi.utils.PacketIMHelper._
import org.csc.dposblk.pbgens.Dposblock.PSCoMine
import org.csc.dposblk.pbgens.Dposblock.PRetCoMine
import org.csc.dposblk.pbgens.Dposblock.PCommand
import org.csc.dposblk.pbgens.Dposblock.PSDutyTermVote
import org.csc.dposblk.pbgens.Dposblock.PDutyTermResult
import org.csc.dposblk.tasks.DCtrl
import onight.tfw.otransio.api.PacketHelper
import org.apache.commons.lang3.StringUtils
import org.csc.bcapi.exception.FBSException
import org.csc.dposblk.pbgens.Dposblock.PDutyTermResult.VoteResult
import org.csc.dposblk.tasks.BlockSync
import org.csc.dposblk.tasks.DTask_DutyTermVote
import org.csc.dposblk.utils.DConfig
import scala.collection.JavaConversions._
import scala.collection.mutable.Map
import org.csc.dposblk.pbgens.Dposblock.PDNode
import scala.collection.mutable.ListBuffer
import org.csc.dposblk.pbgens.Dposblock.DNodeState
import org.csc.dposblk.Daos

@NActorProvider
@Instantiate
@Provides(specifications = Array(classOf[ActorService], classOf[IActor], classOf[CMDService]))
class PDDutyTermVote extends PSMDPoSNet[PSDutyTermVote] {
  override def service = PDDutyTermVoteService
}

//
// http://localhost:8000/fbs/xdn/pbget.do?bd=
object PDDutyTermVoteService extends LogHelper with PBUtils with LService[PSDutyTermVote] with PMNodeHelper {
  override def onPBPacket(pack: FramePacket, pbo: PSDutyTermVote, handler: CompleteHandler) = {
    //    log.debug("DPoS DutyTermVoteService::" + pack.getFrom())
    var ret = PDutyTermResult.newBuilder();
    val net = DCtrl.instance.network;
    if (!DCtrl.isReady() || net == null) {
      ret.setRetCode(-1).setRetMessage("DPoS Network Not READY")
      handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
    } else {
      try {
        MDCSetBCUID(DCtrl.dposNet())
        MDCSetMessageID(pbo.getMessageId)
        val cn = DCtrl.curDN()
        val tm = DCtrl.termMiner();
        ret.setMessageId(pbo.getMessageId);
        ret.setBcuid(cn.getBcuid)
        ret.setRetCode(0).setRetMessage("SUCCESS")
        ret.setCurTermid(tm.getTermId).setCurBlock(cn.getCurBlock).setCurTermSign(tm.getSign)
        ret.setCurTermStartBlock(tm.getBlockRange.getStartBlock).setCurTermEndBlock(tm.getBlockRange.getEndBlock)
        ret.setVoteTermStartBlock(pbo.getBlockRange.getStartBlock).setVoteTermEndBlock(pbo.getBlockRange.getEndBlock)
        if (DConfig.RUN_COMINER != 1) {
          ret.setNodeState(DNodeState.DN_BAKCUP)
        }
        val vq = DCtrl.voteRequest();
        //
        ret.setTermId(pbo.getTermId)
        ret.setSign(pbo.getSign)
        ret.setCurTermSign(tm.getSign);
        ret.setVoteAddress(cn.getCoAddress)

        DTask_DutyTermVote.synchronized({
          if ((StringUtils.isBlank(tm.getSign) || StringUtils.equals(pbo.getBcuid, cn.getBcuid)) || //first init or local vote
            (StringUtils.isBlank(vq.getSign) || vq.getSign.equals(pbo.getLastTermUid) || vq.getSign.equals(pbo.getSign) 
              || (System.currentTimeMillis()-vq.getTermStartMs)>DConfig.DTV_TIMEOUT_SEC) //vq is not zero
            //            && StringUtils.isBlank(vq.getMessageId) || vq.getMessageId.equals(pbo.getLastTermUid))
            && ((tm.getTermId <= pbo.getLastTermId) && tm.getTermId <= pbo.getTermId - 1
              && (
                (pbo.getBlockRange.getStartBlock >= tm.getBlockRange.getStartBlock && pbo.getRewriteTerm != null &&
                  pbo.getRewriteTerm.getBlockLost >= 0 || tm.getTermId <= 1) //for revote
                  || pbo.getBlockRange.getStartBlock >= tm.getBlockRange.getEndBlock) // for continue vote
                  || StringUtils.equals(pbo.getCoAddress, cn.getCoAddress)) && pbo.getMinerQueueCount > 0) {
            //check quantifyminers
            val quantifyMinerByCoAddr = Map[String, PDNode]();
            var inMinerList = false;
            DCtrl.coMinerByUID.filter(p =>
              if (p._2.getCoAddress.equals(pbo.getCoAddress)
                || (p._2.getCurBlock <= pbo.getBlockRange.getStartBlock // 1 - Math.abs(DConfig.BLOCK_DISTANCE_COMINE)
                  &&
                  (pbo.getLastTermId <= p._2.getTermId || pbo.getTermId > p._2.getTermId) //                  &&
                  )) {
                true
              } else {
                log.debug("unquantifyminers:" + p._2.getBcuid + "," + p._2.getCoAddress + ",pblock=" + p._2.getCurBlock
                  + ",cn=" + cn.getCurBlock + ",PID=" + p._2.getTermId + ",pbo.TID=" + pbo.getTermId + ",LPBO.TID=" + pbo.getLastTermId
                  + ",pbtsign=" + p._2.getTermSign + ",pbo.sign=" + pbo.getSign + ",pbo.lasttmsig=" + pbo.getLastTermUid)
                false;
              }).map(f =>
              {
                quantifyMinerByCoAddr.put(f._2.getCoAddress, f._2);
              })
            val lostInMiner = ListBuffer[String]();
            val q = pbo.getMinerQueueList.filter { f =>
              if (f.getMinerCoaddr.equals(cn.getCoAddress)) {
                inMinerList = true;
              }
              val nodeInDnodes = net.directNodeByBcuid.map(cf => cf._2.v_address.equals(f.getMinerCoaddr));
              val nodeInCoMiner = DCtrl.coMinerByUID.map(cf => cf._2.getCoAddress.equals(f.getMinerCoaddr));
              if (nodeInDnodes.size == 0 && nodeInCoMiner == 0) {
                log.debug("unquantifyminers: " + f.getMinerCoaddr + " not in Dnode and Cominer");
                quantifyMinerByCoAddr.remove(f.getMinerCoaddr)
              } else if (nodeInDnodes.size > 0 && nodeInCoMiner == 0) {
                log.debug("add dnode to:CoMiner " + f.getMinerCoaddr);
                lostInMiner.append(f.getMinerCoaddr)
              }
              if (!quantifyMinerByCoAddr.contains(f.getMinerCoaddr)) {
                //                log.debug("UNQuantifyNode:" + f.getMinerCoaddr);
                true;
              } else {
                false
              }
            }
            if (lostInMiner.size > 0) {
//              Scheduler.runOnce(DCtrl.instance.hbTask);
              Daos.ddc.executeNow(BlockSync.workPack, DCtrl.instance.hbTask)
            }
            val reject =
              if (pbo.getBlockRange.getStartBlock != tm.getBlockRange.getEndBlock + 1 && tm.getTermId > 0
                && System.currentTimeMillis() - cn.getLastBlockTime > DConfig.MAX_WAIT_BLK_EPOCH_MS) { //跟上一块确实是超时了才能重新投票
                if (pbo.getRewriteTerm == null) {
                  log.info("Reject DPos TermVote block not a sequence,cn.duty=" + cn.getDutyUid + ",T=" + tm.getTermId + ",PT=" + pbo.getTermId
                    + ",VT=" + vq.getTermId + ",LT=" + pbo.getLastTermId
                    + ",TU=" + tm.getSign + ",LTM=" + tm.getLastTermUid
                    + ",PU=" + pbo.getSign + ",PTM=" + pbo.getLastTermUid
                    + ",VM=" + vq.getMessageId + ",LTM=" + pbo.getLastTermUid
                    + ",PA=" + pbo.getCoAddress + ",CA=" + cn.getCoAddress + ",qsize=" + q.size);
                  ret.setResult(VoteResult.VR_REJECT).setRetMessage("Overrided_Not_TimeOut")
                  true
                } else if (pbo.getTermId > tm.getTermId &&
                  pbo.getBlockRange.getStartBlock > cn.getCurBlock &&
                  (pbo.getBlockRange.getStartBlock > tm.getBlockRange.getEndBlock || pbo.getBlockRange.getStartBlock < tm.getBlockRange.getStartBlock)) {
                  log.info("check vote block too large:" + pbo.getBlockRange.getStartBlock + ",[" + tm.getBlockRange.getStartBlock + "," + tm.getBlockRange.getEndBlock + "],sign="
                    + tm.getSign + ",TID=" + tm.getTermId + ",pib=" + pbo.getTermId + ",curblock=" + cn.getCurBlock)
                  false;
                } else {
                  //check rewrite
                  log.debug("checkMiner --> dutytermvote pbo.getBlockRange.getStartBlock::" + pbo.getBlockRange.getStartBlock);
                  val (isMiner, isOverrided) = DCtrl.checkMiner(pbo.getBlockRange.getStartBlock, pbo.getCoAddress, System.currentTimeMillis(), Thread.currentThread().getName())
                  if (!isOverrided && !isMiner && System.currentTimeMillis() - cn.getLastBlockTime < DConfig.MAX_WAIT_BLK_EPOCH_MS) { //
                    log.info("Not your Miner Voted!!isMiner=" + isMiner + ",isOverrided=" + isOverrided
                      + ",B=" + cn.getCurBlock + ",BS=[" + pbo.getBlockRange.getStartBlock + "," + pbo.getBlockRange.getEndBlock
                      + "],VM=" + vq.getMessageId + ",LTM=" + pbo.getLastTermUid
                      + ",PU=" + pbo.getSign + ",PTM=" + pbo.getLastTermUid
                      + ",TM=[" + tm.getBlockRange.getStartBlock + "," + tm.getBlockRange.getEndBlock
                      + "]:" + (System.currentTimeMillis() - cn.getLastBlockTime));
                    ret.setResult(VoteResult.VR_REJECT).setRetMessage("Not_Miner_Voted")
                    true
                  } else {
                    //should be voting
                    false
                  }
                }
              } else {
                false
              }

            if (!reject) {
//              val inMiner = DCtrl.coMinerByUID.filter { x => x._2.getCoAddress.equals(cn.getCoAddress) }.size > 0;
              val minerMap = Map.empty[String,String];
              pbo.getMinerQueueList.map { x => minerMap.put(x.getMinerCoaddr, x.getMinerCoaddr) }
              
//              if (minerMap.size < DCtrl.coMinerByUID.size / 20 ) {
//                log.info("Reject DPos TermVote CoNode Count too LOW,cocount=" + minerMap.size
//                  + ",minercount=" + DCtrl.coMinerByUID.size + ",cn.duty=" + cn.getDutyUid
//                  + ",T=" + tm.getTermId + ",PT=" + pbo.getTermId
//                  + ",VT=" + vq.getTermId + ",LT=" + pbo.getLastTermId
//                  + ",TU=" + tm.getSign + ",LTM=" + tm.getLastTermUid
//                  + ",PU=" + pbo.getSign + ",PTM=" + pbo.getLastTermUid
//                  + ",VM=" + vq.getMessageId + ",LTM=" + pbo.getLastTermUid
//                  + ",PA=" + pbo.getCoAddress + ",CA=" + cn.getCoAddress + ",qsize=" + q.size);
//                ret.setResult(VoteResult.VR_REJECT).setRetMessage("CoNode Count too LOW,cocount=" + minerMap.size
//                  + ",minercount=" + DCtrl.coMinerByUID.size + ",VT=" + vq.getTermId + ",T=" + tm.getTermId);
//              } else
                if (pbo.getTermId != tm.getTermId + 1 && q.size > 0 && inMinerList) {
                log.info("Reject DPos TermVote Miner not quntified,cn.duty=" + cn.getDutyUid + ",T=" + tm.getTermId + ",PT=" + pbo.getTermId
                  + ",VT=" + vq.getTermId + ",LT=" + pbo.getLastTermId
                  + ",TU=" + tm.getSign + ",LTM=" + tm.getLastTermUid
                  + ",PU=" + pbo.getSign + ",PTM=" + pbo.getLastTermUid
                  + ",VM=" + vq.getMessageId + ",LTM=" + pbo.getLastTermUid
                  + ",PA=" + pbo.getCoAddress + ",CA=" + cn.getCoAddress + ",qsize=" + q.size);
                ret.setResult(VoteResult.VR_REJECT).setRetMessage("MinerNotQuntified:" + "VT=" + vq.getTermId + ",T=" + tm.getTermId);
              } else {
                if (cn.getCurBlock < pbo.getBlockRange.getStartBlock - 10 && tm.getTermId < pbo.getLastTermId) {
                  log.info("Grant DPos Term Vote but Block Height Not Ready:" + cn.getDutyUid + ",T=" + tm.getTermId + ",PT=" + pbo.getTermId
                    + ",VT=" + vq.getTermId + ",LT=" + pbo.getLastTermId
                    + ",B=" + cn.getCurBlock + ",BS=[" + pbo.getBlockRange.getStartBlock + "," + pbo.getBlockRange.getEndBlock
                    + "],VM=" + vq.getMessageId + ",LTM=" + pbo.getLastTermUid
                    + ",PU=" + pbo.getSign + ",PTM=" + pbo.getLastTermUid
                    + ",PA=" + pbo.getCoAddress + ",CA=" + cn.getCoAddress + ",from=" + pbo.getBcuid);
                  ret.setResult(VoteResult.VR_GRANTED).setRetMessage("L_GRANTED")
                  ret.setTermId(pbo.getTermId)
                  ret.setBcuid(cn.getBcuid)
                  ret.setSign(pbo.getSign)
                  ret.setVoteAddress(cn.getCoAddress)
                  DCtrl.instance.updateVoteReq(pbo);
                  //                  BlockSync.tryBackgroundSyncLogs(pbo.getBlockRange.getStartBlock - 1, pbo.getBcuid)(net)
                } else {
                  // 
                  log.info("Grant DPos Term Vote:" + cn.getDutyUid + ",T=" + tm.getTermId + ",PT=" + pbo.getTermId
                    + ",VT=" + vq.getTermId + ",LT=" + pbo.getLastTermId
                    + ",PBS=[" + pbo.getBlockRange.getStartBlock + "," + pbo.getBlockRange.getEndBlock + "]"
                    + ",TBS=[" + tm.getBlockRange.getStartBlock + "," + tm.getBlockRange.getEndBlock + "]"
                    + ",VBS=[" + vq.getBlockRange.getStartBlock + "," + vq.getBlockRange.getEndBlock + "]"
                    + ",VM=" + vq.getMessageId + ",LTM=" + pbo.getLastTermUid
                    + ",PA=" + pbo.getCoAddress + ",CA=" + cn.getCoAddress);
                  ret.setResult(VoteResult.VR_GRANTED).setRetMessage("H_GRANTED")
                  ret.setTermId(pbo.getTermId)
                  ret.setSign(pbo.getSign)
                  ret.setBcuid(cn.getBcuid)

                  ret.setVoteAddress(cn.getCoAddress)
                  DCtrl.instance.updateVoteReq(pbo);
                }
              }
              //
            }
          } else { //line:83
            val nfino = if (pbo.getRewriteTerm != null) {
              "[" + pbo.getRewriteTerm.getBlockLost + "]"
            } else {
              "null";
            }
            log.info("Reject DPos Term Vote: TM=" + tm.getSign + ",PT=" + pbo.getTermId
              + ",VT=" + vq.getTermId + ",PLT=" + pbo.getLastTermId + ",T=" + tm.getTermId
              + ",PBS=[" + pbo.getBlockRange.getStartBlock + "," + pbo.getBlockRange.getEndBlock + "]"
              + ",TBS=[" + tm.getBlockRange.getStartBlock + "," + tm.getBlockRange.getEndBlock + "]"
              + ",VBS=[" + vq.getBlockRange.getStartBlock + "," + vq.getBlockRange.getEndBlock + "]"
              + ",VM=" + vq.getMessageId + ",PLTU=" + pbo.getLastTermUid + ",LTU=" + tm.getLastTermUid
              + ",PA=" + pbo.getCoAddress + ",CA=" + cn.getCoAddress + ",ReWrite=" +
              nfino);
            ret.setResult(VoteResult.VR_REJECT).setRetMessage("TERM_ID_FAILED:" + "TBS=[" + tm.getBlockRange.getStartBlock + "," + tm.getBlockRange.getEndBlock + "]"
              + ",VBS=[" + vq.getBlockRange.getStartBlock + "," + vq.getBlockRange.getEndBlock + "]")
            ret.setTermId(pbo.getTermId)
            ret.setSign(pbo.getSign)
            ret.setVoteAddress(cn.getCoAddress)
            //
          }
          if (StringUtils.isNotBlank(pbo.getSign)) {
            DCtrl.instance.saveVoteReq(pbo);
            DTask_DutyTermVote.notifyAll()
          }

        })

        net.dwallMessage("DTRDOB", Left(ret.build()), pbo.getMessageId, '9');
        //        }

      } catch {
        case e: FBSException => {
          log.error("fbsException:" + e.getMessage, e);
          ret.clear()
          ret.setRetCode(-2).setRetMessage(e.getMessage)
        }
        case t: Throwable => {
          log.error("error:", t);
          ret.clear()
          ret.setRetCode(-3).setRetMessage("" + t.getMessage)
        }
      } finally {
        handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))

      }
    }
  }
  //  override def getCmds(): Array[String] = Array(PWCommand.LST.name())
  override def cmd: String = PCommand.DTV.name();
}
