package org.csc.vrfblk.tasks

import java.util.ArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConversions.`deprecated asScalaBuffer`
import scala.collection.JavaConversions.`deprecated seqAsJavaList`
import scala.collection.mutable.Map

import org.apache.commons.lang3.StringUtils
import org.csc.bcapi.JodaTimeHelper
import org.csc.bcapi.exec.SRunner
import org.csc.evmapi.gens.Block.BlockEntity
import org.csc.evmapi.gens.Tx.MultiTransaction
import org.csc.p22p.action.PMNodeHelper
import org.csc.p22p.node.Network
import org.csc.p22p.node.Node
import org.csc.p22p.utils.LogHelper

import com.google.common.cache.Cache
import com.google.common.cache.CacheBuilder

import onight.tfw.async.CallBack
import onight.tfw.otransio.api.beans.FramePacket
import org.csc.bcapi.gens.Oentity.OValue
import java.math.BigInteger
import org.csc.ckrand.pbgens.Ckrand.VNode
import org.csc.vrfblk.Daos
import org.csc.ckrand.pbgens.Ckrand.PBlockEntry
import org.csc.vrfblk.utils.VConfig
import org.csc.ckrand.pbgens.Ckrand.VNodeState
import org.csc.ckrand.pbgens.Ckrand.PBlockEntryOrBuilder
import org.csc.ckrand.pbgens.Ckrand.PSNodeInfo

//投票决定当前的节点
case class VRFController(network: Network) extends SRunner with PMNodeHelper with LogHelper {
  def getName() = "VCtrl"
  val VRF_NODE_DB_KEY = "CURRENT_VRF_KEY";
  var cur_vnode: VNode.Builder = VNode.newBuilder()
  var isStop: Boolean = false;

  def loadNodeFromDB() = {
    val ov = Daos.vrfpropdb.get(VRF_NODE_DB_KEY).get
    val root_node = network.root();
    if (ov == null) {
      cur_vnode.setBcuid(root_node.bcuid)
        .setCurBlock(1).setCoAddress(root_node.v_address)
        .setBitIdx(root_node.node_idx)
      Daos.vrfpropdb.put(
        VRF_NODE_DB_KEY,
        OValue.newBuilder().setExtdata(cur_vnode.build().toByteString()).build())
    } else {
      cur_vnode.mergeFrom(ov.getExtdata).setBitIdx(root_node.node_idx)
      if (!StringUtils.equals(cur_vnode.getBcuid, root_node.bcuid)) {
        log.warn("load from dnode info not equals with pzp node:" + cur_vnode + ",root=" + root_node)
        cur_vnode.setBcuid(root_node.bcuid);
        syncToDB();
      } else {
        log.info("load from db:OK:" + cur_vnode)
      }
    }
    if (cur_vnode.getCurBlock == 0 || cur_vnode.getCurBlock != Daos.chainHelper.getLastBlockNumber.intValue()) {
      log.info("vrf block Info load from DB:c=" +
        cur_vnode.getCurBlock + " ==> a=" + Daos.chainHelper.getLastBlockNumber);
      cur_vnode.setCurBlock(Daos.chainHelper.getLastBlockNumber.intValue())
      cur_vnode.setCurBlockHash(Daos.chainHelper.GetConnectBestBlockHash());
      syncToDB();
    }

  }
  def syncToDB() {
    Daos.vrfpropdb.put(
      VRF_NODE_DB_KEY,
      OValue.newBuilder().setExtdata(cur_vnode.build().toByteString()).build())
  }

  def updateBlockHeight(blockHeight: Int, blockHash: String) = {
    log.debug("checkMiner --> updateBlockHeight blockHeight::" + blockHeight + " cur_vnode.getCurBlock::" + cur_vnode.getCurBlock);
    if (blockHeight != cur_vnode.getCurBlock) {

      Daos.blkHelper.synchronized({
        cur_vnode.setCurBlockMakeTime(System.currentTimeMillis())
        cur_vnode.setCurBlock(Daos.chainHelper.getLastBlockNumber.intValue())
        if (blockHash != null) {
          cur_vnode.setCurBlockHash(blockHash)
        }
        log.debug("checkMiner --> cur_vnode.setCurBlock::" + cur_vnode.getCurBlock);
        syncToDB()
      })
    }
  }
  def runOnce() = {
    Thread.currentThread().setName("VCtrl");
    implicit val _net = network
    MDCSetBCUID(network);
    MDCRemoveMessageID()
    var continue = true;
    var continuecCC = 0;
    while (continue && !isStop) {
      try {
        continuecCC = continuecCC + 1;
        continue = false;
        log.info("VCTRL=" + cur_vnode.getState + ",B=" + cur_vnode.getCurBlock
          + ",CA=" + cur_vnode.getCoAddress
          + ",MN=" + VCtrl.coMinerByUID.size
          + ",DN=" + network.bitenc.bits.bitCount
          + ",PN=" + network.pendingNodeByBcuid.size
        //          + ",banVote=" + (System.currentTimeMillis() <= DTask_DutyTermVote.ban_for_vote_sec) + ":" + (-1 * JodaTimeHelper.secondIntFromNow(DTask_DutyTermVote.ban_for_vote_sec))
        //          + ",NextSec=" + JodaTimeHelper.secondFromNow(cur_vnode.getDutyEndMs)
        //          + ",SecPass=" + JodaTimeHelper.secondFromNow(cur_vnode.getLastDutyTime)
        );
        cur_vnode.getState match {
          case VNodeState.VN_INIT =>
            //tell other I will join
            loadNodeFromDB();
            BeaconGossip.offerMessage(PSNodeInfo.newBuilder().setVn(cur_vnode).build());
          case VNodeState.VN_SYNC_BLOCK       =>
          case VNodeState.VN_DUTY_BEACON      =>
          case VNodeState.VN_DUTY_BLOCKMAKERS =>
          case VNodeState.VN_DUTY_NOTARY      =>
          case VNodeState.VN_DUTY_SYNC        =>
          case _ =>
            log.warn("unknow state");
        }
        //          case DNodeState.DN_CO_MINER =>
        //
        //            if (!hbTask.onScheduled) {
        ////              Scheduler.scheduleWithFixedDelay(hbTask, 60, DConfig.HEATBEAT_TICK_SEC, TimeUnit.SECONDS);
        //              Daos.ddc.scheduleWithFixedDelaySecond( hbTask,60, DConfig.HEATBEAT_TICK_SEC);
        //              hbTask.onScheduled = true;
        //            }
        //            if (DConfig.RUN_COMINER != 1) {
        //              cur_vnode.setState(DNodeState.DN_BAKCUP)
        //            } else if (DTask_DutyTermVote.runOnce) {
        //              continue = true;
        //              cur_vnode.setState(DNodeState.DN_DUTY_MINER);
        //            } else {
        //              log.debug("cominer run false:" + cur_vnode.getCurBlock + ",vq[" + VCtrl.voteRequest().getBlockRange.getStartBlock
        //                + "," + VCtrl.voteRequest().getBlockRange.getEndBlock + "]" + ",vqid=" + VCtrl.voteRequest().getTermId
        //                + ",vqlid=" + VCtrl.voteRequest().getLastTermId + ",tid=" + term_Miner.getTermId
        //                + ",tq[" + term_Miner.getBlockRange.getStartBlock + "," + term_Miner.getBlockRange.getEndBlock + "]");
        //            }
        //          case DNodeState.DN_DUTY_MINER =>
        //            if (DConfig.RUN_COMINER != 1) {
        //              cur_vnode.setState(DNodeState.DN_BAKCUP)
        //            } else if (term_Miner.getBlockRange.getStartBlock > cur_vnode.getCurBlock + term_Miner.getMinerQueueCount) {
        //              log.debug("cur term force to resync block:" + cur_vnode.getCurBlock + ",vq[" + VCtrl.voteRequest().getBlockRange.getStartBlock
        //                + "," + VCtrl.voteRequest().getBlockRange.getEndBlock + "]" + ",vqid=" + VCtrl.voteRequest().getTermId
        //                + ",vqlid=" + VCtrl.voteRequest().getLastTermId + ",tid=" + term_Miner.getTermId
        //                + ",tq[" + term_Miner.getBlockRange.getStartBlock + "," + term_Miner.getBlockRange.getEndBlock + "]");
        //              continue = true;
        //              cur_vnode.setState(DNodeState.DN_SYNC_BLOCK);
        //            } else if (cur_vnode.getCurBlock >= term_Miner.getBlockRange.getEndBlock && term_Miner.getBlockRange.getEndBlock > 1 //|| VCtrl.voteRequest().getLastTermId >= term_Miner.getTermId
        //            ) {
        //              log.debug("cur term force to end:" + cur_vnode.getCurBlock + ",vq[" + VCtrl.voteRequest().getBlockRange.getStartBlock
        //                + "," + VCtrl.voteRequest().getBlockRange.getEndBlock + "]" + ",vqid=" + VCtrl.voteRequest().getTermId
        //                + ",vqlid=" + VCtrl.voteRequest().getLastTermId + ",tid=" + term_Miner.getTermId
        //                + ",tq[" + term_Miner.getBlockRange.getStartBlock + "," + term_Miner.getBlockRange.getEndBlock + "]");
        //              continue = true;
        //              cur_vnode.setState(DNodeState.DN_CO_MINER);
        //            } else if (DTask_MineBlock.runOnce) {
        //              if (cur_vnode.getCurBlock >= term_Miner.getBlockRange.getEndBlock
        //                || term_Miner.getTermId < vote_Request.getTermId) {
        ////                val sleept = Math.abs((Math.random() * 100000000 % DConfig.DTV_TIME_MS_EACH_BLOCK).asInstanceOf[Long]) + 10;
        //                log.info("cur term WILL end:newblk=" + cur_vnode.getCurBlock + ",term[" + VCtrl.voteRequest().getBlockRange.getStartBlock
        //                  + "," + VCtrl.voteRequest().getBlockRange.getEndBlock + "]" + ",T=" + term_Miner.getTermId + ",try to vote,blk="+cur_vnode.getCurBlock);
        //                continue = true;
        //                cur_vnode.setState(DNodeState.DN_CO_MINER);
        //                //don't sleep for next vote.
        ////                DTask_DutyTermVote.synchronized({
        ////                  DTask_DutyTermVote.wait(sleept)
        ////                });
        //                true
        //              } else {
        //                val pendingSize = Daos.confirmMapDB.getQueueSize;
        //                if (pendingSize > DConfig.WAIT_BLOCK_MIN_TXN) {
        //                  continue = true;
        //                  true
        //                } else {
        //                  false
        //                }
        //              }
        //            } else {
        //              //check who mining.
        //              if (cur_vnode.getCurBlock >= term_Miner.getBlockRange.getEndBlock && term_Miner.getBlockRange.getEndBlock > 1) {
        //                continue = true;
        //                cur_vnode.setState(DNodeState.DN_CO_MINER);
        //                val sleept = Math.abs((Math.random() * 10000000 % DConfig.DTV_TIME_MS_EACH_BLOCK).asInstanceOf[Long]) + 1000;
        //                DTask_DutyTermVote.synchronized({
        //                  DTask_DutyTermVote.wait(sleept)
        //                });
        //                true
        //              } else {
        //                false;
        //              }
        //            }
        //          case DNodeState.DN_SYNC_BLOCK =>
        //            DTask_CoMine.runOnce
        //          case DNodeState.DN_BAKCUP =>
        //            DTask_CoMine.runOnce
        //          case _ =>
        //            log.warn("unknow State:" + cur_vnode.getState);
        //
        //        }

      } catch {
        case e: Throwable =>
          log.warn("v control :Error", e);
      } finally {
        MDCRemoveMessageID()
      }
    }
  }
}

object VCtrl extends LogHelper {
  var instance: VRFController = null;
  def network(): Network = instance.network;
  val coMinerByUID: Map[String, VNode] = Map.empty[String, VNode];
  def curVN(): VNode.Builder = instance.cur_vnode

  def getFastNode(): String = {
    var fastNode = curVN().build();
    coMinerByUID.map { f =>
      if (f._2.getCurBlock > fastNode.getCurBlock) {
        fastNode = f._2;
      }
    }
    fastNode.getBcuid
  }
  //  def curTermMiner(): PSDutyTermVoteOrBuilder = instance.term_Miner

  def isReady(): Boolean = {
    instance.network != null &&
      instance.cur_vnode.getStateValue > VNodeState.VN_INIT_VALUE
  }
  def sleep(sleepMS: Long): Unit = {
    if (sleepMS <= 1) return
    Thread.sleep(sleepMS);
  }
  def checkMiner(block: Int, coaddr: String, mineTime: Long, threadName: String, maxWaitMS: Long = 0L): (Boolean, Boolean) = {
    (true, false)
  }

  val recentBlocks: Cache[Int, PBlockEntry.Builder] = CacheBuilder.newBuilder().expireAfterWrite(30, TimeUnit.SECONDS)
    .maximumSize(1000).build().asInstanceOf[Cache[Int, PBlockEntry.Builder]]

  def loadFromBlock(block: Int): PBlockEntry.Builder = {
    loadFromBlock(block, false)
  }
  def loadFromBlock(block: Int, needBody: Boolean): PBlockEntry.Builder = {
    //    val ov = Daos.dposdb.get("D" + block).get
    //    if (ov != null) {
    //    recentBlocks.synchronized {
    if (block > curVN().getCurBlock) {
      null
    } else {
      val recentblk = recentBlocks.getIfPresent(block);
      if (recentblk != null) {
        return recentblk;
      }
      val blk = Daos.chainHelper.getBlockByNumber(block);
      if (blk != null) {
        if (needBody) {
          val b = PBlockEntry.newBuilder().setBlockHeader(blk.toBuilder().build().toByteString()).setBlockHeight(block)
          recentBlocks.put(block, b);
          b
        } else {
          val b = PBlockEntry.newBuilder().setBlockHeader(blk.toBuilder().clearBody().build().toByteString()).setBlockHeight(block)
          recentBlocks.put(block, b);
          b
        }
      } else {
        log.error("blk not found in AccountDB:" + block);
        null;
      }
    }
  }
}