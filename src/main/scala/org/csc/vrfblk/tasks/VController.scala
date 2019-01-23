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
case class VRFController(network: Network) extends PMNodeHelper with LogHelper {
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
    if (cur_vnode.getCurBlock != Daos.chainHelper.getLastBlockNumber.intValue()) {
      log.info("vrf block Info load from DB:c=" +
        cur_vnode.getCurBlock + " ==> a=" + Daos.chainHelper.getLastBlockNumber);
      if (Daos.chainHelper.getLastBlockNumber.intValue() == 0) {
        cur_vnode.setCurBlock(Daos.chainHelper.getLastBlockNumber.intValue())
        cur_vnode.setCurBlockHash(Daos.chainHelper.getBlockByNumber(0).getHeader.getBlockHash);
      } else {
        cur_vnode.setCurBlock(Daos.chainHelper.getLastBlockNumber.intValue())
        cur_vnode.setCurBlockHash(Daos.chainHelper.GetConnectBestBlockHash());
      }
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

  def startup() = {
    loadNodeFromDB();
    NodeStateSwither.offerMessage(new Initialize())
//    BeaconGossip.offerMessage(PSNodeInfo.newBuilder().setVn(cur_vnode).build());
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