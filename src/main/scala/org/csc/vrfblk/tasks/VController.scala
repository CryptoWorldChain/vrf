package org.csc.vrfblk.tasks

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock

import com.google.common.cache.{Cache, CacheBuilder}
import org.apache.commons.lang3.StringUtils
import org.csc.bcapi.crypto.BitMap
import org.csc.bcapi.gens.Oentity.OValue
import org.csc.ckrand.pbgens.Ckrand.{PBlockEntry, VNode, VNodeState}
import org.csc.p22p.action.PMNodeHelper
import org.csc.p22p.node.{Network, Node}
import org.csc.p22p.utils.LogHelper
import org.csc.vrfblk.Daos
import org.csc.vrfblk.utils.{RandFunction, VConfig}

import scala.collection.JavaConverters._
import scala.collection.mutable.Map

//投票决定当前的节点
case class VRFController(network: Network) extends PMNodeHelper with LogHelper with BitMap {
  def getName() = "VCtrl"

  val VRF_NODE_DB_KEY = "CURRENT_VRF_KEY";
  var cur_vnode: VNode.Builder = VNode.newBuilder()
  var isStop: Boolean = false;

  val heightBlkSeen = new AtomicInteger(0);

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
        log.warn("load from dnode info not equals with pzp node:{" + cur_vnode.toString().replaceAll("\n", ",") + "},root=" + root_node)
        cur_vnode.setBcuid(root_node.bcuid);
        syncToDB();
      } else {
        log.info("load from db:OK:{" + cur_vnode.toString().replaceAll("\n", ", ") + "}")
      }
    }
    if (cur_vnode.getCurBlock != Daos.chainHelper.getLastBlockNumber.intValue()) {
      log.info("vrf block Info load from DB:c=" +
        cur_vnode.getCurBlock + " ==> a=" + Daos.chainHelper.getLastBlockNumber);


      if (Daos.chainHelper.getLastBlockNumber.intValue() == 0) {
        cur_vnode.setCurBlock(Daos.chainHelper.getLastBlockNumber.intValue())
        //读取创世块HASH
        cur_vnode.setCurBlockHash(Daos.enc.hexEnc(Daos.chainHelper.getBlockByNumber(0).getHeader.getHash.toByteArray()));
        cur_vnode.setBeaconHash("")
      } else {
        val blk = Daos.chainHelper.GetConnectBestBlock();
        cur_vnode.setCurBlock(blk.getHeader.getNumber.intValue())
        //当前块BlockHASH
        cur_vnode.setCurBlockHash(Daos.enc.hexEnc(blk.getHeader.getHash.toByteArray()));
        cur_vnode.setBeaconHash(blk.getMiner.getTermid)
      }

      heightBlkSeen.set(cur_vnode.getCurBlock);
      syncToDB();
    }
    if (VConfig.RUN_COMINER == 1) {
      //成为BackUp节点，不参与挖矿
      cur_vnode.setDoMine(true)
    }

  }

  def syncToDB() {
    Daos.vrfpropdb.put(
      VRF_NODE_DB_KEY,
      OValue.newBuilder().setExtdata(cur_vnode.build().toByteString()).build())
  }

  def updateBlockHeight(blockHeight: Int, blockHash: String, extraData: String) = {

    //if (blockHeight != cur_vnode.getCurBlock || (blockHeight == cur_vnode.getCurBlock && !blockHash.equals(cur_vnode.getCurBlockHash))) {

    log.debug("updateBlockHeight blockHeight=" + blockHeight + " blockHash=" + blockHash + " bits=" + extraData)
    Daos.blkHelper.synchronized({
      cur_vnode.setCurBlockRecvTime(System.currentTimeMillis())
      cur_vnode.setCurBlockMakeTime(System.currentTimeMillis())
      val blk = Daos.blkHelper.getBlock(blockHash);
      if (blk.getHeader.getNumber.intValue() == blockHeight) {
        if (blockHeight == cur_vnode.getCurBlock + 1) {
          cur_vnode.setPrevBlockHash(cur_vnode.getCurBlockHash);
        }
        cur_vnode.setCurBlock(blk.getHeader.getNumber.intValue())
        //        cur_vnode.setCurBlock(blockHeight)
        cur_vnode.setVrfRandseeds(blk.getMiner.getBit);
        if (blockHash != null) {
          cur_vnode.setCurBlockHash(blockHash)

          // beaconhash = blockMiner.termId
          // cur_vnode.setBeaconHash(blockHash);
          cur_vnode.setBeaconHash(blk.getMiner.getTermid);
        }
      }
      log.debug("checkMiner --> cur_vnode.setCurBlock::" + cur_vnode.getCurBlock
        + ",hash=" + blockHash + ",seed=" + extraData);
      syncToDB()
    })
    //}
  }

  def startup() = {
    loadNodeFromDB();
    NodeStateSwitcher.offerMessage(new Initialize())
    //    BeaconGossip.offerMessage(PSNodeInfo.newBuilder().setVn(cur_vnode).build());
  }

}

object VCtrl extends LogHelper {
  var instance: VRFController = VRFController(null);

  def network(): Network = instance.network;
  val coMinerByUID: Map[String, VNode] = Map.empty[String, VNode];

  def curVN(): VNode.Builder = instance.cur_vnode

  //防止ApplyBlock时节点Make出相同高度的block,或打出beaconHash错误的block
  val blockLock: ReentrantLock = new ReentrantLock();

  def getFastNode(): String = {
    var fastNode = curVN().build();
    coMinerByUID.map { f =>
      if (f._2.getCurBlock > fastNode.getCurBlock) {
        fastNode = f._2;
      }
    }
    fastNode.getBcuid
  }

  def ensureNode(trybcuid: String): Node = {
    val net = instance.network;
    net.nodeByBcuid(trybcuid) match {
      case net.noneNode =>
        net.directNodeByBcuid.values.toList((Math.abs(Math.random() * 100000) % net.directNodes.size).asInstanceOf[Int]);
      case n: Node =>
        n;
    }
  }

  //  def curTermMiner(): PSDutyTermVoteOrBuilder = instance.term_Miner

  def isReady(): Boolean = {
    instance != null && instance.network != null &&
      instance.cur_vnode != null &&
      instance.cur_vnode.getStateValue > VNodeState.VN_INIT_VALUE
  }

  val recentBlocks: Cache[Int, PBlockEntry.Builder] = CacheBuilder.newBuilder().expireAfterWrite(30, TimeUnit.SECONDS)
    .maximumSize(1000).build().asInstanceOf[Cache[Int, PBlockEntry.Builder]]

  def loadFromBlock(block: Int): Iterable[PBlockEntry.Builder] = {
    loadFromBlock(block, false)
  }

  def loadFromBlock(block: Int, needBody: Boolean): Iterable[PBlockEntry.Builder] = {
    //    val ov = Daos.dposdb.get("D" + block).get
    //    if (ov != null) {
    //    recentBlocks.synchronized {
    if (block > curVN().getCurBlock) {
      null
    } else {
      //      val recentblk = recentBlocks.getIfPresent(block);
      //      if (recentblk != null) {
      //        return recentblk;
      //      }
      val blks = Daos.chainHelper.getBlocksByNumber(block);
      if (blks != null) {
        blks.asScala.filter(f => if (block == 0 || block < VCtrl.curVN().getCurBlock - VConfig.SYNC_SAFE_BLOCK_COUNT) {
          //创世块安全块允许直接广播
          true
        } else {
          // 本地block超出安全高度的是否能校验通过，只有通过的才广播
          val parentBlock = Daos.blkHelper.getBlock(Daos.enc.hexEnc(f.getHeader.getPreHash.toByteArray()));
          val nodebits = if (f.getHeader.getNumber == 1) "" else parentBlock.getMiner.getBit;
          val (hash, sign) = RandFunction.genRandHash(Daos.enc.hexEnc(f.getHeader.getPreHash.toByteArray()), parentBlock.getMiner.getTermid, nodebits);
          if (hash.equals(f.getMiner.getTermid) || f.getHeader.getNumber == 1) {
            true
          } else {
            false
          }
        }).map(f => {
          // 本地block是否能校验通过，只有通过的才广播
          if (needBody) {
            val b = PBlockEntry.newBuilder().setBlockHeader(f.toBuilder().build().toByteString()).setBlockHeight(block)
            recentBlocks.put(block, b);
            b
          } else {
            val b = PBlockEntry.newBuilder().setBlockHeader(f.toBuilder().clearBody().build().toByteString()).setBlockHeight(block)
            recentBlocks.put(block, b);
            b
          }

        })
      } else {
        log.error("blk not found in AccountDB:" + block);
        null;
      }
    }
  }
}