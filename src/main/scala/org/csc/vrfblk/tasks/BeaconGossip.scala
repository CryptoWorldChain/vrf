package org.csc.vrfblk.tasks

import java.util.List
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import org.apache.commons.lang3.StringUtils
import org.csc.ckrand.pbgens.Ckrand.GossipMiner
import org.csc.ckrand.pbgens.Ckrand.PSNodeInfo
import org.csc.ckrand.pbgens.Ckrand.VNode
import org.csc.p22p.action.PMNodeHelper
import org.csc.p22p.core.Votes
import org.csc.p22p.core.Votes.Converge
import org.csc.p22p.utils.LogHelper
import org.csc.vrfblk.utils.VConfig
import org.fc.zippo.dispatcher.SingletonWorkShop

import onight.tfw.outils.serialize.UUIDGenerator
import org.csc.ckrand.pbgens.Ckrand.PSNodeInfoOrBuilder
import org.csc.ckrand.pbgens.Ckrand.PSSyncBlocks
import org.csc.p22p.core.Votes.NotConverge
import org.csc.vrfblk.Daos
import org.csc.ckrand.pbgens.Ckrand.VNodeState
import org.csc.bcapi.exec.SRunner
import org.csc.bcapi.JodaTimeHelper
import org.csc.vrfblk.msgproc.RollbackBlock

//投票决定当前的节点
case class BRDetect(messageId: String, checktime: Long, votebase: Int, beaconHash: String);

object BeaconTask extends SRunner {
  def getName() = "beacontask"

  def runOnce() = {
    log.debug("time check try gossip past=" + JodaTimeHelper.secondFromNow(BeaconGossip.currentBR.checktime) + ",vn.hash=" + VCtrl.curVN().getBeaconHash + ",brhash=" + BeaconGossip.currentBR.beaconHash
      + ",past last block:" + JodaTimeHelper.secondFromNow(VCtrl.curVN().getCurBlockMakeTime));
    if (System.currentTimeMillis() - VCtrl.curVN().getCurBlockRecvTime > VConfig.GOSSIP_TIMEOUT_SEC * 1000) {
      log.debug("do try gossip past=" + JodaTimeHelper.secondFromNow(BeaconGossip.currentBR.checktime) + ",vn.hash=" + VCtrl.curVN().getBeaconHash + ",brhash=" + BeaconGossip.currentBR.beaconHash
        + ",past last block:" + JodaTimeHelper.secondFromNow(VCtrl.curVN().getCurBlockRecvTime));

      BeaconGossip.tryGossip();
    }
  }
}

object BeaconGossip extends SingletonWorkShop[PSNodeInfoOrBuilder] with PMNodeHelper with LogHelper {
  var running: Boolean = true;
  val incomingInfos = new ConcurrentHashMap[String, PSNodeInfoOrBuilder]();
  var currentBR: BRDetect = BRDetect(null, 0, 0, null);

  def isRunning(): Boolean = {
    return running;
  }

  def gossipBlocks() {
    try {
      currentBR = new BRDetect(UUIDGenerator.generate(), 0, VCtrl.network().directNodes.size, VCtrl.curVN().getBeaconHash);
      log.debug("put gossip::" + VCtrl.curVN());
      BeaconGossip.offerMessage(PSNodeInfo.newBuilder().setVn(VCtrl.curVN()).setIsQuery(true));
    } catch {
      case t: Throwable =>
        log.debug("error in gossip blocks:", t);
    }
  }

  def runBatch(items: List[PSNodeInfoOrBuilder]): Unit = {
    MDCSetBCUID(VCtrl.network())
    items.asScala.map(pn =>
      if (StringUtils.equals(pn.getMessageId, currentBR.messageId)) {
        if (pn.getGossipBlockInfo > 0) {
          log.debug("rollback put a new br:from= " + pn.getVn.getBcuid + ",blockheight=" + pn.getGossipMinerInfo.getCurBlock +
            ",hash=" + pn.getGossipMinerInfo.getBeaconHash + ",SEED=" + pn.getGossipMinerInfo.getBlockExtrData);

        } else {
          log.debug("put a new br:from=" + pn.getVn.getBcuid + ",blockheight=" + pn.getVn.getCurBlock + ",hash=" + pn.getVn.getCurBlockHash
            + ",BH=" + pn.getVn.getBeaconHash + ",SEED=" + pn.getVn.getVrfRandseeds + "nodeHeight=" + VCtrl.curVN().getCurBlock);
        }
        incomingInfos.put(pn.getVn.getBcuid, pn);
      })

    log.debug("gossipBlocks:beaconhash.curvn=" + VCtrl.curVN().getBeaconHash + ",br=" + currentBR.beaconHash);

    tryMerge();
    tryGossip();
  }

  def tryGossip() {
    if (System.currentTimeMillis() - currentBR.checktime > VConfig.GOSSIP_TIMEOUT_SEC * 1000
      || !StringUtils.equals(VCtrl.curVN().getBeaconHash, currentBR.beaconHash)) {
      gossipBeaconInfo();
    }
  }

  def gossipBeaconInfo(gossipBlock: Int = -1) {
    val messageId = UUIDGenerator.generate();
    currentBR = new BRDetect(messageId, System.currentTimeMillis(), VCtrl.network().directNodes.size, VCtrl.curVN().getBeaconHash);

    val vn = VCtrl.curVN().clone();
    val lastBlock = Daos.chainHelper.GetConnectBestBlock();
    if (lastBlock != null) {
      vn.setCurBlock(lastBlock.getHeader.getNumber.intValue);
      vn.setCurBlockHash(Daos.enc.hexEnc(lastBlock.getHeader.getHash.toByteArray()));
      vn.setPrevBlockHash(Daos.enc.hexEnc(lastBlock.getHeader.getPreHash.toByteArray()));
    }
    
    val body = PSNodeInfo.newBuilder().setMessageId(messageId).setVn(vn).setIsQuery(true);
    VCtrl.coMinerByUID.map(m => {
      body.addMurs(GossipMiner.newBuilder().setBcuid(m._2.getBcuid).setCurBlock(m._2.getCurBlock))
    })
    //get all vote block
    incomingInfos.clear();
    MDCSetMessageID(messageId);
    if (gossipBlock > 0) {
      body.setGossipBlockInfo(gossipBlock);
    }
    log.debug("gen a new gossipinfo,vcounts=" + currentBR.votebase + ",DN=" + currentBR.votebase
      + ",BH=" + currentBR.beaconHash + ",gossipBlock=" + gossipBlock);
    VCtrl.network().dwallMessage("INFVRF", Left(body.build()), messageId);
  }

  def syncBlock(maxHeight: Int, suggestStartIdx: Int, frombcuid: String) {
    val messageId = UUIDGenerator.generate();
    currentBR = new BRDetect(messageId, System.currentTimeMillis(), VCtrl.network().directNodes.size, VCtrl.curVN().getBeaconHash);
    //
    incomingInfos.clear();

    VCtrl.curVN().setState(VNodeState.VN_DUTY_SYNC)
    val sync = PSSyncBlocks.newBuilder().setStartId(suggestStartIdx)
      .setEndId(Math.min(maxHeight, suggestStartIdx + VConfig.MAX_SYNC_BLOCKS)).setNeedBody(true).setMessageId(messageId).build()
    BlockSync.offerMessage(new SyncBlock(frombcuid, sync))
  }

  def tryMerge(): Unit = {
    val size = incomingInfos.size();
    if (size > 0 && size >= currentBR.votebase * 2 / 3) {
      //
      val checkList = new ListBuffer[VNode]();
      var maxHeight = VCtrl.instance.heightBlkSeen.get;
      var frombcuid = "";
      // var suggestStartIdx = Math.max(1, VCtrl.curVN().getCurBlock - 1);
      var suggestStartIdx = Math.max(1, Daos.chainHelper.getLastBlockNumber() - 1);

      incomingInfos.asScala.values.map({ p =>
        if (p.getVn.getCurBlock > maxHeight) {
          maxHeight = p.getVn.getCurBlock;
          frombcuid = p.getVn.getBcuid;
        }
        if (p.getSugguestStartSyncBlockId < suggestStartIdx 
          && suggestStartIdx > VCtrl.curVN().getCurBlock - VConfig.SYNC_SAFE_BLOCK_COUNT
          && !p.getVn.getBcuid.equals(VCtrl.curVN().getBcuid)) {
          log.debug("set SugguestStartSyncBlockId = " + p.getSugguestStartSyncBlockId + ",from = " + p.getVn.getBcuid);
          suggestStartIdx = p.getSugguestStartSyncBlockId;
        }
        if (p.getGossipBlockInfo > 0) {

          log.debug("rollback setgetGossipBlockInfo= " + p.getGossipMinerInfo.getCurBlock + ",from = " + p.getVn.getBcuid
            + ",hash=" + p.getGossipMinerInfo.getBeaconHash + ",b=" + p.getGossipMinerInfo.getCurBlock);

          log.debug("set vrfrandseed::" + p.getGossipMinerInfo.getBlockExtrData);

          checkList.+=(VNode.newBuilder().setCurBlock(p.getGossipMinerInfo.getCurBlock)
            .setCurBlockHash(p.getGossipMinerInfo.getCurBlockHash)
            .setBeaconHash(p.getGossipMinerInfo.getBeaconHash)
            .setVrfRandseeds(p.getGossipMinerInfo.getBlockExtrData) // netbits
            .build());
        } else {
          log.debug(" beacon gossip:: getCurBlock=" + p.getVn.getCurBlock + " getCurBlockHash==" + p.getVn.getCurBlockHash + " getBeaconHash=" + p.getVn.getBeaconHash + " getVrfRandseeds=" + p.getVn.getVrfRandseeds);

          checkList.+=(p.getVn);
        }
      })
      suggestStartIdx = Math.max(suggestStartIdx, VCtrl.curVN().getCurBlock - VConfig.SYNC_SAFE_BLOCK_COUNT);

      if (maxHeight > VCtrl.instance.heightBlkSeen.get) {
        VCtrl.instance.heightBlkSeen.set(maxHeight);
      }

      //Node State Vote 查看自己是不是2/3中的一员
      Votes.vote(checkList).PBFTVote(n => {
        Some((n.getCurBlock, n.getCurBlockHash, n.getBeaconHash, n.getVrfRandseeds))
      }, currentBR.votebase) match {
        case Converge((height: Int, blockHash: String, hash: String, randseed: String)) =>
          log.info("get merge beacon bh = :" + blockHash + ",hash=" + hash + ",height=" + height + ",currentheight="
            + VCtrl.instance.cur_vnode.getCurBlock + ",suggestStartIdx=" + suggestStartIdx);
          incomingInfos.clear();
          if (maxHeight > VCtrl.curVN().getCurBlock) {
            //sync first
            // syncBlock(maxHeight, suggestStartIdx, frombcuid);
            syncBlock(height, suggestStartIdx.intValue, frombcuid);
          } else {
            NodeStateSwitcher.offerMessage(new BeaconConverge(height, blockHash, hash, randseed));
          }
        case n: NotConverge =>
          log.info("cannot get converge for pbft vote:" + checkList.size + ",incomingInfos=" + incomingInfos.size + ",suggestStartIdx=" + suggestStartIdx
            + ",messageid=" + currentBR.messageId);
          //find

          if (maxHeight > VCtrl.curVN().getCurBlock) {
            //sync first
            incomingInfos.clear();
            syncBlock(maxHeight, suggestStartIdx.intValue, frombcuid);
          } else if (size >= currentBR.votebase * 4 / 5) {
            incomingInfos.clear();
            tryRollbackBlock();
          }
        case n@_ =>
          log.debug("need more results:" + checkList.size + ",incomingInfos=" + incomingInfos.size
            + ",n=" + n + ",vcounts=" + currentBR.votebase + ",suggestStartIdx=" + suggestStartIdx
            + ",messageid=" + currentBR.messageId);
          if (maxHeight > VCtrl.curVN().getCurBlock) {
            //sync first
            incomingInfos.clear();
            syncBlock(maxHeight, suggestStartIdx.intValue, frombcuid);
          } else if (size >= currentBR.votebase * 4 / 5) {
            incomingInfos.clear();
            tryRollbackBlock();
          }
      };

    }
  }

  def tryRollbackBlock() {

    incomingInfos.clear();
    log.info("rollback --> need to , beacon not merge!:curblock = " + VCtrl.curVN().getCurBlock);
    //            BlockProcessor.offerMessage(new RollbackBlock(VCtrl.curVN().getCurBlock - 1))
    var startBlock = VCtrl.curVN().getCurBlock - 1;
    while (startBlock > VCtrl.curVN().getCurBlock - VConfig.SYNC_SAFE_BLOCK_COUNT && startBlock > 0) {
      val blks = Daos.chainHelper.getBlocksByNumber(startBlock);
      if (blks != null && blks.size() == 1) {
        val messageId = UUIDGenerator.generate();
        log.debug("rollback --> start to gossip from starBlock:" + (startBlock));
        BeaconGossip.gossipBeaconInfo(startBlock)
        startBlock = -100;
      } else {
        startBlock = startBlock - 1;
      }
    }
  }
}