package org.brewchain.vrfblk.tasks

import java.util.List
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import org.apache.commons.lang3.StringUtils
import org.brewchain.bcrand.model.Bcrand.GossipMiner
import org.brewchain.bcrand.model.Bcrand.PSNodeInfo
import org.brewchain.bcrand.model.Bcrand.VNode
import org.brewchain.p22p.action.PMNodeHelper
import org.brewchain.p22p.core.Votes
import org.brewchain.p22p.core.Votes.Converge
import org.brewchain.p22p.utils.LogHelper
import org.brewchain.vrfblk.utils.VConfig
import org.fc.zippo.dispatcher.SingletonWorkShop
import org.brewchain.vrfblk.utils.SRunner
import onight.tfw.outils.serialize.UUIDGenerator
import org.brewchain.bcrand.model.Bcrand.PSNodeInfoOrBuilder
import org.brewchain.bcrand.model.Bcrand.PSSyncBlocks
import org.brewchain.p22p.core.Votes.NotConverge
import org.brewchain.vrfblk.Daos
import org.brewchain.bcrand.model.Bcrand.VNodeState
import org.brewchain.mcore.tools.time.JodaTimeHelper
import org.brewchain.vrfblk.msgproc.RollbackBlock
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicInteger
import java.math.BigInteger
import scala.collection.mutable.Buffer
import org.brewchain.vrfblk.action.PSCoinbaseNewService

//投票决定当前的节点
case class BRDetect(messageId: String, checktime: Long, votebase: Int, beaconHash: String);

object BeaconTask extends SRunner {
  def getName() = "beacontask"

  def runOnce() = {
    log.info("time check try gossip past=" + JodaTimeHelper.secondFromNow(BeaconGossip.currentBR.checktime) + ",vn.hash=" + VCtrl.curVN().getBeaconHash + ",brhash=" + BeaconGossip.currentBR.beaconHash
      + ",past last block:" + JodaTimeHelper.secondFromNow(VCtrl.curVN().getCurBlockMakeTime));
    if (System.currentTimeMillis() - VCtrl.curVN().getCurBlockMakeTime > VConfig.GOSSIP_TIMEOUT_SEC * 1000) {
      log.info("do try gossip past=" + JodaTimeHelper.secondFromNow(BeaconGossip.currentBR.checktime) + ",vn.hash=" + VCtrl.curVN().getBeaconHash + ",brhash=" + BeaconGossip.currentBR.beaconHash
        + ",past last block:" + JodaTimeHelper.secondFromNow(VCtrl.curVN().getCurBlockMakeTime));

      BeaconGossip.tryGossip("schedule_tick");
    }
  }
}

object BeaconGossip extends SingletonWorkShop[PSNodeInfoOrBuilder] with PMNodeHelper with LogHelper {
  var running: Boolean = true;
  val incomingInfos = new ConcurrentHashMap[String, PSNodeInfoOrBuilder]();
  var currentBR: BRDetect = BRDetect(null, 0, 0, null);
  var lastSyncBlockHeight: Int = 0;
  var lastSyncBlockCount: Int = 0;
  var lastGossipTime: Long = 0L;
  var rollbackGossipNetBits = "";

  def isRunning(): Boolean = {
    return running;
  }

  def gossipBlocks() {
    this.synchronized {
      try {
        currentBR = new BRDetect(UUIDGenerator.generate(), 0, VCtrl.network().directNodes.size, VCtrl.curVN().getBeaconHash);
        //log.debug("put gossip::" + VCtrl.curVN());
        BeaconGossip.offerMessage(PSNodeInfo.newBuilder().setVn(VCtrl.curVN()).setIsQuery(true));
      } catch {
        case t: Throwable =>
          log.error("error in gossip blocks:", t);
      }
    }
  }
  def clearGossipInfo(): Unit = {
    currentBR = new BRDetect("", 0, VCtrl.network().directNodes.size, VCtrl.curVN().getBeaconHash);
  }
  def runBatch(items: List[PSNodeInfoOrBuilder]): Unit = {
    MDCSetBCUID(VCtrl.network())

    val isize = items.size();
    items.asScala.map(pn =>
      if (StringUtils.equals(pn.getMessageId, currentBR.messageId)) {
        if (pn.getGossipBlockInfo > 0) {
          //log.debug("rollback put a new br:from= " + pn.getVn.getBcuid + ",blockheight=" + pn.getGossipMinerInfo.getCurBlock +
          //  ",hash=" + pn.getGossipMinerInfo.getBeaconHash + ",SEED=" + pn.getGossipMinerInfo.getBlockExtrData);

        } else {
          //log.info("put a new br:from=" + pn.getVn.getBcuid + ",blockheight=" + pn.getVn.getCurBlock + ",hash=" + pn.getVn.getCurBlockHash
          //  + ",BH=" + pn.getVn.getBeaconHash + ",SEED=" + pn.getVn.getVrfRandseeds + "nodeHeight=" + VCtrl.curVN().getCurBlock);
        }
        incomingInfos.put(pn.getVn.getBcuid, pn);
      })

    //log.debug("gossipBlocks:beaconhash.curvn=" + VCtrl.curVN().getBeaconHash + ",br=" + currentBR.beaconHash);

    //log.info("beacongossip runbatch, infos=" + incomingInfos.size() + " items=" + isize);

    if (tryMerge()) {
      tryGossip("merge");
    }
  }

  def tryGossip(reason: String) {
    if (System.currentTimeMillis() - currentBR.checktime > VConfig.GOSSIP_TIMEOUT_SEC * 1000) { //|| !StringUtils.equals(VCtrl.curVN().getBeaconHash, currentBR.beaconHash)) {
      log.info("do gossipBeaconInfo, checktime=" + currentBR.checktime + ",reason=" + reason);
      gossipBeaconInfo();
    }
  }

  def gossipBeaconInfo(gossipBlock: Int = -1) {
    val messageId = UUIDGenerator.generate();
    log.info("start gossipBeaconInfo, infos=" + incomingInfos.size + " msgid=" + messageId + ",gossipBlock=" + gossipBlock)

    var bits = BigInteger.ZERO
    VCtrl.network().directNodes.foreach(f => {
      if (!VConfig.AUTH_NODE_FILTER || VCtrl.haveEnoughToken(f.v_address)) {
        bits = bits.setBit(f.node_idx);
      }
      VCtrl.banMinerByUID.get(f.bcuid) match {
        case Some(v) =>
          if (v._1 < VCtrl.curVN().getCurBlock - VConfig.SYNC_SAFE_BLOCK_COUNT) {
            bits = bits.clearBit(f.node_idx);
          }
        case _ =>
      }

    })

    currentBR = new BRDetect(messageId, System.currentTimeMillis(), bits.bitCount(), VCtrl.curVN().getBeaconHash);
    //    currentBR = new BRDetect(messageId, System.currentTimeMillis(), VCtrl.network().directNodes.size, VCtrl.curVN().getBeaconHash);

    val body = PSNodeInfo.newBuilder().setMessageId(messageId).setVn(VCtrl.curVN()).setIsQuery(true);
    val removeList = ListBuffer.empty[String]
    VCtrl.coMinerByUID.map(m => {
      body.addMurs(GossipMiner.newBuilder().setBcuid(m._2.getBcuid).setCurBlock(m._2.getCurBlock).setCurBlockHash(m._2.getCurBlockHash))

      if (VCtrl.curVN().getCurBlock - m._2.getCurBlock > VConfig.SYNC_SAFE_BLOCK_COUNT) {
        log.info("remove cominer for block distance too large:" + m._2.getCurBlock + "==>" + VCtrl.curVN().getCurBlock)
        removeList.append(m._2.getBcuid)
      }
    })
    removeList.map(n => VCtrl.removeCoMiner(n))
    //get all vote block
    incomingInfos.clear();
    MDCSetMessageID(messageId);
    if (gossipBlock > 0) {
      body.setGossipBlockInfo(gossipBlock);
    }
    log.info("gen a new gossipinfo,vcounts=" + currentBR.votebase + ",DN=" + currentBR.votebase
      + ",BH=" + currentBR.beaconHash + ",gossipBlock=" + gossipBlock);
    //    VCtrl.network().dwallMessage("INFVRF", Left(body.build()), messageId);
    VCtrl.network().bwallMessage("INFVRF", Left(body.build()), bits, messageId);
  }

  def syncBlock(maxHeight: Int, suggestStartIdx: Int, frombcuid: String) {
    val messageId = UUIDGenerator.generate();
    currentBR = new BRDetect(messageId, System.currentTimeMillis(), VCtrl.network().directNodes.size, VCtrl.curVN().getBeaconHash);
    //
    val dbHeight: Int = Math.toIntExact(Daos.chainHelper.getLastConnectedBlockHeight)
    val startID = Math.min(dbHeight, suggestStartIdx)
    val endId = Math.min(maxHeight, startID + VConfig.MAX_SYNC_BLOCKS);
    incomingInfos.clear();
    var fastFromBcuid: String = null;
    val randList = Buffer.empty[String]
    val randSameLocList = Buffer.empty[String]

    VCtrl.coMinerByUID.filter(f => (!f._1.equals(VCtrl.curVN().getBcuid) && f._2.getCurBlock >= endId)).map(f => {
      val bcuid = f._1;
      val vnode = f._2;
      val locktime = VCtrl.syncMinerErrorByBCUID.get(bcuid).getOrElse(0L)
      if (System.currentTimeMillis() - locktime > VConfig.BLOCK_DISTANCE_WAITMS) {
        //        if (StringUtils.isBlank(bcuid) || StringUtils.equals(bcuid, VCtrl.curVN().getBcuid)) {
        //          fastFromBcuid = bcuid;
        //        }
        if (StringUtils.equals(VCtrl.network().nodeByBcuid(bcuid).loc_gwuris, VCtrl.network().root().loc_gwuris)) {
          //          fastFromBcuid = bcuid;
          randSameLocList.append(bcuid)
        } else {
          randList.append(bcuid);
        }
      }
    })
    if (randSameLocList.size > 0) {
      fastFromBcuid = randSameLocList(Math.abs(messageId.hashCode()) % randSameLocList.size);
    } else if (randList.size > 0) {
      fastFromBcuid = randList(Math.abs(messageId.hashCode()) % randList.size)
    } else {
      fastFromBcuid = frombcuid;
    }

    VCtrl.curVN().setState(VNodeState.VN_DUTY_SYNC)
    //从AccountDB中读取丢失高度，防止回滚时当前节点错误块过高或缺失导致起始位置错误

    val sync = PSSyncBlocks.newBuilder().setStartId(startID)
      .setMaxHeight(maxHeight)
      .setEndId(endId).setNeedBody(true).setMessageId(messageId).build()

    log.info("syncblock  maxHeight=" + maxHeight + ",startid=" + startID + ",frombcuid=" + frombcuid + ",fastFromBcuid=" + fastFromBcuid)
    BlockSync.offerMessage(new SyncBlock(fastFromBcuid, sync))
  }

  def tryMerge(): Boolean = {
    val size = incomingInfos.size();
    if (size > 0 && size >= currentBR.votebase * 2 / 3) {
      //
      val checkList = new ListBuffer[(Int, String, String, String)]();
      // var maxHeight = 0; //VCtrl.instance.heightBlkSeen.get;
      var maxHeight = Math.max(0, Daos.chainHelper.getLastConnectedBlockHeight() - 1).intValue;
      var frombcuid = "";
      var rollbackBlock = false;
      var suggestStartIdx = Math.max(1, VCtrl.curVN().getCurBlock - 1);
      // var suggestStartIdx = Math.max(1, Daos.chainHelper.getLastBlockNumber() - 1);
      var maxHeightSeenCount = 0;
      incomingInfos.asScala.values.map({ p =>
        if (p.getVn.getCurBlock > maxHeight) {
          maxHeight = p.getVn.getCurBlock;
          frombcuid = p.getVn.getBcuid;
        }

        if (p.getSugguestStartSyncBlockId < suggestStartIdx
          && p.getSugguestStartSyncBlockId > VCtrl.curVN().getCurBlock - VConfig.SYNC_SAFE_BLOCK_COUNT
          && !p.getVn.getBcuid.equals(VCtrl.curVN().getBcuid)) {
          log.debug("set SugguestStartSyncBlockId = " + p.getSugguestStartSyncBlockId + ",from = " + p.getVn.getBcuid);
          suggestStartIdx = p.getSugguestStartSyncBlockId;
        }
        if (p.hasGossipMinerInfo()) {
          if (p.getGossipBlockInfo > 0) {
            rollbackBlock = true;
            //log.debug("rollback setgetGossipBlockInfo= " + p.getGossipMinerInfo.getCurBlock + ",from = " + p.getVn.getBcuid
            //  + ",hash=" + p.getGossipMinerInfo.getBeaconHash + ",b=" + p.getGossipMinerInfo.getCurBlock);

            //log.debug("set vrfrandseed::" + p.getGossipMinerInfo.getBlockExtrData);

            log.info(" rollbackBlock beacon gossipblock=" + p.getGossipBlockInfo + ":: getCurBlock=" + p.getGossipMinerInfo.getCurBlock + " getCurBlockHash==" + p.getGossipMinerInfo.getCurBlockHash + " getBeaconHash=" + p.getGossipMinerInfo.getBeaconHash + " getVrfRandseeds=" + p.getGossipMinerInfo.getBlockExtrData);
            //          checkList.+=(VNode.newBuilder().setCurBlock(p.getGossipMinerInfo.getCurBlock)
            //            .setCurBlockHash(p.getGossipMinerInfo.getCurBlockHash)
            //            .setBeaconHash(p.getGossipMinerInfo.getBeaconHash)
            //            .setVrfRandseeds(p.getGossipMinerInfo.getBlockExtrData) // netbits
            //            .build());
            checkList.+=((p.getGossipMinerInfo.getCurBlock, p.getGossipMinerInfo.getCurBlockHash, p.getGossipMinerInfo.getBeaconHash, p.getGossipMinerInfo.getBlockExtrData))
          } else {
            log.info(" beacon gossip:: getCurBlock=" + p.getGossipMinerInfo.getCurBlock + " getCurBlockHash==" + p.getGossipMinerInfo.getCurBlockHash + " getBeaconHash=" + p.getGossipMinerInfo.getBeaconHash + " getVrfRandseeds=" + p.getGossipMinerInfo.getBlockExtrData);
            //          checkList.+=(p.getVn);
            checkList.+=((p.getGossipMinerInfo.getCurBlock, p.getGossipMinerInfo.getCurBlockHash, p.getGossipMinerInfo.getBeaconHash, p.getGossipMinerInfo.getBlockExtrData))
          }
        } else {
          log.info("no data for cur block");
          checkList.+=((p.getVn.getCurBlock, p.getVn.getCurBlockHash, p.getVn.getBeaconHash, p.getVn.getVrfRandseeds))
        }
      })
      suggestStartIdx = Math.max(suggestStartIdx, VCtrl.curVN().getCurBlock - VConfig.SYNC_SAFE_BLOCK_COUNT);

      if (maxHeight > VCtrl.instance.heightBlkSeen.get) {
        VCtrl.instance.heightBlkSeen.set(maxHeight);
      }
      incomingInfos.asScala.values.map({ p =>
        if (p.getVn.getCurBlock >= maxHeight) {
          maxHeightSeenCount = maxHeightSeenCount + 1;
        }
      })
      //Node State Vote 查看自己是不是2/3中的一员
      Votes.vote(checkList).PBFTVote(n => {
        Some((n._1, n._2, n._3, n._4))
      }, Math.min(VConfig.MAX_BLOCK_MAKER, currentBR.votebase)) match {
        case Converge((height: Int, blockHash: String, hash: String, randseed: String)) =>
          tryRollBackCounter.set(0);
          log.info("get merge beacon bh = :" + blockHash + ",hash=" + hash + ",height=" + height + ",randseed=" + randseed + ",currentheight="
            + VCtrl.curVN().getCurBlock + ",suggestStartIdx=" + suggestStartIdx + ",rollbackBlock=" + rollbackBlock
            + ",msgid=" + currentBR.messageId + " maxHeight=" + maxHeight + ",frombcuid=" + frombcuid
            +",blockqsize="+PSCoinbaseNewService.queue.size());
          incomingInfos.clear();
          clearGossipInfo();
          if (maxHeight > VCtrl.curVN().getCurBlock &&
            (!rollbackBlock || maxHeightSeenCount >= checkList.size / 3)) {
            //sync first
            // 投出来的最大高度
            if (VCtrl.curVN().getCurBlock + PSCoinbaseNewService.queue.size() * 2 < maxHeight) {
              syncBlock(maxHeight, suggestStartIdx, frombcuid);
            }
          } else {
            if (rollbackBlock) {
              rollbackGossipNetBits = randseed;
              val dbblock = Daos.chainHelper.getBlockByHash(Daos.enc.hexStrToBytes(blockHash));
              if (dbblock != null && dbblock.getHeader.getHeight == height) {
                //rollback是成功的，往下一个块走
                log.info("ConvergeToRollback.ok:height=" + height + ",blockHash=" + blockHash + ",hash=" + hash + ",randseed=" + randseed);
                NodeStateSwitcher.offerMessage(new BeaconConverge(height, blockHash, hash, ""));
              } else {
                log.info("ConvergeToRollback.failed:height=" + height + ",blockHash=" + blockHash + ",dbblock=" + dbblock);
              }
            } else {
              rollbackGossipNetBits = "";
              if (height >= VCtrl.curVN().getCurBlock) {
                NodeStateSwitcher.offerMessage(new BeaconConverge(height, blockHash, hash, randseed));
              }
            }
          }
          false

        case n: NotConverge =>
          // log.info("cannot get converge for pbft vote:" + checkList.size + "/" + currentBR.votebase + ",incomingInfos=" + incomingInfos.size + ",suggestStartIdx=" + suggestStartIdx
          //   + ",messageid=" + currentBR.messageId + ",curblk=" + VCtrl.curVN().getCurBlock + ",maxHeight=" + maxHeight + ",lastSyncBlockCount=" + lastSyncBlockCount + ",lastSyncBlockHeight=" + lastSyncBlockHeight);

          if (lastSyncBlockHeight != suggestStartIdx.intValue()) {
            lastSyncBlockCount = 1;
          } else {
            lastSyncBlockCount = lastSyncBlockCount + 1;
          }
          incomingInfos.clear();
          //          clearGossipInfo();

          log.info("suggestStartIdx="
            + suggestStartIdx + " maxHeight=" + maxHeight + " curblk=" + VCtrl.curVN().getCurBlock + " lastSyncBlockCount=" + lastSyncBlockCount + ",lastSyncBlockHeight=" + lastSyncBlockHeight)
          if (maxHeight > VCtrl.curVN().getCurBlock) {
            //sync first
            // log.debug("try to syncBlock:maxHeight" + maxHeight + ",curblk=" + VCtrl.curVN().getCurBlock + ",suggestStartIdx=" + suggestStartIdx + ",lastSyncBlockCount=" + lastSyncBlockCount + ",lastSyncBlockHeight=" + lastSyncBlockHeight);
            lastSyncBlockHeight = suggestStartIdx;
            syncBlock(maxHeight, suggestStartIdx.intValue, frombcuid);
          } else if (suggestStartIdx > 0) {
            // tryRollbackBlock(suggestStartIdx);
          }
          true
        case n @ _ =>
          log.info("need more results:" + checkList.size + ",incomingInfos=" + incomingInfos.size
            + ",n=" + n + ",vcounts=" + currentBR.votebase + ",suggestStartIdx=" + suggestStartIdx
            + ",messageid=" + currentBR.messageId
            + ",maxHeight=" + maxHeight
            + ",curblock=" + VCtrl.curVN().getCurBlock);

          if (maxHeight > VCtrl.curVN().getCurBlock) {
            //sync first
            //            clearGossipInfo();
            syncBlock(maxHeight, suggestStartIdx.intValue, frombcuid);

            incomingInfos.clear();
          } else if (size >= currentBR.votebase * 4 / 5) {
            //            log.info("try rollback");
            //            clearGossipInfo();
            incomingInfos.clear();
            //            tryRollbackBlock();
          } else {
            log.info("wait more results");
          }
          false
      };
    } else {
      log.info("need more results size=" + size + " vb=" + currentBR.votebase + ",msgid=" + currentBR.messageId)

      size == 0
    }
  }

  val tryRollBackCounter = new AtomicInteger(0);
  def tryRollbackBlock(suggestGossipBlock: Int = VCtrl.curVN().getCurBlock) {

    incomingInfos.clear();
    // log.info("rollback  --> need to , beacon not merge!:curblock = " + VCtrl.curVN().getCurBlock + ",suggestGossipBlock=" + suggestGossipBlock);
    //            BlockProcessor.offerMessage(new RollbackBlock(VCtrl.curVN().getCurBlock - 1))
    var startBlock = suggestGossipBlock - tryRollBackCounter.incrementAndGet() / 3;
    if (tryRollBackCounter.get >= 30) {
      tryRollBackCounter.set(0);
    }
    while (startBlock > VCtrl.curVN().getCurBlock - VConfig.SYNC_SAFE_BLOCK_COUNT && startBlock > 0) {
      val blks = Daos.chainHelper.listBlockByHeight(startBlock);
      if (blks != null && blks.length == 1) {
        log.info("rollback --> start to gossip from starBlock:" + (startBlock));
        BeaconGossip.gossipBeaconInfo(startBlock)
        startBlock = -100;
      } else {
        startBlock = startBlock - 1;
      }
    }
  }
}