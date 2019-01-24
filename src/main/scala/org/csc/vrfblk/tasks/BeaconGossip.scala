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

//投票决定当前的节点
case class BRDetect(messageId: String, checktime: Long, votebase: Int, beaconHash: String);

object BeaconGossip extends SingletonWorkShop[PSNodeInfoOrBuilder] with PMNodeHelper with LogHelper {
  var running: Boolean = true;
  val incomingInfos = new ConcurrentHashMap[String, PSNodeInfoOrBuilder]();
  var currentBR: BRDetect = BRDetect(null, 0, 0, null);

  def isRunning(): Boolean = {
    return running;
  }

  def gossipBlocks() {
    BeaconGossip.offerMessage(PSNodeInfo.newBuilder().setVn(VCtrl.curVN()));
  }
  def runBatch(items: List[PSNodeInfoOrBuilder]): Unit = {
    MDCSetBCUID(VCtrl.network())
    items.asScala.map(pn =>
      if (StringUtils.equals(pn.getMessageId, currentBR.messageId)) {
        log.debug("put a new br:" + pn);
        incomingInfos.put(pn.getVn.getBcuid, pn);
      })

    tryMerge();
    if (System.currentTimeMillis() - currentBR.checktime > VConfig.GOSSIP_TIMEOUT_SEC * 1000
      || !StringUtils.equals(VCtrl.curVN().getBeaconHash, currentBR.beaconHash)) {
      gossipBeaconInfo();
    }
  }

  def gossipBeaconInfo() {
    val messageId = UUIDGenerator.generate();
    currentBR = new BRDetect(messageId, System.currentTimeMillis(), VCtrl.network().directNodes.size,VCtrl.curVN().getBeaconHash);

    val body = PSNodeInfo.newBuilder().setMessageId(messageId).setVn(VCtrl.curVN());
    VCtrl.coMinerByUID.map(m => {
      body.addMurs(GossipMiner.newBuilder().setBcuid(m._2.getBcuid).setCurBlock(m._2.getCurBlock))
    })
    //get all vote block
    incomingInfos.clear();
    MDCSetMessageID(messageId);
    log.debug("gen a new gossipinfo:" + body + ",network=" + VCtrl.network());
    VCtrl.network().dwallMessage("INFVRF", Left(body.build()), messageId);
  }

  def tryMerge(): Unit = {
    val size = incomingInfos.size();
    if (size > 0 && size >= currentBR.votebase * 2 / 3) {
      //
      val checkList = new ListBuffer[VNode]();
      var maxHeight = VCtrl.instance.heightBlkSeen.get;
      var frombcuid = "";
      var suggestStartIdx = VCtrl.instance.cur_vnode.getCurBlock + 1;
      incomingInfos.asScala.values.map({ p =>
        if (p.getVn.getCurBlock > maxHeight) {
          maxHeight = p.getVn.getCurBlock;
          frombcuid = p.getVn.getBcuid;
        }
        checkList.+=(p.getVn);
      })
      if (maxHeight > VCtrl.instance.heightBlkSeen.get) {
        VCtrl.instance.heightBlkSeen.set(maxHeight);
      }
      Votes.vote(checkList).PBFTVote(n => {
        Some((n.getBeaconSign, n.getBeaconHash, Daos.enc.hexEnc(n.getVrfRandseeds.toByteArray())))
      }, currentBR.votebase) match {
        case Converge((sign: String, hash: String, randseed: String)) =>
          log.info("get merge beacon sign = :" + sign + ",hash=" + hash);
          incomingInfos.clear();
          NodeStateSwither.offerMessage(new BeaconConverge(sign, hash));
        case n: NotConverge =>
          log.info("cannot get converge for pbft vote:" + checkList.size + ",incomingInfos=" + incomingInfos.size + ",suggestStartIdx=" + suggestStartIdx
            + ",messageid=" + currentBR.messageId);
          //find
          val messageId = UUIDGenerator.generate();
          currentBR = new BRDetect(messageId, System.currentTimeMillis(), VCtrl.network().directNodes.size,VCtrl.curVN().getBeaconHash);
          //
          incomingInfos.clear();
          if (maxHeight > VCtrl.curVN().getCurBlock) {
            VCtrl.curVN().setState(VNodeState.VN_DUTY_SYNC)
            val sync = PSSyncBlocks.newBuilder().setStartId(suggestStartIdx)
              .setEndId(Math.min(maxHeight, suggestStartIdx + VConfig.MAX_SYNC_BLOCKS)).setNeedBody(true).setMessageId(messageId).build()
            BlockSync.offerMessage(new SyncBlock(frombcuid, sync))
          }
        case n @ _ =>
          log.debug("need more results:" + checkList.size + ",incomingInfos=" + incomingInfos.size
            + ",n=" + n + ",vcounts=" + currentBR.votebase + ",suggestStartIdx=" + suggestStartIdx
            + ",messageid=" + currentBR.messageId);
      };

    }
  }
}