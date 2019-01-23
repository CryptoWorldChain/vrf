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

//投票决定当前的节点
case class BRDetect(messageId: String, checktime: Long, votebase: Int);

object BeaconGossip extends SingletonWorkShop[PSNodeInfoOrBuilder] with PMNodeHelper with LogHelper {
  var running: Boolean = true;
  val incomingInfos = new ConcurrentHashMap[String, PSNodeInfoOrBuilder]();
  var currentBR: BRDetect = BRDetect(null, 0, 0);

  def isRunning(): Boolean = {
    return running;
  }

  def gossipBlocks() {
    BeaconGossip.offerMessage(PSNodeInfo.newBuilder().setVn(VCtrl.curVN()));
  }
  def runBatch(items: List[PSNodeInfoOrBuilder]): Unit = {
    items.asScala.map(pn =>
      if (StringUtils.equals(pn.getMessageId, currentBR.messageId)) {
        log.debug("put a new br:" + pn);
        incomingInfos.put(pn.getVn.getBcuid, pn);
      })

    tryMerge();
    if (System.currentTimeMillis() - currentBR.checktime > VConfig.GOSSIP_TIMEOUT_SEC * 1000) {
      gossipBeaconInfo();
    }
  }

  def gossipBeaconInfo() {
    val messageId = UUIDGenerator.generate();
    currentBR = new BRDetect(messageId, System.currentTimeMillis(), VCtrl.network().directNodes.size);

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
        Some((n.getBeaconSign, n.getBeaconHash))
      }, currentBR.votebase) match {
        case Converge((sign: String, hash: String)) =>
          log.info("get merge beacon sign = :" + sign + ",hash=" + hash);
          NodeStateSwither.offerMessage(new BeaconConverge(sign, hash));
          incomingInfos.clear();
        case _ =>
          log.info("cannot get converge for pbft vote:" + checkList + ",incomingInfos=" + incomingInfos.size + ":" + incomingInfos + ",currentBR=" + currentBR);
          //find
          val messageId = UUIDGenerator.generate();
          currentBR = new BRDetect(messageId, System.currentTimeMillis(), VCtrl.network().directNodes.size);
          //
          val sync = PSSyncBlocks.newBuilder().setStartId(suggestStartIdx)
            .setEndId(Math.min(maxHeight, suggestStartIdx + VConfig.MAX_SYNC_BLOCKS)).setNeedBody(true).setMessageId(messageId).build()
          BlockSync.offerMessage(new SyncBlock(frombcuid, sync))
      };

    }
  }
}