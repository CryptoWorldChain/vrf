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

//投票决定当前的节点
object BeaconGossip extends SingletonWorkShop[PSNodeInfoOrBuilder] with PMNodeHelper with LogHelper {
  var running: Boolean = true;
  val incomingInfos = new ConcurrentHashMap[String, VNode]();
  case class BRDetect(messageId: String, checktime: Long, votebase: Int);
  var currentBR: BRDetect = BRDetect(null, 0, 0);

  def isRunning(): Boolean = {
    return running;
  } 

  def runBatch(items: List[PSNodeInfoOrBuilder]): Unit = {
    items.asScala.map(pn =>
      if (StringUtils.equals(pn.getMessageId, currentBR.messageId)) {
        incomingInfos.put(pn.getVn.getBcuid, pn.getVn);
      })

    tryMerge();
    if (System.currentTimeMillis() - currentBR.checktime > VConfig.GOSSIP_TIMEOUT_SEC) {
      gossipBeaconInfo();
    }
  }

  def gossipBeaconInfo() {
    val messageId = UUIDGenerator.generate();
    currentBR = new BRDetect(UUIDGenerator.generate(), System.currentTimeMillis(), VCtrl.network().directNodes.size);
    val body = PSNodeInfo.newBuilder().setMessageId(messageId).setVn(VCtrl.curVN());
    VCtrl.coMinerByUID.map(m => {
      body.addMurs(GossipMiner.newBuilder().setBcuid(m._2.getBcuid).setCurBlock(m._2.getCurBlock))
    })
    //get all vote block
    VCtrl.network().dwallMessage("INFVRF", Left(body.build()), messageId);
    incomingInfos.clear();
  }

  def tryMerge(): Unit = {

    if (incomingInfos.size() >= currentBR.votebase * 2 / 3) {
      //
      val checkList = new ListBuffer[VNode]();
      incomingInfos.asScala.values.map(checkList.+=(_));

      Votes.vote(checkList).PBFTVote(n => {
        Some((n.getBeaconSign, n.getBeaconHash))
      }, currentBR.votebase) match {
        case Converge((sign: String, hash: String)) =>
          log.info("get merge beacon sign = :" + sign + ",hash=" + hash);
          NodeStateSwither.offerMessage(new BeaconConverge(sign, hash));
          incomingInfos.clear();
        case _ =>
          log.info("cannot get converge for pbft vote:" + checkList);
      };

    }
  }
}