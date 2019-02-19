package org.csc.vrfblk.msgproc

import org.csc.bcapi.crypto.BitMap
import org.csc.ckrand.pbgens.Ckrand.PSCoinbase
import org.csc.p22p.action.PMNodeHelper
import org.csc.p22p.utils.LogHelper
import org.csc.vrfblk.tasks.BlockMessage
import org.csc.vrfblk.tasks.VCtrl
import org.csc.account.util.OEntityBuilder
import org.csc.vrfblk.Daos
import org.csc.p22p.core.Votes

import scala.collection.JavaConverters._
import org.csc.p22p.core.Votes.Converge
import org.csc.p22p.core.Votes.NotConverge
import onight.tfw.outils.serialize.UUIDGenerator
import org.csc.ckrand.pbgens.Ckrand.PSNodeInfo
import org.csc.ckrand.pbgens.Ckrand.GossipMiner
import org.csc.vrfblk.tasks.BeaconGossip

case class RollbackBlock(startBlock: Int) extends BlockMessage with PMNodeHelper with BitMap with LogHelper {

  def proc(): Unit = {

    //to notify other.
    MDCSetBCUID(VCtrl.network())
    val blks = Daos.chainHelper.getBlocksByNumber(startBlock);
    if (blks != null && blks.size() == 1) {
      val messageId = UUIDGenerator.generate();
      log.debug("start to gossip from starBlock:" + (startBlock));
      BeaconGossip.gossipBeaconInfo(startBlock)
    }
  }
}