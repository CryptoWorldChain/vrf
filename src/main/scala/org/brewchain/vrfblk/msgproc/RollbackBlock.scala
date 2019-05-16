package org.brewchain.vrfblk.msgproc

import org.brewchain.core.crypto.BitMap
import org.brewchain.bcrand.model.Bcrand.PSCoinbase
import org.brewchain.p22p.action.PMNodeHelper
import org.brewchain.p22p.utils.LogHelper
import org.brewchain.vrfblk.tasks.BlockMessage
import org.brewchain.vrfblk.tasks.VCtrl
import org.brewchain.vrfblk.Daos
import org.brewchain.p22p.core.Votes

import scala.collection.JavaConverters._
import org.brewchain.p22p.core.Votes.Converge
import org.brewchain.p22p.core.Votes.NotConverge
import onight.tfw.outils.serialize.UUIDGenerator
import org.brewchain.bcrand.model.Bcrand.PSNodeInfo
import org.brewchain.bcrand.model.Bcrand.GossipMiner
import org.brewchain.vrfblk.tasks.BeaconGossip

case class RollbackBlock(startBlock: Int) extends BlockMessage with PMNodeHelper with BitMap with LogHelper {

  def proc(): Unit = {

    //to notify other.
    MDCSetBCUID(VCtrl.network())
    val blks = Daos.chainHelper.listBlockByHeight(startBlock);
    if (blks != null && blks.length == 1) {
      val messageId = UUIDGenerator.generate();
      log.debug("start to gossip from starBlock:" + (startBlock));
      BeaconGossip.gossipBeaconInfo(startBlock)
    }
  }
}