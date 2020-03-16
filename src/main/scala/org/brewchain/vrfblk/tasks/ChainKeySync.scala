package org.brewchain.vrfblk.tasks

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConversions._
import java.math.BigInteger
import org.brewchain.vrfblk.utils.SRunner
import org.brewchain.p22p.action.PMNodeHelper
import org.brewchain.p22p.node.Network
import org.brewchain.p22p.utils.LogHelper
import org.brewchain.vrfblk.utils.VConfig
import org.brewchain.vrfblk.Daos
import org.brewchain.bcrand.model.Bcrand.PSSyncTransaction;
import org.brewchain.bcrand.model.Bcrand.PSSyncTransaction.SyncType;

import onight.tfw.outils.serialize.UUIDGenerator
import org.brewchain.p22p.node.Node
import org.brewchain.bcrand.model.Bcrand.SyncChainKey
import scala.util.Random
import java.util.Date

case class ChainKeySync(network: Network) extends SRunner with PMNodeHelper with LogHelper {
  def getName() = "ChainKeySync"
  def runOnce() = {
    Thread.currentThread().setName("ChainKeySync");
    ChainKeySyncHelper.trySync(network);
  }
}
object ChainKeySyncHelper extends LogHelper {
  var instance: ChainKeySync = ChainKeySync(null);
  def dposNet(): Network = instance.network;

  def trySync(network: Network): Unit = {
    log.debug("start chainkey sync. need refresh:" + Daos.accountHandler.getChainConfig.isEnableRefresh);
    if (Daos.accountHandler.getChainConfig.isEnableRefresh) {
      var vNetwork: Option[Node] = null;
      val miners = VCtrl.coMinerByUID.filter(!_._2.getBcuid.equalsIgnoreCase(VCtrl.instance.cur_vnode.getBcuid))
        .find(p => p._2.getCurBlock > VCtrl.curVN().getCurBlock)
      if (miners.isDefined) {
        vNetwork = network.directNodeByBcuid.get(miners.get._1);
      } else {
        val randomNode = randomNodeInNetwork(network)
        vNetwork = randomNode;
      }

      val msgid = UUIDGenerator.generate();
      var oSyncChainKey = SyncChainKey.newBuilder();
      oSyncChainKey
        .setTimestamp((new Date()).getTime)
        .setTmpPubKey(Daos.enc.bytesToHexStr(Daos.accountHandler.getChainConfig.coinbase_account_public_key));
     
      var signBytes = Daos.enc.sign(Daos.accountHandler.getChainConfig.coinbase_account_private_key, oSyncChainKey.build().toByteArray());
      oSyncChainKey.setSignature(Daos.enc.bytesToHexStr(signBytes));
      VCtrl.network().postMessage("SCKVRF", Left(oSyncChainKey.build()), msgid, vNetwork.get.bcuid, '9')
    }
  }

  def randomNodeInNetwork(network: Network): Option[Node] = {
    val self = VCtrl.curVN()
    val temp: List[Node] = network.directNodes
      .filter(p => !p.bcuid.equals(self.getBcuid))
      .toList
    val indexRange = temp.size;

    if (indexRange < 0 || temp.isEmpty) {
      Option.empty
    } else if (indexRange == 0) {
      Option.apply(temp.get(0))
    } else {
      Option.apply(temp.get(Random.nextInt(indexRange)))
    }
  }
}