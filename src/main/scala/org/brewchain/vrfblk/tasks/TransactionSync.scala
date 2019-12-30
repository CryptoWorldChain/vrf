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

case class TransactionSync(network: Network) extends SRunner with PMNodeHelper with LogHelper {
  def getName() = "TxSync"
  def runOnce() = {
    Thread.currentThread().setName("TxSync");
    TxSync.trySyncTx(network);
  }
}
object TxSync extends LogHelper {
  var instance: TransactionSync = TransactionSync(null);
  def dposNet(): Network = instance.network;
  val lastSyncTime = new AtomicLong(0);
  val lastSyncCount = new AtomicInteger(0);

  def isLimitSyncSpeed(curTime: Long): Boolean = {
    val tps = lastSyncCount.get * 1000 / (Math.abs((curTime - lastSyncTime.get)) + 1);
    if (tps > VConfig.SYNC_TX_TPS_LIMIT) {
      log.warn("speed limit :curTps=" + tps + ",timepass=" + (curTime - lastSyncTime.get) + ",lastSyncCount=" + lastSyncCount);
      true
    } else {
      false
    }

  }
  def trySyncTx(network: Network): Unit = {
    val startTime = System.currentTimeMillis();
    if (!isLimitSyncSpeed(startTime)) {
      val res = Daos.txHelper.getWaitSendTx(VConfig.MAX_TNX_EACH_BROADCAST)
      if (res.getTxHashCount > 0) {
        val msgid = UUIDGenerator.generate();
        val syncTransaction = PSSyncTransaction.newBuilder();
        syncTransaction.setMessageid(msgid);
        syncTransaction.setSyncType(SyncType.ST_WALLOUT);
        syncTransaction.setFromBcuid(network.root().bcuid);
        for (x <- res.getTxHashList) {
          syncTransaction.addTxHash(x)
        }

        for (x <- res.getTxDatasList) {
          syncTransaction.addTxDatas(x)
        }

        // TODO 判断是否有足够余额，只发给有足够余额的节点
        VCtrl.allNodes.foreach(f => {
          val n = f._2;
          if(new BigInteger(n.getAuthBalance()).compareTo(VConfig.AUTH_TOKEN_MIN) >= 0) {
//            var sleepMS = RandFunction.getRandMakeBlockSleep(newblk.getMiner.getTerm, newNetBits, cn.getBitIdx);
//            if (sleepMS < VConfig.BLOCK_MAKE_TIMEOUT_SEC * 1000) {
              VCtrl.network().postMessage("BRTVRF", Left(syncTransaction.build()), msgid, n.getBcuid, '9')
            //}
          } else {
            log.error("cannot broadcast block ");
          }
        })
       
//        network.dwallMessage("BRTVRF", Left(syncTransaction.build()), msgid)
        lastSyncTime.set(startTime)
        lastSyncCount.set(res.getTxHashCount)
      } else {
      }
    }
  }
}