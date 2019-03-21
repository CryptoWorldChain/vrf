package org.csc.vrfblk.tasks

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConversions._

import org.csc.bcapi.exec.SRunner
import org.csc.p22p.action.PMNodeHelper
import org.csc.p22p.node.Network
import org.csc.p22p.utils.LogHelper
import org.csc.vrfblk.utils.VConfig
import org.csc.vrfblk.Daos
import org.csc.ckrand.pbgens.Ckrand.PSSyncTransaction;
import org.csc.ckrand.pbgens.Ckrand.PSSyncTransaction.SyncType;
 
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
      val res = Daos.txHelper.getWaitSendTxToSend(VConfig.MAX_TNX_EACH_BROADCAST)
      if (res.getTxHashCount > 0) {
        log.debug("Try To Synctx:Count="+res.getTxHashCount);
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

        network.wallMessage("BRTVRF", Left(syncTransaction.build()), msgid)
        lastSyncTime.set(startTime)
        lastSyncCount.set(res.getTxHashCount)
      } else {
      }
    }
  }
}