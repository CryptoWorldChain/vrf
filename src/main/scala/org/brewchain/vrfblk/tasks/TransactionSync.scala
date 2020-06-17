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
  val totalSentCount = new AtomicInteger(0);

  def isLimitSyncSpeed(curTime: Long): Boolean = {
    val tps = if (VCtrl.isSafeForMine) {
      lastSyncCount.get * 1000 / (Math.abs((curTime - lastSyncTime.get)) + 1);
    } else {
      lastSyncCount.get * 10000 / (Math.abs((curTime - lastSyncTime.get)) + 1);
    }
    if (tps > VConfig.SYNC_TX_TPS_LIMIT) {
      log.warn("speed limit :curTps=" + tps + ",timepass=" + (curTime - lastSyncTime.get) + ",lastSyncCount=" + lastSyncCount.get);
      true
    } else {
      false
    }

  }
  def trySyncTx(network: Network): Unit = {

    var syncCount = VConfig.MAX_TNX_EACH_BROADCAST;
    while (syncCount > VConfig.MIN_TNX_EACH_BROADCAST) {
      val startTime = System.currentTimeMillis();
      if (!isLimitSyncSpeed(startTime)) {
        val wallspeed = if (VCtrl.isSafeForMine) {
          VConfig.MAX_TNX_EACH_BROADCAST
        } else {
          VConfig.MIN_TNX_EACH_BROADCAST
        }
        val curbit = BigInteger.ZERO.setBit(VCtrl.instance.network.root().node_idx)
        val res = Daos.txHelper.getWaitSendTx(wallspeed, curbit)
        syncCount = res.getTxHashCount;
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
          //        VCtrl.coMinerByUID.foreach(f => {
          //          if (!VConfig.AUTH_NODE_FILTER || VCtrl.haveEnoughToken(f._2.getCoAddress)) {
          //            VCtrl.network().postMessage("BRTVRF", Left(syncTransaction.build()), msgid, f._2.getBcuid, '9')
          //          }
          //        })

          var bits = BigInteger.ZERO //filter(f => !VCtrl.banMinerByUID.containsKey(f._2.getBcuid)).
          VCtrl.coMinerByUID.foreach(f => {
            if (!VConfig.AUTH_NODE_FILTER || VCtrl.haveEnoughToken(f._2.getCoAddress)) {
              bits = bits.setBit(f._2.getBitIdx);
            }
          })
          VCtrl.network().bwallMessage("BRTVRF", Left(syncTransaction.build()), bits.clearBit(VCtrl.instance.network.root().node_idx), msgid, '9')

          // network.dwallMessage("BRTVRF", Left(syncTransaction.build()), msgid)
          lastSyncTime.set(startTime)
          lastSyncCount.set(res.getTxHashCount)
          totalSentCount.addAndGet(res.getTxHashCount);
//          log.info("total sent txcount=" + totalSentCount);
          Thread.sleep(10)
        } else {
          syncCount = 0;
        }
      } else {
        syncCount = 0;
      }
    }
  }
}