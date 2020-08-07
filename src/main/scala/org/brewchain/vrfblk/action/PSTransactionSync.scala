package org.brewchain.vrfblk.action

import java.math.BigInteger
import java.util
import java.util.ArrayList
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.{ LinkedBlockingDeque, LinkedBlockingQueue, TimeUnit }

import com.google.protobuf.ByteString
import onight.oapi.scala.commons.{ LService, PBUtils }
import onight.osgi.annotation.NActorProvider
import onight.tfw.async.CompleteHandler
import onight.tfw.ntrans.api.ActorService
import onight.tfw.ntrans.api.annotation.ActorRequire
import onight.tfw.otransio.api.PacketHelper
import onight.tfw.otransio.api.beans.FramePacket
import onight.tfw.otransio.api.session.CMDService
import onight.tfw.outils.serialize.UUIDGenerator
import onight.tfw.proxy.IActor
import org.apache.commons.lang3.StringUtils
import org.apache.felix.ipojo.annotations.{ Instantiate, Provides }
import org.brewchain.bcrand.model.Bcrand.PSSyncTransaction.SyncType
import org.brewchain.bcrand.model.Bcrand.{ PCommand, PRetSyncTransaction, PSSyncTransaction }
import org.brewchain.mcore.model.Transaction.TransactionInfo
import org.brewchain.p22p.action.PMNodeHelper
import org.brewchain.p22p.utils.LogHelper
import org.brewchain.vrfblk.tasks.VCtrl
import org.brewchain.vrfblk.utils.VConfig
import org.brewchain.vrfblk.{ Daos, PSMVRFNet }
import org.brewchain.vrfblk.utils.TxArrays
import org.brewchain.vrfblk.utils.PendingQueue
import lombok.extern.slf4j.Slf4j;

import scala.collection.JavaConversions._
import org.brewchain.mcore.model.Transaction.TransactionInfo
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.ConcurrentHashMap
import org.apache.felix.ipojo.annotations.Validate

@NActorProvider
@Instantiate
@Provides(specifications = Array(classOf[ActorService], classOf[IActor], classOf[CMDService]))
@Slf4j
class PSTransactionSync extends PSMVRFNet[PSSyncTransaction] {
  override def service = PSTransactionSyncService
}

object PSTransactionSyncService extends LogHelper with PBUtils with LService[PSSyncTransaction] with PMNodeHelper {
  //(Array[Byte], BigInteger)
  var dbBatchSaveList: PendingQueue = null;

  val confirmHashList = new LinkedBlockingQueue[(Array[Byte], BigInteger)]();

  val wallHashList = new LinkedBlockingQueue[ByteString]();

  val totalRecvCount = new AtomicInteger(0);

  val totalRecvCountFromBcuid = new ConcurrentHashMap[String, AtomicInteger]();

  val running = new AtomicBoolean(false);
  val prioritySave = new ReentrantReadWriteLock().writeLock();

  case class BatchRunner(id: Int) extends Runnable {
    def poll(): (ArrayList[TransactionInfo], BigInteger) = {
      val _op = dbBatchSaveList.pollFirst();
      if (_op != null) {
        val op = _op.asInstanceOf[TxArrays];
        val pbo = PSSyncTransaction.newBuilder().mergeFrom(op.getData);
        val dbsaveList = new ArrayList[TransactionInfo]();
        for (x <- pbo.getTxDatasList) {
          var oMultiTransaction = TransactionInfo.newBuilder();
          oMultiTransaction.mergeFrom(x);
          //          if (!StringUtils.equals(VCtrl.curVN().getBcuid, oMultiTransaction.getNode().getNid)) {
          dbsaveList.add(oMultiTransaction.build())
          //          }
        }
        (dbsaveList, op.getBits())
      } else {
        null
      }
    }

    override def run() {

      var cc: Int = 0;

      try {
        var p = poll();
        while (p != null || dbBatchSaveList.size() > 0) {
          if (p != null) {
            cc = cc + 1;
            Daos.txHelper.syncTransactionBatch(p._1, true, p._2);
            if (VConfig.DCTRL_BLOCK_CONFIRMATION_RATIO > 0) {
              if (wallHashList.size() + p._1.size() < VConfig.TX_WALL_MAX_CACHE_SIZE) {
                p._1.map {
                  f => wallHashList.offer(f.getHash);
                }
              } else {
                log.error("drop wallhash list for buffer overflow:mem=" + wallHashList.size() + ",cc=" + p._1.size() + ",config=" + VConfig.TX_WALL_MAX_CACHE_SIZE);
              }
            }

            if (walloutThreadCount.incrementAndGet() < VConfig.PARALL_SYNC_TX_WALLOUT) {
              Daos.ddc.getExecutorService("synctx").submit(walloutRunner);
            } else {
              walloutThreadCount.decrementAndGet();
            }
          }
          p = poll();
        }
      } catch {
        case ier: IllegalStateException =>
          try {
            Thread.sleep(1000)
          } catch {
            case t: Throwable =>
          }
        case t: Throwable =>
          log.error("get error", t);
      } finally {
        batchThreadCount.decrementAndGet();
      }
      log.info("BatchRunner thread exit, processed=" + cc);
    }

  }

  case class ConfirmRunner(id: Int) extends Runnable {
    override def run() {
      running.set(true);
      Thread.currentThread().setName("VRFTx-ConfirmRunner-" + id);
      var cc = 0;
      var totaltxcount = 0;
      var txcount = 10;
      try {
        while (txcount > 0) {
          try {
            txcount = 0;
            var h = confirmHashList.poll(10, TimeUnit.SECONDS);
            while (h != null) {
              txcount = txcount + 1;
              Daos.txHelper.getTmConfirmQueue.increaseConfirm(h._1, h._2);
              h = null;
              //should sleep when too many tx to confirm.
              h = confirmHashList.poll();
            }
            totaltxcount = totaltxcount + txcount;
          } catch {
            case t: Throwable =>
              log.error("get error", t);
          }
        }
      } finally {
        confirmThreadCount.decrementAndGet();
      }
      log.info("confirmRunner thread exit, processed=" + cc + ",totaltx=" + totaltxcount);
    }
  }

  def getNormalNodesBits(): BigInteger = {
    val nodes = VCtrl.network().directNodes.++:(VCtrl.network().pendingNodes)
    var bits = BigInteger.ZERO;
    nodes.foreach(f => {
      if (f.node_idx >= 0) {
        bits = bits.setBit(f.node_idx);
      } else if (f.try_node_idx >= 0) {
        bits = bits.setBit(f.try_node_idx);
      }
    })
    bits;
  }
  case class WalloutRunner(id: Int) extends Runnable {
    override def run() {
      var runcount = 100;
      var cc = 0;
      var totalRuncount = 0;
      try {
        while (runcount > 0) {
          runcount = 0;
          var h = wallHashList.poll(10, TimeUnit.SECONDS);
          if (h != null) {
            val msgid = UUIDGenerator.generate();
            val syncTransaction = PSSyncTransaction.newBuilder();
            syncTransaction.setMessageid(msgid);
            syncTransaction.setSyncType(SyncType.ST_CONFIRM_RECV);
            syncTransaction.setFromBcuid(VCtrl.instance.network.root().bcuid);
            syncTransaction.setConfirmBcuid(VCtrl.instance.network.root().bcuid);
            while (h != null) {
              syncTransaction.addTxHash(h);
              if (syncTransaction.getTxHashCount < VConfig.MAX_TNX_EACH_BROADCAST) {
                h = wallHashList.poll();
              } else {
                h = null;
              }
            }
            runcount = syncTransaction.getTxDatasCount;
            cc = cc + 1;
            totalRuncount = totalRuncount + runcount;
            VCtrl.instance.network.bwallMessage("BRTVRF", Left(syncTransaction.build()),
              getNormalNodesBits.clearBit(VCtrl.instance.network.root().node_idx), msgid)
          }
        }
      } finally {
        walloutThreadCount.decrementAndGet();
      }
      log.info("WalloutRunner thread exit, processed=" + cc + ",totaltx=" + totalRuncount);
    }
  }

  val batchRunner = new BatchRunner(0);
  val confirmRunner = new ConfirmRunner(0);
  val walloutRunner = new WalloutRunner(0);

  //  for (i <- 1 to VConfig.PARALL_SYNC_TX_BATCHBS) {
  //    new Thread(new BatchRunner(i)).start()
  //  }
  //  for (i <- 1 to VConfig.PARALL_SYNC_TX_CONFIRM) {
  //    new Thread(new ConfirmRunner(i)).start()
  //  }
  //  for (i <- 1 to VConfig.PARALL_SYNC_TX_WALLOUT) {
  //    new Thread(new WalloutRunner(i)).start()
  //  }

  val batchThreadCount = new AtomicInteger(0);
  val confirmThreadCount = new AtomicInteger(0);
  val walloutThreadCount = new AtomicInteger(0);

  override def onPBPacket(pack: FramePacket, pbo: PSSyncTransaction, handler: CompleteHandler) = {
    var ret = PRetSyncTransaction.newBuilder();
    if (!VCtrl.isReady()) {
      log.error("drop synctx for Network Not READY");
      ret.setRetCode(-1).setRetMessage(" Network Not READY")
      handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
    } else if (Runtime.getRuntime.freeMemory() / 1024 / 1024 < VConfig.METRIC_SYNCTX_FREE_MEMEORY_MB) {
      ret.setRetCode(-2).setRetMessage("memory low")
      log.error("drop synctx for low memory");
      handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
    } else {
      try {
        MDCSetBCUID(VCtrl.network());
        MDCSetMessageID(pbo.getMessageid);
        var bits = BigInteger.ZERO; //.setBit(VCtrl.instance.network.root().node_idx);
        val confirmNode =
          pbo.getSyncType match {
            case SyncType.ST_WALLOUT =>
              VCtrl.instance.network.nodeByBcuid(pbo.getFromBcuid);
            case _ =>
              VCtrl.instance.network.nodeByBcuid(pbo.getConfirmBcuid);
          }

        if (confirmNode != VCtrl.instance.network.noneNode) {
          bits = bits.or(BigInteger.ZERO.setBit(confirmNode.node_idx));
          totalRecvCount.addAndGet(pbo.getTxHashCount);
         log.info("recv_tx_count = " + pbo.getTxHashCount + ",from=" + pbo.getFromBcuid);
          pbo.getSyncType match {
            case SyncType.ST_WALLOUT =>
              //              ArrayList[MultiTransaction.Builder]
              if (pbo.getTxDatasCount > 0) {
                bits = bits.setBit(VCtrl.instance.network.root().node_idx);
                
                val txarr = new TxArrays(pbo.getMessageid, pbo.toByteArray(), bits);
                
                dbBatchSaveList.addElement(txarr)
                if (batchThreadCount.incrementAndGet() < VConfig.PARALL_SYNC_TX_BATCHBS) {
                  Daos.ddc.getExecutorService("synctx").submit(batchRunner);
                } else {
                  batchThreadCount.decrementAndGet();
                }
              }else{
                 log.error("recv_tx_count is zero = " + pbo.getTxDatasCount + ",from=" + pbo.getFromBcuid);
              }

            case _ =>
              //              if (confirmHashList.size() + pbo.getTxHashCount < VConfig.TX_CONFIRM_MAX_CACHE_SIZE) {
              val fromNode = VCtrl.instance.network.nodeByBcuid(pbo.getFromBcuid);
              if (fromNode != VCtrl.instance.network.noneNode) {
                bits = bits.or(BigInteger.ZERO.setBit(fromNode.node_idx));
              }
              //                log.debug("" + pbo.getTxHashCount);
              //                val tmpList = new ArrayList[(String, BigInteger)](pbo.getTxHashCount);
              pbo.getTxHashList.map { txHash =>
                //                  tmpList.add((Daos.enc.bytesToHexStr(txHash.toByteArray()), bits))
                confirmHashList.add((txHash.toByteArray(), bits));
              }
              if (confirmThreadCount.incrementAndGet() < VConfig.PARALL_SYNC_TX_CONFIRM) {
                Daos.ddc.getExecutorService("synctx").submit(confirmRunner);
              } else {
                confirmThreadCount.decrementAndGet();
              }
            //                confirmHashList.addAll(tmpList)
            //              }
          }

        } else {
          log.error(" drop tx sync. cannot find bcuid from network:" + pbo.getConfirmBcuid + "," + pbo.getFromBcuid + ",synctype=" + pbo.getSyncType);
        }

        ret.setRetCode(1)
      } catch {
        case t: Throwable => {
          log.error("error:" + ",dao.ddc=" + Daos.ddc, t);
          ret.clear()
          ret.setRetCode(-3).setRetMessage("" + t.getMessage)
        }
      } finally {
        handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
      }
    }

    def SyncTransaction2TransactionBuilder(array: Array[Byte]): util.ArrayList[TransactionInfo.Builder] = {
      val pbo = PSSyncTransaction.newBuilder().mergeFrom(array);
      val dbsaveList = new ArrayList[TransactionInfo.Builder]();
      for (x <- pbo.getTxDatasList) {
        val oMultiTransaction = TransactionInfo.newBuilder();
        oMultiTransaction.mergeFrom(x);
        if (!StringUtils.equals(VCtrl.curVN().getBcuid, oMultiTransaction.getNode().getNid)) {
          dbsaveList.add(oMultiTransaction)
        }
      }
      dbsaveList
    }

  }

  override def cmd: String = PCommand.BRT.name();
}
