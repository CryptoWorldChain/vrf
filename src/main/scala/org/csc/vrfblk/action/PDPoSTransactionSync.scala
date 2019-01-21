package org.csc.vrfblk.action

import org.apache.commons.codec.binary.Hex
import org.apache.felix.ipojo.annotations.Instantiate
import org.apache.felix.ipojo.annotations.Provides
import onight.tfw.ntrans.api.ActorService
import onight.tfw.proxy.IActor
import onight.tfw.otransio.api.session.CMDService
import onight.osgi.annotation.NActorProvider
import org.csc.dposblk.PSMDPoSNet
import org.csc.p22p.utils.LogHelper
import onight.oapi.scala.commons.PBUtils
import onight.oapi.scala.commons.LService
import org.csc.p22p.action.PMNodeHelper
import onight.tfw.otransio.api.beans.FramePacket
import onight.tfw.async.CompleteHandler
import org.csc.bcapi.utils.PacketIMHelper._
import org.csc.dposblk.pbgens.Dposblock.PSCoMine
import org.csc.dposblk.pbgens.Dposblock.PRetCoMine
import org.csc.dposblk.pbgens.Dposblock.PCommand
import org.csc.dposblk.pbgens.Dposblock.PSSyncTransaction
import org.csc.dposblk.pbgens.Dposblock.PRetSyncTransaction
import org.csc.dposblk.tasks.DCtrl
import onight.tfw.otransio.api.PacketHelper
import org.csc.bcapi.exception.FBSException
import org.csc.dposblk.Daos
import org.csc.evmapi.gens.Tx.MultiTransaction

import scala.collection.JavaConversions._
import org.apache.commons.lang3.StringUtils
import java.util.ArrayList
import java.math.BigInteger
import org.csc.dposblk.pbgens.Dposblock.PSSyncTransaction.SyncType
import org.csc.account.bean.HashPair
import onight.tfw.outils.serialize.UUIDGenerator
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.TimeUnit
import com.google.protobuf.ByteString
import org.csc.dposblk.utils.DConfig
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.LinkedBlockingDeque
import lombok.Data
import org.csc.account.api.IPengingQueue
import onight.tfw.ntrans.api.annotation.ActorRequire

@NActorProvider
@Instantiate
@Provides(specifications = Array(classOf[ActorService], classOf[IActor], classOf[CMDService]))
class PDPoSTransactionSync extends PSMDPoSNet[PSSyncTransaction] {
  override def service = PDPoSTransactionSyncService
  
  
  @ActorRequire(name = "BlocksPendingQueue", scope = "global")
  var blocksPendingQ: IPengingQueue[Object] = null;

  def getBlocksPendingQ(): IPengingQueue[Object] = {
    return blocksPendingQ;
  }

  def setBlocksPendingQ(ddc: IPengingQueue[Object]) = {
    log.info("setBlocksPendingQ==" + ddc);
    this.blocksPendingQ = ddc;
    PDPoSTransactionSyncService.dbBatchSaveList = ddc;
  }
  
  
//   = new PendingQueue[(Array[Byte], BigInteger)]("batchsavelist", 100);
}

object PDPoSTransactionSyncService extends LogHelper with PBUtils with LService[PSSyncTransaction] with PMNodeHelper {
  val greendbBatchSaveList = new LinkedBlockingDeque[(ArrayList[MultiTransaction.Builder], BigInteger, CompleteHandler)]();
  var dbBatchSaveList: IPengingQueue[Object] = null;//(Array[Byte], BigInteger)
  val confirmHashList = new LinkedBlockingQueue[(String, BigInteger)]();

  val wallHashList = new LinkedBlockingQueue[ByteString]();

  val running = new AtomicBoolean(false);
  val prioritySave = new ReentrantReadWriteLock().writeLock();

  case class BatchRunner(id: Int) extends Runnable {
    def poll(): (ArrayList[MultiTransaction.Builder], BigInteger, CompleteHandler) = {
      val ret = greendbBatchSaveList.poll();
      if (ret != null) {
        ret;
      } else {
        val op = dbBatchSaveList.pollFirst();
        
        if (op != null) {
          val p = op.asInstanceOf[(Array[Byte], BigInteger)];
          val pbo = PSSyncTransaction.newBuilder().mergeFrom(p._1);
          val dbsaveList = new ArrayList[MultiTransaction.Builder]();
          for (x <- pbo.getTxDatasList) {
            var oMultiTransaction = MultiTransaction.newBuilder();
            oMultiTransaction.mergeFrom(x);
            if (!StringUtils.equals(DCtrl.curDN().getBcuid, oMultiTransaction.getTxNode().getBcuid)) {
              dbsaveList.add(oMultiTransaction)
            }
          }
          (dbsaveList, p._2, null)
        } else {
          null
        }
      }
    }
    override def run() {
      running.set(true);
      Thread.currentThread().setName("DPosTx-BatchRunner-" + id);
      while(dbBatchSaveList==null){
        log.debug("wait for batch :")
        Thread.sleep(1000)
      }
        
      while (running.get) {
        try {
          var p = poll();
          while (p != null) {
            //            Daos.txHelper.syncTransactionBatch(oMultiTransaction, bits)
            Daos.txHelper.syncTransactionBatch(p._1, true, p._2);
            if (p._3 != null) {
              p._3.onFinished(null);
            }
            p._1.clear();
            p = null;
            //should sleep when too many tx to confirm.
            if (Daos.confirmMapDB.getQueueSize() < Daos.confirmMapDB.getMaxElementsInMemory
              && Daos.confirmMapDB.size() < Daos.confirmMapDB.getMaxElementsInMemory) {
              p = poll();
            }
          }
          if (p == null) {
            Thread.sleep(500);
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
          try {
            Thread.sleep(10)
          } catch {
            case t: Throwable =>
          }
        }
      }
    }

  }

  case class ConfirmRunner(id: Int) extends Runnable {
    override def run() {
      running.set(true);
      Thread.currentThread().setName("DPosTx-ConfirmRunner-" + id);
      while (running.get) {
        try {
          var h = confirmHashList.poll(10, TimeUnit.SECONDS);
          while (h != null) {
            Daos.txHelper.confirmRecvTx(h._1, h._2);
            h = null;
            //should sleep when too many tx to confirm.
            if (Daos.confirmMapDB.getQueueSize() < Daos.confirmMapDB.getMaxElementsInMemory) {
              h = confirmHashList.poll();
            }
          }
        } catch {
          case t: Throwable =>
            log.error("get error", t);
        } finally {
          try {
            Thread.sleep(100)
          } catch {
            case t: Throwable =>
          }
        }
      }
    }
  }

  case class WalloutRunner(id: Int) extends Runnable {
    override def run() {
      running.set(true);
      Thread.currentThread().setName("DPosTx-WalloutRunner-" + id);
      while (running.get) {
        try {
          var h = wallHashList.poll(10, TimeUnit.SECONDS);
          if (h != null) {
            val msgid = UUIDGenerator.generate();
            val syncTransaction = PSSyncTransaction.newBuilder();
            syncTransaction.setMessageid(msgid);
            syncTransaction.setSyncType(SyncType.ST_CONFIRM_RECV);
            syncTransaction.setFromBcuid(DCtrl.instance.network.root().bcuid);
            syncTransaction.setConfirmBcuid(DCtrl.instance.network.root().bcuid);
            while (h != null) {
              syncTransaction.addTxHash(h);
              h = null;
              if (syncTransaction.getTxHashCount < DConfig.MIN_TNX_EACH_BROADCAST) {
                h = wallHashList.poll(10, TimeUnit.MILLISECONDS);
              } else if (syncTransaction.getTxHashCount < DConfig.MAX_TNX_EACH_BROADCAST) {
                h = wallHashList.poll();
              }
            }
            if (syncTransaction.getTxHashCount > 0) {
              DCtrl.instance.network.wallMessage("BRTDOB", Left(syncTransaction.build()), msgid)
            }
          }
        } catch {
          case t: Throwable =>
            log.error("get error", t);
        } finally {
          try {
            Thread.sleep(10)
          } catch {
            case t: Throwable =>
          }
        }
      }
    }
  }
  for (i <- 1 to DConfig.PARALL_SYNC_TX_BATCHBS) {
    new Thread(new BatchRunner(i)).start()
  }
  for (i <- 1 to DConfig.PARALL_SYNC_TX_CONFIRM) {
    new Thread(new ConfirmRunner(i)).start()
  }
  for (i <- 1 to DConfig.PARALL_SYNC_TX_WALLOUT) {
    new Thread(new WalloutRunner(i)).start()
  }

  override def onPBPacket(pack: FramePacket, pbo: PSSyncTransaction, handler: CompleteHandler) = {
    var ret = PRetSyncTransaction.newBuilder();
    if (!DCtrl.isReady()) {
      ret.setRetCode(-1).setRetMessage("DPoS Network Not READY")
      handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
    } else {
      try {
        MDCSetBCUID(DCtrl.dposNet())
        MDCSetMessageID(pbo.getMessageid)
        //        log.debug("OnSyncTx:" + pbo.getSyncType + ",count=" + pbo.getTxHashCount + ",from=" + pbo.getFromBcuid + ",confirm=" + pbo.getConfirmBcuid);

        var bits = BigInteger.ZERO.setBit(DCtrl.instance.network.root().node_idx);
        val confirmNode =
          pbo.getSyncType match {
            case SyncType.ST_WALLOUT =>
              DCtrl.instance.network.nodeByBcuid(pbo.getFromBcuid);
            case _ =>
              DCtrl.instance.network.nodeByBcuid(pbo.getConfirmBcuid);
          }

        if (confirmNode != DCtrl.instance.network.noneNode) {
          bits = bits.or(BigInteger.ZERO.setBit(confirmNode.node_idx));
          //          log.debug("bits::" + bits + " node_idx::" + confirmNode.node_idx + " setBit::" + bits.setBit(confirmNode.node_idx));

          pbo.getSyncType match {
            case SyncType.ST_WALLOUT =>
              //              ArrayList[MultiTransaction.Builder]
              if (pbo.getTxDatasCount > 0) {
                dbBatchSaveList.addElement((pbo.toByteArray(), bits))
              }

              //                            !!Daos.txHelper.syncTransaction(dbsaveList, bits);

              //resend
              if (DConfig.CREATE_BLOCK_TX_CONFIRM_PERCENT > 0) {
                pbo.getTxHashList.map {
                  f => wallHashList.offer(f);
                }
              }
            //      syncTransaction.addAllTxHash(res.getTxHashList);
            //      syncTransaction.addAllTxDatas(res.getTxDatasList);
            //              DCtrl.instance.network.wallMessage("BRTDOB", Left(syncTransaction.build()), msgid)

            case _ =>
              val fromNode = DCtrl.instance.network.nodeByBcuid(pbo.getFromBcuid);
              if (fromNode != DCtrl.instance.network.noneNode) {
                bits = bits.or(BigInteger.ZERO.setBit(fromNode.node_idx));
              }
              val tmpList = new ArrayList[(String, BigInteger)](pbo.getTxHashCount);
              pbo.getTxHashList.map { txHash =>
                tmpList.add((Hex.encodeHexString(txHash.toByteArray()), bits))
                //                Daos.txHelper.confirmRecvTx(Hex.encodeHexString(txHash.toByteArray()), bits);
              }
              confirmHashList.addAll(tmpList)
          }

        } else {
          log.debug("cannot find bcuid from network:" + pbo.getConfirmBcuid + "," + pbo.getFromBcuid + ",synctype=" + pbo.getSyncType);
        }

        ret.setRetCode(1)
      } catch {
        case t: Throwable => {
          log.error("error:", t);
          ret.clear()
          ret.setRetCode(-3).setRetMessage(t.getMessage)
        }
      } finally {
        handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
      }
    }
  }
  override def cmd: String = PCommand.BRT.name();
}
