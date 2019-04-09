package org.csc.vrfblk.action

import java.math.BigInteger
import java.util
import java.util.ArrayList
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.{LinkedBlockingDeque, LinkedBlockingQueue, TimeUnit}

import com.google.protobuf.ByteString
import onight.oapi.scala.commons.{LService, PBUtils}
import onight.osgi.annotation.NActorProvider
import onight.tfw.async.CompleteHandler
import onight.tfw.ntrans.api.ActorService
import onight.tfw.ntrans.api.annotation.ActorRequire
import onight.tfw.otransio.api.PacketHelper
import onight.tfw.otransio.api.beans.FramePacket
import onight.tfw.otransio.api.session.CMDService
import onight.tfw.outils.serialize.UUIDGenerator
import onight.tfw.proxy.IActor
import org.apache.commons.codec.binary.Hex
import org.apache.commons.lang3.StringUtils
import org.apache.felix.ipojo.annotations.{Instantiate, Provides}
import org.csc.account.api.IPengingQueue
import org.csc.ckrand.pbgens.Ckrand.PSSyncTransaction.SyncType
import org.csc.ckrand.pbgens.Ckrand.{PCommand, PRetSyncTransaction, PSSyncTransaction}
import org.csc.evmapi.gens.Tx.Transaction
import org.csc.p22p.action.PMNodeHelper
import org.csc.p22p.utils.LogHelper
import org.csc.vrfblk.tasks.VCtrl
import org.csc.vrfblk.utils.VConfig
import org.csc.vrfblk.{Daos, PSMVRFNet}

import scala.collection.JavaConversions._

@NActorProvider
@Instantiate
@Provides(specifications = Array(classOf[ActorService], classOf[IActor], classOf[CMDService]))
class PSTransactionSync extends PSMVRFNet[PSSyncTransaction] {
  override def service = PSTransactionSyncService


  @ActorRequire(name = "BlocksPendingQueue", scope = "global")
  var blocksPendingQ: IPengingQueue[Object] = null;

  def getBlocksPendingQ(): IPengingQueue[Object] = {
    return blocksPendingQ;
  }

  def setBlocksPendingQ(ddc: IPengingQueue[Object]) = {
    log.info("setBlocksPendingQ==" + ddc);
    this.blocksPendingQ = ddc;
    PSTransactionSyncService.dbBatchSaveList = ddc;
  }


  //   = new PendingQueue[(Array[Byte], BigInteger)]("batchsavelist", 100);
}

object PSTransactionSyncService extends LogHelper with PBUtils with LService[PSSyncTransaction] with PMNodeHelper {
  val greendbBatchSaveList = new LinkedBlockingDeque[(ArrayList[Transaction.Builder], BigInteger, CompleteHandler)]();
  //(Array[Byte], BigInteger)
  var dbBatchSaveList: IPengingQueue[Object] = null;
  val confirmHashList = new LinkedBlockingQueue[(String, BigInteger)]();

  val wallHashList = new LinkedBlockingQueue[ByteString]();

  val running = new AtomicBoolean(false);
  val prioritySave = new ReentrantReadWriteLock().writeLock();

  case class BatchRunner(id: Int) extends Runnable {
    def poll(): (ArrayList[Transaction.Builder], BigInteger, CompleteHandler) = {
      val ret = greendbBatchSaveList.poll();
      if (ret != null) {
        ret;
      } else {
        val op = dbBatchSaveList.pollFirst();

        if (op != null) {
          val p = op.asInstanceOf[(Array[Byte], BigInteger)];
          val pbo = PSSyncTransaction.newBuilder().mergeFrom(p._1);
          val dbsaveList = new ArrayList[Transaction.Builder]();
          for (x <- pbo.getTxDatasList) {
            var oMultiTransaction = Transaction.newBuilder();
            oMultiTransaction.mergeFrom(x);
            if (!StringUtils.equals(VCtrl.curVN().getBcuid, oMultiTransaction.getNode().getBcuid)) {
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
      Thread.currentThread().setName("VRFTx-BatchRunner-" + id);
      while (dbBatchSaveList == null) {
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
            Daos.txHelper.confirmRecvTx(ByteString.copyFrom(Hex.decodeHex(h._1)), h._2);
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
      Thread.currentThread().setName("VRFTx-WalloutRunner-" + id);
      while (running.get) {
        try {
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
              h = null;
              if (syncTransaction.getTxHashCount < VConfig.MIN_TNX_EACH_BROADCAST) {
                h = wallHashList.poll(10, TimeUnit.MILLISECONDS);
              } else if (syncTransaction.getTxHashCount < VConfig.MAX_TNX_EACH_BROADCAST) {
                h = wallHashList.poll();
              }
            }
            if (syncTransaction.getTxHashCount > 0) {
              VCtrl.instance.network.wallMessage("BRTVRF", Left(syncTransaction.build()), msgid)
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

  for (i <- 1 to VConfig.PARALL_SYNC_TX_BATCHBS) {
    new Thread(new BatchRunner(i)).start()
  }
  for (i <- 1 to VConfig.PARALL_SYNC_TX_CONFIRM) {
    new Thread(new ConfirmRunner(i)).start()
  }
  for (i <- 1 to VConfig.PARALL_SYNC_TX_WALLOUT) {
    new Thread(new WalloutRunner(i)).start()
  }

  override def onPBPacket(pack: FramePacket, pbo: PSSyncTransaction, handler: CompleteHandler) = {
    var ret = PRetSyncTransaction.newBuilder();
    if (!VCtrl.isReady()) {
      ret.setRetCode(-1).setRetMessage("DPoS Network Not READY")
      handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
    } else {
      try {
        MDCSetBCUID(VCtrl.network());
        MDCSetMessageID(pbo.getMessageid);
        var bits = BigInteger.ZERO.setBit(VCtrl.instance.network.root().node_idx);
        val confirmNode =
          pbo.getSyncType match {
            case SyncType.ST_WALLOUT =>
              VCtrl.instance.network.nodeByBcuid(pbo.getFromBcuid);
            case _ =>
              VCtrl.instance.network.nodeByBcuid(pbo.getConfirmBcuid);
          }

        if (confirmNode != VCtrl.instance.network.noneNode) {
          bits = bits.or(BigInteger.ZERO.setBit(confirmNode.node_idx));

          pbo.getSyncType match {
            case SyncType.ST_WALLOUT =>
              //              ArrayList[MultiTransaction.Builder]
              if (pbo.getTxDatasCount > 0) {
                dbBatchSaveList.addElement((pbo.toByteArray(), bits))
                //TransactionSyncProcessor.offerMessage((SyncTransaction2TransactionBuilder(pbo.toByteArray()), bits, null))
              }
              // if (VConfig.CREATE_BLOCK_TX_CONFIRM_PERCENT > 0) {
              if (VConfig.DCTRL_BLOCK_CONFIRMATION_RATIO > 0) {
                pbo.getTxHashList.map {
                  f => wallHashList.offer(f);
                  //f => TransactionHashBrodcastor.offerMessage(f)
                }
              }
            case _ =>
              val fromNode = VCtrl.instance.network.nodeByBcuid(pbo.getFromBcuid);
              if (fromNode != VCtrl.instance.network.noneNode) {
                bits = bits.or(BigInteger.ZERO.setBit(fromNode.node_idx));
              }
              val tmpList = new ArrayList[(String, BigInteger)](pbo.getTxHashCount);
              pbo.getTxHashList.map { txHash =>
                tmpList.add((Hex.encodeHexString(txHash.toByteArray()), bits))
                //TransactionConfirmHashProcessor.offerMessage((Hex.encodeHexString(txHash.toByteArray()), bits))
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


    def SyncTransaction2TransactionBuilder(array: Array[Byte]): util.ArrayList[Transaction.Builder] = {
      val pbo = PSSyncTransaction.newBuilder().mergeFrom(array);
      val dbsaveList = new ArrayList[Transaction.Builder]();
      for (x <- pbo.getTxDatasList) {
        val oMultiTransaction = Transaction.newBuilder();
        oMultiTransaction.mergeFrom(x);
        if (!StringUtils.equals(VCtrl.curVN().getBcuid, oMultiTransaction.getNode().getBcuid)) {
          dbsaveList.add(oMultiTransaction)
        }
      }
      dbsaveList
    }

  }

  override def cmd: String = PCommand.BRT.name();
}
