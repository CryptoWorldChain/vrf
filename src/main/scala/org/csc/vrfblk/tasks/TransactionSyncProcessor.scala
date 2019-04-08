package org.csc.vrfblk.tasks

import java.math.BigInteger
import java.util
import java.util.ArrayList
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ExecutorService, Executors, LinkedBlockingQueue}

import com.google.protobuf.ByteString
import onight.tfw.async.CompleteHandler
import onight.tfw.outils.serialize.UUIDGenerator
import org.apache.commons.codec.binary.Hex
import org.csc.bcapi.crypto.BitMap
import org.csc.ckrand.pbgens.Ckrand.PSSyncTransaction
import org.csc.ckrand.pbgens.Ckrand.PSSyncTransaction.SyncType
import org.csc.evmapi.gens.Tx.Transaction
import org.csc.p22p.action.PMNodeHelper
import org.csc.p22p.utils.LogHelper
import org.csc.vrfblk.Daos
import org.csc.vrfblk.utils.VConfig
import org.fc.zippo.dispatcher.SingletonWorkShop

import scala.collection.JavaConverters._

object TransactionSyncProcessor extends SingletonWorkShop[(ArrayList[Transaction.Builder], BigInteger, CompleteHandler)]
  with PMNodeHelper with BitMap with LogHelper {
  override def isRunning: Boolean = true

  override def runBatch(list: util.List[(util.ArrayList[Transaction.Builder], BigInteger, CompleteHandler)]): Unit = {

    list.asScala.foreach {
      case (transactionList: util.ArrayList[Transaction.Builder], netBits: BigInteger, completeHandler) => {
        Daos.txHelper.syncTransactionBatch(transactionList, true, netBits)
        if (completeHandler != null) {
          completeHandler.onFinished(null)
        }
        transactionList.clear()
        //TODO TXSync confirm无等待，无法抗背压
      }
      case n@_ => log.warn("unknow info:" + n)
    }
  }
}


object TransactionConfirmHashProcessor extends SingletonWorkShop[(String, BigInteger)] with PMNodeHelper with BitMap with LogHelper {

  var running = true

  override def isRunning: Boolean = running

  //List(transactionHash, bits)
  override def runBatch(list: util.List[(String, BigInteger)]): Unit = {
    log.debug("====> start ConfirmTxProcessor size=" + list.size);
    list.asScala.foreach {
      case (txHash, bits) => {
        Daos.txHelper.confirmRecvTx(ByteString.copyFrom(Hex.decodeHex(txHash)), bits)
      }
      case n@_ => log.warn("unknow info:" + n)

    }
  }


}

object TransactionHashBrodcastor extends SingletonWorkShop[ByteString] with PMNodeHelper with BitMap with LogHelper {
  val wallHashList = new LinkedBlockingQueue[ByteString]();

  override def isRunning: Boolean = true

  override def runBatch(list: util.List[ByteString]): Unit = {
    wallHashList.addAll(list)
    if (wallHashList.size() < VConfig.MIN_TNX_EACH_BROADCAST || wallHashList.size() < VConfig.MAX_TNX_EACH_BROADCAST) {
      log.debug(s"current transaction size:${wallHashList.size()}")
      return
    }
    val msgid = UUIDGenerator.generate()

    val syncTransaction = PSSyncTransaction.newBuilder()
      .setMessageid(msgid)
      .setSyncType(SyncType.ST_CONFIRM_RECV)
      .setFromBcuid(VCtrl.instance.network.root().bcuid)
      .setConfirmBcuid(VCtrl.instance.network.root().bcuid)

    syncTransaction.addAllTxHash(wallHashList)
    if (syncTransaction.getTxHashCount > 0) {
      VCtrl.instance.network.wallMessage("BRTVRF", Left(syncTransaction.build()), msgid)
    }
  }

}

object TransactionSyncBoot extends PMNodeHelper with BitMap with LogHelper {
  val started: AtomicBoolean = new AtomicBoolean(false)

  def startAll() = {
    if (started.compareAndSet(false, true)) {
      Array(TransactionSyncProcessor, TransactionConfirmHashProcessor, TransactionHashBrodcastor)
        .foreach(p => {
          p.startup(Daos.ddc.getExecutorService("vrf"))
        })

      //val all_sync_tx_batchbs: Int = VConfig.PARALL_SYNC_TX_BATCHBS
      //val all_sync_tx_confirm: Int = VConfig.PARALL_SYNC_TX_CONFIRM
      //val all_sync_tx_wallout: Int = VConfig.PARALL_SYNC_TX_WALLOUT
      //
      //TransactionSyncProcessor.startup(Executors.newFixedThreadPool(all_sync_tx_batchbs))
      //TransactionConfirmHashProcessor.startup(Executors.newFixedThreadPool(all_sync_tx_confirm))
      //TransactionHashBrodcastor.startup(Executors.newFixedThreadPool(all_sync_tx_wallout))
    }

  }
}