package org.brewchain.vrfblk.tasks

import java.util.List

import org.brewchain.bcrand.model.Bcrand.PSNodeInfo
import org.brewchain.p22p.action.PMNodeHelper
import org.brewchain.p22p.utils.LogHelper
import org.fc.zippo.dispatcher.SingletonWorkShop
import scala.collection.JavaConverters._
import org.apache.commons.lang3.StringUtils
import org.brewchain.vrfblk.Daos
import scala.util.Random
import org.brewchain.vrfblk.utils.RandFunction
import org.brewchain.bcrand.model.Bcrand.VNodeState
import java.math.BigInteger
import onight.tfw.otransio.api.beans.FramePacket
import onight.tfw.otransio.api.PacketHelper
import org.brewchain.vrfblk.utils.VConfig
import org.brewchain.vrfblk.utils.TxCache
import org.brewchain.vrfblk.utils.BlkTxCalc
import org.brewchain.bcrand.model.Bcrand.PSCoinbase
import onight.tfw.outils.serialize.UUIDGenerator
import org.brewchain.bcrand.model.Bcrand.PBlockEntry
import org.brewchain.core.crypto.BitMap
import com.google.protobuf.ByteString
import org.brewchain.vrfblk.msgproc.MPCreateBlock
import org.brewchain.vrfblk.msgproc.MPRealCreateBlock
import org.brewchain.vrfblk.msgproc.ApplyBlock
import org.brewchain.vrfblk.msgproc.SyncApplyBlock
import scala.collection.JavaConverters._
import org.brewchain.vrfblk.msgproc.NotaryBlock
import java.util.concurrent.ConcurrentHashMap
import com.sun.org.apache.bcel.internal.generic.DDIV

trait BlockMessage {
  def proc(): Unit;
}

object BlockProcessor extends SingletonWorkShop[BlockMessage] with PMNodeHelper with BitMap with LogHelper {
  var running: Boolean = true;

  def isRunning(): Boolean = {
    return running;
  }
  val NewBlockFP = PacketHelper.genPack("NEWBLOCK", "__VRF", "", true, 9);

  val processHash = new ConcurrentHashMap[String, String]();

  def offerBlock(t: ApplyBlock): Unit = {
    var put = false;
    processHash.synchronized({
      if (processHash.containsKey(t.pbo.getBeaconHash)) {
        log.debug("omit applyblock:" + t.pbo.getMessageId + ",beaconhash=" + t.pbo.getBeaconHash);
      } else {
        processHash.put(t.pbo.getMessageId, t.pbo.getBeaconHash)
        processHash.put(t.pbo.getBeaconHash, t.pbo.getBeaconHash)
        put = true;
      }
    })
    if (put) {
      super.offerMessage(t)
    }
  }
  
  def offerNotaryBlock(t: NotaryBlock): Unit = {
    var put = false;
    processHash.synchronized({
      if (processHash.containsKey(t.pbo.getMessageId)) {
        log.debug("omit applyblock:" + t.pbo.getMessageId + ",beaconhash=" + t.pbo.getBeaconHash);
      } else {
        processHash.put(t.pbo.getMessageId, t.pbo.getBeaconHash)
        put = true;
      }
    })
    if (put) {
      super.offerMessage(t)
    }
  }
  def offerSyncBlock(t: SyncApplyBlock): Unit = {
    var put = false;
    processHash.synchronized({
      val key = Daos.enc.bytesToHexStr(t.block.getHeader.getHash.toByteArray());
      if (processHash.containsKey(key)) {
        log.debug("omit applySyncblock:" +t.block.getHeader.getHash);
      } else {
        processHash.put(key,key)
        put = true;
      }
    })
    if (put) {
      super.offerMessage(t)
    }
  }
  
  def runBatch(items: List[BlockMessage]): Unit = {
    MDCSetBCUID(VCtrl.network())
    //单线程执行
    for (m <- items.asScala) {
      //should wait
      m match {
        case blkInfo: MPCreateBlock =>
          //          log.debug("get newblock info:" + blkInfo.beaconHash + "," + hexToMapping(blkInfo.netBits));
          var sleepMS = RandFunction.getRandMakeBlockSleep(blkInfo.beaconHash, blkInfo.blockbits, VCtrl.curVN().getBitIdx);
          log.debug("block maker sleep = " + sleepMS + ",bitidx=" + VCtrl.curVN().getBitIdx)
          var isFirstMaker = false;
          if (sleepMS < VConfig.BLOCK_MAKE_TIMEOUT_SEC * 1000) {
            isFirstMaker = true;
          }
          log.info("exec create block background running:" + blkInfo.beaconHash + "," + hexToMapping(blkInfo.netBits) + ",sleep :" + sleepMS);
          Daos.ddc.executeNow(NewBlockFP, new Runnable() {
            def run() {
              do {
                //while (sleepMS > 0 && (Daos.chainHelper.getLastBlockNumber() == 0 || Daos.chainHelper.GetConnectBestBlock() == null || blkInfo.preBeaconHash.equals(Daos.chainHelper.GetConnectBestBlock().getMiner.getTermid))) {
                Thread.sleep(Math.min(100, sleepMS));
                sleepMS = sleepMS - 100;
                if (isFirstMaker && Daos.txHelper.getTmConfirmQueue.size() > VConfig.WAIT_BLOCK_MIN_TXN) {
                  log.error("wake up for block queue too large :" + isFirstMaker + ",sleepMS=" + sleepMS + ",waitBlock.size=" + Daos.txHelper.getTmConfirmQueue.size);
                  sleepMS = 0;

                }
              } while (sleepMS > 0 && VCtrl.curVN().getBeaconHash.equals(blkInfo.beaconHash));
              if (VCtrl.curVN().getBeaconHash.equals(blkInfo.beaconHash)) {
                log.debug("wait up to create block:" + blkInfo.beaconHash + ",sleep still:" + sleepMS);
                BlockProcessor.offerMessage(new MPRealCreateBlock(blkInfo.netBits, blkInfo.blockbits, blkInfo.notarybits, blkInfo.beaconHash, blkInfo.preBeaconHash, blkInfo.beaconSig, blkInfo.witnessNode, blkInfo.needHeight))
              } else {
                log.debug("cancel create block:" + blkInfo.beaconHash + ",sleep still:" + sleepMS);
              }
            }
          })
        case blk: ApplyBlock =>
          processHash.remove(blk.pbo.getMessageId)
          processHash.remove(blk.pbo.getBeaconHash);
          try {
            blk.proc();
          } finally {
          }
        case blk: SyncApplyBlock =>
          processHash.remove(Daos.enc.bytesToHexStr(blk.block.getHeader.getHash.toByteArray()))
          blk.proc();
        case blk: NotaryBlock =>
          processHash.remove(blk.pbo.getMessageId)
          blk.proc();
        case blk: MPRealCreateBlock =>
          log.info("MPRealCreateBlock need=" + blk.needHeight + " curbh=" + VCtrl.curVN().getCurBlock)
          if (VCtrl.curVN().getBeaconHash.equals(blk.beaconHash)
            && blk.needHeight == (VCtrl.curVN().getCurBlock + 1)) {
            blk.proc();
          } else {
            log.debug("cancel create block:" + blk.beaconHash + " current:" + VCtrl.curVN().getBeaconHash);
          }
        case n @ _ =>
          log.warn("unknow info:" + n);
      }
      //      Daos.ddc.executeNow(arg0, arg1, arg2)
    }
  }
}