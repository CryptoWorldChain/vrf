package org.csc.vrfblk.tasks

import java.util.List

import org.csc.ckrand.pbgens.Ckrand.PSNodeInfo
import org.csc.p22p.action.PMNodeHelper
import org.csc.p22p.utils.LogHelper
import org.fc.zippo.dispatcher.SingletonWorkShop
import scala.collection.JavaConverters._
import org.apache.commons.lang3.StringUtils
import org.csc.vrfblk.Daos
import scala.util.Random
import org.csc.vrfblk.utils.RandFunction
import org.csc.ckrand.pbgens.Ckrand.VNodeState
import java.math.BigInteger
import onight.tfw.otransio.api.beans.FramePacket
import onight.tfw.otransio.api.PacketHelper
import org.csc.vrfblk.utils.VConfig
import org.csc.vrfblk.utils.TxCache
import org.csc.vrfblk.utils.BlkTxCalc
import org.csc.ckrand.pbgens.Ckrand.PSCoinbase
import onight.tfw.outils.serialize.UUIDGenerator
import org.csc.ckrand.pbgens.Ckrand.PBlockEntry
import org.csc.bcapi.crypto.BitMap
import com.google.protobuf.ByteString
import org.csc.vrfblk.msgproc.MPCreateBlock
import org.csc.vrfblk.msgproc.MPRealCreateBlock
import org.csc.vrfblk.msgproc.ApplyBlock
import org.csc.vrfblk.msgproc.SyncApplyBlock
import scala.collection.JavaConverters._
import org.csc.vrfblk.msgproc.NotaryBlock
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
      val key = Daos.enc.hexEnc(t.block.getHeader.getHash.toByteArray());
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
                if (isFirstMaker && Daos.confirmMapDB.size() > VConfig.WAIT_BLOCK_MIN_TXN) {
                  log.error("wake up for block queue too large :" + isFirstMaker + ",sleepMS=" + sleepMS + ",waitBlock.size=" + Daos.confirmMapDB.size);
                  sleepMS = 0;

                }
              } while (sleepMS > 0 && VCtrl.curVN().getBeaconHash.equals(blkInfo.beaconHash));

              //if (VCtrl.blockLock.tryLock()) {
              //try {
              if (VCtrl.curVN().getBeaconHash.equals(blkInfo.beaconHash)) {
                //if (Daos.chainHelper.GetConnectBestBlock() == null
                //  || blkInfo.preBeaconHash.equals(Daos.chainHelper.GetConnectBestBlock().getMiner.getTermid)
                //  || Daos.chainHelper.getLastBlockNumber() == 0) {
                //create block.
                log.debug("wait up to create block:" + blkInfo.beaconHash + ",sleep still:" + sleepMS);
                // blkInfo.proc();
                BlockProcessor.offerMessage(new MPRealCreateBlock(blkInfo.netBits, blkInfo.blockbits, blkInfo.notarybits, blkInfo.beaconHash, blkInfo.preBeaconHash, blkInfo.beaconSig, blkInfo.witnessNode, blkInfo.needHeight))
              } else {
                log.debug("cancel create block:" + blkInfo.beaconHash + ",sleep still:" + sleepMS);
              }
              //} finally {
              log.debug("UNLOCK")
              //VCtrl.blockLock.unlock()
              //}
              //} else {
              //  log.error(s"LOCK Failed! some Thread Working Right now beaconHash:${blkInfo.beaconHash}, " +
              //    s"DAOHeight:${Daos.chainHelper.getLastBlockNumber()},sleep still:${sleepMS}")
              //}
            }
          })
        case blk: ApplyBlock =>
          processHash.remove(blk.pbo.getMessageId)
          processHash.remove(blk.pbo.getBeaconHash);

          log.debug("apply block:" + blk.pbo.getBeaconHash + ",netbits=" + new String(blk.pbo.getVrfCodes.toByteArray()) + ",blockheight="
            + blk.pbo.getBlockHeight);
          //if (VCtrl.blockLock.tryLock()) {
          try {
            blk.proc();
          } finally {
            //log.debug("UNLOCK")
            //VCtrl.blockLock.unlock()
          }
        //}
        case blk: SyncApplyBlock =>
          processHash.remove(Daos.enc.hexEnc(blk.block.getHeader.getHash.toByteArray()))
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