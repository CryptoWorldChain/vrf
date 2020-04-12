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
import org.brewchain.mcore.crypto.BitMap
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
      } else {
        processHash.put(key, key)
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
          val startupMS = System.currentTimeMillis();
          var sleepMS = blkInfo.sleepMs.longValue();
          var isFirstMaker = false;
          if (sleepMS <= Daos.mcore.getBlockMineTimeoutMs()) {
            isFirstMaker = true;
          }
          log.error("make block sleep=" + sleepMS);
          var checkcc = 0;
          var lastCominercc = VCtrl.coMinerByUID.size
          VCtrl.instance.waitCreateBlockBeacon =  blkInfo.beaconHash
          Daos.ddc.executeNow(NewBlockFP, new Runnable() {
            def run() {
              
              do {
                //while (sleepMS > 0 && (Daos.chainHelper.getLastBlockNumber() == 0 || Daos.chainHelper.GetConnectBestBlock() == null || blkInfo.preBeaconHash.equals(Daos.chainHelper.GetConnectBestBlock().getMiner.getTermid))) {
                Thread.sleep(Math.min(100, sleepMS));
                sleepMS = sleepMS - 100;
                if (isFirstMaker && Daos.txHelper.getTmConfirmQueue.size() > VConfig.WAIT_BLOCK_MIN_TXN) {
                  sleepMS = 0;
                }
                checkcc = checkcc + 1;
                if (!isFirstMaker && sleepMS > 1000 && (lastCominercc != VCtrl.coMinerByUID.size || checkcc % 100 == 0)) {
                  lastCominercc = VCtrl.coMinerByUID.size
                  val ranInt: Int = new BigInteger(blkInfo.beaconHash, 16).intValue().abs;
                  var newBits = BigInteger.ZERO;
                  VCtrl.coMinerByUID.map(f => {
                    if (!VCtrl.isBanforMiner(blkInfo.needHeight, f._1)) {
                      newBits = newBits.setBit(f._2.getBitIdx)
                    }
                  })
                  newBits = newBits.and(blkInfo.netBits);
                  val (state, newblockbits, natarybits, realSleepMs, firstBlockMakerBitIndex) = RandFunction.chooseGroups(ranInt, newBits, VCtrl.curVN().getBitIdx);
                  if (System.currentTimeMillis() - startupMS > realSleepMs + 500) {
                    log.info("waitup for realSleepMs should waitup,realSleepMs=" + realSleepMs + ",newBits=" + newBits.toString(2) + ",netBits=" + blkInfo.netBits.toString(2));
                    sleepMS = 0;
                  } else {
                    sleepMS = realSleepMs.longValue() - (System.currentTimeMillis() - startupMS) - 100;
                  }

                }
              } while (sleepMS > 100 && VCtrl.curVN().getBeaconHash.equals(blkInfo.beaconHash) && !VCtrl.isBanforMiner(blkInfo.needHeight)
                  &&StringUtils.equals(blkInfo.beaconHash, VCtrl.instance.waitCreateBlockBeacon));

              if (VCtrl.curVN().getBeaconHash.equals(blkInfo.beaconHash) && !VCtrl.isBanforMiner(blkInfo.needHeight) && StringUtils.equals(blkInfo.beaconHash, VCtrl.instance.waitCreateBlockBeacon)) {
                //              log.error("MPRealCreateBlock:start");
                BlockProcessor.offerMessage(new MPRealCreateBlock(blkInfo.netBits, blkInfo.blockbits, blkInfo.notarybits, blkInfo.beaconHash, blkInfo.preBeaconHash, blkInfo.beaconSig, blkInfo.witnessNode, blkInfo.needHeight))
              } else {
                log.warn("cancel create block:" + blkInfo.beaconHash +",waitbeacon="+VCtrl.instance.waitCreateBlockBeacon + ",sleep still:" + sleepMS
                  + ",ban=" + VCtrl.banMinerByUID.get(VCtrl.curVN().getBcuid).getOrElse((0, 0l))
                  );
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
          if (VCtrl.curVN().getBeaconHash.equals(blk.beaconHash)
            && blk.needHeight == (VCtrl.curVN().getCurBlock + 1) && !VCtrl.isBanforMiner(blk.needHeight)) {
            blk.proc();
          } else {
            log.warn("cancel create block:" + blk.beaconHash + " current:" + VCtrl.curVN().getBeaconHash + " blk.needHeight=" + blk.needHeight + " curblock=" + VCtrl.curVN().getCurBlock
              + ",ban=" + VCtrl.banMinerByUID.get(VCtrl.curVN().getBcuid).getOrElse((0, 0l)));
          }
        case n @ _ =>
          log.warn("unknow info:" + n);
      }
      //      Daos.ddc.executeNow(arg0, arg1, arg2)
    }
  }
}