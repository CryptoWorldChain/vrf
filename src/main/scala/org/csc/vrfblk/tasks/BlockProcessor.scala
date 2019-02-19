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
import org.csc.vrfblk.msgproc.ApplyBlock
import scala.collection.JavaConverters._
import org.csc.vrfblk.msgproc.NotaryBlock

trait BlockMessage {
  def proc(): Unit;
}

object BlockProcessor extends SingletonWorkShop[BlockMessage] with PMNodeHelper with BitMap with LogHelper {
  var running: Boolean = true;

  def isRunning(): Boolean = {
    return running;
  }

  val NewBlockFP = PacketHelper.genPack("NEWBLOCK", "__VRF", "", true, 9);
  var blockMakeCheckHash: String = null;
  def runBatch(items: List[BlockMessage]): Unit = {
    MDCSetBCUID(VCtrl.network())
    items.asScala.map(m => {
      //should wait
      m match {
        case blkInfo: MPCreateBlock =>
          //          log.debug("get newblock info:" + blkInfo.beaconHash + "," + hexToMapping(blkInfo.netBits));
          var sleepMS = RandFunction.getRandMakeBlockSleep(blkInfo.beaconHash, blkInfo.blockbits, VCtrl.curVN().getBitIdx);
          log.debug("block maker sleep = " + sleepMS + ",bitidx=" + VCtrl.curVN().getBitIdx)
          blockMakeCheckHash = blkInfo.beaconHash;
          log.debug("exec create block background running:" + blockMakeCheckHash + ",sleep :" + sleepMS);
          Daos.ddc.executeNow(NewBlockFP, new Runnable() {
            def run() {
              while (sleepMS > 0 && VCtrl.curVN().getBeaconHash.equals(blockMakeCheckHash)) {
                Thread.sleep(Math.min(100, sleepMS));
                sleepMS = sleepMS - 100;
              }
              if (VCtrl.curVN().getBeaconHash.equals(blockMakeCheckHash)) {
                //create block.
                log.debug("wait up to create block:"+blockMakeCheckHash+ ",sleep still:" + sleepMS);
                blkInfo.proc();
                
              } else {
                log.debug("cancel create block:" + blockMakeCheckHash + ",sleep still:" + sleepMS);
              }
            }
          })
        case blk: ApplyBlock =>
          log.debug("apply block:" + blk.pbo.getBeaconHash + ",netbits=" + new String(blk.pbo.getVrfCodes.toByteArray()) + ",blockheight="
            + blk.pbo.getBlockHeight);
          blk.proc();
        case blk: NotaryBlock =>
          blk.proc();
          
        case n @ _ =>
          log.warn("unknow info:" + n);
      }
      //      Daos.ddc.executeNow(arg0, arg1, arg2)
    })
  }

}