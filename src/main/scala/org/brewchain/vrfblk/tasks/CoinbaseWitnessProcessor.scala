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
import java.util.concurrent.ExecutorService

object CoinbaseWitnessProcessor extends SingletonWorkShop[BlockMessage] with PMNodeHelper with BitMap with LogHelper {
  var running: Boolean = true;

  def isRunning(): Boolean = {
    return running;
  }

  val processHash = new ConcurrentHashMap[String, String]();

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
  override def getName(): String = {
    "witness"
  }

  override def startup(exec: ExecutorService) {
    for (i <- 1 to VConfig.THREAD_COUNT_WITNESS) {
      exec.submit(this);
    }
  }

  def runBatch(items: List[BlockMessage]): Unit = {
    MDCSetBCUID(VCtrl.network())
    //单线程执行
    for (m <- items.asScala) {
      //should wait
      m match {
        case blk: NotaryBlock =>
          processHash.remove(blk.pbo.getMessageId)
          blk.proc();
        case n @ _ =>
          log.warn("unknow info:" + n);
      }
      //      Daos.ddc.executeNow(arg0, arg1, arg2)
    }
  }
}