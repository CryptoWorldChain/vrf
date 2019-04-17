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
import org.csc.vrfblk.msgproc.SyncApplyBlock
import org.csc.ckrand.pbgens.Ckrand.PSSyncBlocks
import onight.tfw.async.CallBack
import org.csc.ckrand.pbgens.Ckrand.PRetSyncBlocks
import scala.collection.JavaConverters._
import org.csc.evmapi.gens.Block.BlockEntity
import org.csc.evmapi.gens.Block.BlockEntityOrBuilder

trait SyncInfo {
  //  def proc(): Unit;
}

case class GossipRecentBlocks(bestBlockHash: String) extends SyncInfo;
case class SyncBlock(fromBuid: String, reqBody: PSSyncBlocks) extends SyncInfo;

object BlockSync extends SingletonWorkShop[SyncInfo] with PMNodeHelper with BitMap with LogHelper {
  var running: Boolean = true;

  def isRunning(): Boolean = {
    return running;
  }

  def runBatch(items: List[SyncInfo]): Unit = {
    MDCSetBCUID(VCtrl.network())
    items.asScala.foreach(m => {
      //should wait
      m match {
        case syncInfo: GossipRecentBlocks =>

        case syncInfo: SyncBlock =>
          log.debug("syncInfo =" + syncInfo.toString().replaceAll("\n", ","));

          val messageid = UUIDGenerator.generate();
          // 尝试根据bcuid确认一个节点，如果节点不存在，从网络中随机取一个
          val randn = VCtrl.ensureNode(syncInfo.fromBuid);
          val start = System.currentTimeMillis();
          // 请求一组block，执行applyBlock方法
          VCtrl.network().asendMessage("SYNVRF", syncInfo.reqBody, randn,
            new CallBack[FramePacket] {
              def onSuccess(fp: FramePacket) = {
                val end = System.currentTimeMillis();

                //
                MDCSetBCUID(VCtrl.network());
                MDCSetMessageID(messageid)
                try {
                  if (fp.getBody == null) {
                    log.debug("send SYNVRF error:to " + randn.bcuid + ",cost=" + (end - start) + ",s=" + syncInfo.reqBody + ",ret=null")
                  } else {
                    val ret = PRetSyncBlocks.newBuilder().mergeFrom(fp.getBody);
                    log.debug("send SYNVRF success:to " + randn.bcuid + ",cost=" + (end - start) + ",s=" + syncInfo.reqBody.getStartId + ",ret=" +
                      ret.getRetCode + ",count=" + ret.getBlockHeadersCount)

                    if (ret.getRetCode() == 0) { //same message

                      val realmap = ret.getBlockHeadersList.asScala; //.filter { p => p.getBlockHeight >= syncInfo.reqBody.getStartId && p.getBlockHeight <= syncInfo.reqBody.getEndId }
                      //            if (realmap.size() == endIdx - startIdx + 1) {
                      log.debug("realBlockCount=" + realmap.size);
                      realmap.foreach { b =>
                        //同步执行 apply 并验证返回结果
                        // applyblock
                        val block = BlockEntity.newBuilder().mergeFrom(b.getBlockHeader);
                        BlockProcessor.offerMessage(new SyncApplyBlock(block));
                      }
                    }
                  }
                } catch {
                  case t: Throwable =>
                    log.warn("error In SyncBlock:" + t.getMessage, t);
                } finally {
                  //try gossip againt
                  BeaconGossip.gossipBlocks();
                }
              }
              def onFailed(e: java.lang.Exception, fp: FramePacket) {
                val end = System.currentTimeMillis();
                MDCSetBCUID(VCtrl.network());
                MDCSetMessageID(messageid)
                log.error("send SYNDOB ERROR :to " + randn.bcuid + ",cost=" + (end - start) + ",s=" + syncInfo + ",uri=" + randn.uri + ",e=" + e.getMessage, e)
                BeaconGossip.gossipBlocks();
              }
            })
        case n @ _ =>
          log.warn("unknow info:" + n);
      }
      //      Daos.ddc.executeNow(arg0, arg1, arg2)
    })
  }

}