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
    items.asScala.map(m => {
      //should wait
      m match {
        case syncInfo: GossipRecentBlocks =>

        case syncInfo: SyncBlock =>
          log.debug("syncInfo =" + syncInfo);

          val messageid = UUIDGenerator.generate();
          val randn = VCtrl.ensureNode(syncInfo.fromBuid);
          val start = System.currentTimeMillis();
          VCtrl.network().asendMessage("SYNVRF", syncInfo.reqBody, randn,
            new CallBack[FramePacket] {
              def onSuccess(fp: FramePacket) = {
                val end = System.currentTimeMillis();
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

                      var maxid: Int = 0
                      val realmap = ret.getBlockHeadersList.asScala; //.filter { p => p.getBlockHeight >= syncInfo.reqBody.getStartId && p.getBlockHeight <= syncInfo.reqBody.getEndId }
                      //            if (realmap.size() == endIdx - startIdx + 1) {
                      log.debug("realBlockCount=" + realmap.size);
                      var lastSuccessBlock: BlockEntityOrBuilder = null;
                      realmap.map { b =>
                        val block = BlockEntity.newBuilder().mergeFrom(b.getBlockHeader);
                        val vres = Daos.blkHelper.ApplyBlock(block, true);
                        if (vres.getCurrentNumber >= b.getBlockHeight) {
                          if (vres.getCurrentNumber > maxid) {
                            lastSuccessBlock = block
                            maxid = vres.getCurrentNumber.intValue();
                          }
                          log.info("sync block height ok=" + b.getBlockHeight + ",dbh=" + vres.getCurrentNumber+",hash="+block.getHeader.getBlockHash+",seed="+
                            block.getHeader.getExtraData);

                        } else {
                          log.debug("sync block height failed=" + b.getBlockHeight + ",dbh=" + vres.getCurrentNumber + ",curBlock=" + maxid+",hash="+block.getHeader.getBlockHash
                              +",prev="+block.getHeader.getParentHash+",seed="+
                            block.getHeader.getExtraData);
                        }
                      }
                      log.debug("checkMiner --> maxid::" + maxid)
                      if (maxid > 0) {
                        VCtrl.instance.updateBlockHeight(maxid,lastSuccessBlock.getHeader.getBlockHash, lastSuccessBlock.getHeader.getExtraData)
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