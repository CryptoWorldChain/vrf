package org.csc.vrfblk.tasks

import org.csc.p22p.node.Network
import org.csc.p22p.utils.LogHelper
import onight.tfw.outils.serialize.UUIDGenerator
import onight.tfw.async.CallBack
import onight.tfw.otransio.api.beans.FramePacket

import scala.collection.JavaConversions._
import org.csc.bcapi.gens.Oentity.OValue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.TimeUnit
import org.csc.p22p.action.PMNodeHelper
import org.csc.bcapi.exec.SRunner
import org.csc.dposblk.pbgens.Dposblock.PRetSyncBlocks
import org.csc.dposblk.pbgens.Dposblock.PSSyncBlocks
import org.csc.dposblk.utils.DConfig

//获取其他节点的term和logidx，commitidx
case class DTask_SyncBlock(startIdx: Int, endIdx: Int,
    network: Network, fastNodeID: String,
    runCounter: AtomicLong, syncSafeBlock: Boolean = true, needBody: Boolean = false) extends SRunner with PMNodeHelper with LogHelper {
  def getName(): String = "SyncBlock:" + startIdx + "-" + (endIdx)

  def runOnce() = {
    //
    try {
      Thread.currentThread().setName(getName);
      MDCSetBCUID(DCtrl.dposNet())
      val messageid = UUIDGenerator.generate();
      MDCSetMessageID(messageid)

      val sync = PSSyncBlocks.newBuilder().setStartId(startIdx)
        .setEndId(endIdx).setDn(DCtrl.curDN()).setMessageId(messageid).setNeedBody(needBody).build()
      val start = System.currentTimeMillis();
      val n = network.nodeByBcuid(fastNodeID);
      //find random node.
      val dnodes = DCtrl.coMinerByUID.filter(f => f._2.getCurBlock >= endIdx
        && network.directNodeByBcuid.get(f._1).nonEmpty
        && network.directNodeByBcuid.get(f._1) != network.noneNode
        && !f._1.equals(network.root().bcuid))
        .map(f => network.directNodeByBcuid.get(f._1).get)

      val randn = if (n != network.noneNode) {
        //        log.error("dnodes size is 0")
        n
      } else {
        network.directNodeByBcuid.values.toList.get((Math.abs(Math.random() * 100000) % dnodes.size).asInstanceOf[Int])
        n
      }

      if (randn == null || randn == network.noneNode) {
        log.error("cannot found node from Network:" + network.netid + ",bcuid=" + fastNodeID + ",rand=" + randn)
      } else {
        var success = false;
        var trycc:Int = 0;
        while (trycc < 3 && !success) {
          trycc = trycc + 1
          network.sendMessage("SYNDOB", sync, randn, new CallBack[FramePacket] {
            def onSuccess(fp: FramePacket) = {
              val end = System.currentTimeMillis();
              MDCSetBCUID(DCtrl.dposNet());
              MDCSetMessageID(messageid)
              try {
                if (fp.getBody == null) {
                  log.debug("send SYNDOB error:to " + randn.bcuid + ",cost=" + (end - start) + ",s=" + startIdx + ",e=" + endIdx + ",ret=null")
                } else {
                  val ret = PRetSyncBlocks.newBuilder().mergeFrom(fp.getBody);
                  log.debug("send SYNDOB success:to " + randn.bcuid + ",cost=" + (end - start) + ",s=" + startIdx + ",e=" + endIdx + ",ret=" +
                    ret.getRetCode + ",count=" + ret.getBlockHeadersCount)

                  if (ret.getRetCode() == 0) { //same message

                    var maxid: Int = 0
                    val realmap = ret.getBlockHeadersList.filter { p => p.getBlockHeight >= startIdx && p.getBlockHeight <= endIdx }
                    //            if (realmap.size() == endIdx - startIdx + 1) {
                    log.debug("realBlockCount=" + realmap.size);
                    realmap.map { b =>
                      val (acceptedHeight, blockwanted) = DCtrl.saveBlock(b, needBody);
                      if (acceptedHeight >= b.getBlockHeight) {
                        log.debug("sync block height ok=" + b.getBlockHeight + ",dbh=" + acceptedHeight);
                      } else {
                        log.debug("sync block height failed=" + b.getBlockHeight + ",dbh=" + acceptedHeight + ",curBlock=" + DCtrl.curDN().getCurBlock);
                        if (acceptedHeight == DCtrl.curDN().getCurBlock &&
                          (DCtrl.curDN().getCurBlock + 1 == b.getBlockHeight ||
                            DCtrl.curDN().getCurBlock + 2 == b.getBlockHeight)) {
                          if (syncSafeBlock) {
                            log.error("try to sync prev-safe blocks:start=" + DCtrl.curDN().getCurBlock + ",count=" + DConfig.SYNC_SAFE_BLOCK_COUNT + ",from=" + fastNodeID)
                            new DTask_SyncBlock(DCtrl.curDN().getCurBlock - DConfig.SYNC_SAFE_BLOCK_COUNT, DCtrl.curDN().getCurBlock + 4,
                              network, fastNodeID, new AtomicLong(1), needBody).runOnce();
                          }
                        }
                      }
                      if (acceptedHeight > maxid) {
                        maxid = acceptedHeight;
                      }
                    }
                    log.debug("checkMiner --> maxid::" + maxid)
                    if (maxid > 0) {
                      DCtrl.instance.updateBlockHeight(maxid,"")
                    }
                    success = true;
                  }
                }
              } catch {
                case t: Throwable =>
                  log.warn("error In SyncBlock:" + t.getMessage, t);
              }
            }
            def onFailed(e: java.lang.Exception, fp: FramePacket) {
              val end = System.currentTimeMillis();
              MDCSetBCUID(DCtrl.dposNet());
              MDCSetMessageID(messageid)
              log.error("send SYNDOB ERROR :to " + fastNodeID + ",cost=" + (end - start) + ",s=" + startIdx + ",e=" + endIdx + ",uri=" + randn.uri + ",e=" + e.getMessage, e)
            }
          }, '8', 10000)
        }
      }
    } catch {
      case e: Throwable =>
        log.error("SyncError:" + e.getMessage, e)
    } finally {
      runCounter.decrementAndGet();
      log.debug("finished sync block:" + startIdx + ",to=" + endIdx + ",fastNodeID=" + fastNodeID);
      Thread.currentThread().setName("dpos-pool");
    }
  }
}
