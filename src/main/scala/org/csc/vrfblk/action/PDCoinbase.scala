
package org.csc.vrfblk.action

import java.math.BigInteger
import java.util.ArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConversions.`deprecated asScalaBuffer`

import org.apache.commons.lang3.StringUtils
import org.apache.felix.ipojo.annotations.Instantiate
import org.apache.felix.ipojo.annotations.Provides
import org.csc.dposblk.PSMDPoSNet
import org.csc.dposblk.pbgens.Dposblock.PCommand
import org.csc.dposblk.pbgens.Dposblock.PRetCoinbase
import org.csc.dposblk.pbgens.Dposblock.PSCoinbase
import org.csc.dposblk.tasks.BlockSync
import org.csc.dposblk.tasks.DCtrl
import org.csc.dposblk.tasks.DTask_DutyTermVote
import org.csc.dposblk.utils.DConfig
import org.csc.p22p.action.PMNodeHelper
import org.csc.p22p.utils.LogHelper

import com.google.common.cache.Cache
import com.google.common.cache.CacheBuilder

import onight.oapi.scala.commons.LService
import onight.oapi.scala.commons.PBUtils
import onight.osgi.annotation.NActorProvider
import onight.tfw.async.CompleteHandler
import onight.tfw.otransio.api.PacketHelper
import onight.tfw.otransio.api.beans.FramePacket
import onight.tfw.otransio.api.session.CMDService
import onight.tfw.proxy.IActor
import onight.tfw.ntrans.api.ActorService
import org.csc.evmapi.gens.Tx.MultiTransaction

@NActorProvider
@Instantiate
@Provides(specifications = Array(classOf[ActorService], classOf[IActor], classOf[CMDService]))
class PDCoinbaseM extends PSMDPoSNet[PSCoinbase] {
  override def service = PDCoinbase
}

//
// http://localhost:8000/fbs/xdn/pbget.do?bd=
object PDCoinbase extends LogHelper with PBUtils with LService[PSCoinbase] with PMNodeHelper {
  val running = new AtomicBoolean(true);
  val queue = new LinkedBlockingQueue[PSCoinbase]
  val pendingBlockCache: Cache[Int, String] = CacheBuilder.newBuilder().expireAfterWrite(40, TimeUnit.SECONDS)
    .maximumSize(1000).build().asInstanceOf[Cache[Int, String]]

  object ApplyRunner extends Runnable {

    override def run() {
      running.set(true);
      Thread.currentThread().setName("PDCoinbase Runner");
      while (running.get) {
        try {
          var h = queue.poll(10, TimeUnit.SECONDS);
          if (h != null) {
            bgApplyBlock(h);
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
  new Thread(ApplyRunner).start();

  def isPendingBlock(blockheight: Int): Boolean = {
    pendingBlockCache.getIfPresent(blockheight) != null;
  }
  override def onPBPacket(pack: FramePacket, pbo: PSCoinbase, handler: CompleteHandler) = {
    //    log.debug("Mine Block From::" + pack.getFrom())
    var ret = PRetCoinbase.newBuilder();
    if (!DCtrl.isReady()) {
      log.debug("DCtrl not ready");
      ret.setRetCode(-1).setRetMessage("DPoS Network Not READY")
      handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
    } else {
      ret.setRetCode(0).setRetMessage("BGRunniner")
      pendingBlockCache.put(pbo.getBlockHeight, pbo.getBcuid)
      queue.offer(pbo);
      handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
    }
  }
  def bgApplyBlock(pbo: PSCoinbase) {
    try {
      MDCSetBCUID(DCtrl.dposNet())
      MDCSetMessageID(pbo.getMessageId)
      //
      val cn = DCtrl.curDN()
//      log.debug("coinbase pbo.getBlockHeight::" + pbo.getBlockHeight);
      if (pbo.getTxbodiesCount > 0) {
        val arrayList = new ArrayList[MultiTransaction.Builder]();
        pbo.getTxbodiesList.map { x => arrayList.add(MultiTransaction.newBuilder().mergeFrom(x)) }
        log.debug("try to save body,size=" + arrayList.size);
        try {
          val cdl = new CountDownLatch(1);
          val completeHandler = new CompleteHandler {
            override def onFinished(packet: FramePacket) {
              cdl.countDown()
            }
            override def onFailed(e: Exception) {
              cdl.countDown()
            }
          }
          PDPoSTransactionSyncService.greendbBatchSaveList.addFirst((arrayList, BigInteger.ZERO.setBit(DCtrl.dposNet().directNodeByBcuid(pbo.getBcuid).node_idx), completeHandler))
          cdl.await(30, TimeUnit.SECONDS);
        } catch {
          case t: Throwable =>
          log.error("error in applying tx for block:"+pbo.getBlockHeight+",txcount="+arrayList.size()+",dblist="+PDPoSTransactionSyncService.dbBatchSaveList.size())
        } finally {
        }
      }
      if (pbo.getTxcount <= 0 && pbo.getBlockHeight >= cn.getCurBlock) {
        DCtrl.emptyBlock.incrementAndGet();
      } else if (pbo.getTxcount >= DConfig.MAX_TNX_EACH_BLOCK * 0.8 && pbo.getBlockHeight >= cn.getCurBlock - 1) {
        DCtrl.emptyBlock.set(0);
      }
      var nextMiner: String = null;
      if (pbo.getTermId == cn.getTermId) {
        if (pbo.getBlockHeight < cn.getTermEndBlock) {
          DCtrl.minerByBlockHeight(pbo.getBlockHeight + 1) match {
            case Some(coaddr) =>
              DCtrl.coMinerByUID.map(kvs => {
                if (kvs._2.getCoAddress.equals(coaddr)) {
                  nextMiner = kvs._2.getBcuid;
                }
              })
            case None =>
          }
        }
      }
      if (!StringUtils.equals(pbo.getCoAddress, cn.getCoAddress)) {
        //          cn.synchronized {
        if (StringUtils.equals(pbo.getCoAddress, cn.getCoAddress) || pbo.getBlockHeight > cn.getCurBlock) {

          if (pbo.getTermId >= DCtrl.termMiner().getTermId ||
            DCtrl.checkMiner(pbo.getBlockHeight, pbo.getCoAddress, pbo.getMineTime, Thread.currentThread().getName())._1) {
            val (acceptHeight, blockWant) = DCtrl.saveBlock(pbo.getBlockEntry);
            acceptHeight match {
              case n if n > 0 && n < pbo.getBlockHeight =>
                //                  ret.setResult(CoinbaseResult.CR_PROVEN)
                log.info("newblock:UU,H=" + pbo.getBlockHeight + ",DB=" + n + ":coadr=" + pbo.getCoAddress + ",MN=" + DCtrl.coMinerByUID
                  .size + ",DN=" + DCtrl.dposNet().directNodeByIdx.size + ",PN=" + DCtrl.dposNet().pendingNodeByBcuid.size + ",CN=" + DCtrl.termMiner().getCoNodes
                  + ",T=" + DCtrl.termMiner().getTermId + "," + pbo.getTermSign + ",B=" + pbo.getBlockEntry.getSign + ",LB=" + DCtrl.termMiner().getBlockRange.getEndBlock
                  + ",TX=" + pbo.getTxcount + ",NextM=" + nextMiner);
                if (pbo.getTermId > DCtrl.termMiner().getTermId && DTask_DutyTermVote.possibleTermID.size() < DConfig.MAX_POSSIBLE_TERMID) {
                  DTask_DutyTermVote.possibleTermID.put(pbo.getTermId, pbo.getBcuid + "," + pbo.getBlockHeight);
                }
                DCtrl.bestheight.set(n);
                BlockSync.tryBackgroundSyncLogs(blockWant, pbo.getBcuid, false)(DCtrl.dposNet())
                PDQueryDutyTermService.queryVote()
              case n if n > 0 =>
                log.info("newblock:OK,H=" + pbo.getBlockHeight + ",DB=" + n + ":coadr=" + pbo.getCoAddress + ",MN=" + DCtrl.coMinerByUID
                  .size + ",DN=" + DCtrl.dposNet().directNodeByIdx.size + ",PN=" + DCtrl.dposNet().pendingNodeByBcuid.size + ",CN=" + DCtrl.termMiner().getCoNodes
                  + ",T=" + DCtrl.termMiner().getTermId + "," + pbo.getTermSign + ",B=" + pbo.getBlockEntry.getSign + ",LB=" + DCtrl.termMiner().getBlockRange.getEndBlock
                  + ",TX=" + pbo.getTxcount + ",NextM=" + nextMiner)
                DCtrl.bestheight.set(n);
                if (pbo.getTermId > DCtrl.termMiner().getTermId && DTask_DutyTermVote.possibleTermID.size() < DConfig.MAX_POSSIBLE_TERMID) {
                  DTask_DutyTermVote.possibleTermID.put(pbo.getTermId, pbo.getBcuid + "," + pbo.getBlockHeight);
                }
                if (0
                    == DCtrl.termMiner().getTermId || StringUtils.isBlank(DCtrl.termMiner().getSign)) {
                  //get term...
                  DTask_DutyTermVote.checkPossibleTerm(DCtrl.voteRequest())(DCtrl.dposNet())
                }
                if (DCtrl.termMiner().getTermId != pbo.getTermId) {
                  //sync termid
                  log.info("try to change local vote.,T=" + DCtrl.termMiner().getTermId + ",PT=" + pbo.getTermId + ",tsign=" +
                    DCtrl.termMiner().getSign + ",pbtermsign=" + pbo.getTermSign);
                  PDQueryDutyTermService.queryVote()
                }
              //                  ret.setResult(CoinbaseResult.CR_PROVEN)
              case n @ _ =>
                log.info("newblock:NO,H=" + pbo.getBlockHeight + ",DB=" + n + ":coadr=" + pbo.getCoAddress + ",MN=" + DCtrl.coMinerByUID
                  .size + ",DN=" + DCtrl.dposNet().directNodeByIdx.size + ",PN=" + DCtrl.dposNet().pendingNodeByBcuid.size + ",CN=" + DCtrl.termMiner().getCoNodes
                  + ",T=" + DCtrl.termMiner().getTermId + "," + pbo.getTermSign + ",B=" + pbo.getBlockEntry.getSign + ",LB=" + DCtrl.termMiner().getBlockRange.getEndBlock
                  + ",TX=" + pbo.getTxcount + ",NextM=" + nextMiner)
              //                  ret.setResult(CoinbaseResult.CR_REJECT)
            }
          } else {
            log.info("Miner not for the block:Block=" + pbo.getBlockHeight + ",CA=" + pbo.getCoAddress + ",sign=" + pbo.getBlockEntry.getSign + ",from=" + pbo.getBcuid
              + ",PTID=" + pbo.getTermId + ",TID=" + DCtrl.termMiner().getTermId);
            //              ret.setResult(CoinbaseResult.CR_REJECT)
          }
        } else {
          log.info("Current Miner Height is not consequence,PBOH=" + pbo.getBlockHeight + ",CUR=" + cn.getCurBlock
            + ",CA=" + pbo.getCoAddress + ",sign=" + pbo.getBlockEntry.getSign + ",from=" + pbo.getBcuid + ",termid=" + DCtrl.termMiner().getTermId
            + ",TX=" + pbo.getTxcount);
          //            ret.setResult(CoinbaseResult.CR_REJECT)
        }
        if (pbo.getTermId > DCtrl.termMiner().getTermId) {
          log.info("local term id lower than block:pbot=" + pbo.getTermId + ",tm=" + DCtrl.termMiner().getTermId + ",H=" + pbo.getBlockHeight + ",DBH=" + cn.getCurBlock + ":coadrr=" + pbo.getCoAddress + ",MN=" + DCtrl.coMinerByUID.size + ",DN=" + DCtrl.dposNet().directNodeByIdx.size + ",PN=" + DCtrl.dposNet().pendingNodeByBcuid.size
            + ",TX=" + pbo.getTxcount);
          if (DTask_DutyTermVote.possibleTermID.size() < DConfig.MAX_POSSIBLE_TERMID) {
            DTask_DutyTermVote.possibleTermID.put(pbo.getTermId, pbo.getBcuid + "," + pbo.getBlockHeight);
          }
          if (DCtrl.termMiner().getTermId != pbo.getTermId) {
            //sync termid
            PDQueryDutyTermService.queryVote()
          }
        }
        //          }
      } else {
        log.info("newblock:ok,H=" + pbo.getBlockHeight + ",DB=" + pbo.getBlockHeight + ":Local=" + pbo.getCoAddress + ",MN=" + DCtrl.coMinerByUID
          .size + ",DN=" + DCtrl.dposNet().directNodeByIdx.size + ",PN=" + DCtrl.dposNet().pendingNodeByBcuid.size + ",CN=" + DCtrl.termMiner().getCoNodes
          + ",T=" + DCtrl.termMiner().getTermId + "," + pbo.getTermSign + ",B=" + pbo.getBlockEntry.getSign + ",LB=" + DCtrl.termMiner().getBlockRange.getEndBlock
          + ",TX=" + pbo.getTxcount + ",NextM=" + nextMiner)
        DCtrl.bestheight.set(pbo.getBlockHeight);
        //          ret.setResult(CoinbaseResult.CR_PROVEN)
      }

    } catch {
      case t: Throwable => {
        log.error("error:", t);
      }
    } finally {
    }
  }
  //  override def getCmds(): Array[String] = Array(PWCommand.LST.name())
  override def cmd: String = PCommand.MIN.name();
}
