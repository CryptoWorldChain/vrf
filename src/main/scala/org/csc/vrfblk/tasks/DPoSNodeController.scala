package org.csc.vrfblk.tasks

import java.util.ArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConversions.`deprecated asScalaBuffer`
import scala.collection.JavaConversions.`deprecated seqAsJavaList`
import scala.collection.mutable.Map

import org.apache.commons.lang3.StringUtils
import org.csc.bcapi.JodaTimeHelper
import org.csc.bcapi.exec.SRunner
import org.csc.dposblk.Daos
import org.csc.dposblk.pbgens.Dposblock.DNodeState
import org.csc.dposblk.pbgens.Dposblock.PBlockEntry
import org.csc.dposblk.pbgens.Dposblock.PBlockEntryOrBuilder
import org.csc.dposblk.pbgens.Dposblock.PDNode
import org.csc.dposblk.pbgens.Dposblock.PRetGetTransaction
import org.csc.dposblk.pbgens.Dposblock.PSDutyTermVote
import org.csc.dposblk.pbgens.Dposblock.PSGetTransaction
import org.csc.dposblk.utils.BlkTxCalc
import org.csc.dposblk.utils.DConfig
import org.csc.evmapi.gens.Block.BlockEntity
import org.csc.evmapi.gens.Tx.MultiTransaction
import org.csc.p22p.action.PMNodeHelper
import org.csc.p22p.node.Network
import org.csc.p22p.node.Node
import org.csc.p22p.utils.LogHelper

import com.google.common.cache.Cache
import com.google.common.cache.CacheBuilder

import onight.tfw.async.CallBack
import onight.tfw.otransio.api.beans.FramePacket
import org.csc.bcapi.gens.Oentity.OValue
import java.math.BigInteger

//投票决定当前的节点
case class DPosNodeController(network: Network) extends SRunner with PMNodeHelper with LogHelper {
  def getName() = "DCTRL"
  val DPOS_NODE_DB_KEY = "CURRENT_DPOS_KEY";
  val DPOS_NODE_DB_TERM = "CURRENT_DPOS_TERM";
  var cur_dnode: PDNode.Builder = PDNode.newBuilder()
  var term_Miner: PSDutyTermVote.Builder = PSDutyTermVote.newBuilder();
  var vote_Request: PSDutyTermVote.Builder = PSDutyTermVote.newBuilder();

  var isStop: Boolean = false;

  def updateVoteReq(pbo: PSDutyTermVote): Unit = {
    vote_Request = pbo.toBuilder()
    //    cur_dnode.setNodeCount(vote_Request.getCoNodes)
    //    syncToDB();
  }

  def saveVoteReq(pbo: PSDutyTermVote): Unit = {
    Daos.dposvotedb.put(
      "TERM-TEMP-" + pbo.getSign,
      OValue.newBuilder().setExtdata(pbo.toByteString()).build())
  }

  def loadVoteReq(sign: String): PSDutyTermVote.Builder = {
    val ov = Daos.dposvotedb.get("TERM-TEMP-" + sign).get
    if (ov != null) {
      PSDutyTermVote.newBuilder().mergeFrom(ov.getExtdata)
    } else {
      PSDutyTermVote.newBuilder()
    }
  }
  def loadNodeFromDB(): PDNode.Builder = {
    val ov = Daos.dpospropdb.get(DPOS_NODE_DB_KEY).get
    val root_node = network.root();
    if (ov == null) {
      cur_dnode.setBcuid(root_node.bcuid)
        .setCurBlock(1).setCoAddress(root_node.v_address)
        .setBitIdx(root_node.node_idx)
      Daos.dpospropdb.put(
        DPOS_NODE_DB_KEY,
        OValue.newBuilder().setExtdata(cur_dnode.build().toByteString()).build())
    } else {
      cur_dnode.mergeFrom(ov.getExtdata).setBitIdx(root_node.node_idx)
      if (!StringUtils.equals(cur_dnode.getBcuid, root_node.bcuid)) {
        log.warn("load from dnode info not equals with pzp node:" + cur_dnode + ",root=" + root_node)
        cur_dnode.setBcuid(root_node.bcuid);
        syncToDB();
      } else {
        log.info("load from db:OK:" + cur_dnode)
      }
    }

    if (cur_dnode.getCurBlock != Daos.actdb.getLastBlockNumber.intValue()) {
      log.warn("dpos block height Info not Equal to AccountDB:c=" +
        cur_dnode.getCurBlock + " ==> a=" + Daos.actdb.getLastBlockNumber);
      cur_dnode.setCurBlock(Daos.actdb.getLastBlockNumber.intValue())
      syncToDB()
    }

    val termov = Daos.dpospropdb.get(DPOS_NODE_DB_TERM).get
    if (termov == null) {
      Daos.dpospropdb.put(
        DPOS_NODE_DB_TERM,
        OValue.newBuilder().setExtdata(term_Miner.build().toByteString()).build())
    } else {
      if (DConfig.FORCE_RESET_VOTE_TERM == 1) {
        log.debug("force reset termreq:");
        Daos.dpospropdb.put(
          DPOS_NODE_DB_TERM,
          OValue.newBuilder().setExtdata(term_Miner.build().toByteString()).build())
      } else {
        term_Miner.mergeFrom(termov.getExtdata)
        log.debug("load from db:=" + DConfig.FORCE_RESET_VOTE_TERM);
      }
    }
    cur_dnode.setLastTermSign(term_Miner.getLastTermUid)
      .setTermId(term_Miner.getTermId)
      .setTermSign(term_Miner.getSign)
      .setTermStartBlock(term_Miner.getBlockRange.getStartBlock)
      .setTermEndBlock(term_Miner.getBlockRange.getEndBlock)
  }
  def syncToDB() {

    //    log.debug("checkMiner --> update cur_dnode::" + cur_dnode)
    Daos.dpospropdb.put(
      DPOS_NODE_DB_KEY,
      OValue.newBuilder().setExtdata(cur_dnode.build().toByteString()).build())
  }
  def updateTerm() = {
    cur_dnode.setDutyUid(term_Miner.getSign).setDutyStartMs(term_Miner.getTermStartMs)
      .setDutyEndMs(term_Miner.getTermEndMs).setBitIdx(DCtrl.dposNet().root().node_idx)
    //    cur_dnode.setTermId(term_Miner.getTermId).setl;
    cur_dnode.setLastTermSign(term_Miner.getLastTermUid)
      .setTermId(term_Miner.getTermId)
      .setTermSign(term_Miner.getSign)
      .setTermStartBlock(term_Miner.getBlockRange.getStartBlock)
      .setTermEndBlock(term_Miner.getBlockRange.getEndBlock)

    Daos.dpospropdb.put(DPOS_NODE_DB_TERM,
      OValue.newBuilder().setExtdata(term_Miner.build().toByteString()).build())
    val lostM = Map[String, Node]();
    term_Miner.getMinerQueueList.map { mq =>
      if (DCtrl.coMinerByUID.filter(f => mq.getMinerCoaddr.equals(f._2.getCoAddress)).size == 0) {
        network.directNodes.map { dn =>
          if (dn.v_address.equals(mq.getMinerCoaddr)) {
            lostM.put(dn.bcuid, dn);
          }
        }
      }
    }
    if (lostM.size > 0) {
      hbTask.trySyncMinerInfo(lostM.values, network)
    }

  }
  def updateBlockHeight(blockHeight: Int, blockHash: String) = {
    log.debug("checkMiner --> updateBlockHeight blockHeight::" + blockHeight + " cur_dnode.getCurBlock::" + cur_dnode.getCurBlock);
    if (blockHeight != cur_dnode.getCurBlock) {

      Daos.blkHelper.synchronized({
        //      if (cur_dnode.getCurBlock < blockHeight) {
        cur_dnode.setLastBlockTime(System.currentTimeMillis())
        //cur_dnode.setCurBlock(blockHeight)
        cur_dnode.setCurBlock(Daos.actdb.getLastBlockNumber.intValue())
        if(blockHash!=null)  
        {
          cur_dnode.setCurBlockHash(blockHash)
        }
        log.debug("checkMiner --> cur_dnode.setCurBlock::" + cur_dnode.getCurBlock);
        syncToDB()
        //      }
      })
    }
  }
  val hbTask = DTask_HeatBeat();
  def runOnce() = {
    Thread.currentThread().setName("DCTRL");
    implicit val _net = network
    MDCSetBCUID(network);
    MDCRemoveMessageID()
    var continue = true;
    var continuecCC = 0;
    while (continue && !isStop && continuecCC<DConfig.DCTRL_CONTINUE_LOOP_COUNT) {
      try {
        continuecCC = continuecCC + 1;
        MDCSetMessageID(term_Miner.getSign)
        continue = false;
        log.info("DCTRL.RunOnce:S=" + cur_dnode.getState + ",B=" + cur_dnode.getCurBlock
          + ",CA=" + cur_dnode.getCoAddress
          + ",BCUID=" + term_Miner.getBcuid
          + ",MN=" + DCtrl.coMinerByUID.size
          + ",DN=" + network.bitenc.bits.bitCount
          + ",CN=" + term_Miner.getCoNodes
          + ",PN=" + network.pendingNodeByBcuid.size
          + ",DU=" + cur_dnode.getDutyUid
          + ",VT=" + vote_Request.getTermId
          + ",TM=" + term_Miner.getTermId
          + ",TU=" + term_Miner.getSign
          + ",VU=" + vote_Request.getSign
          + ",banVote=" + (System.currentTimeMillis() <= DTask_DutyTermVote.ban_for_vote_sec) + ":" + (-1 * JodaTimeHelper.secondIntFromNow(DTask_DutyTermVote.ban_for_vote_sec))
          + ",NextSec=" + JodaTimeHelper.secondFromNow(cur_dnode.getDutyEndMs)
          + ",SecPass=" + JodaTimeHelper.secondFromNow(cur_dnode.getLastDutyTime));
        cur_dnode.getState match {
          case DNodeState.DN_INIT =>
            //tell other I will join
            loadNodeFromDB();
            continue = DTask_CoMine.runOnce match {
              case n: PDNode if n == cur_dnode =>
                log.debug("dpos cominer init ok:" + n);
//                Scheduler.scheduleWithFixedDelay(hbTask, 60, DConfig.HEATBEAT_TICK_SEC, TimeUnit.SECONDS);
                Daos.ddc.scheduleWithFixedDelaySecond(hbTask, 60, DConfig.HEATBEAT_TICK_SEC);
                hbTask.onScheduled = true;
                true;
              case n: PDNode if !n.equals(cur_dnode) =>
                log.debug("dpos waiting for init:" + n);
                false
              case x @ _ =>
                log.debug("not ready:" + x);
                false
            }
          case DNodeState.DN_CO_MINER =>

            if (!hbTask.onScheduled) {
//              Scheduler.scheduleWithFixedDelay(hbTask, 60, DConfig.HEATBEAT_TICK_SEC, TimeUnit.SECONDS);
              Daos.ddc.scheduleWithFixedDelaySecond( hbTask,60, DConfig.HEATBEAT_TICK_SEC);
              hbTask.onScheduled = true;
            }
            if (DConfig.RUN_COMINER != 1) {
              cur_dnode.setState(DNodeState.DN_BAKCUP)
            } else if (DTask_DutyTermVote.runOnce) {
              continue = true;
              cur_dnode.setState(DNodeState.DN_DUTY_MINER);
            } else {
              log.debug("cominer run false:" + cur_dnode.getCurBlock + ",vq[" + DCtrl.voteRequest().getBlockRange.getStartBlock
                + "," + DCtrl.voteRequest().getBlockRange.getEndBlock + "]" + ",vqid=" + DCtrl.voteRequest().getTermId
                + ",vqlid=" + DCtrl.voteRequest().getLastTermId + ",tid=" + term_Miner.getTermId
                + ",tq[" + term_Miner.getBlockRange.getStartBlock + "," + term_Miner.getBlockRange.getEndBlock + "]");
            }
          case DNodeState.DN_DUTY_MINER =>
            if (DConfig.RUN_COMINER != 1) {
              cur_dnode.setState(DNodeState.DN_BAKCUP)
            } else if (term_Miner.getBlockRange.getStartBlock > cur_dnode.getCurBlock + term_Miner.getMinerQueueCount) {
              log.debug("cur term force to resync block:" + cur_dnode.getCurBlock + ",vq[" + DCtrl.voteRequest().getBlockRange.getStartBlock
                + "," + DCtrl.voteRequest().getBlockRange.getEndBlock + "]" + ",vqid=" + DCtrl.voteRequest().getTermId
                + ",vqlid=" + DCtrl.voteRequest().getLastTermId + ",tid=" + term_Miner.getTermId
                + ",tq[" + term_Miner.getBlockRange.getStartBlock + "," + term_Miner.getBlockRange.getEndBlock + "]");
              continue = true;
              cur_dnode.setState(DNodeState.DN_SYNC_BLOCK);
            } else if (cur_dnode.getCurBlock >= term_Miner.getBlockRange.getEndBlock && term_Miner.getBlockRange.getEndBlock > 1 //|| DCtrl.voteRequest().getLastTermId >= term_Miner.getTermId
            ) {
              log.debug("cur term force to end:" + cur_dnode.getCurBlock + ",vq[" + DCtrl.voteRequest().getBlockRange.getStartBlock
                + "," + DCtrl.voteRequest().getBlockRange.getEndBlock + "]" + ",vqid=" + DCtrl.voteRequest().getTermId
                + ",vqlid=" + DCtrl.voteRequest().getLastTermId + ",tid=" + term_Miner.getTermId
                + ",tq[" + term_Miner.getBlockRange.getStartBlock + "," + term_Miner.getBlockRange.getEndBlock + "]");
              continue = true;
              cur_dnode.setState(DNodeState.DN_CO_MINER);
            } else if (DTask_MineBlock.runOnce) {
              if (cur_dnode.getCurBlock >= term_Miner.getBlockRange.getEndBlock
                || term_Miner.getTermId < vote_Request.getTermId) {
//                val sleept = Math.abs((Math.random() * 100000000 % DConfig.DTV_TIME_MS_EACH_BLOCK).asInstanceOf[Long]) + 10;
                log.info("cur term WILL end:newblk=" + cur_dnode.getCurBlock + ",term[" + DCtrl.voteRequest().getBlockRange.getStartBlock
                  + "," + DCtrl.voteRequest().getBlockRange.getEndBlock + "]" + ",T=" + term_Miner.getTermId + ",try to vote,blk="+cur_dnode.getCurBlock);
                continue = true;
                cur_dnode.setState(DNodeState.DN_CO_MINER);
                //don't sleep for next vote.
//                DTask_DutyTermVote.synchronized({
//                  DTask_DutyTermVote.wait(sleept)
//                });
                true
              } else {
                val pendingSize = Daos.confirmMapDB.getQueueSize;
                if (pendingSize > DConfig.WAIT_BLOCK_MIN_TXN) {
                  continue = true;
                  true
                } else {
                  false
                }
              }
            } else {
              //check who mining.
              if (cur_dnode.getCurBlock >= term_Miner.getBlockRange.getEndBlock && term_Miner.getBlockRange.getEndBlock > 1) {
                continue = true;
                cur_dnode.setState(DNodeState.DN_CO_MINER);
                val sleept = Math.abs((Math.random() * 10000000 % DConfig.DTV_TIME_MS_EACH_BLOCK).asInstanceOf[Long]) + 1000;
                DTask_DutyTermVote.synchronized({
                  DTask_DutyTermVote.wait(sleept)
                });
                true
              } else {
                false;
              }
            }
          case DNodeState.DN_SYNC_BLOCK =>
            DTask_CoMine.runOnce
          case DNodeState.DN_BAKCUP =>
            DTask_CoMine.runOnce
          case _ =>
            log.warn("unknow State:" + cur_dnode.getState);

        }

      } catch {
        case e: Throwable =>
          log.warn("dpos control :Error", e);
      } finally {
        MDCRemoveMessageID()
      }
    }
  }
}

object DCtrl extends LogHelper {
  var instance: DPosNodeController = DPosNodeController(null);
  def dposNet(): Network = instance.network;
  //  val superMinerByUID: Map[String, PDNode] = Map.empty[String, PDNode];
  val coMinerByUID: Map[String, PDNode] = Map.empty[String, PDNode];
  def curDN(): PDNode.Builder = instance.cur_dnode
  def termMiner(): PSDutyTermVote.Builder = instance.term_Miner
  def voteRequest(): PSDutyTermVote.Builder = instance.vote_Request

  def getFastNode(): String = {
    var fastNode = curDN().build();
    coMinerByUID.map { f =>
      if (f._2.getCurBlock > fastNode.getCurBlock) {
        fastNode = f._2;
      }
    }
    fastNode.getBcuid
  }
  //  def curTermMiner(): PSDutyTermVoteOrBuilder = instance.term_Miner

  def isReady(): Boolean = {
    instance.network != null &&
      instance.cur_dnode.getStateValue > DNodeState.DN_INIT_VALUE
  }
  def sleep(sleepMS: Long): Unit = {
    if (sleepMS <= 1) return
    Thread.sleep(sleepMS);
  }
  def checkMiner(block: Int, coaddr: String, mineTime: Long, threadName: String, maxWaitMS: Long = 0L): (Boolean, Boolean) = {
    val tm = termMiner().getBlockRange;
    val lastBlkTime = if (block == 1) termMiner().getTermStartMs else
      Math.max(Daos.blkHelper.GetBestBlock().getHeader.getTimestamp, termMiner().getTermStartMs);
    val blkshouldMineMS = tm.getEachBlockMs + lastBlkTime
    val minblkshouldMineMS = lastBlkTime + DConfig.BLK_MIN_EPOCH_MS;

    log.debug("checkMiner --> block::" + block + " curDN.getCurBlock::" + curDN.getCurBlock + " tm.getEndBlock::" + tm.getEndBlock + " tm.getStartBlock::" + tm.getStartBlock + " coaddr::" + coaddr)
    if (block > tm.getEndBlock || block < tm.getStartBlock) {
      log.debug("checkMiner:False,block too large:" + block + ",[" + tm.getStartBlock + "," + tm.getEndBlock + "],sign="
        + termMiner.getSign + ",TID=" + termMiner.getTermId)
      val maxblk = Math.min(block + 2, tm.getEndBlock)
      log.debug("checkMiner --> maxblk::" + maxblk + " curDN.getCurBlock::" + curDN.getCurBlock)
      if (maxblk > curDN.getCurBlock && System.currentTimeMillis() > blkshouldMineMS + DConfig.BLK_EPOCH_MS * 2) {
        val fastuid = DCtrl.getFastNode();
        if (!StringUtils.equals(fastuid, curDN.getBcuid)) {
          BlockSync.tryBackgroundSyncLogs(maxblk, fastuid, true)(DCtrl.dposNet())
        }
      }
      (false, false)
    } else {
      val realblkMineMS = mineTime;
      val termblockLeft = block - tm.getEndBlock
      minerByBlockHeight(block) match {
        case Some(n) =>
          if (coaddr.equals(n)) {
            //if (DCtrl.termMiner().getMinerQueue(101-DCtrl.termMiner().getBlockRange.getStartBlock).getBlockHeight == block + 1) {
            if (realblkMineMS < blkshouldMineMS) {
              val pendingSize = Daos.confirmMapDB.getQueueSize;
              if (pendingSize >= DConfig.WAIT_BLOCK_MIN_TXN) {
                if (realblkMineMS < minblkshouldMineMS && pendingSize < DConfig.WAIT_BLOCK_MAX_TXN) {
                  log.info("wait for time to My Miner:txsize=" + pendingSize + ":Should=" + minblkshouldMineMS + ",realblkminesec=" + realblkMineMS + ",eachBlockMS=" + tm.getEachBlockMs + ",TermLeft=" + termblockLeft
                    + ",TID=" + termMiner().getTermId + ",TS=" + termMiner().getSign + ",bh=" + block);
                  sleep(Math.min(maxWaitMS, minblkshouldMineMS - realblkMineMS));
                }
              } else {
                log.info("wait for time to My Miner:Should=" + blkshouldMineMS + ",realblkminesec=" + realblkMineMS + ",eachBlockMS=" + tm.getEachBlockMs + ",TermLeft=" + termblockLeft
                  + ",TID=" + termMiner().getTermId + ",TS=" + termMiner().getSign + ",bh=" + block);
                sleep(Math.min(maxWaitMS, blkshouldMineMS - realblkMineMS));
              }
            }
            //            log.debug("checkMiner --> realblkMineMS::" + realblkMineMS + " blkshouldMineMS::" + blkshouldMineMS + " n::" + n + " coaddr::" + coaddr)
            (true, false)
            //            } else {
            //              log.debug("checkMiner --> current height::" + block + " want height::" + DCtrl.termMiner().getMinerQueue(101-DCtrl.termMiner().getBlockRange.getStartBlock).getBlockHeight)
            //              (false, false)
            //            }
          } else {
            if (block >= 1 && realblkMineMS > blkshouldMineMS + DConfig.MAX_WAIT_BLK_EPOCH_MS) {
              minerByBlockHeight(block + ((realblkMineMS - blkshouldMineMS) / DConfig.MAX_WAIT_BLK_EPOCH_MS).asInstanceOf[Int]) match {
                case Some(nn) =>
                  log.debug("Override miner for Next:check:" + blkshouldMineMS + ",realblkmine=" + realblkMineMS + ",n=" + n
                    + ",next=" + nn + ",coaddr=" + coaddr + ",block=" + (block) + ",TermLeft=" + termblockLeft + ",Result=" + coaddr.equals(nn)
                    + ",TID=" + termMiner().getTermId + ",TS=" + termMiner().getSign); //try to revote.
                  (coaddr.equals(nn), true)
                case None =>
                  log.debug("wait for Miner:Should=" + blkshouldMineMS + ",Real=" + realblkMineMS + ",eachBlockMS=" + tm.getEachBlockMs + ",TermLeft=" + termblockLeft
                    + ",TID=" + termMiner().getTermId + ",TS=" + termMiner().getSign);
                  (false, true)
              }
            } else {

              log.debug("wait for timeout to Other Miner:ShouldT=" + (blkshouldMineMS + DConfig.MAX_WAIT_BLK_EPOCH_MS) + ",realblkmine=" + realblkMineMS + ",eachBlockSec=" + tm.getEachBlockMs
                + ",TermLeft=" + termblockLeft);
              if (realblkMineMS < blkshouldMineMS) {
                sleep(Math.min(maxWaitMS, blkshouldMineMS - realblkMineMS));
              } else {
                //request block if not sync
                val nextwait = minerByBlockHeight(block + 1) match {
                  case Some(n) =>
                    if (coaddr.equals(n)) {
                      0
                    } else {
                      3
                    }
                  case _ =>
                    3
                }

                if (System.currentTimeMillis() > DCtrl.curDN().getLastBlockTime + (DConfig.BLK_WAIT_SYNC_SEC) * 1000) {
                  var bestfastUID = ""
                  coMinerByUID.map { f =>
                    if (f._2.getCoAddress.equals(n)) {
                      bestfastUID = f._2.getBcuid;
                    }
                  }
                  if (nextwait > 0) {
                    Thread.sleep(nextwait * 1000);
                  }
                  BlockSync.tryBackgroundSyncLogs(block, bestfastUID, true)(dposNet());
                }
              }
              (false, false)
            }

          }
        case None =>
          if (maxWaitMS >= 1 && realblkMineMS < blkshouldMineMS) {
            //            log.debug("wait for time to Mine:Should=" + blkshouldMineMS + ",realblkminesec=" + realblkMineMS + ",eachBlockSec=" + tm.getEachBlockSec + ",TermLeft=" + termblockLeft);
            sleep(Math.min(maxWaitMS, blkshouldMineMS - realblkMineMS));
          }
          (false, false)
      }
    }
  }
  def minerByBlockHeight(block: Int): Option[String] = {
    val tm = termMiner().getBlockRange;
    if (block >= tm.getStartBlock && block <= tm.getEndBlock) {
      Some(termMiner().getMinerQueue(block - tm.getStartBlock)
        .getMinerCoaddr)
    } else if (block > tm.getStartBlock && termMiner().getMinerQueueCount > 0) {
      Some(termMiner().getMinerQueue((block - tm.getStartBlock)
        % termMiner().getMinerQueueCount)
        .getMinerCoaddr)
    } else {
      None
    }
  }
  def createNewBlock(txc: Int, confirmTimes: Int): (BlockEntity, java.util.List[MultiTransaction]) = {
    Thread.currentThread().setName("createblock-" + txc + "," + (curDN().getCurBlock + 1));
    Daos.blkHelper.synchronized({
      val txs = Daos.txHelper.getWaitBlockTx(txc, //只是打块！其中某些成功广播的tx，默认是80%
        confirmTimes);

      val newblk = Daos.blkHelper.CreateNewBlock(txs, "", DCtrl.termMiner().getSign);
      //      val newblk = Daos.blkHelper.CreateNewBlock(DCtrl.termMiner().getMaxTnxEachBlock, //只是打块！其中某些成功广播的tx，默认是80%
      //        (DCtrl.termMiner().getCoNodes * DConfig.CREATE_BLOCK_TX_CONFIRM_PERCENT / 100).asInstanceOf[Int],
      //        "", DCtrl.termMiner().getSign);
      val newblockheight = curDN().getCurBlock + 1
      if (newblk == null || newblk.getHeader == null) {
        log.debug("new block header is null: ch=" + newblockheight + ",dbh=" + newblk);
        (null, null)
      } else if (newblockheight != newblk.getHeader.getNumber) {
        log.debug("mining error: ch=" + newblockheight + ",dbh=" + newblk.getHeader.getNumber);
        (null, null)
      } else {
        (newblk, txs)
      }
    })

  }

  val bestheight = new AtomicLong(0);

  val emptyBlock = new AtomicLong(0);

  def saveBlock(b: PBlockEntryOrBuilder, needBody: Boolean = false, reentry: Int = 0): (Int, Int) = {
    Thread.currentThread().setName("saveBlock-" + b.getBlockHeight + "/" + (curDN().getCurBlock + 1));
    this.synchronized({
      if (!b.getCoinbaseBcuid.equals(DCtrl.curDN().getBcuid)) {
        val startupApply = System.currentTimeMillis();
        val vres = Daos.blkHelper.ApplyBlock(b.getBlockHeader, needBody);

        if (vres.getTxHashsCount > 0 && reentry < 3) {
          log.info("must sync transaction first.:reentry=" + reentry + ",losthash=" + vres.getTxHashsCount + ",height=" + b.getBlockHeight);
          var distance = 0
          var found = false;
          DCtrl.termMiner().getMinerQueueList.map(f =>
            if (f.getBlockHeight > b.getBlockHeight) {
              if (!found) {
                if (!f.getMinerCoaddr.equals(DCtrl.curDN().getCoAddress)) {
                  distance = distance + 1;
                } else {
                  found = true;
                }
              }
            })
          var sleep = Math.min(Math.abs((Math.random() * 100000) % (distance + 1) / 2), 5).asInstanceOf[Long]
          if (distance <= 0) {
            sleep = 0;
          } else if (b.getBlockHeight < DCtrl.bestheight.get - DConfig.DTV_BLOCKS_EACH_MINER + 1) {

            sleep = (Math.abs((Math.random() * 100000) % 200)).asInstanceOf[Long] + 100;
            log.info("strdob need to sleep, but block heigh to low distance=" + distance + ",sleep=" + sleep + ",height=" +
              b.getBlockHeight + ",best_height=" + DCtrl.bestheight.get);

            Thread.sleep(sleep);
          } else {

            val waitingList = new ArrayList[String]();
            try {
              for (str <- vres.getTxHashsList) {
                if (str != null) {
                  waitingList.add(str);
                }
              }
            } catch {
              case t: Throwable =>
            }
            while (sleep > 0) {
              log.info("strdob need to sleep, distance=" + distance + ",sleep=" + sleep + ",waitingListcount=" +
                waitingList.size + "/" + vres.getTxHashsCount);

              Thread.sleep(800);
              //
              if (sleep > 1) {
                val rmvlist = waitingList.filter { txHash =>
                  Daos.txHelper.isExistsWaitBlockTx(txHash) || Daos.txHelper.isExistsTransaction(txHash)
                }
                rmvlist.map { txhash =>
                  if (txhash != null) {
                    try {
                      waitingList.remove(txhash);
                    } catch {
                      case t: Throwable =>

                    } finally {
                    }
                  }
                }
                if (waitingList.size == 0) {
                  log.info("wake up manual for waitList is zero:count=" + vres.getTxHashsCount + ",sleep=" + sleep);
                  sleep = 0;
                }
              }
              sleep = sleep - 1;
            }
          }
          val res = Daos.blkHelper.ApplyBlock(b.getBlockHeader, needBody);
          if (res.getTxHashsCount <= 0) {
            if (res.getCurrentNumber > 0) {
              log.debug("checkMiner.2 --> updateBlockHeight::" + res.getCurrentNumber.intValue())
              DCtrl.instance.updateBlockHeight(res.getCurrentNumber.intValue(), if (res.getCurrentNumber.intValue() == b.getBlockHeight) b.getSign else null)
              (res.getCurrentNumber.intValue(), res.getWantNumber.intValue())
            } else {
              (res.getCurrentNumber.intValue(), res.getWantNumber.intValue())
            }
          } else {
            log.info("must sync transaction still first.:reentry=" + reentry + ",losthash=" + vres.getTxHashsCount + ",height=" + b.getBlockHeight);

            val reqTx = PSGetTransaction.newBuilder();
            for (txHash <- res.getTxHashsList) {
              if (res.getTxHashsCount < 20) {
                log.info("request hash=" + txHash + ",blocknumber=" + b.getBlockHeight + ",reentry=" + reentry);
              }
              reqTx.addTxHash(txHash);
            }
            val miner = BlockEntity.parseFrom(b.getBlockHeader);
            // DCtrl.coMinerByUID.find(p => { p._2.getCurBlock > curDN().getCurBlock) {
            // val randomNode = dposNet().directNodes.filter { x => !x.bcuid.equals(DCtrl.curDN().getBcuid ) }.toList
            var oNetwork = dposNet().directNodeByBcuid.get(miner.getMiner.getBcuid)
            if (!oNetwork.nonEmpty) {
              log.info("not found block minner")
              val blockNode = DCtrl.coMinerByUID.filter(p => p._2.getCurBlock > curDN().getCurBlock).toList
              if (blockNode.size != 0) {
                oNetwork = dposNet().directNodeByBcuid.get(blockNode.get(0)._2.getBcuid);
              } else {
                log.info("random block minner")
                val randomNode = dposNet().directNodes.filter { x => !x.bcuid.equals(DCtrl.curDN().getBcuid) }.toList
                oNetwork = Option(randomNode.get(scala.util.Random.nextInt(randomNode.size)));
              }
            }
            // .getOrElse(randomNode.get(scala.util.Random.nextInt(randomNode.size)))
            val start = System.currentTimeMillis();
            val cdl = new CountDownLatch(1);
            var retTx = PRetGetTransaction.newBuilder();
            var successed = false;
            val cb = new CallBack[FramePacket] {
              def onSuccess(fp: FramePacket) = {
                try {
                  if (!successed) {
                    retTx = if (fp.getBody != null) {
                      PRetGetTransaction.newBuilder().mergeFrom(fp.getBody);
                    } else {
                      log.info("not found sync transaction");
                      null;
                    }
                    if (retTx != null && retTx.getTxContentCount > 0) {
                      val txlist = retTx.getTxContentList.map { MultiTransaction.newBuilder().mergeFrom(_) }
                      Daos.txHelper.syncTransactionBatch(txlist.toList, false,
                        new BigInteger("0").setBit(oNetwork.get.node_idx));
                      //                      log.error("sync transaction all done total::retsize=" + retTx.getTxContentList.size() + "==>reqsize=" + reqTx.getTxHashCount);
                      //                    if (res.getTxHashsCount < 100) {
                      //                      for (txHash <- txlist) {
                      //                        log.error("response hash=" + txHash + ",blocknumber=" + b.getBlockHeight);
                      //                      }
                      //                    }
                      successed = true;
                      cdl.countDown();
                    } else {
                      log.error("retTx is null:" + retTx + ",from=" + oNetwork.get.bcuid + ",blockminer=" + miner.getMiner.getBcuid + ",fp=" + fp);

                    }
                  }
                } finally {
                }
              }
              def onFailed(e: java.lang.Exception, fp: FramePacket) {
                log.error("sync transaction error::cost=" + (System.currentTimeMillis() - start) + e.getMessage + ":" + oNetwork.get.bcuid + ",uri=" + oNetwork.get.uri, e)
              }
            };
            var cc = 0;
            val startt = System.currentTimeMillis();

            while (cdl.getCount > 0 && cc < 6 && !successed) {
              try {
                if (cc > 3) {
                  val randomNode = dposNet().directNodes.filter { x => !x.bcuid.equals(DCtrl.curDN().getBcuid) }.toList
                  oNetwork = Option(randomNode.get(scala.util.Random.nextInt(randomNode.size)));
                }
                //                log.error("send srtdob to " + oNetwork.get.bcuid + ",blockheight=" + b.getBlockHeight + ",cc=" + cc);
                dposNet().asendMessage("SRTDOB", reqTx.build(), oNetwork.get, cb, '9');
                cc = cc + 1;
                cdl.await(30, TimeUnit.SECONDS);
              } catch {
                case t: Throwable =>
                  log.error("get Error in get strdob:", t);
              }
            }
            //            log.error("==SyncTX COST=" + (System.currentTimeMillis() - startt) + ",blocknumber=" + b.getBlockHeight);
            saveBlock(b, needBody, reentry + 1)
          }
        } else if (vres.getCurrentNumber > 0) {
          log.debug("checkMiner --> updateBlockHeight::" + vres.getCurrentNumber.intValue()+",blk.height="+b.getBlockHeight+",wantNumber="+vres.getWantNumber.intValue())
          DCtrl.instance.updateBlockHeight(vres.getCurrentNumber.intValue(), if (vres.getCurrentNumber.intValue() == b.getBlockHeight) b.getSign else null)
          if (vres.getCurrentNumber.intValue() == b.getBlockHeight) {
            BlkTxCalc.adjustTx(System.currentTimeMillis() - startupApply)
          }
          (vres.getCurrentNumber.intValue(), vres.getWantNumber.intValue())
        } else {
          (vres.getCurrentNumber.intValue(), vres.getWantNumber.intValue())
        }
      } else {
        log.debug("checkMiner --> updateBlockHeight::" + b.getBlockHeight)
        DCtrl.instance.updateBlockHeight(b.getBlockHeight, b.getSign)
        (b.getBlockHeight, b.getBlockHeight)
      }
    }) //synchronized
  }

  val recentBlocks: Cache[Int, PBlockEntry.Builder] = CacheBuilder.newBuilder().expireAfterWrite(30, TimeUnit.SECONDS)
    .maximumSize(1000).build().asInstanceOf[Cache[Int, PBlockEntry.Builder]]

  def loadFromBlock(block: Int): PBlockEntry.Builder = {
    loadFromBlock(block, false)
  }
  def loadFromBlock(block: Int, needBody: Boolean): PBlockEntry.Builder = {
    //    val ov = Daos.dposdb.get("D" + block).get
    //    if (ov != null) {
    //    recentBlocks.synchronized {
    if (block > curDN.getCurBlock) {
      null
    } else {
      val recentblk = recentBlocks.getIfPresent(block);
      if (recentblk != null) {
        return recentblk;
      }
      val blk = Daos.actdb.getBlockByNumber(block);
      if (blk != null) {
        if (needBody) {
          val b = PBlockEntry.newBuilder().setBlockHeader(blk.toBuilder().build().toByteString()).setBlockHeight(block)
          recentBlocks.put(block, b);
          b
        } else {
          val b = PBlockEntry.newBuilder().setBlockHeader(blk.toBuilder().clearBody().build().toByteString()).setBlockHeight(block)
          recentBlocks.put(block, b);
          b
        }
      } else {
        log.error("blk not found in AccountDB:" + block);
        null;
      }
    }
  }
}