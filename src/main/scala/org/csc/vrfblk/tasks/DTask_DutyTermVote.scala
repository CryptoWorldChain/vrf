package org.csc.vrfblk.tasks

import java.util.HashMap
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConversions.`deprecated asScalaBuffer`
import scala.collection.JavaConversions.`deprecated bufferAsJavaList`
import scala.collection.JavaConversions.`deprecated mapAsScalaConcurrentMap`
import scala.collection.JavaConversions.`deprecated mutableMapAsJavaMap`
import scala.collection.mutable.Buffer
import scala.collection.mutable.Map

import org.apache.commons.lang3.StringUtils
import org.csc.bcapi.JodaTimeHelper
import org.csc.bcapi.gens.Oentity.OKey
import org.csc.dposblk.Daos
import org.csc.dposblk.pbgens.Dposblock.DNodeState
import org.csc.dposblk.pbgens.Dposblock.PDNode
import org.csc.dposblk.pbgens.Dposblock.PDutyTermResult
import org.csc.dposblk.pbgens.Dposblock.PDutyTermResult.VoteResult
import org.csc.dposblk.pbgens.Dposblock.PSDutyTermVote
import org.csc.dposblk.pbgens.Dposblock.PSDutyTermVote.BlockRange
import org.csc.dposblk.pbgens.Dposblock.PSDutyTermVote.RewriteTerm
import org.csc.dposblk.pbgens.Dposblock.PSDutyTermVote.TermBlock
import org.csc.dposblk.utils.BlkTxCalc
import org.csc.dposblk.utils.DConfig
import org.csc.p22p.core.Votes
import org.csc.p22p.core.Votes.Converge
import org.csc.p22p.core.Votes.NotConverge
import org.csc.p22p.core.Votes.Undecisible
import org.csc.p22p.node.Network
import org.csc.p22p.utils.LogHelper

import com.google.protobuf.ByteString

import onight.tfw.outils.serialize.UUIDGenerator
import java.lang.Long

//获取其他节点的term和logidx，commitidx
object DTask_DutyTermVote extends LogHelper {

  var ban_for_vote_sec = 0L;
  def sleepToNextVote(): Unit = {
    val ban_sec = (Math.abs(Math.random() * 100000 % (DConfig.BAN_MAXSEC_FOR_VOTE_REJECT - DConfig.BAN_MINSEC_FOR_VOTE_REJECT)) +
      DConfig.BAN_MINSEC_FOR_VOTE_REJECT).longValue()
    //    log.debug("Undecisible but not converge.ban sleep=" + ban_sec)
    log.debug("ban for vote sleep:" + ban_sec + " seconds");
    ban_for_vote_sec = System.currentTimeMillis() + ban_sec * 1000;
    //DTask_DutyTermVote.synchronized({
    //      DTask_DutyTermVote.wait(ban_sec * 1000)
    //    })
  }
  val omitNodes: Map[String, PDNode] = Map.empty;
  def clearRecords(votelist: Buffer[PDutyTermResult.Builder]): Unit = {
    Daos.dposvotedb.batchDelete(votelist.map { p =>
      OKey.newBuilder().setData(
        ByteString.copyFromUtf8(
          "V" + p.getTermId + "-" + p.getSign + "-"
            + p.getBcuid)).build()
    }.toArray)
  }
  val possibleTermID = new ConcurrentHashMap[Long, String]();

  def checkPossibleTerm(vq: PSDutyTermVote.Builder)(implicit network: Network): Boolean = {
    var hasConverge = false;
    possibleTermID.map(f => {
      val bcuid = f._2.split(",").apply(0);
      if (DCtrl.coMinerByUID.containsKey(bcuid)) {
        val records = Daos.dposvotedb.listBySecondKey("D" + f._1);
        val reclist: Buffer[PDutyTermResult.Builder] = records.get.map { p =>
          PDutyTermResult.newBuilder().mergeFrom(p.getValue.getExtdata);
        };
        val realist = reclist.filter { p => DCtrl.coMinerByUID.containsKey(p.getBcuid) };
        if (realist.size > 0) {
          hasConverge = hasConverge || checkVoteDBList(records.get.size(), realist, vq);
        } else {
          possibleTermID.remove(f._1);
        }
      }
    })
    hasConverge;
  }
  def checkVoteDB(vq: PSDutyTermVote.Builder, termId: Long = 0)(implicit network: Network): Boolean = {
    val records = Daos.dposvotedb.listBySecondKey("D" + (vq.getTermId match {
      case 0 => Math.max(DCtrl.termMiner().getTermId + 1, termId);
      case _ => vq.getTermId
    }));
    val reclist: Buffer[PDutyTermResult.Builder] = records.get.map { p =>
      PDutyTermResult.newBuilder().mergeFrom(p.getValue.getExtdata);
    };
    val realist = reclist.filter { p =>
      DCtrl.coMinerByUID.containsKey(p.getBcuid)
    };
    if (JodaTimeHelper.secondIntFromNow(vq.getTermStartMs) > DConfig.DTV_TIMEOUT_SEC) {
      DCtrl.voteRequest().clear()
    }
    log.debug("check db status:vq[=" + vq.getBlockRange.getStartBlock + ","
      + vq.getBlockRange.getEndBlock + "],T="
      + vq.getTermId
      + ",sign=" + vq.getSign
      + ",N=" + vq.getCoNodes
      + ",dbsize=" + records.get.size()
      + ",realsize=" + realist.size())
    if (realist.size > 0) {
      checkVoteDBList(records.get.size(), realist, vq);
    } else {
      clearRecords(reclist);
      false
    }
  }
  def checkVoteDBList(recordsize: Int, realist: Buffer[PDutyTermResult.Builder], vq: PSDutyTermVote.Builder)(implicit network: Network): Boolean = {

    if (realist.size() == 0) {
      DCtrl.voteRequest().clear()
      checkPossibleTerm(vq);
    } else if ((recordsize + 1) >= vq.getCoNodes * DConfig.VOTE_QUORUM_RATIO / 100
      || (System.currentTimeMillis() - vq.getTermStartMs > DConfig.MAX_TIMEOUTSEC_FOR_REVOTE * 1000)) {
      //      log.debug("try to vote:" + records.get.size());
      val signmap = Map[String, Buffer[PDutyTermResult.Builder]]();
      realist.map { x =>
        val buff = signmap.get(x.getSign) match {
          case Some(_buffn) => _buffn
          case None => {
            val r = Buffer[PDutyTermResult.Builder]();
            signmap.put(x.getSign, r)
            r
          }
        }
        buff.append(x)
      }
      var hasConverge = false;
      var banForLocal = false;
      signmap.map { kv =>
        val sign = kv._1
        val votelist = kv._2
        val dbtempvote = DCtrl.instance.loadVoteReq(sign);
        log.debug("dbtempvote=" + dbtempvote.getSign + ",vid=" + dbtempvote.getTermId + ",TID=" + DCtrl.termMiner().getTermId + ",sign=" + sign + ",size=" + votelist.size
          + ",N=" + dbtempvote.getCoNodes);
        if (StringUtils.equals(dbtempvote.getSign, sign)) {
          if (dbtempvote.getTermId > DCtrl.termMiner().getTermId) {
            val result = Votes.vote(votelist) //.filter { p => dbtempvote.getMinerQueueList.filter { x => p.getVoteAddress.equals(x.getMinerCoaddr) }.size > 0 })
              .PBFTVote({ p =>
                Some(p.getResult)
              }, DCtrl.coMinerByUID.size) match {
                case Converge(n) =>
                  //          log.debug("converge:" + n); 
                  if (n == VoteResult.VR_GRANTED //&& System.currentTimeMillis() - dbtempvote.getTermStartMs < DConfig.MAX_TIMEOUTSEC_FOR_REVOTE
                    && dbtempvote.getTermStartMs >= DCtrl.instance.term_Miner.getTermStartMs) {
                    log.debug("Vote Granted will be the new terms:T="
                      + dbtempvote.getTermId
                      + ",curr=" + DCtrl.curDN().getCoAddress
                      + ",sign=" + dbtempvote.getSign
                      + ",N=" + dbtempvote.getCoNodes + ":"
                      + dbtempvote.getMinerQueueList.foldLeft("")((a, b) => a + "\n\t" + b.getBlockHeight + "=" + b.getMinerCoaddr));
                    DCtrl.instance.term_Miner = dbtempvote
                    DCtrl.instance.updateTerm()
                    hasConverge = true;
                    clearRecords(votelist);
                    //remove cominer
                    if (DCtrl.coMinerByUID.size > dbtempvote.getCoNodes) {
                      log.debug("run re recheck heatbeat right now:MN=" + DCtrl.coMinerByUID.size + ",CN=" + dbtempvote.getCoNodes);
//                      Scheduler.runOnce(DCtrl.instance.hbTask)
                      Daos.ddc.executeNow(BlockSync.workPack,DCtrl.instance.hbTask);
                    }
                    true
                  } else if (n == VoteResult.VR_REJECT) {
                    clearRecords(votelist);
                    if (StringUtils.equals(dbtempvote.getCoAddress, DCtrl.instance.cur_dnode.getCoAddress)) {
                      banForLocal = true;
                    }
                    false
                  } else {
                    clearRecords(votelist);
                    if (StringUtils.equals(dbtempvote.getCoAddress, DCtrl.instance.cur_dnode.getCoAddress)) {
                      banForLocal = true;
                    }
                    false
                  }
                case n: Undecisible =>
                  log.debug("Undecisible:dbsize=" + votelist.size + ",T=" + dbtempvote.getTermId
                    + ",curr=" + DCtrl.curDN().getCoAddress
                    + ",sign=" + dbtempvote.getSign + ",N=" + dbtempvote.getCoNodes);
                  if (System.currentTimeMillis() - dbtempvote.getTermStartMs > DConfig.MAX_TIMEOUTSEC_FOR_REVOTE * 1000) {
                    log.debug("clear timeout vote after:" + JodaTimeHelper.secondFromNow(dbtempvote.getTermStartMs) + ",max=" + DConfig.MAX_TIMEOUTSEC_FOR_REVOTE * 1000 + ",sign=" + dbtempvote.getSign
                      + ",dbsize=" + votelist.size)
                    clearRecords(votelist);
                  } else {
                    //resend vote..
                    if (dbtempvote.getSign.equals(vq.getSign) && votelist.size < DCtrl.coMinerByUID.size * 2 / 3) {
                      log.debug("resend revote message");
                      network.dwallMessage("DTVDOB", Left(DCtrl.voteRequest().build()), vq.getMessageId, '9');
                    }
                  }
                  false
                case n: NotConverge =>
                  log.debug("NotConverge=" + votelist.size + ",T=" + dbtempvote.getTermId
                    + ",curr=" + DCtrl.curDN().getCoAddress
                    + ",sign=" + dbtempvote.getSign + ",N=" + dbtempvote.getCoNodes);

                  clearRecords(votelist);
                  if (StringUtils.equals(dbtempvote.getCoAddress, DCtrl.instance.cur_dnode.getCoAddress)) {
                    banForLocal = true;
                  }
                  false
                case a @ _ =>
                  log.debug("unknow result =" + votelist.size + ",T=" + dbtempvote.getTermId
                    + ",curr=" + DCtrl.curDN().getCoAddress
                    + ",sign=" + dbtempvote.getSign + ",N=" + dbtempvote.getCoNodes + ",a=" + a);
                  clearRecords(votelist);

                  false
              }
            if (result) {
              hasConverge = result
            }
          } else { //unclean data
            clearRecords(votelist);
          }
        }
      }
      if (!hasConverge && banForLocal) {
        DCtrl.voteRequest().clear()
        DCtrl.curDN().clearDutyUid();
        val fastNode = DTask_CoMine.getfastNode(network, UUIDGenerator.generate());
        if (fastNode != null && !fastNode.getBcuid.equals(DCtrl.curDN().getBcuid)
          && fastNode.getCurBlock > DCtrl.curDN().getCurBlock) {
          BlockSync.trySyncBlock(fastNode.getCurBlock, fastNode.getBcuid);
        }

        sleepToNextVote();
      } else if (hasConverge) {
        DCtrl.voteRequest().clear()
        wallOutTermGrantResult(network);
      }
      hasConverge
    } else {

      log.debug("check status Not enough results:B[=" + vq.getBlockRange.getStartBlock + ","
        + vq.getBlockRange.getEndBlock + "],T="
        + vq.getTermId
        + ",sign=" + vq.getSign
        + ",N=" + vq.getCoNodes
        + ",dbsize=" + recordsize)
      false
    }
  }
  def runOnce(implicit network: Network): Boolean = {
    Thread.currentThread().setName("RTask_RequestVote");
    DTask_DutyTermVote.synchronized({
      val cn = DCtrl.curDN();
      val tm = DCtrl.termMiner();
      val vq = DCtrl.voteRequest()
      log.info("dutyvote:vq.tid=" + vq.getTermId + ",B=" + cn.getCurBlock + ",vq[" + vq.getBlockRange.getStartBlock + "," + vq.getBlockRange.getEndBlock + "]"
        + ",tm[" + tm.getBlockRange.getStartBlock + "," + tm.getBlockRange.getEndBlock + "]"
        + ",vq.lasttermuid=" + vq.getLastTermUid + ",tm.sign=" + tm.getSign + ",vqsign=" + vq.getSign + ",tid=" + tm.getTermId + ",banVote=" + (System.currentTimeMillis() <= ban_for_vote_sec) + ":" + (-1 * JodaTimeHelper.secondIntFromNow(ban_for_vote_sec)))
      if ((cn.getCurBlock + DConfig.DTV_BEFORE_BLK >= tm.getBlockRange.getEndBlock
        && vq.getBlockRange.getStartBlock >= tm.getBlockRange.getEndBlock
        && vq.getBlockRange.getStartBlock >= cn.getCurBlock
        && vq.getTermId > 0
        || (StringUtils.isNotBlank(vq.getLastTermUid) && vq.getLastTermId.equals(tm.getTermId)
          && tm.getTermId > 0) && JodaTimeHelper.secondIntFromNow(vq.getTermStartMs) <= DConfig.DTV_TIMEOUT_SEC)) {
        checkVoteDB(vq)
      } else if ((cn.getCurBlock + DConfig.DTV_BEFORE_BLK >= tm.getBlockRange.getEndBlock
        || JodaTimeHelper.secondIntFromNow(cn.getLastBlockTime) > DConfig.DTV_TIMEOUT_SEC)
        && System.currentTimeMillis() > ban_for_vote_sec &&
        ((cn.getCurBlock + DConfig.DTV_BEFORE_BLK >= tm.getBlockRange.getEndBlock) ||
          JodaTimeHelper.secondIntFromNow(cn.getLastBlockTime) > DConfig.DTV_TIMEOUT_SEC) &&
          (cn.getCurBlock + 1 >= tm.getBlockRange.getStartBlock)
          && vq.getTermId <= tm.getTermId + 1) {
        //        cn.setCominerStartBlock(1)
        val msgid = UUIDGenerator.generate();
        MDCSetMessageID(msgid);

        var canvote = if (JodaTimeHelper.secondIntFromNow(cn.getLastBlockTime) < DConfig.DTV_TIMEOUT_SEC &&
          StringUtils.isNotBlank(tm.getSign) && tm.getCoNodes > 1) {
          val idx = (Math.abs(tm.getSign.hashCode()) % tm.getMinerQueueCount)
          val coMiner = tm.getMinerQueue(idx).getMinerCoaddr;
          if (coMiner.equals(cn.getCoAddress)) {
            true;
          } else {
            log.debug("can not vote: current cominer not in last term:" + coMiner + ",curr=" + cn.getCoAddress + ",tmsign=" +
              tm.getSign + ",queuecount=" + tm.getMinerQueueCount + ",idx=" + idx);
            false;
          }
        } else {
          log.debug("can vote:timepost=" + JodaTimeHelper.secondIntFromNow(cn.getLastBlockTime) + ",DTVTIMOUT=" + DConfig.DTV_TIMEOUT_SEC + ",past.ensd=" +
            JodaTimeHelper.secondIntFromNow(tm.getTermEndMs));
          //log
          JodaTimeHelper.secondIntFromNow(cn.getLastBlockTime) > DConfig.DTV_TIMEOUT_SEC
        }
        var maxtermid = tm.getTermId;
        DCtrl.coMinerByUID.map(p => if (p._2.getTermId > maxtermid) { maxtermid = p._2.getTermId })

        if (tm.getMinerQueueCount > 0 && cn.getCurBlock > 0 && tm.getMinerQueueList.filter { m => m.getMinerCoaddr.equals(cn.getCoAddress) }.size == 0) {
          log.debug("cannot vote:term miner queue not include current:" + cn.getCoAddress);
          canvote = false;
        }
        //check if possible term large than mine.
        val termMaxThanSelf = if (DCtrl.coMinerByUID.size < 3) {
          possibleTermID.filter(f => {
            val arrass = f._2.split(",");
            if (f._1 > tm.getTermId && Long.parseLong(arrass(1)) > cn.getCurBlock) {
              log.debug("get termid than self:" + f._2 + ",curTerm=" + tm.getTermEndMs + ",cur.blk=" + cn.getCurBlock + ",cominersize=" + DCtrl.coMinerByUID.size);
              true
            } else {
              false
            }
          }).size
        } else { 0 };
        if (canvote && termMaxThanSelf <= 0) {
          //      log.debug("try vote new term:");
          if (checkPossibleTerm(vq)) {
            log.debug("get possible term:");
            true
          } else if (VoteTerm(network)) {
            false
          } else {
            checkVoteDB(vq)
          }
        } else {
          log.debug("cannot vote Sec=" + JodaTimeHelper.secondIntFromNow(tm.getTermEndMs) + ",DV=" + DConfig.DTV_TIMEOUT_SEC
            + ",co=" + tm.getCoNodes + ",termMax:=" + termMaxThanSelf);
          checkVoteDB(vq, maxtermid)
        }
      } else if (cn.getCurBlock >= tm.getBlockRange.getStartBlock && cn.getCurBlock <= tm.getBlockRange.getEndBlock
        && tm.getMinerQueueList.filter(x => x.getMinerCoaddr.equals(cn.getCoAddress)).size > 0) {
        true
      } else {
        log.debug("cannot do vote ");
        checkVoteDB(vq)
      }
    })
  }
  def VoteTerm(implicit network: Network, omitCoaddr: String = "", overridedBlock: Int = 0): Boolean = {
    val msgid = UUIDGenerator.generate();
    MDCSetMessageID(msgid);
    DCtrl.coMinerByUID.filter(p => {
      network.nodeByBcuid(p._1) == network.noneNode //|| StringUtils.equals(omitCoaddr, p._2.getCoAddress)
    }).map { p =>
      log.debug("remove Node:" + p._1);
      DCtrl.coMinerByUID.remove(p._1);
    }
    val cn = DCtrl.curDN();
    val tm = DCtrl.termMiner();
    val vq = DCtrl.voteRequest()

    val quantifyminers = DCtrl.coMinerByUID.filter(p =>
      //- DConfig.DTV_MUL_BLOCKS_EACH_TERM * (tm.getMinerQueueCount)

      //      if (!StringUtils.equals(omitCoaddr, p._2.getCoAddress) &&
      //        (p._2.getCurBlock >= cn.getCurBlock &&
      //          (tm.getLastTermId == p._2.getTermId
      //            || tm.getTermId >= p._2.getTermId) &&
      //            (StringUtils.isBlank(tm.getSign) || StringUtils.equals(p._2.getTermSign, tm.getSign) ||
      //              StringUtils.equals(p._2.getTermSign, tm.getLastTermUid))))

      if (p._2.getCoAddress.equals(cn.getCoAddress)
        || (!StringUtils.equals(omitCoaddr, p._2.getCoAddress)
          &&
          (
            p._2.getCurBlock >= tm.getBlockRange.getStartBlock - 1 - Math.abs(DConfig.BLOCK_DISTANCE_COMINE)
            &&
            (tm.getLastTermId == p._2.getTermId || tm.getTermId <= p._2.getTermId) &&
            (StringUtils.isBlank(tm.getSign)
              || StringUtils.isNotBlank(p._2.getTermSign) &&
              (StringUtils.equals(p._2.getTermSign, tm.getSign) ||
                StringUtils.equals(p._2.getTermSign, tm.getSign)))))) {
        true
      } else {
        log.debug("remove unquantifyminers:" + p._2.getBcuid + "," + p._2.getCoAddress + ",pblock=" + p._2.getCurBlock
          + ",cn=" + cn.getCurBlock
          + ",termstartblk=" + tm.getBlockRange.getStartBlock + "/" + (tm.getBlockRange.getStartBlock - 1 - Math.abs(DConfig.BLOCK_DISTANCE_COMINE))
          + ",TID=" + tm.getTermId + ",LTID=" + tm.getLastTermId + ",PT=" + p._2.getTermId
          + ",pbtsign=" + p._2.getTermSign + ",tmsign=" + tm.getSign + ",lasttmsig=" + tm.getLastTermUid + ",omitCoaddr=" + omitCoaddr)
        false;
      })
    var canvote = true;
    var highest = cn.getCurBlock;
    if (quantifyminers.size > 0) {

      Votes.vote(quantifyminers.map(p => p._2).toList).PBFTVote({ p => Some(p.getTermSign, p.getTermId, p.getTermStartBlock, p.getTermEndBlock) }, quantifyminers.size) match {
        case c: Converge => //get termid
          val (sign: String, termid: Long, startBlk: Int, endBlk: Int) = c.decision.asInstanceOf[(String, Long, Int, Int)];
          if (((StringUtils.isBlank(sign) || sign.equals(tm.getSign) && termid == tm.getTermId) &&
            cn.getCurBlock >= startBlk - 1 - DConfig.DTV_BEFORE_BLK &&
            cn.getCurBlock <= endBlk) || startBlk <= 0 || endBlk <= 0) {
            canvote = true;
          } else {
            canvote = false;
            log.debug("can not vote: pbft converge but not equals to local :sign=" + sign + ",curtermsign=" + tm.getSign
              + ",termid=" + termid + ",curtermid=" + tm.getTermId + ",startBlk=" +
              startBlk + ",endBlk=" + endBlk + ",curblock=" + cn.getCurBlock);
          }
        case n @ _ => //cannot converge, find the max size.
          log.debug("can not merge: " + n + ",curtermsign=" + tm.getSign + ",startBlk=" +
            tm.getBlockRange.getStartBlock + ",endBlk=" + tm.getBlockRange.getEndBlock + ",curtermid=" + tm.getTermId + ",curblock=" + cn.getCurBlock);
          DCtrl.coMinerByUID.map(p => {
            if (p._2.getTermId > tm.getTermId && p._2.getTermStartBlock >= cn.getCurBlock) {
              log.debug("cannot vote:sign=" + p._2.getTermSign + ",termid=" + p._2.getTermId + ",startblk=" + p._2.getTermStartBlock + ",endblk=" + p._2.getTermEndBlock + "->" + p._2.getBcuid + ",tm.termid=" + tm.getTermId + ",vq.termid=" + vq.getTermId);
              canvote = false;
            }
            if (p._2.getCurBlock > highest) {
              highest = p._2.getCurBlock;
            }
          })
      }
    }

    if (canvote && (quantifyminers.size >= DCtrl.coMinerByUID.size * 2 / 3) || cn.getCurBlock == highest) {

      val newterm = PSDutyTermVote.newBuilder();
      val conodescount = Math.min(quantifyminers.size, DConfig.DTV_MAX_SUPER_MINER);
      val mineBlockCount = DConfig.DTV_MUL_BLOCKS_EACH_TERM * conodescount * DConfig.DTV_BLOCKS_EACH_MINER;
      val startBlk = cn.getCurBlock + 1;
      newterm.setBlockRange(BlockRange.newBuilder()
        .setStartBlock(startBlk)
        .setEndBlock(startBlk + mineBlockCount - 1)
        .setEachBlockMs(DConfig.BLK_EPOCH_MS))
        .setCoNodes(conodescount)
        .setMessageId(msgid)
        .setCoAddress(DCtrl.instance.cur_dnode.getCoAddress)
        .setCwsGuaranty(DConfig.MAX_CWS_GUARANTY)
        .setSliceId(1)
        .setMaxTnxEachBlock(BlkTxCalc.getBestBlockTxCount(DCtrl.termMiner().getMaxTnxEachBlock))
        .setBcuid(cn.getBcuid)
        .setTermStartMs(System.currentTimeMillis());

      if (overridedBlock > 0 && StringUtils.isNotBlank(omitCoaddr)) {
        log.debug("overrideBlockedVoteWithOmit!TID=" + tm.getTermId + ",Tuid=" + tm.getSign + ",block=" + overridedBlock + ",cur=" + cn.getCurBlock);
        newterm.setRewriteTerm(RewriteTerm.newBuilder().setBlockLost(overridedBlock)
          .setRewriteMs(System.currentTimeMillis()).setTermStartMs(tm.getTermStartMs))
      } else if (newterm.getBlockRange.getStartBlock <= tm.getBlockRange.getEndBlock) {
        log.debug("overrideBlockedVoteRevote!TID=" + tm.getTermId + ",TMuid=" + tm.getSign + ",NTMUID=" + newterm.getSign
          + ",TBS=[" + tm.getBlockRange.getStartBlock + "," + tm.getBlockRange.getEndBlock + "]"
          + ",NewTBS=[" + newterm.getBlockRange.getStartBlock + "," + newterm.getBlockRange.getEndBlock + "]"
          + ",cur=" + cn.getCurBlock);
        newterm.setRewriteTerm(RewriteTerm.newBuilder().setBlockLost(tm.getBlockRange.getEndBlock - newterm.getBlockRange.getStartBlock)
          .setRewriteMs(System.currentTimeMillis()).setTermStartMs(tm.getTermStartMs))
      }
      newterm.setTermEndMs(System.currentTimeMillis() +
        Math.max(DConfig.BLK_EPOCH_MS, DConfig.BLK_NOOP_EPOCH_MS) * mineBlockCount);

      newterm.setTermId(tm.getTermId + 1)
        .setLastTermId(tm.getTermId)
        .setLastTermUid(tm.getSign)
        .setSign(msgid)

      val rand = Math.random() * 1000
      //      val rdns = scala.util.Random.shuffle(quantifyminers);
      //      log.debug(" rdns=" + rdns.foldLeft("")((a, b) => a + "," + b._1));
      var i = newterm.getBlockRange.getStartBlock;
      var bitcc = BigInt(0);
      // add stable nodes.

      //quantifyminers
      while (newterm.getMinerQueueCount < mineBlockCount) {
        //add stable nodes
        val stablebcuid = new HashMap[String, String]();
        quantifyminers.map { x =>
          if (newterm.getMinerQueueCount < mineBlockCount) {
            val s = network.joinNetwork.statupNodes.filter { p => StringUtils.equals(p.bcuid(), x._2.getBcuid) };
            if (s.size > 0) {
              stablebcuid.put(x._2.getBcuid, x._2.getCoAddress)
              for (bi <- 1 to DConfig.DTV_BLOCKS_EACH_MINER) {
                newterm.addMinerQueue(TermBlock.newBuilder().setBlockHeight(i)
                  .setMinerCoaddr(x._2.getCoAddress))
                i = i + 1;
              }
            }
          }
        }
        scala.util.Random.shuffle(quantifyminers).map { x =>
          if (!stablebcuid.containsKey(x._2.getBcuid) && newterm.getMinerQueueCount < mineBlockCount) {
            //              log.debug(" add miner at Queue," + x._2.getCoAddress + ",blockheight=" + i);
            for (bi <- 1 to DConfig.DTV_BLOCKS_EACH_MINER) {
              newterm.addMinerQueue(TermBlock.newBuilder().setBlockHeight(i)
                .setMinerCoaddr(x._2.getCoAddress))
              i = i + 1;
            }
          }
        }
      }

      log.debug("try to vote:newterm=" + newterm.getTermId + ",curterm=" + tm.getTermId
        + ",tm_end_past=" + JodaTimeHelper.secondIntFromNow(tm.getTermEndMs) + ",lastsig=" + tm.getSign
        + ",sec,vN=" + DCtrl.coMinerByUID.size + ",cN=" + conodescount + ",sign=" + newterm.getSign + ",mineQ=" + newterm.getMinerQueueList.foldLeft("")((a, b) => a + "\n\t" + b.getBlockHeight + "=" + b.getMinerCoaddr))
      DCtrl.instance.vote_Request = newterm;
      network.dwallMessage("DTVDOB", Left(DCtrl.voteRequest().build()), msgid, '9');
      true
    } else {
      log.debug("No more quaitify node can vote:");
      if (JodaTimeHelper.secondIntFromNow(cn.getLastBlockTime) > DConfig.DTV_TIMEOUT_SEC && System.currentTimeMillis() > ban_for_vote_sec) {
        DCtrl.voteRequest().clear()
        DCtrl.curDN().clearDutyUid();
      }
      false
    }
  }

  def wallOutTermGrantResult(implicit network: Network): Unit = {

    val cn = DCtrl.curDN();
    val tm = DCtrl.termMiner();
    var ret = PDutyTermResult.newBuilder();
    ret.setMessageId(tm.getMessageId);
    ret.setBcuid(cn.getBcuid)
    ret.setRetCode(0).setRetMessage("SUCCESS")
    ret.setCurTermid(tm.getTermId).setCurBlock(cn.getCurBlock).setCurTermSign(tm.getSign)

    if (DConfig.RUN_COMINER != 1) {
      ret.setNodeState(DNodeState.DN_BAKCUP)
    }
    //    val vq = DCtrl.voteRequest();
    //
    ret.setTermId(tm.getTermId)
    ret.setSign(tm.getSign)
    ret.setCurTermSign(tm.getSign);
    ret.setVoteAddress(cn.getCoAddress)

    ret.setResult(VoteResult.VR_APPLY)
    ret.setTermId(tm.getTermId)
    ret.setBcuid(cn.getBcuid)
    ret.setSign(tm.getSign)
    ret.setVoteAddress(cn.getCoAddress)
    network.wallOutsideMessage("DTRDOB", Left(ret.build()), tm.getMessageId, '9');
  }

}
