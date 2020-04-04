package org.brewchain.vrfblk.msgproc

import java.math.BigInteger
import java.util.concurrent.TimeUnit

import com.google.protobuf.ByteString
import onight.tfw.outils.serialize.UUIDGenerator
import org.brewchain.mcore.crypto.BitMap
import org.brewchain.bcrand.model.Bcrand.{ BlockWitnessInfo, PBlockEntry, PSCoinbase }
import org.brewchain.mcore.model.Block.BlockInfo
import org.brewchain.mcore.model.Transaction.TransactionInfo
import org.brewchain.p22p.action.PMNodeHelper
import org.brewchain.p22p.utils.LogHelper
import org.brewchain.vrfblk.Daos
import org.brewchain.vrfblk.tasks.{ BlockMessage, VCtrl }
import org.brewchain.vrfblk.utils.{ BlkTxCalc, TxCache, VConfig }
import org.brewchain.mcore.tools.bytes.BytesHelper

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import org.brewchain.vrfblk.utils.RandFunction
import org.brewchain.bcrand.model.Bcrand.VNodeState
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.ArrayList
import scala.collection.mutable.Seq
import scala.collection.mutable.Buffer
import java.util.HashSet

case class MPRealCreateBlock(netBits: BigInteger, blockbits: BigInteger, notarybits: BigInteger, beaconHash: String, preBeaconHash: String, beaconSig: String, witnessNode: BlockWitnessInfo, needHeight: Int) extends BlockMessage with PMNodeHelper with BitMap with LogHelper {

  def newBlockFromAccount(txc: Int, confirmTimes: Int, beaconHash: String, voteInfos: String): (BlockInfo, java.util.List[TransactionInfo]) = {
    val starttx = System.currentTimeMillis();

    val LOAD_THREADS = 10;
    val cdl = new CountDownLatch(LOAD_THREADS);
    val txs = new ArrayList[TransactionInfo];
    for (i <- 1 to LOAD_THREADS) {
      new Thread(new Runnable() {
        def run() = {
          try {
            val tx = Daos.txHelper.getWaitBlockTx(
              txc / LOAD_THREADS, //只是打块！其中某些成功广播的tx，默认是80%
              confirmTimes);
            if (tx != null && tx.size() > 0) {
              txs.synchronized({
                txs.addAll(tx);
              })

            }
          } finally {
            cdl.countDown();
          }
        }
      }).start();
    }
    cdl.await();
    //load  more tx when tx broadcast is failed
    if (txs.size() < VConfig.MIN_TNX_EACH_BLOCK) {
      val tx = Daos.txHelper.getWaitBlockTx(
        VConfig.MIN_TNX_EACH_BLOCK - txs.size, //只是打块！其中某些成功广播的tx，默认是80%
        1);
      if (tx != null && tx.size() > 0) {
        txs.addAll(tx);
      }
    }
    //奖励节点
    val excitationAddress: ListBuffer[String] = new ListBuffer()
    if (witnessNode.getBeaconHash.equals(beaconHash)) {
      excitationAddress.appendAll(witnessNode.getWitnessList.asScala.map(node => node.getCoAddress).toList)
    }
    val startblk = System.currentTimeMillis();
    if (Daos.chainHelper.getLastConnectedBlockHeight >= needHeight && needHeight > 0) {
      Daos.chainHelper.rollBackTo(needHeight - 1);
    }
    val newblk = Daos.blkHelper.createBlock(txs, BytesHelper.EMPTY_BYTE_ARRAY, beaconHash, voteInfos);
    //    newblk.getMiner.getBits
    val endblk = System.currentTimeMillis();
    (newblk, txs)
  }

  var lastMakeBlockHeight = 0;
  var lastMakeBlockCounter = 0;

  def proc(): Unit = {
    val start = System.currentTimeMillis();
    val cn = VCtrl.curVN();
    MDCSetBCUID(VCtrl.network())
    try {
      //需要广播的节点数量
      val cominerAccount: Int = Math.min(VConfig.MAX_BLOCK_MAKER * 2 / 3, VCtrl.coMinerByUID.size * VConfig.DCTRL_BLOCK_CONFIRMATION_RATIO / 100)

      var newNetBits = BigInteger.ZERO
      val existCominerBits = mapToBigInt(cn.getCominers).bigInteger;
      val curtime = System.currentTimeMillis();
      var banMinerCount = VCtrl.banMinerByUID.size;
      VCtrl.coMinerByUID.foreach(f => {

        val islock_block = VCtrl.isBanforMiner(cn.getCurBlock + 1, f._2.getBcuid); ; //.get(f._2.getBcuid).getOrElse(0);

        log.info("minecheck:" + f._2.getBcuid + ":bits="
          + (VCtrl.network().node_bits().testBit(f._2.getBitIdx)) + ",time="
          + ((curtime - f._2.getLastBeginMinerTime) > VConfig.BLOCK_DISTANCE_WAITMS)
          + ",stat=" + f._2.getState
          + ",height=" + f._2.getCurBlock
          + ",curheighcheck=" + (f._2.getCurBlock >= VCtrl.curVN().getCurBlock - VConfig.BLOCK_DISTANCE_NETBITS)
          + ",lockblock=" + VCtrl.banMinerByUID.get(f._2.getBcuid).getOrElse((0, 0L))
          + ",islock_block=" + islock_block
          + ",result=" + "==>" + VCtrl.curVN().getCurBlock);

        if ( //other nodes
        VCtrl.network().node_bits().testBit(f._2.getBitIdx) &&
          (curtime - f._2.getLastBeginMinerTime) > VConfig.BLOCK_DISTANCE_WAITMS &&
          (f._2.getState == VNodeState.VN_DUTY_BLOCKMAKERS || f._2.getState == VNodeState.VN_DUTY_NOTARY
            || f._2.getState == VNodeState.VN_DUTY_SYNC) &&
            f._2.getCurBlock > VConfig.BLOCK_DISTANCE_NETBITS &&
            f._2.getCurBlock >= VCtrl.curVN().getCurBlock - VConfig.BLOCK_DISTANCE_NETBITS
            //             && mapToBigInt(f._2.getCominers).bigInteger.and(existCominerBits).equals(existCominerBits)
            || f._2.getBcuid.equals(VCtrl.curVN().getBcuid)) {
          if (VCtrl.banMinerByUID.contains(f._2.getBcuid)) {
            if (islock_block) {
              log.info("minecheck: remove miner for banMiner:" + VCtrl.banMinerByUID.get(f._2.getBcuid));
            } else {
              banMinerCount = banMinerCount - 1;
              VCtrl.banMinerByUID.remove(f._2.getBcuid)
              newNetBits = newNetBits.setBit(f._2.getBitIdx);
            }
          } else {
            newNetBits = newNetBits.setBit(f._2.getBitIdx);
          }

        }
      })
      //}

      if (lastMakeBlockCounter > VConfig.MAX_CONTINUE_BLOCK && newNetBits.bitCount() > 3) {
        newNetBits = newNetBits.clearBit(cn.getBitIdx)
      }
      val strnetBits = hexToMapping(newNetBits);
      // BlkTxCalc.getBestBlockTxCount(VConfig.MAX_TNX_EACH_BLOCK)
      val wallAccount = Math.max(1, cominerAccount - banMinerCount)
      log.error("minecheck: miner,confirm=" + wallAccount + ",netcount=" + newNetBits.bitCount() + ",strnetBits=" + strnetBits + ",nodes.count=" + VCtrl.coMinerByUID.size + ",newNetBits=" + newNetBits.toString(2));

      val (newblk, txs) = newBlockFromAccount(
        VConfig.MAX_TNX_EACH_BLOCK, wallAccount, beaconHash,
        strnetBits);

      if (newblk == null) {
        log.debug("mining error: ch=" + cn.getCurBlock);
      } else {
        // TODO 更新pnode，dnode的节点balance
        // VCtrl.refreshNodeBalance();

        val newblockheight = newblk.getHeader.getHeight.intValue()
        if (lastMakeBlockHeight == newblockheight - 1) {
          lastMakeBlockCounter = lastMakeBlockCounter + 1;
        } else {
          lastMakeBlockCounter = 0;
        }
        lastMakeBlockHeight = newblockheight;

        //        log.debug("MineNewBlock:" + newblk);
        val now = System.currentTimeMillis();
        log.info("mining check ok :new block=" + newblockheight + ",CO=" + cn.getCoAddress
          + ",MaxTnx=" + VConfig.MAX_TNX_EACH_BLOCK + ",hash=" + Daos.enc.bytesToHexStr(newblk.getHeader.getHash.toByteArray()) + " wall=" + wallAccount + " beacon=" + beaconHash);
        val newCoinbase = PSCoinbase.newBuilder()
          .setBlockHeight(newblockheight).setCoAddress(cn.getCoAddress)
          .setCoAddress(cn.getCoAddress)
          .setMessageId(UUIDGenerator.generate())
          .setBcuid(cn.getBcuid)
          .setBlockEntry(PBlockEntry.newBuilder().setBlockHeight(newblockheight)
            .setCoinbaseBcuid(cn.getBcuid).setBlockhash(Daos.enc.bytesToHexStr(newblk.getHeader.getHash.toByteArray()))
            //          .setBlockHeader(newblk.toBuilder().clearBody().build().toByteString())
            .setBlockHeader(newblk.toByteString()) //.toBuilder().clearBody().build().toByteString())
            .setSign(Daos.enc.bytesToHexStr(newblk.getHeader.getHash.toByteArray())))
          .setSliceId(VConfig.SLICE_ID)
          .setTxcount(txs.size())
          .setBeaconBits(strnetBits)
          .setBeaconSign(beaconSig)
          .setBeaconHash(beaconHash)
          .setBlockSeeds(ByteString.copyFrom(blockbits.toByteArray()))
          .setPrevBeaconHash(cn.getBeaconHash)
          .setPrevBlockSeeds(ByteString.copyFrom(cn.getVrfRandseeds.getBytes))
          .setVrfCodes(ByteString.copyFrom(strnetBits.getBytes))
          .setWitnessBits(hexToMapping(notarybits))

        //        .setBeaconHash(Daos.enc.hexEnc(newblk.getHeader.getHash.toByteArray()))

        log.info("set beacon hash=" + newblk.getMiner.getTerm);
        cn.setCurBlock(newblockheight)
          .setBeaconHash(newblk.getMiner.getTerm)
          .setBeaconSign(beaconSig)
          .setCurBlockHash(Daos.enc.bytesToHexStr(newblk.getHeader.getHash.toByteArray()))
          .setCurBlockMakeTime(now)
          .setCurBlockRecvTime(now)
          .setPrevBlockHash(newCoinbase.getPrevBeaconHash)
          .setVrfRandseeds(newblk.getMiner.getBits)

        VCtrl.instance.syncToDB()
        if (System.currentTimeMillis() - start > VConfig.ADJUST_BLOCK_TX_MAX_TIMEMS) {
          for (i <- 1 to 2) {
            BlkTxCalc.adjustTx(System.currentTimeMillis() - start)
          }
        } else {
          BlkTxCalc.adjustTx(System.currentTimeMillis() - start)
        }
        val (newhash, sign) = RandFunction.genRandHash(Daos.enc.bytesToHexStr(newblk.getHeader.getHash.toByteArray()), newblk.getMiner.getTerm, newblk.getMiner.getBits);
        //      newhash, prevhash, mapToBigInt(netbits).bigInteger
        val ranInt: Int = new BigInteger(newhash, 16).intValue().abs;
        val (state, newblockbits, natarybits, sleepMs, firstBlockMakerBitIndex) = RandFunction.chooseGroups(ranInt, newNetBits, cn.getBitIdx);

        //        , netIndexs: Array[Int], curArrayIndex: Int;
        val keys = VCtrl.network().nodesByLocID.keySet
        val sentkeyset = new HashSet[String];
        val sentbcuid = new HashSet[String];
        TxCache.cacheTxs(txs);

        VCtrl.coMinerByUID.foreach(f => {
          val pn = f._2;
          var dn = VCtrl.network().directNodeByBcuid.getOrElse(pn.getBcuid, VCtrl.network().noneNode)
          if (newblockbits.testBit(pn.getBitIdx)) {
            if (firstBlockMakerBitIndex == pn.getBitIdx) {
              log.info("found next first maker:" + pn.getBcuid + ",coadd=" + pn.getCoAddress + ",nextblock=" + (newblk.getHeader.getHeight + 1));
              VCtrl.network().postMessage("CBNVRF", Left(newCoinbase.build()), newCoinbase.getMessageId, pn.getBcuid, '9')
              sentbcuid.add(pn.getBcuid)
              if (dn.loc_gwuris.contains(dn.uri)) {
                sentkeyset.add(dn.loc_id)
              }
              dn = null;
            }
          }
          if (dn != null && dn != VCtrl.network().noneNode) {
            if (!sentkeyset.contains(dn.loc_id) && dn.loc_gwuris.contains(dn.uri)) {
              log.info("send to loc mainer:" + pn.getBcuid + ",nextblock=" + (newblk.getHeader.getHeight + 1));
              VCtrl.network().postMessage("CBNVRF", Left(newCoinbase.build()), newCoinbase.getMessageId, pn.getBcuid, '9')
              sentbcuid.add(pn.getBcuid)
              sentkeyset.add(dn.loc_id)
            }
          }
          //        log.info("choose group state=" + state + " blockbits=" + blockbits + " notarybits=" + notarybits + " bcuid=" + pn.getBcuid)
        })

        newCoinbase.setBlockEntry(PBlockEntry.newBuilder().setBlockHeight(newblockheight)
          .setCoinbaseBcuid(cn.getBcuid).setBlockhash(Daos.enc.bytesToHexStr(newblk.getHeader.getHash.toByteArray()))
          .setBlockHeader(newblk.toBuilder().clearBody().build().toByteString())
          .setSign(Daos.enc.bytesToHexStr(newblk.getHeader.getHash.toByteArray())))

        // TODO 判断是否有足够余额，只发给有足够余额的节点
        var bits = BigInteger.ZERO

        val nodes = VCtrl.network().directNodes.++:(VCtrl.network().pendingNodes)

        nodes.foreach(f => {
          if (!sentbcuid.contains(f.bcuid)) {
            if (f.node_idx >= 0) {
              bits = bits.setBit(f.node_idx);
            } else if (f.try_node_idx >= 0) {
              bits = bits.setBit(f.try_node_idx);
            }
          }
        })
        VCtrl.coMinerByUID.map(f => {
          if (!sentbcuid.contains(f._1)) {
            bits = bits.setBit(f._2.getBitIdx);
          }
          if (newblockheight - f._2.getCurBlock > VConfig.SYNC_SAFE_BLOCK_COUNT) {
            bits = bits.clearBit(f._2.getBitIdx)
          }
        })
        log.info("bits-nodes.counts=" + bits.bitCount() + ",cominer=" + VCtrl.coMinerByUID.size);
        VCtrl.network().bwallMessage("CBNVRF", Left(newCoinbase.build()), bits, newCoinbase.getMessageId, '9')
        //      VCtrl.network().postMessage("CBNVRF", Left(newCoinbase.build()), newCoinbase.getMessageId, f._2.getBcuid, '9')
        //      VCtrl.allNodes.foreach(f => {
        //          val n = f._2;
        //          if(Integer.parseInt(n.getAuthBalance()) >= VConfig.AUTH_TOKEN_MIN) {
        ////            var sleepMS =   RandFunction.getRandMakeBlockSleep(newblk.getMiner.getTerm, newNetBits, cn.getBitIdx);
        ////            if (sleepMS < VConfig.BLOCK_MAKE_TIMEOUT_SEC * 1000) {
        //              log.info("broadcast block " + newblockheight + " to :" + n.getBcuid + " address:" + n.getCoAddress);
        //              VCtrl.network().postMessage("CBNVRF", Left(newCoinbase.build()), newCoinbase.getMessageId, n.getBcuid, '9')
        //            //}
        //          } else {
        //            log.error("cannot broadcast block ");
        //          }
        //        })

        //       VCtrl.network().dwallMessage("CBNVRF", Left(newCoinbase.build()), newCoinbase.getMessageId, '9')

      }
    } catch {
      case t: Throwable => {
        log.error("error in realcreateblock:", t);
      }
    }

  }
}