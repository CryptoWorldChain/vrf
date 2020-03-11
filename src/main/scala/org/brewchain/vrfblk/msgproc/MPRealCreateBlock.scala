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

  def proc(): Unit = {
    val start = System.currentTimeMillis();
    val cn = VCtrl.curVN();
    MDCSetBCUID(VCtrl.network())
    //需要广播的节点数量
    val wallAccount: Int = VCtrl.coMinerByUID.size * VConfig.DCTRL_BLOCK_CONFIRMATION_RATIO / 100

    var newNetBits = BigInteger.ZERO
    val existCominerBits = mapToBigInt(cn.getCominers).bigInteger;
    VCtrl.coMinerByUID.foreach(f => {
      log.info("check:"+f._2.getBcuid+":"+f._2.getCominers+"==>"+cn.getCominers+",result="+
          mapToBigInt(f._2.getCominers).bigInteger.and(existCominerBits).equals(existCominerBits)+",height="+f._2.getCurBlock+"==>"+VCtrl.curVN().getCurBlock);
      
      if ( //other nodes
      f._2.getCurBlock >= VCtrl.curVN().getCurBlock - VConfig.BLOCK_DISTANCE_NETBITS
        && mapToBigInt(f._2.getCominers).bigInteger.and(existCominerBits).equals(existCominerBits)
        || f._2.getBcuid.equals(VCtrl.curVN().getBcuid)) {
        newNetBits = newNetBits.setBit(f._2.getBitIdx);
      }
    })
    //}

    val strnetBits = hexToMapping(newNetBits);
    // BlkTxCalc.getBestBlockTxCount(VConfig.MAX_TNX_EACH_BLOCK)

    log.error("MPRealCreateBlock:start confirm=" + wallAccount+ ",netcount="+newNetBits.bitCount()+ ",strnetBits=" + strnetBits + ",nodes.count=" + VCtrl.coMinerByUID.size + ",newNetBits=" + newNetBits.toString(2));

    val (newblk, txs) = newBlockFromAccount(
      VConfig.MAX_TNX_EACH_BLOCK, wallAccount, beaconHash,
      strnetBits);

    if (newblk == null) {
      log.debug("mining error: ch=" + cn.getCurBlock);
    } else {
      // TODO 更新pnode，dnode的节点balance
      // VCtrl.refreshNodeBalance();

      val newblockheight = newblk.getHeader.getHeight.intValue()
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
      VCtrl.coMinerByUID.filter(!_._2.getBcuid.equalsIgnoreCase(cn.getBcuid)).foreach(f => {
        val pn = f._2;
        val (state, blockbits, notarybits) = RandFunction.chooseGroups(newblk.getMiner.getTerm, newNetBits, pn.getBitIdx)
        if (state == VNodeState.VN_DUTY_BLOCKMAKERS) {
          var sleepMS = RandFunction.getRandMakeBlockSleep(newblk.getMiner.getTerm, blockbits, pn.getBitIdx);
          if (sleepMS < VConfig.BLOCK_MAKE_TIMEOUT_SEC * 1000) {
            log.info("found next first maker:" + pn.getBcuid + ",nextblock=" + (newblk.getHeader.getHeight + 1));
            VCtrl.network().postMessage("CBNVRF", Left(newCoinbase.build()), newCoinbase.getMessageId, pn.getBcuid, '9')
          }
        }
        log.info("choose group state=" + state + " blockbits=" + blockbits + " notarybits=" + notarybits+ " bcuid="+pn.getBcuid)
      })

      newCoinbase.setBlockEntry(PBlockEntry.newBuilder().setBlockHeight(newblockheight)
        .setCoinbaseBcuid(cn.getBcuid).setBlockhash(Daos.enc.bytesToHexStr(newblk.getHeader.getHash.toByteArray()))
        .setBlockHeader(newblk.toBuilder().clearBody().build().toByteString())
        .setSign(Daos.enc.bytesToHexStr(newblk.getHeader.getHash.toByteArray())))
      
       // TODO 判断是否有足够余额，只发给有足够余额的节点
        VCtrl.coMinerByUID.foreach(f => {
          if (!VConfig.AUTH_NODE_FILTER || VCtrl.haveEnoughToken(f._2.getCoAddress)) {
            VCtrl.network().postMessage("CBNVRF", Left(newCoinbase.build()), newCoinbase.getMessageId, f._2.getBcuid, '9')
          }
        })
//      VCtrl.allNodes.foreach(f => {
//          val n = f._2;
//          if(Integer.parseInt(n.getAuthBalance()) >= VConfig.AUTH_TOKEN_MIN) {
////            var sleepMS = RandFunction.getRandMakeBlockSleep(newblk.getMiner.getTerm, newNetBits, cn.getBitIdx);
////            if (sleepMS < VConfig.BLOCK_MAKE_TIMEOUT_SEC * 1000) {
//              log.info("broadcast block " + newblockheight + " to :" + n.getBcuid + " address:" + n.getCoAddress);
//              VCtrl.network().postMessage("CBNVRF", Left(newCoinbase.build()), newCoinbase.getMessageId, n.getBcuid, '9')
//            //}
//          } else {
//            log.error("cannot broadcast block ");
//          }
//        })
      TxCache.cacheTxs(txs);
      // VCtrl.network().dwallMessage("CBNVRF", Left(newCoinbase.build()), newCoinbase.getMessageId, '9')

    }

  }
}