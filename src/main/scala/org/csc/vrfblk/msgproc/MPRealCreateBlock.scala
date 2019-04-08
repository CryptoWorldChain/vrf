package org.csc.vrfblk.msgproc

import java.math.BigInteger
import java.util.concurrent.TimeUnit

import com.google.protobuf.ByteString
import onight.tfw.outils.serialize.UUIDGenerator
import org.csc.bcapi.crypto.BitMap
import org.csc.ckrand.pbgens.Ckrand.{BlockWitnessInfo, PBlockEntry, PSCoinbase}
import org.csc.evmapi.gens.Block.BlockEntity
import org.csc.evmapi.gens.Tx.Transaction
import org.csc.p22p.action.PMNodeHelper
import org.csc.p22p.utils.LogHelper
import org.csc.vrfblk.Daos
import org.csc.vrfblk.tasks.{BlockMessage, VCtrl}
import org.csc.vrfblk.utils.{BlkTxCalc, TxCache, VConfig}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

case class MPRealCreateBlock(netBits: BigInteger, blockbits: BigInteger, notarybits: BigInteger, beaconHash: String, preBeaconHash: String, beaconSig: String, witnessNode: BlockWitnessInfo, needHeight: Int) extends BlockMessage with PMNodeHelper with BitMap with LogHelper {

def newBlockFromAccount(txc: Int, confirmTimes: Int, beaconHash: String, voteInfos: String): (BlockEntity, java.util.List[Transaction]) = {
    val starttx = System.currentTimeMillis();
    val txs = Daos.txHelper.getWaitBlockTx(
      txc, //只是打块！其中某些成功广播的tx，默认是80%
      confirmTimes);

    //奖励节点
    val excitationAddress: ListBuffer[String] = new ListBuffer()
    if (witnessNode.getBeaconHash.equals(beaconHash)) {
      excitationAddress.appendAll(witnessNode.getWitnessList.asScala.map(node => node.getCoAddress).toList)
    }

    log.info(s"netbits:${witnessNode.getNetbitx}; witnessBits:${witnessNode.getWitnessList.asScala.map(_.getBitIdx)}; " +
      s"beaconHash:${beaconHash}; excitationAddress:${excitationAddress.mkString("[", ",", "]")};")

    //@volatile var result: (BlockEntity, java.util.List[Transaction]) = (null, null)

    log.info(s"LOCK to NewBlock time:${System.currentTimeMillis()}")
    val startblk = System.currentTimeMillis();
    val newblk = Daos.blkHelper.createNewBlock(txs, "", beaconHash, excitationAddress.asJava, voteInfos);
    //extradata,term
    val endblk = System.currentTimeMillis();

    log.debug("new block ok:beaconHash=" + beaconHash+ " txms=" + (startblk - starttx) + ",blkms=" + (endblk - startblk));

    //val newblockheight = VCtrl.curVN().getCurBlock + 1
    //if (newblk == null || newblk.getHeader == null) {
    //  log.debug("new block header is null: ch=" + newblockheight + ",dbh=" + newblk);
    //  result = (null, null)
    //} else if (newblockheight != newblk.getHeader.getNumber) {
    //  log.debug("mining error: ch=" + newblockheight + ",dbh=" + newblk.getHeader.getNumber);
    //  result = (null, null)
    //} else {
    //  result = (newblk, txs)
    //}

    (newblk, txs)
    //result
  }

  def proc(): Unit = {
    val start = System.currentTimeMillis();
    val cn = VCtrl.curVN();
    MDCSetBCUID(VCtrl.network())

    //需要广播的节点数量
    val wallAccount: Int = VCtrl.coMinerByUID.size * VConfig.DCTRL_BLOCK_CONFIRMATION_RATIO / 100

    //var newNetBits = BigInteger.ZERO
    // log.debug("tryNotifyState netBits=" + nodeBit.bitCount() + " size=" + VCtrl.coMinerByUID.size)
    //if (newNetBits.bitCount() < VCtrl.coMinerByUID.size) {
     var newNetBits = BigInteger.ZERO
      VCtrl.coMinerByUID.foreach(f => {
        newNetBits = newNetBits.setBit(f._2.getBitIdx);
      })
    //}

    val strnetBits = hexToMapping(newNetBits);
    val (newblk, txs) = newBlockFromAccount(
      BlkTxCalc.getBestBlockTxCount(VConfig.MAX_TNX_EACH_BLOCK), wallAccount, beaconHash,
      strnetBits);

    if (newblk == null) {
      log.debug("mining error: ch=" + cn.getCurBlock);
    } else {

      val newblockheight = newblk.getHeader.getNumber.intValue()
      //        log.debug("MineNewBlock:" + newblk);
      val now = System.currentTimeMillis();
      log.debug("mining check ok :new block=" + newblockheight + ",CO=" + cn.getCoAddress
        + ",MaxTnx=" + VConfig.MAX_TNX_EACH_BLOCK + ",hash=" + Daos.enc.hexEnc(newblk.getHeader.getHash.toByteArray()) + " wall=" + wallAccount);
      val newCoinbase = PSCoinbase.newBuilder()
        .setBlockHeight(newblockheight).setCoAddress(cn.getCoAddress)
        .setCoAddress(cn.getCoAddress)
        .setMessageId(UUIDGenerator.generate())
        .setBcuid(cn.getBcuid)
        .setBlockEntry(PBlockEntry.newBuilder().setBlockHeight(newblockheight)
          .setCoinbaseBcuid(cn.getBcuid).setBlockhash(Daos.enc.hexEnc(newblk.getHeader.getHash.toByteArray()))
          .setBlockHeader(newblk.toBuilder().clearBody().build().toByteString())
          //.setBlockMiner(newblk)
          .setSign(Daos.enc.hexEnc(newblk.getHeader.getHash.toByteArray())))
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
      cn.setCurBlock(newblockheight)
        .setBeaconHash(newblk.getMiner.getTermid)
        .setBeaconSign(beaconSig)
        .setCurBlockHash(Daos.enc.hexEnc(newblk.getHeader.getHash.toByteArray()))
        .setCurBlockMakeTime(now)
        .setCurBlockRecvTime(now)
        .setPrevBlockHash(newCoinbase.getPrevBeaconHash)
        .setVrfRandseeds(newblk.getMiner.getBit)

      VCtrl.instance.syncToDB()
      if (System.currentTimeMillis() - start > VConfig.ADJUST_BLOCK_TX_MAX_TIMEMS) {
        for (i <- 1 to 2) {
          BlkTxCalc.adjustTx(System.currentTimeMillis() - start)
        }
      } else {
        BlkTxCalc.adjustTx(System.currentTimeMillis() - start)
      }
      VCtrl.network().dwallMessage("CBNVRF", Left(newCoinbase.build()), newCoinbase.getMessageId, '9')
      TxCache.cacheTxs(txs);
    }

  }
}