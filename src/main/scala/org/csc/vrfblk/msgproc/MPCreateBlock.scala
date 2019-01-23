package org.csc.vrfblk.msgproc

import org.csc.vrfblk.Daos
import java.math.BigInteger
import org.csc.p22p.action.PMNodeHelper
import org.csc.bcapi.crypto.BitMap
import org.csc.p22p.utils.LogHelper
import org.csc.evmapi.gens.Block.BlockEntity
import org.csc.evmapi.gens.Tx.MultiTransaction
import org.csc.vrfblk.tasks.VCtrl
import org.csc.vrfblk.tasks.BlockMessage
import org.csc.vrfblk.utils.BlkTxCalc
import org.csc.vrfblk.utils.VConfig
import org.csc.ckrand.pbgens.Ckrand.PSCoinbase
import org.csc.ckrand.pbgens.Ckrand.PBlockEntry
import onight.tfw.outils.serialize.UUIDGenerator
import com.google.protobuf.ByteString
import org.csc.vrfblk.utils.TxCache

case class MPCreateBlock(blockbits: BigInteger, notarybits: BigInteger, beaconHash: String, beaconSig: String) extends BlockMessage with PMNodeHelper with BitMap with LogHelper {

  def newBlockFromAccount(txc: Int, confirmTimes: Int, beaconHash: String, voteInfos: String): (BlockEntity, java.util.List[MultiTransaction]) = {
    val txs = Daos.txHelper.getWaitBlockTx(
      txc, //只是打块！其中某些成功广播的tx，默认是80%
      confirmTimes);
    val newblk = Daos.blkHelper.CreateNewBlock(txs, voteInfos, beaconHash);
    val newblockheight = VCtrl.curVN().getCurBlock + 1
    if (newblk == null || newblk.getHeader == null) {
      log.debug("new block header is null: ch=" + newblockheight + ",dbh=" + newblk);
      (null, null)
    } else if (newblockheight != newblk.getHeader.getNumber) {
      log.debug("mining error: ch=" + newblockheight + ",dbh=" + newblk.getHeader.getNumber);
      (null, null)
    } else {
      (newblk, txs)
    }
  }

  def proc(): Unit = {
    val start = System.currentTimeMillis();
    val cn = VCtrl.curVN();
    val strblkBits = hexToMapping(blockbits);
    val (newblk, txs) = newBlockFromAccount(
      BlkTxCalc.getBestBlockTxCount(VConfig.MAX_TNX_EACH_BLOCK), 0, beaconHash,
      strblkBits);

    if (newblk == null) {
      log.debug("mining error: ch=" + cn.getCurBlock);
    } else {

      val newblockheight = newblk.getHeader.getNumber.intValue()
      //        log.debug("MineNewBlock:" + newblk);
      val now = System.currentTimeMillis();
      log.debug("mining check ok :new block=" + newblockheight + ",CO=" + cn.getCoAddress
        + ",MaxTnx=" + VConfig.MAX_TNX_EACH_BLOCK + ",hash=" + newblk.getHeader.getBlockHash);
      val newCoinbase = PSCoinbase.newBuilder()
        .setBlockHeight(newblockheight).setCoAddress(cn.getCoAddress)
        .setCoAddress(cn.getCoAddress)
        .setMessageId(UUIDGenerator.generate())
        .setBcuid(cn.getBcuid)
        .setBlockEntry(PBlockEntry.newBuilder().setBlockHeight(newblockheight)
          .setCoinbaseBcuid(cn.getBcuid).setBlockhash(newblk.getHeader.getBlockHash)
          .setBlockHeader(newblk.toBuilder().clearBody().build().toByteString())
          //.setBlockMiner(newblk)
          .setSign(newblk.getHeader.getBlockHash))
        .setSliceId(VConfig.SLICE_ID)
        .setTxcount(txs.size())
        .setBeaconBits(strblkBits)
        .setBeaconSign(beaconSig)
        .setBeaconHash(beaconHash)
        .setBlockSeeds(ByteString.copyFrom(blockbits.toByteArray()))
        .setPrevBeaconHash(cn.getBeaconHash)
        .setPrevBlockSeeds(cn.getVrfRandseeds)
        .setVrfCodes(ByteString.copyFrom(blockbits.toByteArray()))
        .setWitnessBits(hexToMapping(notarybits))

      cn.setCurBlock(newblockheight)
        .setBeaconHash(beaconHash)
        .setBeaconSign(beaconSig)
        .setCurBlockHash(newblk.getHeader.getBlockHash)
        .setCurBlockMakeTime(now)
        .setCurBlockRecvTime(now)
        .setPrevBlockHash(newCoinbase.getPrevBeaconHash)
      VCtrl.instance.syncToDB()
      if (System.currentTimeMillis() - start > VConfig.ADJUST_BLOCK_TX_MAX_TIMEMS) {
        for (i <- 1 to 2) {
          BlkTxCalc.adjustTx(System.currentTimeMillis() - start)
        }
      } else {
        BlkTxCalc.adjustTx(System.currentTimeMillis() - start)
      }
      // write to
      //
      //            log.debug("mindob newblockheight::" + newblockheight + " cn.getCoAddress::" + cn.getCoAddress + " termid::" + DCtrl.termMiner().getTermId + " cn.getBcuid::" + cn.getBcuid)
      VCtrl.network().wallMessage("CBNVRF", Left(newCoinbase.build()), newCoinbase.getMessageId, '9')
      TxCache.cacheTxs(txs);
    }

  }

}