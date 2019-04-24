package org.csc.vrfblk.msgproc

import java.math.BigInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ CountDownLatch, TimeUnit }

import onight.tfw.async.CallBack
import onight.tfw.otransio.api.beans.FramePacket
import org.apache.commons.lang3.StringUtils
import org.csc.account.gens.Blockimpl.AddBlockResponse
import org.csc.bcapi.crypto.BitMap
import org.csc.ckrand.pbgens.Ckrand.{ PBlockEntryOrBuilder, PRetGetTransaction, PSCoinbase, PSGetTransaction }
import org.csc.evmapi.gens.Block.BlockEntity
import org.csc.evmapi.gens.Tx.Transaction
import org.csc.p22p.action.PMNodeHelper
import org.csc.p22p.node.{ Network, Node }
import org.csc.p22p.utils.LogHelper
import org.csc.vrfblk.Daos
import org.csc.vrfblk.tasks._
import org.csc.vrfblk.utils.{ BlkTxCalc, RandFunction, VConfig }

import scala.collection.JavaConverters._
import scala.util.Random
import org.csc.ckrand.pbgens.Ckrand.PBlockEntry
import com.google.protobuf.ByteString

case class SyncApplyBlock(block: BlockEntity.Builder) extends BlockMessage with PMNodeHelper with BitMap with LogHelper {
  def proc() {
    try {
      
//      val newCoinbase = PSCoinbase.newBuilder()
//      .setBlockHeight(block.getHeader.getNumber.intValue()).setCoAddress(
//          Daos.enc.hexEnc(block.getMiner.getAddress.toByteArray()))
//      .setMessageId(block.getMiner.getTermid)
//      .setBcuid(block.getMiner.getBcuid)
//      .setBlockEntry(PBlockEntry.newBuilder().setBlockHeight(block.getHeader.getNumber.intValue())
//        .setCoinbaseBcuid(block.getMiner.getBcuid).setBlockhash(Daos.enc.hexEnc(block.getHeader.getHash.toByteArray()))
//        .setBlockHeader(block.getHeader.toByteString())
//        //.setBlockMiner(newblk)
//        .setSign(Daos.enc.hexEnc(block.getHeader.getHash.toByteArray())))
//      .setSliceId(VConfig.SLICE_ID)
//      .setTxcount(block.getBody.getTxsCount)
//      .setBeaconBits(block.getMiner.getBit)
//      .setBeaconSign(block.getMiner.getTermid)
//      .setBeaconHash(block.getMiner.getTermid)
//      .setBlockSeeds(block.bo
//          ByteString.copyFrom(blockbits.toByteArray()))
//      .setPrevBeaconHash(cn.getBeaconHash)
//      .setPrevBlockSeeds(ByteString.copyFrom(cn.getVrfRandseeds.getBytes))
//      .setVrfCodes(ByteString.copyFrom(strnetBits.getBytes))
//      .setWitnessBits(hexToMapping(notarybits))
      
      val vres = Daos.blkHelper.ApplyBlock(block, true);
      var lastSuccessBlock = Daos.chainHelper.GetConnectBestBlock();
      var maxid: Int = 0

      if (vres.getCurrentNumber >= block.getHeader.getNumber) {
        if (vres.getCurrentNumber > maxid) {
          maxid = block.getHeader.getNumber.intValue();
        }
        log.info("sync block height ok=" + block.getHeader.getNumber + ",dbh=" + vres.getCurrentNumber + ",hash=" + Daos.enc.hexEnc(block.getHeader.getHash.toByteArray()) + ",seed=" +
          block.getMiner.getBit);
      } else {
        log.info("sync block height failed=" + block.getHeader.getNumber + ",dbh=" + vres.getCurrentNumber + ",curBlock=" + maxid + ",hash=" + Daos.enc.hexEnc(block.getHeader.getHash.toByteArray())
          + ",prev=" + Daos.enc.hexEnc(block.getHeader.getPreHash.toByteArray()) + ",seed=" +
          block.getMiner.getBit);
      }
      if (maxid > 0) {
        VCtrl.instance.updateBlockHeight(VCtrl.getPriorityBlockInBeaconHash(lastSuccessBlock));
        // VCtrl.instance.updateBlockHeight(maxid, Daos.enc.hexEnc(lastSuccessBlock.getHeader.getHash.toByteArray()), lastSuccessBlock.getMiner.getBit)
      }
    } finally {
      BlockSync.syncBlockInQueue.decrementAndGet();
//      log.info("value=" + BlockSync.syncBlockInQueue.get);
      if (BlockSync.syncBlockInQueue.get <= 0) {
        BeaconGossip.gossipBlocks();
      }
    }
  }
}