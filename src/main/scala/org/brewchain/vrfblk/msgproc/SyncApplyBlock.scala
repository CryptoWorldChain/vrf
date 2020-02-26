package org.brewchain.vrfblk.msgproc

import java.math.BigInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ CountDownLatch, TimeUnit }

import onight.tfw.async.CallBack
import onight.tfw.otransio.api.beans.FramePacket
import org.apache.commons.lang3.StringUtils
import org.brewchain.mcore.crypto.BitMap
import org.brewchain.bcrand.model.Bcrand.{ PBlockEntryOrBuilder, PRetGetTransaction, PSCoinbase, PSGetTransaction }
import org.brewchain.mcore.model.Block.BlockInfo
import org.brewchain.mcore.model.Transaction
import org.brewchain.p22p.action.PMNodeHelper
import org.brewchain.p22p.node.{ Network, Node }
import org.brewchain.p22p.utils.LogHelper
import org.brewchain.vrfblk.Daos
import org.brewchain.vrfblk.tasks._
import org.brewchain.vrfblk.utils.{ BlkTxCalc, RandFunction, VConfig }

import scala.collection.JavaConverters._
import scala.util.Random
import org.brewchain.bcrand.model.Bcrand.PBlockEntry
import com.google.protobuf.ByteString

case class SyncApplyBlock(block: BlockInfo.Builder) extends BlockMessage with PMNodeHelper with BitMap with LogHelper {
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
      
      val vres = Daos.blkHelper.syncBlock(block, true);
      var lastSuccessBlock = Daos.chainHelper.getLastConnectBlock
      var maxid: Int = 0

      if (vres.getCurrentHeight >= block.getHeader.getHeight) {
        if (vres.getCurrentHeight > maxid) {
          maxid = block.getHeader.getHeight.intValue();
        }
        log.info("sync block height ok=" + block.getHeader.getHeight + ",dbh=" + vres.getCurrentHeight + ",hash=" + Daos.enc.bytesToHexStr(block.getHeader.getHash.toByteArray()) + ",seed=" +
          block.getMiner.getBits);
      } else {
        log.info("sync block height failed=" + block.getHeader.getHeight + ",dbh=" + vres.getCurrentHeight + ",curBlock=" + maxid + ",hash=" + Daos.enc.bytesToHexStr(block.getHeader.getHash.toByteArray())
          + ",prev=" + Daos.enc.bytesToHexStr(block.getHeader.getParentHash.toByteArray()) + ",seed=" +
          block.getMiner.getBits);
      }
      if (maxid > 0) {
        VCtrl.instance.updateBlockHeight(VCtrl.getPriorityBlockInBeaconHash(lastSuccessBlock));
        // VCtrl.instance.updateBlockHeight(maxid, Daos.enc.hexEnc(lastSuccessBlock.getHeader.getHash.toByteArray()), lastSuccessBlock.getMiner.getBit)
      }
    } finally {
      BlockSync.syncBlockInQueue.decrementAndGet();
//      log.info("value=" + BlockSync.syncBlockInQueue.get);
      if (BlockSync.syncBlockInQueue.get <= 0) {
        log.info("BlockSync.syncBlockInQueue,need gossip block again:"+BlockSync.syncBlockInQueue.get);
        BeaconGossip.gossipBlocks();
      }
    }
  }
}