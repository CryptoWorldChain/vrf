package org.csc.vrfblk.msgproc

import java.math.BigInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{CountDownLatch, TimeUnit}

import onight.tfw.async.CallBack
import onight.tfw.otransio.api.beans.FramePacket
import org.apache.commons.lang3.StringUtils
import org.csc.account.gens.Blockimpl.AddBlockResponse
import org.csc.bcapi.crypto.BitMap
import org.csc.ckrand.pbgens.Ckrand.{PBlockEntryOrBuilder, PRetGetTransaction, PSCoinbase, PSGetTransaction}
import org.csc.evmapi.gens.Block.BlockEntity
import org.csc.evmapi.gens.Tx.Transaction
import org.csc.p22p.action.PMNodeHelper
import org.csc.p22p.node.{Network, Node}
import org.csc.p22p.utils.LogHelper
import org.csc.vrfblk.Daos
import org.csc.vrfblk.tasks._
import org.csc.vrfblk.utils.{BlkTxCalc, RandFunction, VConfig}

import scala.collection.JavaConverters._
import scala.util.Random
case class SyncApplyBlock(block: BlockEntity.Builder) extends BlockMessage with PMNodeHelper with BitMap with LogHelper {
    def proc() {
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
            VCtrl.instance.updateBlockHeight(maxid, Daos.enc.hexEnc(lastSuccessBlock.getHeader.getHash.toByteArray()), lastSuccessBlock.getMiner.getBit)
        }
    }
}