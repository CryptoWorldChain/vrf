package org.csc.vrfblk.msgproc

import org.csc.bcapi.crypto.BitMap
import org.csc.ckrand.pbgens.Ckrand.PSCoinbase
import org.csc.p22p.action.PMNodeHelper
import org.csc.p22p.utils.LogHelper
import org.csc.vrfblk.tasks.BlockMessage
import org.apache.commons.lang3.StringUtils
import org.csc.vrfblk.tasks.VCtrl
import java.util.concurrent.atomic.AtomicLong
import org.csc.ckrand.pbgens.Ckrand.PBlockEntryOrBuilder
import org.csc.vrfblk.Daos
import org.csc.vrfblk.utils.BlkTxCalc
import org.csc.vrfblk.tasks.NodeStateSwither
import org.csc.vrfblk.tasks.StateChange
import org.csc.vrfblk.utils.RandFunction
import com.google.protobuf.ByteString
import org.csc.evmapi.gens.Block.BlockEntity

case class ApplyBlock(pbo: PSCoinbase) extends BlockMessage with PMNodeHelper with BitMap with LogHelper {

  val bestheight = new AtomicLong(0);

  val emptyBlock = new AtomicLong(0);

  def saveBlock(b: PBlockEntryOrBuilder, needBody: Boolean = false): (Int, Int) = {
    val block = BlockEntity.newBuilder().mergeFrom(b.getBlockHeader);

    if (!b.getCoinbaseBcuid.equals(VCtrl.curVN().getBcuid)) {
      val startupApply = System.currentTimeMillis();
      val vres = Daos.blkHelper.ApplyBlock(block, needBody);
      if (vres.getTxHashsCount > 0) {
        log.info("must sync transaction first,losttxcount=" + vres.getTxHashsCount + ",height=" + b.getBlockHeight);
        (vres.getCurrentNumber.intValue(), vres.getWantNumber.intValue())
      } else if (vres.getCurrentNumber > 0) {
        log.debug("checkMiner --> updateBlockHeight::" + vres.getCurrentNumber.intValue() + ",blk.height=" + b.getBlockHeight + ",wantNumber=" + vres.getWantNumber.intValue())
        VCtrl.instance.updateBlockHeight(
          vres.getCurrentNumber.intValue(), if (vres.getCurrentNumber.intValue() == b.getBlockHeight) b.getSign else null, block.getHeader.getExtraData)
        if (vres.getCurrentNumber.intValue() == b.getBlockHeight) {
          BlkTxCalc.adjustTx(System.currentTimeMillis() - startupApply)
        }
        VCtrl.instance.updateBlockHeight(b.getBlockHeight, b.getSign,block.getHeader.getExtraData)
        (vres.getCurrentNumber.intValue(), vres.getWantNumber.intValue())
      } else {
        (vres.getCurrentNumber.intValue(), vres.getWantNumber.intValue())
      }
      
    } else {
//      log.debug("checkMiner --> updateBlockHeight::" + b.getBlockHeight)
      VCtrl.instance.updateBlockHeight(b.getBlockHeight, b.getSign,block.getHeader.getExtraData)
      (b.getBlockHeight, b.getBlockHeight)
    }
  }
  def tryNotifyState() {
    //    if(VCtrl.instance.b

    val (hash, sign) = RandFunction.genRandHash(pbo.getBlockEntry.getBlockhash, pbo.getPrevBeaconHash, VCtrl.network().node_strBits)
    NodeStateSwither.offerMessage(new StateChange(sign, hash, pbo.getBlockEntry.getBlockhash));

  }
  def proc() {
    val cn = VCtrl.curVN();
    if (StringUtils.equals(pbo.getCoAddress, cn.getCoAddress) || pbo.getBlockHeight > cn.getCurBlock) {
      val (acceptHeight, blockWant) = saveBlock(pbo.getBlockEntry);
      acceptHeight match {
        case n if n > 0 && n < pbo.getBlockHeight =>
          //                  ret.setResult(CoinbaseResult.CR_PROVEN)
          log.info("applyblock:UU,H=" + pbo.getBlockHeight + ",DB=" + n + ":coadr=" + pbo.getCoAddress
            .size + ",DN=" + VCtrl.network().directNodeByIdx.size + ",PN=" + VCtrl.network().pendingNodeByBcuid.size
            + ",B=" + pbo.getBlockEntry.getSign
            + ",TX=" + pbo.getTxcount);
        case n if n > 0 =>
          val vstr=
          if(StringUtils.equals(pbo.getCoAddress, cn.getCoAddress) ){
            "MY"
          }else{
            "OK"
          }
          log.info("applyblock:"+vstr+",H=" + pbo.getBlockHeight + ",DB=" + n + ":coadr=" + pbo.getCoAddress
            .size + ",DN=" + VCtrl.network().directNodeByIdx.size + ",PN=" + VCtrl.network().pendingNodeByBcuid.size
            + ",MN=" + VCtrl.coMinerByUID.size
            + ",NBits=" + new String(pbo.getVrfCodes.toByteArray())
              + ",B=" + pbo.getBlockEntry.getSign
            + ",TX=" + pbo.getTxcount);
          bestheight.set(n);
          tryNotifyState();
        case n @ _ =>
          log.info("applyblock:NO,H=" + pbo.getBlockHeight + ",DB=" + n + ":coadr=" + pbo.getCoAddress
            .size + ",DN=" + VCtrl.network().directNodeByIdx.size + ",PN=" + VCtrl.network().pendingNodeByBcuid.size
            + ",B=" + pbo.getBlockEntry.getSign
            + ",TX=" + pbo.getTxcount);
      }
    }

  }
}