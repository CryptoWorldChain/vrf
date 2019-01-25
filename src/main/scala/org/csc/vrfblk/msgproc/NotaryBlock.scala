package org.csc.vrfblk.msgproc

import org.csc.bcapi.crypto.BitMap
import org.csc.ckrand.pbgens.Ckrand.PSCoinbase
import org.csc.p22p.action.PMNodeHelper
import org.csc.p22p.utils.LogHelper
import org.csc.vrfblk.tasks.BlockMessage
import org.csc.vrfblk.tasks.VCtrl

case class NotaryBlock(pbo: PSCoinbase) extends BlockMessage with PMNodeHelper with BitMap with LogHelper {

  def proc() {

    //to notify other.
    MDCSetBCUID(VCtrl.network())
    log.info("notaryblock,H=" + pbo.getBlockHeight + ":coadr=" + pbo.getCoAddress + ",DN=" + VCtrl.network().directNodeByIdx.size + ",PN=" + VCtrl.network().pendingNodeByBcuid.size
      + ",MN=" + VCtrl.coMinerByUID.size
      + ",from=" + pbo.getBcuid 
      +",NB=" + new String(pbo.getVrfCodes.toByteArray())
      + ",VB=" + pbo.getWitnessBits
      + ",B=" + pbo.getBlockEntry.getSign
      + ",TX=" + pbo.getTxcount);

  }
}