package org.csc.vrfblk.msgproc

import org.csc.bcapi.crypto.BitMap
import org.csc.ckrand.pbgens.Ckrand.PSCoinbase
import org.csc.p22p.action.PMNodeHelper
import org.csc.p22p.utils.LogHelper
import org.csc.vrfblk.tasks.BlockMessage

case class NotaryBlock(coinbase: PSCoinbase) extends BlockMessage with PMNodeHelper with BitMap with LogHelper {

  def proc() {

  }
}