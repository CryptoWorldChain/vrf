package org.csc.vrfblk.msgproc

import java.math.BigInteger

import org.csc.bcapi.crypto.BitMap
import org.csc.ckrand.pbgens.Ckrand.BlockWitnessInfo
import org.csc.p22p.action.PMNodeHelper
import org.csc.p22p.utils.LogHelper
import org.csc.vrfblk.tasks.BlockMessage

case class MPCreateBlock(netBits: BigInteger, blockbits: BigInteger, notarybits: BigInteger, beaconHash: String, preBeaconHash: String, beaconSig: String, witnessNode: BlockWitnessInfo, needHeight: Int) extends BlockMessage with PMNodeHelper with BitMap with LogHelper {
    def proc(): Unit = {
    }
}