package org.brewchain.vrfblk.msgproc

import java.math.BigInteger

import org.brewchain.mcore.crypto.BitMap
import org.brewchain.bcrand.model.Bcrand.BlockWitnessInfo
import org.brewchain.p22p.action.PMNodeHelper
import org.brewchain.p22p.utils.LogHelper
import org.brewchain.vrfblk.tasks.BlockMessage

case class MPCreateBlock(netBits: BigInteger, blockbits: BigInteger, notarybits: BigInteger, beaconHash: String, preBeaconHash: String, beaconSig: String, witnessNode: BlockWitnessInfo, needHeight: Int,sleepMs:Int) extends BlockMessage with PMNodeHelper with BitMap with LogHelper {
    def proc(): Unit = {
    }
}