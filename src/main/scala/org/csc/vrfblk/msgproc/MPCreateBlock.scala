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

case class MPCreateBlock(netBits: BigInteger, blockbits: BigInteger, notarybits: BigInteger, beaconHash: String, preBeaconHash: String, beaconSig: String, witnessNode: BlockWitnessInfo, needHeight: Int) extends BlockMessage with PMNodeHelper with BitMap with LogHelper {
    def proc(): Unit = {
    }
}