package org.csc.vrfblk.utils

import java.util.concurrent.TimeUnit

import org.csc.evmapi.gens.Tx.MultiTransaction

import com.google.common.cache.Cache
import com.google.common.cache.CacheBuilder

import onight.oapi.scala.traits.OLog
import org.csc.p22p.Daos
import org.apache.commons.lang3.StringUtils
import org.csc.vrfblk.tasks.VCtrl
import java.math.BigInteger
import org.csc.bcapi.crypto.BitMap
import org.csc.ckrand.pbgens.Ckrand.VNodeState
import scala.collection.mutable.ListBuffer

object RandFunction extends OLog with BitMap {

  def genRandHash(curblockHash: String, prevRandHash: String, nodebits: String): (String, String) = {
    val content = Array(curblockHash, prevRandHash, nodebits).mkString(",");
    val hash = Daos.enc.sha256Encode(Daos.enc.hexDec(content));
    val sign = Daos.enc.ecSignHex(
      VCtrl.network().root().pri_key,
      hash);
    (Daos.enc.hexEnc(hash), sign)
  }

  def reasonableRandInt(beaconHexSeed: String, netBits: BigInteger): (BigInteger, BigInteger) = { //4 roles.
    val blockbits = new BigInteger(beaconHexSeed.substring(0, beaconHexSeed.length() / 2), 16);
    val rightbits = new BigInteger(beaconHexSeed.substring(beaconHexSeed.length() / 2 + 1), 16);
    val votebits = rightbits.andNot(blockbits.and(netBits))

    if (blockbits.bitCount() >= netBits.bitCount() / 4 && votebits.bitCount > netBits.bitCount() / 4) {
      //cannot product block maker
      return (blockbits, votebits);
    } else {
      val deeprand = Daos.enc.hexEnc(Daos.enc.sha256Encode(blockbits.toByteArray()));
      reasonableRandInt(deeprand, netBits);
    }
  }

  def chooseGroups(beaconHexSeed: String, strBits: String, curIdx: Int): (VNodeState, BigInteger,BigInteger) = {
    val (blockbits, votebits) = reasonableRandInt(beaconHexSeed, mapToBigInt(strBits).bigInteger);
    if (blockbits.testBit(curIdx)) {
      (VNodeState.VN_DUTY_BLOCKMAKERS, blockbits,votebits)
    } else if (votebits.testBit(curIdx)) {
      (VNodeState.VN_DUTY_NOTARY, blockbits,votebits)
    } else {
      (VNodeState.VN_DUTY_SYNC, blockbits,votebits)
    }
  }
  def getRandMakeBlockSleep(beaconHash: String, blockbits: BigInteger, curIdx: Int):Long =  {
    var testcc = 0;
    var testBits = blockbits;
    var indexInBits = 0;
    while (testcc < 10240 && testBits.bitCount() > 0) {
      if (blockbits.testBit(testcc)) {
        testBits = testBits.clearBit(testcc);
        if (curIdx == testcc) {
          testBits = BigInteger.ZERO;
        }
        indexInBits = indexInBits + 1;
      }
    }
    val ranInt = new BigInteger(beaconHash, 16).abs(); //.multiply(BigInteger.valueOf(curIdx));
    val stepRange = ranInt.mod(BigInteger.valueOf(blockbits.bitCount())).intValue();
    (indexInBits - stepRange + blockbits.bitCount()) % blockbits.bitCount() * VConfig.BLOCK_MAKE_TIMEOUT_SEC
    +VConfig.BLK_EPOCH_MS
  }
}