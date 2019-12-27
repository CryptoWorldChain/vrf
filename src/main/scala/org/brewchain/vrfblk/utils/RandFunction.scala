package org.brewchain.vrfblk.utils

import java.util.concurrent.TimeUnit

import onight.oapi.scala.traits.OLog
import org.apache.commons.lang3.StringUtils
import org.brewchain.vrfblk.tasks.VCtrl
import java.math.BigInteger
import org.brewchain.core.crypto.BitMap
import org.brewchain.bcrand.model.Bcrand.VNodeState
import scala.collection.mutable.ListBuffer
import org.brewchain.p22p.utils.LogHelper
import com.google.common.math.BigIntegerMath
import java.util.BitSet
import org.brewchain.vrfblk.Daos

object RandFunction extends LogHelper with BitMap {

  def genRandHash(curblockHash: String, prevRandHash: String, nodebits: String): (String, String) = {
    val content = Array(curblockHash, prevRandHash, nodebits).mkString(",");
    val hash = Daos.enc.sha256(content.getBytes);
    val sign = Daos.enc.bytesToHexStr(Daos.enc.sign(
      Daos.enc.hexStrToBytes(VCtrl.network().root().pri_key),
      hash));
    (Daos.enc.bytesToHexStr(hash), sign)
  }
  def bigIntAnd(x: BigInteger, y: BigInteger): BigInteger = {
    var t = BigInteger.ZERO;
    var testx = x;
    var index = 0;
    while (testx.bitCount() > 0 && index < 1024000) {
      if (testx.testBit(index) && y.testBit(index)) {
        t = t.setBit(index);
      }
      testx = testx.clearBit(index);
      index = index + 1
    }
    return t;
  }
  def reasonableRandInt(beaconHexSeed: String, netBits: BigInteger, blockMakerCount: Int, notaryCount: Int): (BigInteger, BigInteger) = { //4 roles.
    val subleft = beaconHexSeed.substring(0, beaconHexSeed.length() / 2);
    val subright = beaconHexSeed.substring(beaconHexSeed.length() / 2 + 1);
    val leftbits = new BigInteger(subleft + subleft.reverse, 16); //.
    val blockbits = bigIntAnd(netBits, leftbits)
    val rightbits = new BigInteger(subright + subright.reverse, 16);

    val votebits = bigIntAnd(netBits, rightbits); //andNot(blockbits)
//    log.error("leftbits=" + leftbits + " rightbits=" + rightbits + " beaconHexSeed=" + beaconHexSeed);
//    log.error("blockbits=" + blockbits.bitCount() + " votebits=" + votebits.bitCount + " notaryCount=" + notaryCount + " blockMakerCount=" + blockMakerCount + " beaconHexSeed=" + beaconHexSeed);

    if (blockbits.bitCount() >= blockMakerCount && votebits.bitCount >= notaryCount) {
      //cannot product block maker
      return (blockbits, votebits);
    } else {
      val deeprand = Daos.enc.bytesToHexStr(Daos.enc.sha256(subleft.getBytes)) + Daos.enc.bytesToHexStr(Daos.enc.sha256(subright.getBytes))
      reasonableRandInt(deeprand, netBits, blockMakerCount, notaryCount);
    }
  }

  def chooseGroups(beaconHexSeed: String, netBits: BigInteger, curIdx: Int): (VNodeState, BigInteger, BigInteger) = {
    val netBitsCount = netBits.bitCount();
    val blockMakerCount: Int = Math.max(1, netBitsCount / 2);
   // val notaryCount: Int = Math.max(1, (netBits.bitCount() - blockMakerCount) / 3);
    val notaryCount: Int = Math.max(1, netBitsCount / 3);
    if (netBits.bitCount() < 2) {
//      log.error("netBits=" + netBits.bitCount() + " notaryCount=" + notaryCount + " blockMakerCount=" + blockMakerCount);
      (VNodeState.VN_DUTY_BLOCKMAKERS, netBits, netBits)
    } else {
        log.error("start originNetBits=" + netBits.bitCount() + "netBits=" + netBitsCount + " notaryCount=" + notaryCount + " blockMakerCount=" + blockMakerCount)
//      log.error("originNetBits=" + netBits.bitCount() + "netBits=" + netBitsCount + " notaryCount=" + notaryCount + " blockMakerCount=" + blockMakerCount);
      val (blockbits, votebits) = reasonableRandInt(beaconHexSeed, netBits, blockMakerCount, notaryCount);
        log.error("end blockbits=" + blockbits + " votebits=" + votebits)
      // TODO 如果金额不足，不能成为BLOCKMAKER
      if (Daos.accountHandler.getTokenBalance(Daos.accountHandler.getAccountOrCreate(Daos.chainHelper.getChainConfig.coinbase_account_address_bytestring), VConfig.AUTH_TOKEN).compareTo(VConfig.AUTH_TOKEN_MIN) < 0) {
        log.error("balance not enough");
        (VNodeState.VN_DUTY_SYNC, blockbits, votebits)
      }
      if (blockbits.testBit(curIdx)) {
        (VNodeState.VN_DUTY_BLOCKMAKERS, blockbits, votebits)
      } else if (votebits.testBit(curIdx)) {
        (VNodeState.VN_DUTY_NOTARY, blockbits, votebits)
      } else {
        (VNodeState.VN_DUTY_SYNC, blockbits, votebits)
      }
    }
  }
  def getRandMakeBlockSleep(beaconHash: String, blockbits: BigInteger, curIdx: Int): Long = {
    var testBits = blockbits;
    var indexInBits = 0;
    var testcc = 0;
    while (testcc < 1024000 && testBits.bitCount() > 0) {
      if (blockbits.testBit(testcc)) {
        testBits = testBits.clearBit(testcc);
        if (curIdx == testcc) {
          testBits = BigInteger.ZERO;
        } else {
          indexInBits = indexInBits + 1;
        }
      }
      testcc = testcc + 1;
    }
    val ranInt = new BigInteger(beaconHash, 16).abs(); //.multiply(BigInteger.valueOf(curIdx));
    val stepRange = ranInt.mod(BigInteger.valueOf(blockbits.bitCount())).intValue();
    return ((indexInBits + stepRange) % (blockbits.bitCount())) * VConfig.BLOCK_MAKE_TIMEOUT_SEC*1000 + VConfig.BLK_EPOCH_MS
  }
}