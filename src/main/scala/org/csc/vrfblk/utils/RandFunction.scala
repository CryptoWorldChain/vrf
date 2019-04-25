package org.csc.vrfblk.utils

import java.util.concurrent.TimeUnit

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
import org.csc.p22p.utils.LogHelper
import com.google.common.math.BigIntegerMath
import java.util.BitSet

object RandFunction extends LogHelper with BitMap {

  def genRandHash(curblockHash: String, prevRandHash: String, nodebits: String): (String, String) = {
    val content = Array(curblockHash, prevRandHash, nodebits).mkString(",");
    val hash = Daos.enc.sha256Encode(content.getBytes);
    val sign = Daos.enc.ecSignHex(
      VCtrl.network().root().pri_key,
      hash);
    //log.debug("curblockHash::" + curblockHash + " prevRandHash::"+prevRandHash + " nodebits::"+nodebits + " result::" + Daos.enc.hexEnc(hash));

    (Daos.enc.hexEnc(hash), sign)
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

    //    log.debug("reasonableRandInt,blockbits=" + leftbits.toString(2) + ",votebits=" + rightbits.toString(2) + ",netBits=" + netBits.toString(2));

    val votebits = bigIntAnd(netBits, rightbits); //andNot(blockbits)
    //    log.debug("reasonableRandInt::bb.count=" + blockbits.bitCount() + ",vb.count=" + votebits.bitCount() + ",net.length=" + netBits.bitLength());
    if (blockbits.bitCount() >= blockMakerCount && votebits.bitCount >= notaryCount) {
      //cannot product block maker
      return (blockbits, votebits);
    } else {
      val deeprand = Daos.enc.hexEnc(Daos.enc.sha256Encode(subleft.getBytes)) + Daos.enc.hexEnc(Daos.enc.sha256Encode(subright.getBytes))
      reasonableRandInt(deeprand, netBits, blockMakerCount, notaryCount);
    }
  }

  def chooseGroups(beaconHexSeed: String, netBits: BigInteger, curIdx: Int): (VNodeState, BigInteger, BigInteger) = {
    val blockMakerCount: Int = Math.max(1, netBits.bitCount() / 2);
    val notaryCount: Int = Math.max(1, netBits.bitCount() / 3);
    val (blockbits, votebits) = reasonableRandInt(beaconHexSeed, netBits, blockMakerCount, notaryCount);
    log.debug("chooseGroups,blockbits=" + blockbits.toString(2) + ",votebits=" +  votebits.toString(2) + ",curIdx=" + curIdx
        +",BH="+VCtrl.curVN().getBeaconHash+",B="+VCtrl.curVN().getCurBlock
       +",TC="+netBits.bitCount()+",MC="+blockbits.bitCount()+",NC="+ votebits.bitCount());
    if (blockbits.testBit(curIdx)) {
      (VNodeState.VN_DUTY_BLOCKMAKERS, blockbits, votebits)
    } else if (votebits.testBit(curIdx)) {
      (VNodeState.VN_DUTY_NOTARY, blockbits, votebits)
    } else {
      (VNodeState.VN_DUTY_SYNC, blockbits, votebits)
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
    log.debug("calc rand sleep,indexInBits=" + indexInBits + ",stepRange=" + stepRange + ",bitcount=" + blockbits.bitCount());
    return ((indexInBits + stepRange) % (blockbits.bitCount())) * VConfig.BLOCK_MAKE_TIMEOUT_SEC*1000 + VConfig.BLK_EPOCH_MS
  }
}