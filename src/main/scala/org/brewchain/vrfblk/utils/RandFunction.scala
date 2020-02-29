package org.brewchain.vrfblk.utils

import java.util.concurrent.TimeUnit

import onight.oapi.scala.traits.OLog
import org.apache.commons.lang3.StringUtils
import org.brewchain.vrfblk.tasks.VCtrl
import java.math.BigInteger
import org.brewchain.mcore.crypto.BitMap
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
  def reasonableRandInt(beaconHexSeed: String, netBits: BigInteger, blockMakerCount: Int, notaryCount: Int, trycc: Int = 0): (BigInteger, BigInteger) = { //4 roles.
    val subleft = beaconHexSeed.substring(0, beaconHexSeed.length() / 2);
    val subright = beaconHexSeed.substring(beaconHexSeed.length() / 2 + 1);
    val leftbits = new BigInteger(subleft, 16); //.
    val blockbits = bigIntAnd(netBits, leftbits)
    val rightbits = new BigInteger(subright, 16);
    val votebits = bigIntAnd(netBits, rightbits).andNot(blockbits)
    if (blockbits.bitCount() >= blockMakerCount && votebits.bitCount >= notaryCount) {
      return (blockbits, votebits);
    } else if (trycc >= 1000) {
      log.error("max try to run random bits. :trycc=" + trycc);
      return (netBits, netBits);
    } else {

      val deeprand = Daos.enc.bytesToHexStr(Daos.enc.sha256(subleft.getBytes)) + Daos.enc.bytesToHexStr(Daos.enc.sha256(subright.getBytes))
      reasonableRandInt(deeprand, netBits, blockMakerCount, notaryCount, trycc + 1);
    }
  }

  def chooseGroups(beaconHexSeed: String, netBits: BigInteger, curIdx: Int): (VNodeState, BigInteger, BigInteger) = {
    val netBitsCount = netBits.bitCount();
    val blockMakerCount: Int = Math.max(1, netBitsCount / 2);
    // val notaryCount: Int = Math.max(1, (netBits.bitCount() - blockMakerCount) / 3);
    val notaryCount: Int = Math.max(1, netBitsCount / 3);

    if (netBits.bitCount() < 3 && netBits.bitCount() > 0) {
      if (netBits.testBit(curIdx)) {
        log.debug("chooseGroups.blockmaker.netBits=" + netBits.bitCount() + " notaryCount=" + notaryCount + " blockMakerCount=" + blockMakerCount);
        (VNodeState.VN_DUTY_BLOCKMAKERS, netBits, BigInteger.ZERO)
      } else {
        log.debug("chooseGroups.sync.netBits=" + netBits.bitCount() + " notaryCount=" + notaryCount + " blockMakerCount=" + blockMakerCount);
        (VNodeState.VN_DUTY_NOTARY, netBits, BigInteger.ZERO)
      }
    } else {
      val (blockbits, votebits) = reasonableRandInt(beaconHexSeed, netBits, blockMakerCount, notaryCount);
      log.debug("chooseGroups.start originNetBits=" + netBits.bitCount() + ",netBits=" + netBitsCount + " notaryCount=" + notaryCount + " blockMakerCount=" + blockMakerCount
          +",isblock="+blockbits.testBit(curIdx)+",isnotary="+votebits.testBit(curIdx))
          
      //      //      log.error("originNetBits=" + netBits.bitCount() + "netBits=" + netBitsCount + " notaryCount=" + notaryCount + " blockMakerCount=" + blockMakerCount);
      //      val (blockbits, votebits) = reasonableRandInt(beaconHexSeed, netBits, blockMakerCount, notaryCount);
//      log.debug("chooseGroups.end blockbits=" + blockbits + " votebits=" + votebits)
      
      // TODO 如果金额不足，不能成为BLOCKMAKER
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

    if (testBits.testBit(curIdx)) {
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
      val ranInt = new BigInteger(beaconHash, 16).abs();
      val stepRange = ranInt.mod(BigInteger.valueOf(blockbits.bitCount())).intValue().abs;
      val sleepms = ((indexInBits + stepRange) % (blockbits.bitCount())) * VConfig.BLOCK_MAKE_TIMEOUT_SEC * 1000 + VConfig.BLK_EPOCH_MS
      //       log.info(s"getRandMakeBlockSleep:sleepms=${sleepms}:testcc=${testcc},indexInBits=${indexInBits},curIdx=${curIdx},blockbits.bitCount=${blockbits.bitCount()},stepRange=${stepRange},beaconHash=${beaconHash}");
      sleepms;
    } else {
      VConfig.BLOCK_MAKE_TIMEOUT_SEC * 1000 + VConfig.BLK_EPOCH_MS
    }

  }
}