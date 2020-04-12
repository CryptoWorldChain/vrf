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
import scala.collection.mutable.Buffer

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
  //  def reasonableRandInt(beaconHexSeed: String, netBits: BigInteger, blockMakerCount: Int, notaryCount: Int, trycc: Int = 0): (BigInteger, BigInteger) = { //4 roles.
  //
  //
  //    val subleft = beaconHexSeed.substring(0, beaconHexSeed.length() / 2);
  //    val subright = beaconHexSeed.substring(beaconHexSeed.length() / 2 + 1);
  //    val leftbits = new BigInteger(subleft, 16); //.
  //    val blockbits = bigIntAnd(netBits, leftbits)
  //    val rightbits = new BigInteger(subright, 16);
  //    val votebits = bigIntAnd(netBits, rightbits).andNot(blockbits)
  //    if (blockbits.bitCount() >= blockMakerCount && votebits.bitCount >= notaryCount) {
  //      return (blockbits, votebits);
  //    } else if (trycc >= 1000) {
  //      log.error("max try to run random bits. :trycc=" + trycc);
  //      return (netBits, netBits);
  //    } else {
  //
  //      val deeprand = Daos.enc.bytesToHexStr(Daos.enc.sha256(subleft.getBytes)) + Daos.enc.bytesToHexStr(Daos.enc.sha256(subright.getBytes))
  //      reasonableRandInt(deeprand, netBits, blockMakerCount, notaryCount, trycc + 1);
  //    }
  //  }

  def chooseGroups(ranInt: Int, netBits: BigInteger, curBitIdx: Int): (VNodeState, BigInteger, BigInteger, Int, Int) = {
    val nodeCounts = netBits.bitCount();
    _chooseGroups(ranInt, netBits, curBitIdx, nodeCounts);
  }
  
  def _chooseGroups(ranInt: Int, netBits: BigInteger, curBitIdx: Int,nodeCounts:Int): (VNodeState, BigInteger, BigInteger, Int, Int) = {
    val blockMakerCount: Int = Math.min(VConfig.MAX_BLOCK_MAKER,Math.max(1, nodeCounts / 2));
    // val notaryCount: Int = Math.max(1, (netBits.bitCount() - blockMakerCount) / 3);
    val notaryCount: Int = Math.min(VConfig.MAX_BLOCK_NOTARY,Math.max(1, nodeCounts / 3));
    var firstBlockMakerBitIndex = -1;
    if (nodeCounts <= 1) {
      if (netBits.testBit(curBitIdx)) {
        log.debug("chooseGroups.blockmaker.netBits=" + netBits.bitCount() + " notaryCount=" + nodeCounts + " blockMakerCount=" + blockMakerCount);
        (VNodeState.VN_DUTY_BLOCKMAKERS, netBits, BigInteger.ZERO, Daos.mcore.getBlockEpochMS(), ranInt % nodeCounts)
      } else {
        log.debug("chooseGroups.sync.netBits=" + netBits.bitCount() + " notaryCount=" + notaryCount + " blockMakerCount=" + blockMakerCount);
        (VNodeState.VN_DUTY_NOTARY, netBits, BigInteger.ZERO, Daos.mcore.getBlockEpochMS(), ranInt % nodeCounts)
      }
    } else {
      val randInx = Math.abs(ranInt.abs) % nodeCounts;
      var blockbits = BigInteger.ZERO;
      var votebits = BigInteger.ZERO;

      val netIndexs = Buffer.empty[Int];

      var testBits = netBits;
      var indexInBits = 0;
      var testcc = 0;
      var foundCount = 0;
      while (testcc < 1024000 && testBits.bitCount() > 0) {
        if (netBits.testBit(testcc)) {
          netIndexs.append(testcc);
          testBits = testBits.clearBit(testcc);
          if (curBitIdx == testcc) {
            indexInBits = foundCount;
          }
          if (firstBlockMakerBitIndex < 0 && ((randInx + foundCount) % nodeCounts == 0)) {
            firstBlockMakerBitIndex = testcc;
          }
          if ((randInx + foundCount) % nodeCounts < blockMakerCount) {
            blockbits = blockbits.setBit(testcc);
          } else if ((randInx + foundCount) % nodeCounts < blockMakerCount + notaryCount) {
            votebits = votebits.setBit(testcc);
          }
          
          foundCount = foundCount + 1;
        }
        testcc = testcc + 1;
      }
      //      val stepRange = ranInt.mod(BigInteger.valueOf(blockbits.bitCount())).intValue().abs;
      

      if ((randInx + indexInBits) % nodeCounts < blockMakerCount) {
        val sleepms = ((indexInBits + randInx) % nodeCounts) * Daos.mcore.getBlockMineTimeoutMs() + Daos.mcore.getBlockEpochMS()
        log.info(s"chooseGroups=BLOCKMAKER,sleepms=${sleepms}:testcc=${testcc},indexInBits=${indexInBits},curIdx=${indexInBits}/${(indexInBits + randInx) % nodeCounts} ,blockbits.bitCount=${blockbits.bitCount()},ranInt=${ranInt}/${randInx}");
        (VNodeState.VN_DUTY_BLOCKMAKERS, blockbits, votebits, sleepms, firstBlockMakerBitIndex)
      } else if ((randInx + indexInBits) % nodeCounts < blockMakerCount + notaryCount) {
        val sleepms = ((indexInBits + randInx) % (blockMakerCount + notaryCount)) * Daos.mcore.getBlockMineTimeoutMs() + Daos.mcore.getBlockEpochMS()
        log.info(s"chooseGroups=NOTRAY,sleepms=${sleepms}:testcc=${testcc},indexInBits=${indexInBits},curIdx=${indexInBits}/${(indexInBits + randInx) % nodeCounts} ,blockbits.bitCount=${blockbits.bitCount()},ranInt=${ranInt}/${randInx}");
        (VNodeState.VN_DUTY_NOTARY, blockbits, votebits, sleepms, firstBlockMakerBitIndex)
      } else {
        val sleepms = ((indexInBits + randInx) % (nodeCounts)) * Daos.mcore.getBlockMineTimeoutMs() * 1000 + Daos.mcore.getBlockEpochMS()
        log.info(s"chooseGroups=SYNC,sleepms=${sleepms}:testcc=${testcc},indexInBits=${indexInBits},curIdx=${indexInBits}/${(indexInBits + randInx) % nodeCounts} ,blockbits.bitCount=${blockbits.bitCount()},ranInt=${ranInt}/${randInx}");
        (VNodeState.VN_DUTY_SYNC, blockbits, votebits, sleepms, firstBlockMakerBitIndex)
      }
    }
  }
}