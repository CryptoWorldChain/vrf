package org.brewchain.vrfblk.tasks

import java.math.BigInteger
import java.util.List

import onight.tfw.otransio.api.PacketHelper
import org.brewchain.mcore.crypto.BitMap
import org.brewchain.bcrand.model.Bcrand
import org.brewchain.bcrand.model.Bcrand.{ BlockWitnessInfo, PSNodeInfo, VNodeState }
import org.brewchain.p22p.action.PMNodeHelper
import org.brewchain.p22p.utils.LogHelper
import org.brewchain.vrfblk.Daos
import org.brewchain.vrfblk.msgproc.MPCreateBlock
import org.brewchain.vrfblk.utils.{ RandFunction, VConfig }
import org.fc.zippo.dispatcher.SingletonWorkShop

import scala.collection.JavaConverters._
import java.util.ArrayList

trait StateMessage {

}

case class BeaconConverge(height: Int, beaconSign: String, beaconHash: String, randseed: String) extends StateMessage;

//状态转化器
case class StateChange(newsign: String, newhash: String, prevhash: String, netbits: String, height: Int) extends StateMessage;

case class Initialize() extends StateMessage;

object NodeStateSwitcher extends SingletonWorkShop[StateMessage] with PMNodeHelper with LogHelper with BitMap {
  var running: Boolean = true;

  def isRunning(): Boolean = {
    return running;
  }

  val NotaryBlockFP = PacketHelper.genPack("NOTARYBLOCK", "__VRF", "", true, 9);

  var notaryCheckHash: String = null;

  def notifyStateChange(hash: String, preHash: String, netbits1: BigInteger, height: Int) {
    var netBits = netbits1;
    val sign = VCtrl.curVN().getBeaconSign;
    if (netBits.bitCount() <= 0) {
      if (VCtrl.curVN().getVrfRandseeds != null && VCtrl.curVN().getVrfRandseeds.size > 0) {
        log.debug("netbits reset:seed=" + VCtrl.curVN().getVrfRandseeds + ",net=" + VCtrl.network().bitenc.strEnc + ",netb=" +
          VCtrl.network().bitenc.bits.bigInteger.bitCount() + "[" + VCtrl.network().bitenc.bits.bigInteger.toString(2) + "]"
          + ",b=" + mapToBigInt(VCtrl.curVN().getVrfRandseeds).bigInteger.bitCount()
          + "[" + mapToBigInt(VCtrl.curVN().getVrfRandseeds).bigInteger.toString(2) + "]");
      } else {
        log.debug("netbits reset:seed=" + VCtrl.curVN().getVrfRandseeds + ",net=" + VCtrl.network().bitenc.strEnc + ",netb=" +
          VCtrl.network().bitenc.bits.bigInteger.bitCount() + "[" + VCtrl.network().bitenc.bits.bigInteger.toString(2) + "]");
      }
      VCtrl.coMinerByUID.map(f => {
        if (f._2.getCurBlock >= (VCtrl.curVN().getCurBlock - VConfig.BLOCK_DISTANCE_NETBITS)) {
          netBits = netBits.setBit(f._2.getBitIdx);
        }
      })
    }
    val ranInt: Int = new BigInteger(hash, 16).intValue().abs;
    val (state, newblockbits, natarybits, sleepMs, firstBlockMakerBitIndex) = RandFunction.chooseGroups(ranInt, netBits, VCtrl.curVN().getBitIdx);
    VCtrl.curVN().setState(state);
    //    var (state, blockbits, notarybits) = RandFunction.chooseGroups(hash, netBits, VCtrl.curVN().getBitIdx)
    //    if (netbits1.bitCount() > 0 && VCtrl.network().bitenc.bits.bigInteger.and(blockbits).bitCount() <= 0) {
    //      log.info("network cannot find block maker ,try again");
    //      notifyStateChange(hash, preHash, BigInteger.ZERO, height);
    //    } else {
    if (newblockbits.testBit(VCtrl.curVN().getBitIdx)) {
      VCtrl.curVN().setState(VNodeState.VN_DUTY_BLOCKMAKERS)
      val myWitness = new ArrayList[Bcrand.VNode];
      VCtrl.coMinerByUID.map { pair =>
        if (natarybits.testBit(pair._2.getBitIdx)) {
          myWitness.add(pair._2)
        }
      }
      val blockWitness: BlockWitnessInfo.Builder = BlockWitnessInfo.newBuilder()
        .setBeaconHash(hash)
        .setBlockheight(height)
        .setNetbitx(netBits.toString(16))
        .addAllWitness(myWitness)

      val blkInfo = new MPCreateBlock(netBits, newblockbits, natarybits, hash, preHash, sign, blockWitness.build, height + 1,sleepMs);
      BlockProcessor.offerMessage(blkInfo);
    } else if (natarybits.testBit(VCtrl.curVN().getBitIdx)) {
      var timeOutMS = newblockbits.bitCount() * Daos.mcore.getBlockMineTimeoutMs();
      notaryCheckHash = VCtrl.curVN().getBeaconHash;

      Daos.ddc.executeNow(NotaryBlockFP, new Runnable() {
        def run() {
          while (timeOutMS > 0 && VCtrl.curVN().getBeaconHash.equals(notaryCheckHash)) {
            Thread.sleep(Math.min(100, timeOutMS));
            timeOutMS = timeOutMS - 100;
          }
          if (VCtrl.curVN().getBeaconHash.equals(notaryCheckHash)) {
            //decide to make block
            log.info("nortary block need gossip block");
            BeaconGossip.gossipBlocks();//重新发起投票!!
            
          }else{
            //新的块来了，要验证是否正确
          }
        }
      })
      VCtrl.curVN().setState(state)
    }
  }

  def runBatch(items: List[StateMessage]): Unit = {
    MDCSetBCUID(VCtrl.network())
    if (items != null) {
      items.asScala.map(m => {
        m match {
          case BeaconConverge(height, blockHash, hash, seed) => {
            // parentBlock.Hash, beaconHash, netBits
            log.error("blockHash=" + blockHash + " beaconHash=" + hash + " seed=" + seed);
            val (newhash, sign) = RandFunction.genRandHash(blockHash, hash, seed)
            NodeStateSwitcher.offerMessage(new StateChange(sign, newhash, hash, seed, height));
          }
          case StateChange(newsign, newhash, prevhash, netbits, height) => {
            //if (height==0 || (height > 0 && VCtrl.curVN().getBeaconHash.equals(prevhash))) {
            log.info("state change cur hash=" + VCtrl.curVN().getBeaconHash + " prevhash=" + prevhash + " newhash=" + newhash + " netbits=" + netbits
                +",height="+height);
            VCtrl.curVN().setBeaconSign(newsign).setBeaconHash(newhash).setVrfRandseeds(netbits).setPrevBlockHash(prevhash);
            // VCtrl.curVN().setBeaconSign(newsign).setBeaconHash(prevhash).setVrfRandseeds(netbits);
            notifyStateChange(newhash, prevhash, mapToBigInt(netbits).bigInteger, height);
            //} else {
            //  log.error("beacon hash not equal. chash=" + VCtrl.curVN().getBeaconHash + " hash=" + prevhash);
            //}
          }
          case init: Initialize => {
            if (VCtrl.curVN().getState == VNodeState.VN_INIT) {
              val block = Daos.chainHelper.getMaxConnectBlock;
              if (block != null) {
                if (VCtrl.curVN().getCurBlock > 0) {
                  val (hash, sign) = RandFunction.genRandHash(
                    VCtrl.curVN().getCurBlockHash,
                    VCtrl.curVN().getPrevBlockHash, block.getMiner.getBits);

                  log.error("init beacon hash=" + hash);
                  VCtrl.curVN().setBeaconHash(hash).setBeaconSign(sign).setCurBlockHash(hash);
                } else {

                }
              }
              BeaconGossip.offerMessage(PSNodeInfo.newBuilder().setVn(VCtrl.curVN()).setIsQuery(true));
            }
          }
        }
      })
    }
  }
}