package org.csc.vrfblk.tasks

import java.math.BigInteger
import java.util.List

import onight.tfw.otransio.api.PacketHelper
import org.csc.bcapi.crypto.BitMap
import org.csc.ckrand.pbgens.Ckrand
import org.csc.ckrand.pbgens.Ckrand.{BlockWitnessInfo, PSNodeInfo, VNodeState}
import org.csc.p22p.action.PMNodeHelper
import org.csc.p22p.utils.LogHelper
import org.csc.vrfblk.Daos
import org.csc.vrfblk.msgproc.MPCreateBlock
import org.csc.vrfblk.utils.{RandFunction, VConfig}
import org.fc.zippo.dispatcher.SingletonWorkShop

import scala.collection.JavaConverters._

trait StateMessage {

}

case class BeaconConverge(height: Int, beaconSign: String, beaconHash: String, randseed: String) extends StateMessage;

//状态转化器
case class StateChange(newsign: String, newhash: String, prevhash: String) extends StateMessage;

case class Initialize() extends StateMessage;

object NodeStateSwitcher extends SingletonWorkShop[StateMessage] with PMNodeHelper with LogHelper with BitMap {
  var running: Boolean = true;

  def isRunning(): Boolean = {
    return running;
  }

  val NotaryBlockFP = PacketHelper.genPack("NOTARYBLOCK", "__VRF", "", true, 9);

  var notaryCheckHash: String = null;

  def notifyStateChange() {
    val hash = VCtrl.curVN().getBeaconHash;
    val sign = VCtrl.curVN().getBeaconSign;
    log.info(s"stateChange,BEACON=${hash},SIGN=${sign}")
    var netBits = BigInteger.ZERO;
    try {
      if (VCtrl.curVN().getVrfRandseeds != null && VCtrl.curVN().getVrfRandseeds.length() > 0) {
        netBits = mapToBigInt(VCtrl.curVN().getVrfRandseeds).bigInteger;
      }
      netBits = RandFunction.bigIntAnd(netBits, VCtrl.network().bitenc.bits.bigInteger);
    } catch {
      case t: Throwable =>
        log.debug("set netbits error:" + t.getMessage, t);
    }
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

      netBits = BigInteger.ZERO; //(VCtrl.network().node_strBits).bigInteger;
      VCtrl.coMinerByUID.map(f => {
        netBits = netBits.setBit(f._2.getBitIdx);
      })
    }
    val (state, blockbits, notarybits) = RandFunction.chooseGroups(hash, netBits, VCtrl.curVN().getBitIdx)
    log.debug("get new state == " + state + ",blockbits=" + blockbits.toString(2) + ",notarybits=" + notarybits.toString(2)
      + ",hash=" + hash + ",curblk=" + VCtrl.curVN().getCurBlock);
    state match {
      case VNodeState.VN_DUTY_BLOCKMAKERS =>
        VCtrl.curVN().setState(state)
        val myWitness = VCtrl.coMinerByUID.filter {
          case (bcuid: String, node: Ckrand.VNode) => {
            val state = RandFunction.chooseGroups(hash, netBits, node.getBitIdx) _1;
            log.debug(s"node:${bcuid} state:${state}")
            VNodeState.VN_DUTY_NOTARY.equals(state)
          }
        }.map {
          case (bcuid: String, node: Ckrand.VNode) => node
        }.toList

        val blockWitness: BlockWitnessInfo.Builder = BlockWitnessInfo.newBuilder()
          .setBeaconHash(hash)
          .setBlockheight(VCtrl.curVN().getCurBlock)
          .setNetbitx(netBits.toString(16))
          .addAllWitness(myWitness.asJava)

        val blkInfo = new MPCreateBlock(netBits, blockbits, notarybits, hash, sign, blockWitness.build);
        BlockProcessor.offerMessage(blkInfo);
      case VNodeState.VN_DUTY_NOTARY | VNodeState.VN_DUTY_SYNC =>
        var timeOutMS = blockbits.bitCount() * VConfig.BLOCK_MAKE_TIMEOUT_SEC * 1000;
        notaryCheckHash = VCtrl.curVN().getBeaconHash;
        log.debug("exec notary block background running:" + notaryCheckHash + ",sleep still:" + timeOutMS);

        Daos.ddc.executeNow(NotaryBlockFP, new Runnable() {
          def run() {
            while (timeOutMS > 0 && VCtrl.curVN().getBeaconHash.equals(notaryCheckHash)) {
              Thread.sleep(Math.min(100, timeOutMS));
              timeOutMS = timeOutMS - 100;
            }
            if (VCtrl.curVN().getBeaconHash.equals(notaryCheckHash)) {
              //decide to make block
              log.debug(s"reconsider oldBEACON:${notaryCheckHash}newBEACON:${VCtrl.curVN().getBeaconHash},sleep still:" + timeOutMS );
              BeaconGossip.gossipBlocks();
            } else {
              log.debug(s"reconsider oldBEACON:${notaryCheckHash}newBEACON:${VCtrl.curVN().getBeaconHash},sleep still:" + timeOutMS );
            }
          }
        })
        VCtrl.curVN().setState(state)
      case _ =>
        VCtrl.curVN().setState(state)
        log.debug("unknow state:" + state);
    }

  }

  def runBatch(items: List[StateMessage]): Unit = {
    MDCSetBCUID(VCtrl.network())
    items.asScala.map(m => {
      m match {
        case BeaconConverge(height, sign, hash, seed) => {

          log.info("set new beacon seed:height=" + height + ",sign=" + sign + ",seed=" + seed + ",hash=" + hash); //String pubKey, String hexHash, String sign hex
          //          if (height >= VCtrl.curVN().getCurBlock) {
          VCtrl.curVN().setBeaconSign(sign).setBeaconHash(hash).setVrfRandseeds(seed).setCurBlockHash(hash)
            .setCurBlock(height);
          notifyStateChange();
          //          } else {
          //            log.debug("do nothing network converge height[" + height + "] less than local[" + VCtrl.curVN().getCurBlock + "]");
          //          }
        }
        case StateChange(newsign, newhash, prevhash) => {
          log.info("get new statechange,hash={},prevhash={},localbeanhash={}", newhash, prevhash, VCtrl.curVN().getBeaconHash);
          if (VCtrl.curVN().getBeaconHash.equals(prevhash)) {
            //@TODO !should verify...
            VCtrl.curVN().setBeaconSign(newsign).setBeaconHash(newhash);
            //.setCurBlockHash(newhash);
            notifyStateChange();
          }
        }
        case init: Initialize => {
          if (VCtrl.curVN().getState == VNodeState.VN_INIT) {
            val block = Daos.blkHelper.getBlock(VCtrl.curVN().getCurBlockHash);
            val nodeBit = VCtrl.curVN().getCurBlock == 0
            val (hash, sign) = RandFunction.genRandHash(
              VCtrl.curVN().getCurBlockHash,
              VCtrl.curVN().getPrevBlockHash, block.getMiner.getBit);
            VCtrl.curVN().setBeaconHash(hash).setBeaconSign(sign).setCurBlockHash(hash);
            BeaconGossip.offerMessage(PSNodeInfo.newBuilder().setVn(VCtrl.curVN()).setIsQuery(true));
          }
        }
      }
    })
  }

}