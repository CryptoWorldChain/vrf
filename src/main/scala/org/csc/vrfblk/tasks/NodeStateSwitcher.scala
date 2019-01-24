package org.csc.vrfblk.tasks

import java.util.List

import org.csc.ckrand.pbgens.Ckrand.PSNodeInfo
import org.csc.p22p.action.PMNodeHelper
import org.csc.p22p.utils.LogHelper
import org.fc.zippo.dispatcher.SingletonWorkShop
import scala.collection.JavaConverters._
import org.apache.commons.lang3.StringUtils
import org.csc.vrfblk.Daos
import scala.util.Random
import org.csc.vrfblk.utils.RandFunction
import org.csc.ckrand.pbgens.Ckrand.VNodeState
import org.csc.vrfblk.msgproc.MPCreateBlock
import org.csc.bcapi.crypto.BitMap
import java.math.BigInteger

trait StateMessage {

}
case class BeaconConverge(beaconSign: String, beaconHash: String) extends StateMessage;
//状态转化器
case class StateChange(newsign: String, newhash: String, prevhash: String) extends StateMessage;

case class Initialize() extends StateMessage;

object NodeStateSwither extends SingletonWorkShop[StateMessage] with PMNodeHelper with LogHelper with BitMap {
  var running: Boolean = true;

  def isRunning(): Boolean = {
    return running;
  }

  def notifyStateChange() {
    val hash = VCtrl.curVN().getBeaconHash;
    val sign = VCtrl.curVN().getBeaconSign;
    var netBits = BigInteger.ZERO;
    try {
      if (VCtrl.curVN().getVrfRandseeds != null&&VCtrl.curVN().getVrfRandseeds.size()>0) {
        netBits = new BigInteger(VCtrl.curVN().getVrfRandseeds.toByteArray());
      }
    } catch {
      case t: Throwable =>
        log.debug("set netbits error:"+t.getMessage);
    }
    if (netBits.bitCount() <= 0) {
      netBits = BigInteger.ZERO; //(VCtrl.network().node_strBits).bigInteger;
      VCtrl.coMinerByUID.map(f => {
        netBits = netBits.setBit(f._2.getBitIdx);
      })
    }
    val (state, blockbits, notarybits) = RandFunction.chooseGroups(hash, netBits, VCtrl.curVN().getBitIdx)
    log.debug("get new state == " + state + ",blockbits=" + blockbits.toString(2) + ",notarybits=" + notarybits.toString(2));
    state match {
      case VNodeState.VN_DUTY_BLOCKMAKERS =>
        VCtrl.curVN().setState(state)
        val blkInfo = new MPCreateBlock(netBits, blockbits, notarybits, hash, sign);
        BlockProcessor.offerMessage(blkInfo);
      case VNodeState.VN_DUTY_NOTARY =>
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
        case BeaconConverge(sign, hash) => {
          log.info("set new beacon seed:" + sign); //String pubKey, String hexHash, String sign hex
          VCtrl.curVN().setBeaconSign(sign).setBeaconHash(hash);
          notifyStateChange();
        }
        case StateChange(newsign, newhash, prevhash) => {
          log.info("get new statechange:sig={},hash={},prevhash={},localbeanhash={}", newsign, newhash, prevhash, VCtrl.curVN().getBeaconHash);
          if (VCtrl.curVN().getBeaconHash.equals(prevhash)) {
            //@TODO !should verify...
            VCtrl.curVN().setBeaconSign(newsign).setBeaconHash(newhash);
            notifyStateChange();
          }
        }
        case init: Initialize => {
          val (hash, sign) = RandFunction.genRandHash(
            VCtrl.curVN().getCurBlockHash,
            VCtrl.curVN().getPrevBlockHash, VCtrl.network().node_strBits);
          VCtrl.curVN().setBeaconHash(hash).setBeaconSign(sign);
          BeaconGossip.offerMessage(PSNodeInfo.newBuilder().setVn(VCtrl.curVN()));
        }
      }
    })
  }

}