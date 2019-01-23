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
import org.csc.vrfblk.msgproc.CreateNewBlock

trait StateMessage {

}
case class BeaconConverge(beaconSign: String, beaconHash: String) extends StateMessage;
//状态转化器
case class StateChange(newsign: String, newhash: String, prevhash: String) extends StateMessage;

case class Initialize() extends StateMessage;

object NodeStateSwither extends SingletonWorkShop[StateMessage] with PMNodeHelper with LogHelper {
  var running: Boolean = true;

  def isRunning(): Boolean = {
    return running;
  }

  def notifyStateChange() {
    val hash = VCtrl.curVN().getBeaconHash;
    val sign = VCtrl.curVN().getBeaconSign;
    val (state, blockbits, notarybits) = RandFunction.chooseGroups(hash, VCtrl.network().node_strBits, VCtrl.curVN().getBitIdx)
    state match {
      case VNodeState.VN_DUTY_BLOCKMAKERS =>
        val blkInfo = new CreateNewBlock(blockbits, notarybits, hash, sign);
        BlockProcessor.offerMessage(blkInfo);
      case VNodeState.VN_DUTY_NOTARY =>

      case _                         =>
    }

  }
  def runBatch(items: List[StateMessage]): Unit = {
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