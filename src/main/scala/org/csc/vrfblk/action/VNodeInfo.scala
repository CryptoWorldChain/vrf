package org.csc.vrfblk.action

import org.apache.felix.ipojo.annotations.Instantiate
import org.apache.felix.ipojo.annotations.Provides
import onight.tfw.ntrans.api.ActorService
import onight.tfw.proxy.IActor
import onight.tfw.otransio.api.session.CMDService
import onight.osgi.annotation.NActorProvider
import org.csc.p22p.utils.LogHelper
import onight.oapi.scala.commons.PBUtils
import onight.oapi.scala.commons.LService
import org.csc.p22p.action.PMNodeHelper
import onight.tfw.otransio.api.beans.FramePacket
import onight.tfw.async.CompleteHandler
import org.csc.bcapi.utils.PacketIMHelper._
import onight.tfw.otransio.api.PacketHelper
import org.csc.bcapi.exception.FBSException
import scala.collection.JavaConversions._
import org.csc.vrfblk.PSMVRFNet
import org.csc.ckrand.pbgens.Ckrand.PSNodeInfo
import org.csc.ckrand.pbgens.Ckrand.PRetNodeInfo
import org.csc.vrfblk.tasks.VCtrl
import org.csc.ckrand.pbgens.Ckrand.PCommand
import org.csc.ckrand.pbgens.Ckrand.GossipMiner
import org.csc.p22p.node.PNode
import org.csc.vrfblk.tasks.BeaconGossip
import org.apache.commons.lang3.StringUtils
import org.csc.vrfblk.utils.VConfig
import org.csc.vrfblk.tasks.NodeStateSwitcher
import org.csc.vrfblk.tasks.Initialize

@NActorProvider
@Instantiate
@Provides(specifications = Array(classOf[ActorService], classOf[IActor], classOf[CMDService]))
class VNodeInfo extends PSMVRFNet[PSNodeInfo] {
  override def service = VNodeInfoService
}

//
// http://localhost:8000/fbs/xdn/pbget.do?bd=
object VNodeInfoService extends LogHelper with PBUtils with LService[PSNodeInfo] with PMNodeHelper {
  override def onPBPacket(pack: FramePacket, pbo: PSNodeInfo, handler: CompleteHandler) = {

    var ret = PRetNodeInfo.newBuilder();
    val network = networkByID("vrf")
    if (network == null) {
      ret.setRetCode(-1).setRetMessage("unknow network:")
      handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
    } else {
      try {
        MDCSetBCUID(network);
        VCtrl.coMinerByUID.map(m => {
          ret.addMurs(GossipMiner.newBuilder().setBcuid(m._2.getBcuid).setCurBlock(m._2.getCurBlock))
        })
        ret.setVn(VCtrl.curVN())
        MDCSetMessageID(pbo.getMessageId);
        if (StringUtils.isBlank(pack.getFrom())) {
        
        } else {
          log.debug("getNodeInfo::" + pack.getFrom() + ",blockheight=" + pbo.getVn.getCurBlock + ",remotestate=" + pbo.getVn.getState
            + ",curheight=" + VCtrl.curVN().getCurBlock + ",curstate=" + VCtrl.curVN().getState + ",DN=" + network.directNodes.size + ",MN=" + VCtrl.coMinerByUID.size)
          if (StringUtils.equals(pack.getFrom(), network.root.bcuid) || StringUtils.equals(pbo.getMessageId, BeaconGossip.currentBR.messageId)) {
            if (network.nodeByBcuid(pack.getFrom()) != network.noneNode) {
              VCtrl.coMinerByUID.put(pbo.getVn.getBcuid, pbo.getVn);
              BeaconGossip.offerMessage(pbo);
            }
          } else {
            network.nodeByBcuid(pack.getFrom()) match {
              case network.noneNode =>
              case n: PNode =>
                if (pbo.getVn.getCurBlock >= VCtrl.curVN().getCurBlock - VConfig.BLOCK_DISTANCE_COMINE) {
                  log.debug("add cominer:" + pbo.getVn.getBcuid + ",blockheight=" + pbo.getVn.getCurBlock + ",cur=" + VCtrl.curVN().getCurBlock);
                  VCtrl.coMinerByUID.put(pbo.getVn.getBcuid, pbo.getVn);
                }
//                if (!VCtrl.isReady()) {
//                  NodeStateSwither.offerMessage(new Initialize());
//                }
                val psret = PSNodeInfo.newBuilder().setMessageId(pbo.getMessageId).setVn(VCtrl.curVN());
                network.postMessage("INFVRF", Left(psret.build()), pbo.getMessageId, n._bcuid);
              case _ =>
            }
          }
        }
      } catch {
        case e: FBSException => {
          ret.clear()
          ret.setRetCode(-2).setRetMessage(e.getMessage)
        }
        case t: Throwable => {
          log.error("error:", t);
          ret.clear()
          ret.setRetCode(-3).setRetMessage(t.getMessage)
        }
      } finally {
        handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
      }
    }
  }
  //  override def getCmds(): Array[String] = Array(PWCommand.LST.name())
  override def cmd: String = PCommand.INF.name();
}
