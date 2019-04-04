package org.csc.vrfblk.action

import onight.oapi.scala.commons.{LService, PBUtils}
import onight.osgi.annotation.NActorProvider
import onight.tfw.async.CompleteHandler
import onight.tfw.ntrans.api.ActorService
import onight.tfw.otransio.api.PacketHelper
import onight.tfw.otransio.api.beans.FramePacket
import onight.tfw.otransio.api.session.CMDService
import onight.tfw.proxy.IActor
import org.apache.felix.ipojo.annotations.{Instantiate, Provides}
import org.csc.ckrand.pbgens.Ckrand.{PCommand, PRetHealthCheck, VNode}
import org.csc.p22p.action.PMNodeHelper
import org.csc.p22p.utils.LogHelper
import org.csc.vrfblk.PSMVRFNet
import org.csc.vrfblk.tasks.VCtrl

import scala.collection.JavaConverters._


@NActorProvider
@Instantiate
@Provides(specifications = Array(classOf[ActorService], classOf[IActor], classOf[CMDService]))
class PSHealthCheck extends PSMVRFNet[VNode] {
  override def service: LService[VNode] = PSHealthCheckService
}

object PSHealthCheckService extends LogHelper with PBUtils with LService[VNode] with PMNodeHelper {

  override def cmd: String = PCommand.VNI.name()

  override def onPBPacket(pack: FramePacket, pbo: VNode, handler: CompleteHandler): Unit = {
    val ret = PRetHealthCheck.newBuilder()
    if (VCtrl.isReady()) {
      val current = VCtrl.curVN()
      val miners = VCtrl.coMinerByUID.values.toList

      ret.setRetCode(1)
        .setRetMessage("SUCCESS")
        .setCnNode(current)
        .setDirectNode(VCtrl.network.directNodes.map(b => b.bcuid).mkString("[", ",", "]"))
        .setDirectNode(VCtrl.network.pendingNodes.map(b => b.bcuid).mkString("[", ",", "]"))
        .addAllCoMiners(miners.asJava)
    } else {
      ret.setRetCode(-1).setRetMessage("VNode Not Ready")
    }
    handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
  }
}
