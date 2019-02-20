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
import org.csc.ckrand.pbgens.Ckrand.PSSyncBlocks
import org.csc.ckrand.pbgens.Ckrand.PRetSyncBlocks
import org.csc.vrfblk.tasks.VCtrl
import org.csc.vrfblk.tasks.VCtrl
import org.csc.ckrand.pbgens.Ckrand.PCommand
import org.csc.ckrand.pbgens.Ckrand.PSNodeGraceShutDown
import org.csc.p22p.node.PNode

@NActorProvider
@Instantiate
@Provides(specifications = Array(classOf[ActorService], classOf[IActor], classOf[CMDService]))
class PSNodeSOSModule extends PSMVRFNet[PSNodeGraceShutDown] {
  override def service = PSNodeSOS
}

//
// http://localhost:8000/fbs/xdn/pbget.do?bd=
object PSNodeSOS extends LogHelper with PBUtils with LService[PSNodeGraceShutDown] with PMNodeHelper {
  override def onPBPacket(pack: FramePacket, pbo: PSNodeGraceShutDown, handler: CompleteHandler) = {
    //    log.debug("BlockSyncService:" + pack.getFrom())
    var ret = PSNodeGraceShutDown.newBuilder();
    if (!VCtrl.isReady()) {
      handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
    } else {
      try {
        log.debug("node shutdown:" + pack.getFrom() + ",reason=" + pbo.getReason)
        val network = VCtrl.network();
        network.nodeByBcuid(pack.getFrom()) match {
              case network.noneNode =>
              case n: PNode =>
                network.removeDNode(n)
                network.removePendingNode(n);
        }
      } catch {
        case e: FBSException => {
          ret.clear()
        }
        case t: Throwable => {
          log.error("error:", t);
        }
      } finally {
        handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
      }
    }
  }
  //  override def getCmds(): Array[String] = Array(PWCommand.LST.name())
  override def cmd: String = PCommand.SOS.name();
}
