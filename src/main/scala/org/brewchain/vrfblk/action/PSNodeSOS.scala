package org.brewchain.vrfblk.action

import org.apache.felix.ipojo.annotations.Instantiate
import org.apache.felix.ipojo.annotations.Provides
import onight.tfw.ntrans.api.ActorService
import onight.tfw.proxy.IActor
import onight.tfw.otransio.api.session.CMDService
import onight.osgi.annotation.NActorProvider
import org.brewchain.p22p.utils.LogHelper
import onight.oapi.scala.commons.PBUtils
import onight.oapi.scala.commons.LService
import org.brewchain.p22p.action.PMNodeHelper
import onight.tfw.otransio.api.beans.FramePacket
import onight.tfw.async.CompleteHandler
import org.brewchain.p22p.utils.PacketIMHelper._

import onight.tfw.otransio.api.PacketHelper
import org.brewchain.p22p.exception.FBSException

import scala.collection.JavaConversions._
import org.brewchain.vrfblk.PSMVRFNet
import org.brewchain.bcrand.model.Bcrand.PSSyncBlocks
import org.brewchain.bcrand.model.Bcrand.PRetSyncBlocks
import org.brewchain.vrfblk.tasks.VCtrl
import org.brewchain.vrfblk.tasks.VCtrl
import org.brewchain.bcrand.model.Bcrand.PCommand
import org.brewchain.bcrand.model.Bcrand.PSNodeGraceShutDown
import org.brewchain.p22p.node.PNode

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
        log.info("node shutdown:" + pack.getFrom() + ",reason=" + pbo.getReason)
        val network = VCtrl.network();
        VCtrl.removeCoMiner(pack.getFrom())
        
        
//        network.nodeByBcuid(pack.getFrom()) match {
//              case network.noneNode =>
//              case n: PNode =>
//                network.removeDNode(n)
//                network.removePendingNode(n);
//        }
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
