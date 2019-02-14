
package org.csc.vrfblk.action

import org.apache.felix.ipojo.annotations.Instantiate
import org.apache.felix.ipojo.annotations.Provides
import org.csc.ckrand.pbgens.Ckrand.PCommand
import org.csc.ckrand.pbgens.Ckrand.PSCoinbase
import org.csc.p22p.action.PMNodeHelper
import org.csc.p22p.utils.LogHelper
import org.csc.vrfblk.PSMVRFNet
import org.csc.vrfblk.tasks.BlockProcessor
import org.csc.vrfblk.tasks.VCtrl

import onight.oapi.scala.commons.LService
import onight.oapi.scala.commons.PBUtils
import onight.osgi.annotation.NActorProvider
import onight.tfw.async.CompleteHandler
import onight.tfw.otransio.api.PacketHelper
import onight.tfw.otransio.api.beans.FramePacket
import onight.tfw.ntrans.api.ActorService
import onight.tfw.proxy.IActor
import onight.tfw.otransio.api.session.CMDService
import org.csc.vrfblk.msgproc.ApplyBlock
import org.csc.vrfblk.msgproc.NotaryBlock
import org.csc.vrfblk.tasks.Initialize
import org.csc.vrfblk.tasks.NodeStateSwitcher

@NActorProvider
@Instantiate
@Provides(specifications = Array(classOf[ActorService], classOf[IActor], classOf[CMDService]))
class PSCoinbaseW extends PSMVRFNet[PSCoinbase] {
  override def service = PSCoinbaseWitness
}

//
// http://localhost:8000/fbs/xdn/pbget.do?bd=
object PSCoinbaseWitness extends LogHelper with PBUtils with LService[PSCoinbase] with PMNodeHelper {
  override def onPBPacket(pack: FramePacket, pbo: PSCoinbase, handler: CompleteHandler) = {
    //    log.debug("Mine Block From::" + pack.getFrom())
    if (!VCtrl.isReady()) {
      log.debug("VCtrl not ready");
//     ! NodeStateSwitcher.offerMessage(new Initialize());
      handler.onFinished(PacketHelper.toPBReturn(pack, pbo))
    } else {
      BlockProcessor.offerMessage(new NotaryBlock(pbo));
      handler.onFinished(PacketHelper.toPBReturn(pack, pbo))
    }
  }

  //  override def getCmds(): Array[String] = Array(PWCommand.LST.name())
  override def cmd: String = PCommand.CBW.name();
}
