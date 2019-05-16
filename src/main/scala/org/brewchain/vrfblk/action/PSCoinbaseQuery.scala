
package org.brewchain.vrfblk.action

import org.apache.felix.ipojo.annotations.Instantiate
import org.apache.felix.ipojo.annotations.Provides
import org.brewchain.bcrand.model.Bcrand.PCommand
import org.brewchain.bcrand.model.Bcrand.PSCoinbase
import org.brewchain.p22p.action.PMNodeHelper
import org.brewchain.p22p.utils.LogHelper
import org.brewchain.vrfblk.PSMVRFNet
import org.brewchain.vrfblk.tasks.BlockProcessor
import org.brewchain.vrfblk.tasks.VCtrl

import onight.oapi.scala.commons.LService
import onight.oapi.scala.commons.PBUtils
import onight.osgi.annotation.NActorProvider
import onight.tfw.async.CompleteHandler
import onight.tfw.otransio.api.PacketHelper
import onight.tfw.otransio.api.beans.FramePacket
import onight.tfw.ntrans.api.ActorService
import onight.tfw.proxy.IActor
import onight.tfw.otransio.api.session.CMDService
import org.brewchain.vrfblk.msgproc.ApplyBlock

@NActorProvider
@Instantiate
@Provides(specifications = Array(classOf[ActorService], classOf[IActor], classOf[CMDService]))
class PDCoinbaseQ extends PSMVRFNet[PSCoinbase] {
  override def service = PDCoinbaseQuery
}

//
// http://localhost:8000/fbs/xdn/pbget.do?bd=
object PDCoinbaseQuery extends LogHelper with PBUtils with LService[PSCoinbase] with PMNodeHelper {
  override def onPBPacket(pack: FramePacket, pbo: PSCoinbase, handler: CompleteHandler) = {
    //    log.debug("Mine Block From::" + pack.getFrom())
    if (!VCtrl.isReady()) {
      log.debug("VCtrl not ready");
      handler.onFinished(PacketHelper.toPBReturn(pack, pbo))
    } else {
//      BlockProcessor.offerMessage(new ApplyBlock(pbo));
      handler.onFinished(PacketHelper.toPBReturn(pack, pbo))
    }
  }

  //  override def getCmds(): Array[String] = Array(PWCommand.LST.name())
  override def cmd: String = PCommand.CBR.name();
}