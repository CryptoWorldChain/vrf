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
import org.csc.ckrand.pbgens.Ckrand.PCommand

@NActorProvider
@Instantiate
@Provides(specifications = Array(classOf[ActorService], classOf[IActor], classOf[CMDService]))
class PSBlockSync extends PSMVRFNet[PSSyncBlocks] {
  override def service = PSBlockSyncService
}

//
// http://localhost:8000/fbs/xdn/pbget.do?bd=
object PSBlockSyncService extends LogHelper with PBUtils with LService[PSSyncBlocks] with PMNodeHelper {
  override def onPBPacket(pack: FramePacket, pbo: PSSyncBlocks, handler: CompleteHandler) = {
    //    log.debug("BlockSyncService:" + pack.getFrom())
    var ret = PRetSyncBlocks.newBuilder();
    if (!VCtrl.isReady()) {
      ret.setRetCode(-1).setRetMessage("VRF Network Not READY")
      handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
    } else {
      try {
        val startTime = System.currentTimeMillis();
        MDCSetBCUID(VCtrl.network())
        MDCSetMessageID(pbo.getMessageId)
        ret.setMessageId(pbo.getMessageId);
        //
        val cn = VCtrl.curVN()
        ret.setRetCode(0).setRetMessage("SUCCESS")
        for (
          id <- pbo.getStartId to pbo.getEndId
        ) {
          val b = VCtrl.loadFromBlock(id, pbo.getNeedBody);
          if (b != null) {
            b.map(bs => {
              if (bs !=null) {
                ret.addBlockHeaders(bs);
              }
            })
          }
        }
        pbo.getBlockIdxList.map { id =>
          val b = VCtrl.loadFromBlock(id);
          if (b != null) {
            b.map(bs => {
              ret.addBlockHeaders(bs);
            })
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
          ret.setRetCode(-3)
        }
      } finally {
        handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
      }
    }
  }
  //  override def getCmds(): Array[String] = Array(PWCommand.LST.name())
  override def cmd: String = PCommand.SYN.name();
}
