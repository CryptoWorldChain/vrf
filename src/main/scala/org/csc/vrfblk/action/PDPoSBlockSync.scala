package org.csc.vrfblk.action

import org.apache.felix.ipojo.annotations.Instantiate
import org.apache.felix.ipojo.annotations.Provides
import onight.tfw.ntrans.api.ActorService
import onight.tfw.proxy.IActor
import onight.tfw.otransio.api.session.CMDService
import onight.osgi.annotation.NActorProvider
import org.csc.dposblk.PSMDPoSNet
import org.csc.p22p.utils.LogHelper
import onight.oapi.scala.commons.PBUtils
import onight.oapi.scala.commons.LService
import org.csc.p22p.action.PMNodeHelper
import onight.tfw.otransio.api.beans.FramePacket
import onight.tfw.async.CompleteHandler
import org.csc.bcapi.utils.PacketIMHelper._
import org.csc.dposblk.pbgens.Dposblock.PSCoMine
import org.csc.dposblk.pbgens.Dposblock.PRetCoMine
import org.csc.dposblk.pbgens.Dposblock.PCommand
import org.csc.dposblk.pbgens.Dposblock.PSSyncBlocks
import org.csc.dposblk.pbgens.Dposblock.PRetSyncBlocks
import org.csc.dposblk.tasks.DCtrl
import onight.tfw.otransio.api.PacketHelper
import org.csc.bcapi.exception.FBSException
import org.csc.dposblk.Daos

import scala.collection.JavaConversions._
@NActorProvider
@Instantiate
@Provides(specifications = Array(classOf[ActorService], classOf[IActor], classOf[CMDService]))
class PDPoSBlockSync extends PSMDPoSNet[PSSyncBlocks] {
  override def service = PDPoSBlockSyncService
}

//
// http://localhost:8000/fbs/xdn/pbget.do?bd=
object PDPoSBlockSyncService extends LogHelper with PBUtils with LService[PSSyncBlocks] with PMNodeHelper {
  override def onPBPacket(pack: FramePacket, pbo: PSSyncBlocks, handler: CompleteHandler) = {
//    log.debug("BlockSyncService:" + pack.getFrom())
    var ret = PRetSyncBlocks.newBuilder();
    if (!DCtrl.isReady()) {
      ret.setRetCode(-1).setRetMessage("DPoS Network Not READY")
      handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
    } else {
      try {
        val startTime=System.currentTimeMillis();
        MDCSetBCUID(DCtrl.dposNet())
        MDCSetMessageID(pbo.getMessageId)
        ret.setMessageId(pbo.getMessageId);
        //
        val cn = DCtrl.curDN()
        ret.setRetCode(0).setRetMessage("SUCCESS")
        for (
          id <- pbo.getStartId to pbo.getEndId
        ) {
            val b = DCtrl.loadFromBlock(id, pbo.getNeedBody);
            if (b != null) {
              ret.addBlockHeaders(b);
            }
        }
        pbo.getBlockIdxList.map { id =>
          val b = DCtrl.loadFromBlock(id);
          if (b != null) {
            ret.addBlockHeaders(b);
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
