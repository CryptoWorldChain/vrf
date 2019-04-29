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

/**
  * 请求同步块
  */
object PSBlockSyncService extends LogHelper with PBUtils with LService[PSSyncBlocks] with PMNodeHelper {
  override def onPBPacket(pack: FramePacket, pbo: PSSyncBlocks, handler: CompleteHandler) = {
    val ret = PRetSyncBlocks.newBuilder();
    if (!VCtrl.isReady()) {
      ret.setRetCode(-1).setRetMessage("VRF Network Not READY")
      handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
    } else {
      try {
        MDCSetBCUID(VCtrl.network())
        MDCSetMessageID(pbo.getMessageId)
        ret.setMessageId(pbo.getMessageId);
        ret.setRetCode(0).setRetMessage("SUCCESS")
        //同步块总大小(K)
        var totalSize = 0
        //同步block总共40M
        val maxTotalSize = 100 * 1024 * 1024

        for (
          id <- pbo.getStartId to pbo.getEndId if totalSize <= maxTotalSize
        ) {
          val b = VCtrl.loadFromBlock(id, pbo.getNeedBody)
          if (b != null) {
              b.map(bs => {
                if (bs !=null && totalSize <= maxTotalSize) {
                  totalSize = totalSize + bs.build().toByteArray().size;
                  if (totalSize <= maxTotalSize) {
                    ret.addBlockHeaders(bs);
                  } else {
                    log.info("package too large. size=" + totalSize + " blk=" + id)
                  } 
                }
              })
          }
        }
        pbo.getBlockIdxList.map { id =>
          val b = VCtrl.loadFromBlock(id, pbo.getNeedBody);
          if (b != null) {
            b.map(bs => {
              totalSize = totalSize + bs.build().toByteArray().size;
            })
            if (totalSize <= maxTotalSize) {
              b.map(bs => {
                if (bs !=null) {
                  totalSize = totalSize + bs.build().toByteArray().size;
                  log.info("blk=" + id + " size=" + bs.build().toByteArray().size)
                  if (totalSize <= maxTotalSize) {
                    ret.addBlockHeaders(bs);
                  } else {
                    log.info("package too large. size=" + totalSize + " blk=" + id);
                  }
                }
              })
            } else {
              log.info("package too large. size=" + totalSize + " blk=" + id);
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
