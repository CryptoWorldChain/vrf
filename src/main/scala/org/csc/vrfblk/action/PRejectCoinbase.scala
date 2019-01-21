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
import org.csc.dposblk.pbgens.Dposblock.PCommand
import org.csc.dposblk.pbgens.Dposblock.PSCoinbase
import org.csc.dposblk.pbgens.Dposblock.PRetCoMine
import org.csc.dposblk.pbgens.Dposblock.PRetCoinbase
import org.csc.dposblk.tasks.DCtrl
import onight.tfw.otransio.api.PacketHelper
import org.csc.dposblk.pbgens.Dposblock.PRetCoinbase.CoinbaseResult
import org.csc.bcapi.exception.FBSException
import org.apache.commons.lang3.StringUtils
import org.csc.dposblk.pbgens.Dposblock.PBlockEntry
import org.csc.dposblk.tasks.BlockSync
import org.csc.dposblk.tasks.DTask_DutyTermVote
import org.csc.dposblk.utils.DConfig
import org.csc.dposblk.pbgens.Dposblock.PSRejectCoinbase
import java.util.concurrent.ConcurrentHashMap
import java.util.HashMap

@NActorProvider
@Instantiate
@Provides(specifications = Array(classOf[ActorService], classOf[IActor], classOf[CMDService]))
class PRejectCoinbaseM extends PSMDPoSNet[PSRejectCoinbase] {
  override def service = PRejectCoinbase
}

//
// http://localhost:8000/fbs/xdn/pbget.do?bd=
object PRejectCoinbase extends LogHelper with PBUtils with LService[PSRejectCoinbase] with PMNodeHelper {
  val rejectBlockMap = new HashMap[Int, HashMap[String, String]]();
  override def onPBPacket(pack: FramePacket, pbo: PSRejectCoinbase, handler: CompleteHandler) = {
    //    log.debug("Mine Block From::" + pack.getFrom())
    var ret = PRetCoinbase.newBuilder();
    if (!DCtrl.isReady()) {
      log.debug("DCtrl not ready");
      ret.setRetCode(-1).setRetMessage("DPoS Network Not READY")
      handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
    } else {
      try {
        MDCSetBCUID(DCtrl.dposNet())
        MDCSetMessageID(pbo.getMessageId)
        //
        val cn = DCtrl.curDN()
        ret.setRetCode(0).setRetMessage("SUCCESS")
        log.debug("coinbase pbo.getRejectBlockHeight::" + pbo.getBlockHeight + ",hash=" + pbo.getBlockHash);
        rejectBlockMap.synchronized({
          var rejectNodes = rejectBlockMap.get(pbo.getBlockHeight);
          if (rejectNodes == null) {
            rejectNodes = new HashMap[String, String]();
            rejectBlockMap.put(pbo.getBlockHeight, rejectNodes);
          }
          rejectNodes.put(pbo.getCoAddress, pbo.getBlockHash);
          if (rejectNodes.size() >= DCtrl.coMinerByUID.size * 2 / 3) {
            log.debug("Get Network Reject");
          }
        }) //end of sync
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
  override def cmd: String = PCommand.BRJ.name();
}
