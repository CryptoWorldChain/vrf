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
import org.csc.dposblk.pbgens.Dposblock.PSNodeInfo
import onight.tfw.otransio.api.PacketHelper
import org.csc.bcapi.exception.FBSException
import org.csc.dposblk.pbgens.Dposblock.PCommand
import org.csc.dposblk.pbgens.Dposblock.PRetNodeInfo
import org.csc.dposblk.tasks.DCtrl
import scala.collection.JavaConversions._
import org.csc.dposblk.pbgens.Dposblock.PDNode
import org.csc.dposblk.pbgens.Dposblock.PSRhrCheck
import org.csc.dposblk.pbgens.Dposblock.PRetRhrCheck
import org.csc.bcapi.JodaTimeHelper
import org.csc.dposblk.tasks.DTask_DutyTermVote

@NActorProvider
@Instantiate
@Provides(specifications = Array(classOf[ActorService], classOf[IActor], classOf[CMDService]))
class PDPoSRhrCheck extends PSMDPoSNet[PSRhrCheck] {
  override def service = PDPoSRhrCheckService
}

//
// http://localhost:8000/fbs/xdn/pbget.do?bd=
object PDPoSRhrCheckService extends LogHelper with PBUtils with LService[PSRhrCheck] with PMNodeHelper {
  override def onPBPacket(pack: FramePacket, pbo: PSRhrCheck, handler: CompleteHandler) = {
    log.debug("onPBPacket::" + pbo)
    var ret = PRetRhrCheck.newBuilder();
    val network = networkByID("dpos")
    if (network == null) {
      ret.setRetCode(-1).setRetMessage("unknow network:")
      handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
    } else {
      try {
        MDCSetBCUID(network);
        ret.setRetCode(0).setRetMessage("OK")
        val cur_dnode = DCtrl.curDN();
        ret.setBanforvote((-1 * JodaTimeHelper.secondIntFromNow(DTask_DutyTermVote.ban_for_vote_sec)))
        .setBlockheight(cur_dnode.getCurBlock)
        .setBlockhash(cur_dnode.getCurBlockHash)
        .setCoaddr(cur_dnode.getCoAddress)
        .setLastblocktime(cur_dnode.getLastBlockTime)
        .setMaxblockheightseen(DCtrl.bestheight.get)
        .setMaxtermidseedn(cur_dnode.getTermId)
        .setStatus(cur_dnode.getState.name())
        .setTermid(cur_dnode.getTermId)
        .setTermuid(cur_dnode.getTermSign)
        .setTimepasslastblk(JodaTimeHelper.secondIntFromNow(cur_dnode.getLastBlockTime))
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
  override def cmd: String = PCommand.RHR.name();
}
