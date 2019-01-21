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
import org.csc.dposblk.pbgens.Dposblock.PSDutyTermVote
import org.csc.dposblk.pbgens.Dposblock.PDutyTermResult
import org.csc.dposblk.tasks.DCtrl
import onight.tfw.otransio.api.PacketHelper
import org.apache.commons.lang3.StringUtils
import org.csc.bcapi.exception.FBSException
import org.csc.dposblk.pbgens.Dposblock.PDutyTermResult.VoteResult
import org.csc.dposblk.Daos
import org.csc.bcapi.gens.Oentity.OValue
import org.csc.bcapi.gens.Oentity.OKey
import com.google.protobuf.ByteString
import org.csc.dposblk.tasks.DTask_DutyTermVote
import org.csc.dposblk.utils.DConfig
import org.csc.dposblk.pbgens.Dposblock.DNodeState

@NActorProvider
@Instantiate
@Provides(specifications = Array(classOf[ActorService], classOf[IActor], classOf[CMDService]))
class PDPoSDutyTermResult extends PSMDPoSNet[PDutyTermResult] {
  override def service = PDPoSDutyTermResult
}

//
// http://localhost:8000/fbs/xdn/pbget.do?bd=
object PDPoSDutyTermResult extends LogHelper with PBUtils with LService[PDutyTermResult] with PMNodeHelper {
  override def onPBPacket(pack: FramePacket, pbo: PDutyTermResult, handler: CompleteHandler) = {
    //    log.debug("DPoS DutyTermResult::" + pack.getFrom())
    var ret = PDutyTermResult.newBuilder();
    val net = DCtrl.instance.network;
    if (!DCtrl.isReady() || net == null) {
      ret.setRetCode(-1).setRetMessage("DPoS Network Not READY")
      handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
    } else {
      try {
        MDCSetBCUID(DCtrl.dposNet())
        MDCSetMessageID(pbo.getMessageId)
        ret.setMessageId(pbo.getMessageId);
        ret.setRetCode(0).setRetMessage("SUCCESS")
        if (pbo.getRetCode != 0) {
          log.debug("Get Wrong Result:" + pbo);
        }
        val cn = DCtrl.curDN()
        //        val vq = DCtrl.voteRequest();
        //
        //        this.synchronized({
        //          if ((StringUtils.isNotBlank(cn.getDutyUid))
        //            && (StringUtils.isNotBlank(vq.getMessageId)
        //              && vq.getMessageId.equals(pbo.getMessageId))
        //              && (vq.getTermId == pbo.getTermId)) {
        // 
        //            val records = Daos.dposdb.listBySecondKey("D" + vq.getTermId + "-" + vq.getSign)
        if (pbo.getResult != VoteResult.VR_APPLY) {
          Daos.dposvotedb.put(
            "V" + pbo.getTermId + "-" + pbo.getSign + "-"
              + pbo.getBcuid,
            OValue.newBuilder()
              .setCount(pbo.getTermId)
              .setSecondKey("D" + pbo.getTermId)
              .setExtdata(pbo.toByteString())
              .setInfo(pbo.getSign)
              .setNonce(pbo.getResultValue).build())

          if (DTask_DutyTermVote.possibleTermID.size() < DConfig.MAX_POSSIBLE_TERMID) {
            DTask_DutyTermVote.possibleTermID.put(pbo.getTermId, pbo.getBcuid + "," + pbo.getCurBlock);
          }
          log.debug("Get DPos Term Vote Result:du=" + cn.getDutyUid + ",PT=" + pbo.getTermId + ",T=" + DCtrl.termMiner().getTermId
            + ",sign=" + pbo.getSign + ",VA=" + pbo.getVoteAddress + ",FROM=" + pbo.getBcuid + ",Result=" + pbo.getResult + ",PB=" + pbo.getCurBlock + ",CB=" + DCtrl.curDN().getCurBlock
            + ",MSG=" + pbo.getRetMessage);
        }
        //save bc info
        if (pbo.getNodeState != DNodeState.DN_BAKCUP) {

          DCtrl.coMinerByUID.get(pbo.getBcuid) match {
            case Some(p) =>
              if (pbo.getResult == VoteResult.VR_REJECT) {
                DCtrl.coMinerByUID.put(pbo.getBcuid, p.toBuilder().setCurBlock(pbo.getCurBlock).setTermId(pbo.getCurTermid).setTermSign(pbo.getCurTermSign)
                  .setTermEndBlock(pbo.getCurTermEndBlock)
                  .setTermStartBlock(pbo.getCurTermStartBlock)
                  .build());
              } else {
                log.debug("update term:" + pbo.getCurTermid + ",for bcuid=" + pbo.getBcuid + ",termid=" + pbo.getTermId + ",sign=" + pbo.getSign + ",result=" + pbo.getResult)
                val (n_termid, n_termsig) =
                  if (pbo.getCurBlock < pbo.getVoteTermStartBlock - 10 && pbo.getCurTermid < pbo.getTermId - 2) {
                    (pbo.getCurTermid, pbo.getCurTermSign)
                  } else {
                    (pbo.getTermId, pbo.getSign)
                  }
                DCtrl.coMinerByUID.put(pbo.getBcuid, p.toBuilder().setCurBlock(pbo.getCurBlock).setTermId(n_termid).setTermSign(n_termsig)
                  .setTermEndBlock(pbo.getVoteTermEndBlock)
                  .setTermStartBlock(pbo.getVoteTermStartBlock)
                  .build());

              }
            case None =>
              log.debug("unknow cominer:" + pbo.getBcuid + ",term:" + pbo.getCurTermid + ",for bcuid=" + pbo.getBcuid + ",termid=" + pbo.getTermId + ",sign=" + pbo.getSign + ",result=" + pbo.getResult);
          }
          DTask_DutyTermVote.synchronized({
            DTask_DutyTermVote.notifyAll()
          })
        }
        //
        //          }
        //        })
        //        }

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
  override def cmd: String = PCommand.DTR.name();
}
