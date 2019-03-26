package org.csc.vrfblk.action

import com.google.protobuf.ByteString
import onight.oapi.scala.commons.{LService, PBUtils}
import onight.osgi.annotation.NActorProvider
import onight.tfw.async.CompleteHandler
import onight.tfw.ntrans.api.ActorService
import onight.tfw.otransio.api.{PackHeader, PacketHelper}
import onight.tfw.otransio.api.beans.FramePacket
import onight.tfw.otransio.api.session.CMDService
import onight.tfw.proxy.IActor
import org.apache.commons.codec.binary.Hex
import org.apache.felix.ipojo.annotations.{Instantiate, Provides}
import org.csc.ckrand.pbgens.Ckrand.{PCommand, PRetGetTransaction, PSGetTransaction}
import org.csc.p22p.action.PMNodeHelper
import org.csc.p22p.utils.LogHelper
import org.csc.vrfblk.{Daos, PSMVRFNet}
import org.csc.vrfblk.tasks.VCtrl
import org.csc.vrfblk.utils.TxCache

import scala.collection.JavaConverters._

@Instantiate
@NActorProvider
@Provides(specifications = Array(classOf[ActorService], classOf[IActor], classOf[CMDService]))
class PSGetTransactionByHash extends PSMVRFNet[PSGetTransaction] {
  override def service: LService[PSGetTransaction] = PSGetTransactionService
}

object PSGetTransactionService extends LService[PSGetTransaction] with PBUtils with LogHelper with PMNodeHelper {
  override def cmd: String = PCommand.SRT.name()

  override def onPBPacket(pack: FramePacket, pbo: PSGetTransaction, handler: CompleteHandler): Unit = {
    val ret = PRetGetTransaction.newBuilder()
    if (VCtrl.isReady()) {
      try {
        val from = pack.getExtProp(PackHeader.PACK_FROM)
        log.info(s"SRT SYNCTX request ${from}, need TX COUNT${pbo.getTxHashList.size()} ,self bcuid=${VCtrl.curVN().getBcuid}")

        var i = 0
        for (wantedHash: String <- pbo.getTxHashList.asScala) {
          val transactionX = TxCache.getTx(wantedHash) match {
            case t if t != null => t
            case _ => {
              val t = Daos.txHelper.GetTransaction(ByteString.copyFrom(Hex.decodeHex(wantedHash)))
              TxCache.recentBlkTx.put(wantedHash, t)
              t
            }
          }
          if (transactionX != null) {
            ret.addTxContent(ByteString.copyFrom(transactionX.toByteArray))
          } else {
            i += 1
            if (i < 11) {
              log.info(s"can not get tx by HASH ${wantedHash}")
            }
          }
        }

        ret.setRetCode(1).setRetMessage("SUCCESS")

      } catch {
        case t: Throwable => ret.clear().setRetCode(-3).setRetMessage(t.getMessage)
      }

    } else {
      ret.setRetCode(-1).setRetMessage("V Node Not ready")
    }
    handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
  }
}
