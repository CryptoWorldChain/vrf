package org.brewchain.vrfblk.action

import com.google.protobuf.ByteString
import onight.oapi.scala.commons.{ LService, PBUtils }
import onight.osgi.annotation.NActorProvider
import onight.tfw.async.CompleteHandler
import onight.tfw.ntrans.api.ActorService
import onight.tfw.otransio.api.{ PackHeader, PacketHelper }
import onight.tfw.otransio.api.beans.FramePacket
import onight.tfw.otransio.api.session.CMDService
import onight.tfw.proxy.IActor
import org.apache.commons.codec.binary.Hex
import org.apache.felix.ipojo.annotations.{ Instantiate, Provides }
import org.brewchain.bcrand.model.Bcrand.{ PCommand, PRetGetTransaction, PSGetTransaction }
import org.brewchain.p22p.action.PMNodeHelper
import org.brewchain.p22p.utils.LogHelper
import org.brewchain.vrfblk.{ Daos, PSMVRFNet }
import org.brewchain.vrfblk.tasks.VCtrl
import org.brewchain.vrfblk.utils.TxCache

import scala.collection.JavaConverters._
import org.brewchain.p22p.utils.PacketIMHelper._
import org.brewchain.vrfblk.utils.VConfig

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
//    if (Runtime.getRuntime.freeMemory() / 1024 / 1024 < VConfig.METRIC_SYNCTX_FREE_MEMEORY_MB) {
//      ret.setRetCode(-2).setRetMessage("memory low")
//      log.debug("ban sync block for low memory");
//      handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
//    } else 
      if (VCtrl.isReady()) {
      try {
        val from = pack.getFrom()
        var i = 0
        for (wantedHash: String <- pbo.getTxHashList.asScala) {
          val transactionX = TxCache.getTx(wantedHash) match {
            case t if t != null => t
            case _ => {
              val t = Daos.txHelper.getTransaction(Daos.enc.hexStrToBytes(wantedHash))
              if (t != null) {
                TxCache.recentBlkTx.put(wantedHash, t)
              }
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
        case t: Throwable =>
          log.error("get txerror", t);
          ret.clear().setRetCode(-3).setRetMessage("" + t)
      } finally {
        handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
      }
    } else {
      ret.setRetCode(-1).setRetMessage("V Node Not ready")
      handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))

    }
  }
}
