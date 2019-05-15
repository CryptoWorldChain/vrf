
package org.brewchain.vrfblk.action

import org.apache.felix.ipojo.annotations.Instantiate
import org.apache.felix.ipojo.annotations.Provides
import org.brewchain.bcrand.model.Bcrand.PCommand
import org.brewchain.bcrand.model.Bcrand.PSCoinbase
import org.brewchain.core.model.Block.BlockInfo;
import org.brewchain.p22p.action.PMNodeHelper
import org.brewchain.p22p.utils.LogHelper
import org.brewchain.vrfblk.PSMVRFNet
import org.brewchain.vrfblk.tasks.BlockProcessor
import org.brewchain.vrfblk.tasks.VCtrl

import org.brewchain.vrfblk.utils.RandFunction
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
import org.brewchain.vrfblk.tasks.NodeStateSwitcher
import org.brewchain.vrfblk.tasks.Initialize
import org.brewchain.vrfblk.Daos
import org.brewchain.vrfblk.tasks.BeaconGossip
import org.apache.commons.lang3.StringUtils
import org.brewchain.bcrand.model.Bcrand.VNodeState
import org.brewchain.vrfblk.utils.VConfig

@NActorProvider
@Instantiate
@Provides(specifications = Array(classOf[ActorService], classOf[IActor], classOf[CMDService]))
class PSCoinbaseNew extends PSMVRFNet[PSCoinbase] {
  override def service = PSCoinbaseNewService
}

//
// http://localhost:8000/fbs/xdn/pbget.do?bd=
object PSCoinbaseNewService extends LogHelper with PBUtils with LService[PSCoinbase] with PMNodeHelper {
  override def onPBPacket(pack: FramePacket, pbo: PSCoinbase, handler: CompleteHandler) = {
    //    log.debug("Mine Block From::" + pack.getFrom())
    if (!VCtrl.isReady()) {
      log.debug("VCtrl not ready:");
      handler.onFinished(PacketHelper.toPBReturn(pack, pbo))
      // NodeStateSwitcher.offerMessage(new Initialize());
    } else {
      MDCSetBCUID(VCtrl.network())
      MDCSetMessageID(pbo.getMessageId)
      log.debug("Get New Block:H=" + pbo.getBlockEntry.getBlockHeight + " from=" + pbo.getBcuid + ",BH=" + pbo.getBlockEntry.getBlockhash);
      // 校验beaconHash和区块hash是否匹配，排除异常区块
      val block = BlockInfo.newBuilder().mergeFrom(pbo.getBlockEntry.getBlockHeader);
      val parentBlock = Daos.chainHelper.getBlockByHash(block.getHeader.getParentHash.toByteArray());
      if (parentBlock == null) {
        if (VCtrl.curVN().getState != VNodeState.VN_INIT
          && VCtrl.curVN().getState != VNodeState.VN_SYNC_BLOCK
          && VCtrl.curVN().getCurBlock + VConfig.MAX_SYNC_BLOCKS > pbo.getBlockHeight ) {
          BlockProcessor.offerBlock(new ApplyBlock(pbo)); //need to sync or gossip
        }else{
          
        }

      } else {
        val nodebits = parentBlock.getMiner.getBit;
        val (hash, sign) = RandFunction.genRandHash(Daos.enc.hexEnc(block.getHeader.getPreHash.toByteArray()), parentBlock.getMiner.getTermid, nodebits);
        if (hash.equals(block.getMiner.getTermid) || block.getHeader.getNumber == 1) {
          BlockProcessor.offerMessage(new ApplyBlock(pbo));
        } else {
          //if rollback
          if (StringUtils.isNotBlank(BeaconGossip.rollbackGossipNetBits)) {
            val (rollbackhash, rollblacksign) = RandFunction.genRandHash(Daos.enc.hexEnc(parentBlock.getHeader.getHash.toByteArray()), parentBlock.getMiner.getTermid, BeaconGossip.rollbackGossipNetBits);
            if (rollbackhash.equals(block.getMiner.getTermid)) {
              log.info("rollback hash apply:rollbackhash=" + rollbackhash + ",blockheight=" + pbo.getBlockHeight);
              BlockProcessor.offerMessage(new ApplyBlock(pbo));
            } else {
              log.warn("beaconhash.rollback not equal:height=" + block.getHeader.getNumber + ":: BH=" + pbo.getBlockEntry.getBlockhash
                + " prvbh=" + Daos.enc.hexEnc(block.getHeader.getPreHash.toByteArray()) + " dbprevbh=" + Daos.enc.hexEnc(parentBlock.getHeader.getHash.toByteArray())
                + " termid=" + block.getMiner.getTermid + " ptermid=" + parentBlock.getMiner.getTermid
                + " need=" + rollbackhash + " get=" + pbo.getBeaconHash
                + " prevBeaconHash=" + pbo.getPrevBeaconHash + " BeaconBits=" + nodebits
                + ",rollbackseed=" + BeaconGossip.rollbackGossipNetBits)
            }
          } else {
            log.warn("beaconhash not equal:: BH=" + pbo.getBlockEntry.getBlockhash + " prvbh=" + Daos.enc.hexEnc(block.getHeader.getPreHash.toByteArray()) + " termid=" + block.getMiner.getTermid + " ptermid=" + parentBlock.getMiner.getTermid + " need=" + hash + " get=" + pbo.getBeaconHash + " prevBeaconHash=" + pbo.getPrevBeaconHash + " BeaconBits=" + nodebits)
          }
        }
      }

      handler.onFinished(PacketHelper.toPBReturn(pack, pbo))
    }
  }

  //  override def getCmds(): Array[String] = Array(PWCommand.LST.name())
  override def cmd: String = PCommand.CBN.name();
}
