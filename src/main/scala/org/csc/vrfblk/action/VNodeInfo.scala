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
import org.csc.ckrand.pbgens.Ckrand.{GossipMiner, PCommand, PRetNodeInfo, PSNodeInfo, VNodeState}
import org.csc.vrfblk.tasks.VCtrl
import org.csc.p22p.node.PNode
import org.csc.vrfblk.tasks.BeaconGossip
import org.apache.commons.lang3.StringUtils
import org.csc.vrfblk.utils.VConfig
import org.csc.vrfblk.tasks.NodeStateSwitcher
import org.csc.vrfblk.tasks.Initialize
import org.csc.vrfblk.Daos
import com.google.protobuf.ByteString

@NActorProvider
@Instantiate
@Provides(specifications = Array(classOf[ActorService], classOf[IActor], classOf[CMDService]))
class VNodeInfo extends PSMVRFNet[PSNodeInfo] {
  override def service = VNodeInfoService
}

//
// http://localhost:8000/fbs/xdn/pbget.do?bd=
object VNodeInfoService extends LogHelper with PBUtils with LService[PSNodeInfo] with PMNodeHelper {
  override def onPBPacket(pack: FramePacket, pbo: PSNodeInfo, handler: CompleteHandler) = {

    var ret = PRetNodeInfo.newBuilder();
    val network = networkByID("vrf")
    if (network == null) {
      ret.setRetCode(-1).setRetMessage("unknow network:")
      handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
    } else {
      try {
        MDCSetBCUID(network);
        VCtrl.coMinerByUID.map(m => {
          ret.addMurs(GossipMiner.newBuilder().setBcuid(m._2.getBcuid).setCurBlock(m._2.getCurBlock))
        })
        ret.setVn(VCtrl.curVN())
        MDCSetMessageID(pbo.getMessageId);
        if (StringUtils.isBlank(pack.getFrom())) {

        } else {
          log.debug("getNodeInfo::" + pack.getFrom() + ",blockheight=" + pbo.getVn.getCurBlock + ",remotestate=" + pbo.getVn.getState
            + ",curheight=" + VCtrl.curVN().getCurBlock + ",curstate=" + VCtrl.curVN().getState + ",DN=" + network.directNodes.size + ",MN=" + VCtrl.coMinerByUID.size)
          if (StringUtils.equals(pack.getFrom(), network.root.bcuid) || StringUtils.equals(pbo.getMessageId, BeaconGossip.currentBR.messageId)) {
            // 如果消息是自己发的
            if (network.nodeByBcuid(pack.getFrom()) != network.noneNode && StringUtils.isNotBlank(pbo.getVn.getBcuid)) {
              val self = pbo.getVn

              if (self.getDoMine) {
                VCtrl.coMinerByUID.put(self.getBcuid, self);
              }
              // log.debug("current cominer::" + VCtrl.coMinerByUID);
              val psret = PSNodeInfo.newBuilder().setMessageId(pbo.getMessageId);
              if (pbo.getGossipBlockInfo == 0) {
                // 取vn的currentBlock
                val blk = Daos.blkHelper.getBlock(pbo.getVn.getCurBlockHash);
                if (blk == null) {
                  BeaconGossip.offerMessage(pbo);
                } else {
                  psret.setGossipBlockInfo(pbo.getGossipBlockInfo)
                  psret.setGossipMinerInfo(GossipMiner.newBuilder().setBcuid(blk.getMiner.getBcuid)
                    .setCurBlockHash(Daos.enc.hexEnc(blk.getHeader.getHash.toByteArray()))
                    .setBlockExtrData(blk.getMiner.getBit)
                    .setBeaconHash(blk.getMiner.getTermid)
                    .setCurBlock(pbo.getGossipBlockInfo))

                  psret.setVn(pbo.getVn.toBuilder().setBeaconHash(blk.getMiner.getTermid).setVrfRandseeds(blk.getMiner.getBit));
                  BeaconGossip.offerMessage(psret);
                }
              } else {
                val blks = Daos.chainHelper.getBlocksByNumber(pbo.getGossipBlockInfo);
                if (blks != null && blks.size() >= 1) {
                  val blk = blks.get(0);
                  // pbo中的beaconhash应与block保持一致
                  // 在apply成功之后会计算新的beaconhash，所以currentBlock的beaconHash!=pbo.getBeaconHash 
                  // 第一块的beanconHash = 创世块的hash
                  psret.setGossipBlockInfo(pbo.getGossipBlockInfo)
                  psret.setGossipMinerInfo(GossipMiner.newBuilder().setBcuid(blk.getMiner.getBcuid)
                    .setCurBlockHash(Daos.enc.hexEnc(blk.getHeader.getHash.toByteArray()))
                    .setBlockExtrData(blk.getMiner.getBit)
                    .setBeaconHash(blk.getMiner.getTermid)
                    .setCurBlock(pbo.getGossipBlockInfo))

                  psret.setVn(pbo.getVn.toBuilder().setBeaconHash(blk.getMiner.getTermid).setVrfRandseeds(blk.getMiner.getBit));
                }
                BeaconGossip.offerMessage(psret);
              }

              // 等待后续执行pbft
              //              if (pbo.getGossipBlockInfo > 0) {
              //                val psret = PSNodeInfo.newBuilder().setMessageId(pbo.getMessageId).setVn(pbo.getVn);
              //                psret.setGossipBlockInfo(pbo.getGossipBlockInfo)
              //
              //                  psret.setGossipMinerInfo(GossipMiner.newBuilder().setBcuid(blk.getMiner.getBcuid)
              //                    .setCurBlockHash(Daos.enc.hexEnc(blk.getHeader.getHash.toByteArray()))
              //                    .setBlockExtrData(blk.getMiner.getBit)
              //                    .setBeaconHash(blk.getMiner.getTermid)
              //                    .setCurBlock(pbo.getGossipBlockInfo))
              //                  log.debug("rollback --> getBlockBlock=" + pbo.getGossipBlockInfo
              //                      +",lheight="+blk.getHeader.getNumber.intValue() +"GossipBEACONHash="+psret.getGossipMinerInfo.getBeaconHash);
              //
              //                BeaconGossip.offerMessage(psret);
              //              } else {
              //                log.debug("pbo::" + pbo)
              //
              //                pbo.setBeaconHash(blk.getMiner.getTermid).setBlockExtrData(blk.getMiner.getBit)
              //                BeaconGossip.offerMessage(pbo);
              //              }
            }
          } else {
            // 其它节点
            // 返回自己的信息
            network.nodeByBcuid(pack.getFrom()) match {
              case network.noneNode =>
              case n: PNode =>
                if (pbo.getVn.getCurBlock >= VCtrl.curVN().getCurBlock - VConfig.BLOCK_DISTANCE_COMINE && StringUtils.isNotBlank(pbo.getVn.getBcuid)) {
                  // 成为打快节点
                  //log.debug("add cominer:" + pbo.getVn.getBcuid + ",blockheight=" + pbo.getVn.getCurBlock + ",cur=" + VCtrl.curVN().getCurBlock);
                  val friendNode = pbo.getVn
                  if (friendNode.getDoMine) {
                    VCtrl.coMinerByUID.put(friendNode.getBcuid, friendNode);
                  }
                  // log.debug("current cominer::" + VCtrl.coMinerByUID);
                }

                val psret = PSNodeInfo.newBuilder().setMessageId(pbo.getMessageId).setVn(VCtrl.curVN());

                if (pbo.getGossipBlockInfo > 0) {
                  val blks = Daos.chainHelper.getBlocksByNumber(pbo.getGossipBlockInfo);
                  psret.setGossipBlockInfo(pbo.getGossipBlockInfo)
                  if (blks != null && blks.size() >= 1) {
                    val blk = blks.get(0);
                    psret.setGossipMinerInfo(GossipMiner.newBuilder().setBcuid(blk.getMiner.getBcuid)
                      .setCurBlockHash(Daos.enc.hexEnc(blk.getHeader.getHash.toByteArray()))
                      .setBlockExtrData(blk.getMiner.getBit)
                      .setBeaconHash(blk.getMiner.getTermid)
                      .setCurBlock(pbo.getGossipBlockInfo))
                    log.debug("rollback --> getBlockBlock=" + pbo.getGossipBlockInfo + ",blksize=" + blks.size()
                      + ",rheight=" + blk.getHeader.getNumber.intValue());
                  }
                } else {
                  var startBlock = pbo.getVn.getCurBlock;
                  while (startBlock > pbo.getVn.getCurBlock - VConfig.SYNC_SAFE_BLOCK_COUNT && startBlock > 0) {
                    val blks = Daos.chainHelper.getBlocksByNumber(startBlock);
                    if (blks != null && blks.size() == 1) {
                      psret.setSugguestStartSyncBlockId(startBlock);
                      startBlock = -100;
                    } else {
                      startBlock = startBlock - 1;
                    }
                  }
                }
                if (pbo.getIsQuery) {
                  psret.setIsQuery(false);
                  network.postMessage("INFVRF", Left(psret.build()), pbo.getMessageId, n._bcuid);
                }
              case _ =>
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
          ret.setRetCode(-3).setRetMessage(t.getMessage)
        }
      } finally {
        handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
      }
    }
  }

  //  override def getCmds(): Array[String] = Array(PWCommand.LST.name())
  override def cmd: String = PCommand.INF.name();
}
