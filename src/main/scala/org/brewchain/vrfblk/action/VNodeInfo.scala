package org.brewchain.vrfblk.action

import org.apache.felix.ipojo.annotations.Instantiate
import org.apache.felix.ipojo.annotations.Provides
import onight.tfw.ntrans.api.ActorService
import onight.tfw.proxy.IActor
import onight.tfw.otransio.api.session.CMDService
import onight.osgi.annotation.NActorProvider
import org.brewchain.p22p.utils.LogHelper
import onight.oapi.scala.commons.PBUtils
import onight.oapi.scala.commons.LService
import org.brewchain.p22p.action.PMNodeHelper
import onight.tfw.otransio.api.beans.FramePacket
import onight.tfw.async.CompleteHandler
import org.brewchain.p22p.utils.PacketIMHelper._

import onight.tfw.otransio.api.PacketHelper
import org.brewchain.p22p.exception.FBSException

import scala.collection.JavaConversions._
import org.brewchain.vrfblk.PSMVRFNet
import org.brewchain.bcrand.model.Bcrand.{ GossipMiner, PCommand, PRetNodeInfo, PSNodeInfo, VNodeState }
import org.brewchain.vrfblk.tasks.VCtrl
import org.brewchain.p22p.node.PNode
import org.brewchain.vrfblk.tasks.BeaconGossip
import org.apache.commons.lang3.StringUtils
import org.brewchain.vrfblk.utils.VConfig
import org.brewchain.vrfblk.tasks.NodeStateSwitcher
import org.brewchain.vrfblk.tasks.Initialize
import org.brewchain.vrfblk.Daos
import com.google.protobuf.ByteString
import org.brewchain.mcore.model.Account.AccountInfo
import org.brewchain.mcore.actuators.tokencontracts20.TokensContract20.TokenRC20Info
import org.brewchain.mcore.concurrent.AccountInfoWrapper
import org.brewchain.mcore.actuators.tokencontracts20.TokensContract20.TokenRC20Value
import org.brewchain.mcore.tools.bytes.BytesHelper
import org.brewchain.bcrand.model.Bcrand.VNode
import org.brewchain.p22p.node.Network

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
    // if (network == null) {
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
        if (pbo.getMessageId != null) {
          MDCSetMessageID(pbo.getMessageId);
        }
        //        log.info("VNodeInfo::from=" + pbo.getVn.getBcuid);
        if (StringUtils.equals(pack.getFrom(), network.root.bcuid) || StringUtils.equals(pbo.getMessageId, BeaconGossip.currentBR.messageId)) {
          // 如果消息是自己发的
          if (network.nodeByBcuid(pack.getFrom()) != network.noneNode && StringUtils.isNotBlank(pbo.getVn.getBcuid)) {
            val vn = pbo.getVn

            if (StringUtils.equals(pack.getFrom(), network.root.bcuid) || vn.getDoMine) {
              if (pbo.getVn.getCurBlock >= VCtrl.curVN().getCurBlock - VConfig.BLOCK_DISTANCE_COMINE && StringUtils.isNotBlank(pbo.getVn.getBcuid)
                && pbo.getVn.getCurBlock >= VCtrl.instance.heightBlkSeen.get - VConfig.BLOCK_DISTANCE_COMINE * 3) {
                log.info("gossip put into cominer bcuid=" + vn.getBcuid + " address=" + vn.getCoAddress + ",block=" + pbo.getVn.getCurBlock);
                //val currentCoinbaseAccount = Daos.accountHandler.getAccountOrCreate(ByteString.copyFrom(Daos.enc.hexStrToBytes(self.getCoAddress)));
                //if (Daos.accountHandler.getTokenBalance(currentCoinbaseAccount, VConfig.AUTH_TOKEN).compareTo(VConfig.AUTH_TOKEN_MIN) >= 0) {
                VCtrl.addCoMiner(vn);
              } else {
                //                log.info("remove cominer for block not height enough:my.cur=" + VCtrl.curVN().getCurBlock + ",my.lastseenheight=" + VCtrl.instance.heightBlkSeen.get
                //                  + ",dist.cur=" + pbo.getVn.getCurBlock);
                //                VCtrl.coMinerByUID.remove(vn.getBcuid);
              }
            } else {
              log.info("remove cominer bcuid=" + vn.getBcuid + " address=" + vn.getCoAddress + ",do-Mine=" + vn.getDoMine + ",from=" + pack.getFrom());
              VCtrl.removeCoMiner(vn.getBcuid);
            }
            // log.debug("current cominer::" + VCtrl.coMinerByUID);
//            val psret = PSNodeInfo.newBuilder().setMessageId(pbo.getMessageId);
            BeaconGossip.offerMessage(pbo);
//            if (pbo.getGossipBlockInfo == 0) {
//              // 取vn的currentBlock
//              val blk = Daos.chainHelper.getBlockByHeight(pbo.getVn.getCurBlock);
//              if (blk == null) {
//                BeaconGossip.offerMessage(pbo);
//              } else {
//                //                log.error("set beacon hash =" + blk.getMiner.getTerm + " height=" + blk.getHeader.getHeight + " hash=" + Daos.enc.bytesToHexStr(blk.getHeader.getHash.toByteArray()))
//                psret.setGossipMinerInfo(GossipMiner.newBuilder().setBcuid(blk.getMiner.getNid)
//                  .setCurBlockHash(Daos.enc.bytesToHexStr(blk.getHeader.getHash.toByteArray()))
//                  .setBlockExtrData(blk.getMiner.getBits)
//                  .setBeaconHash(blk.getMiner.getTerm)
//                  .setCurBlock(pbo.getVn.getCurBlock))
//
//                psret.setVn(pbo.getVn.toBuilder().setBeaconHash(blk.getMiner.getTerm).setVrfRandseeds(blk.getMiner.getBits));
//                BeaconGossip.offerMessage(psret);
//              }
//            } else {
//              val blks = Daos.chainHelper.listBlockByHeight(pbo.getGossipBlockInfo);
//              if (blks != null && blks.length >= 1) {
//                val blk = blks(0);
//                // pbo中的beaconhash应与block保持一致
//                // 在apply成功之后会计算新的beaconhash，所以currentBlock的beaconHash!=pbo.getBeaconHash
//                // 第一块的beanconHash = 创世块的hash
//                log.error("set ret beacon hash =" + blk.getMiner.getTerm + " height=" + pbo.getGossipBlockInfo + " hash=" + Daos.enc.bytesToHexStr(blk.getHeader.getHash.toByteArray()))
//
//                psret.setGossipBlockInfo(pbo.getGossipBlockInfo)
//                psret.setGossipMinerInfo(GossipMiner.newBuilder().setBcuid(blk.getMiner.getNid)
//                  .setCurBlockHash(Daos.enc.bytesToHexStr(blk.getHeader.getHash.toByteArray()))
//                  .setBlockExtrData(blk.getMiner.getBits)
//                  .setBeaconHash(blk.getMiner.getTerm)
//                  .setCurBlock(pbo.getGossipBlockInfo))
//
//                psret.setVn(pbo.getVn.toBuilder().setBeaconHash(blk.getMiner.getTerm).setVrfRandseeds(blk.getMiner.getBits));
//              }
//              BeaconGossip.offerMessage(psret);
//            }

            //              }
          }
        } else {
          // 其它节点
          // 返回自己的信息网络中节点查询: (DN -> PN -> None)
          network.nodeByBcuid(pack.getFrom()) match {
            case network.noneNode => {
//              log.info("nonenode=" + pack.getFrom() + " msgid=" + pbo.getMessageId+",pack="+pack)
            }
            case n: PNode =>
              addMurCominer(pbo, network);
              val friendNode = pbo.getVn
              if (pbo.getVn.getCurBlock >= VCtrl.curVN().getCurBlock - VConfig.BLOCK_DISTANCE_COMINE && StringUtils.isNotBlank(pbo.getVn.getBcuid)
                && pbo.getVn.getCurBlock >= VCtrl.instance.heightBlkSeen.get - VConfig.BLOCK_DISTANCE_COMINE) {
                // 成为打快节点
                //log.debug("add cominer:" + pbo.getVn.getBcuid + ",blockheight=" + pbo.getVn.getCurBlock + ",cur=" + VCtrl.curVN().getCurBlock);

                if (friendNode.getDoMine) {
                  log.info("put into cominer bcuid=" + friendNode.getBcuid + " address=" + friendNode.getCoAddress + ",height=" + pbo.getVn.getCurBlock);

                  // TODO 判断是否有足够token
                  if (!VConfig.AUTH_NODE_FILTER || VCtrl.haveEnoughToken(friendNode.getCoAddress)) {
                    VCtrl.addCoMiner(friendNode);
                  }
                } else {
                  //                  log.info("remove cominer bcuid=" + friendNode.getBcuid + " address=" + friendNode.getCoAddress);
                  //                  log.info("remove cominer for block not height enough:test2:my.cur=" + VCtrl.curVN().getCurBlock + ",my.lastseenheight=" + VCtrl.instance.heightBlkSeen.get
                  //                    + ",dist.cur=" + pbo.getVn.getCurBlock);

                  //                  VCtrl.removeCoMiner(friendNode.getBcuid);
                }
                // log.debug("current cominer::" + VCtrl.coMinerByUID);
              } else if (friendNode.getDoMine) {
                // log.info("remove cominer bcuid=" + friendNode.getBcuid + " address=" + friendNode.getCoAddress);
                // VCtrl.removeCoMiner(friendNode.getBcuid);

              }

              val psret = PSNodeInfo.newBuilder().setMessageId(pbo.getMessageId).setVn(VCtrl.curVN());

              if (pbo.getGossipBlockInfo > 0) {
                val blks = Daos.chainHelper.listBlockByHeight(pbo.getGossipBlockInfo);
                psret.setGossipBlockInfo(pbo.getGossipBlockInfo)
                if (blks != null && blks.length >= 1) {
                  val blk = blks(0);
//                  log.error("set beacon hash =" + blk.getMiner.getTerm + ",height=" + blk.getHeader.getHeight)

                  psret.setGossipMinerInfo(GossipMiner.newBuilder().setBcuid(blk.getMiner.getNid)
                    .setCurBlockHash(Daos.enc.bytesToHexStr(blk.getHeader.getHash.toByteArray()))
                    .setBlockExtrData(blk.getMiner.getBits)
                    .setBeaconHash(blk.getMiner.getTerm)
                    .setCurBlock(pbo.getGossipBlockInfo))
                  log.info("ret rollback --> getBlockBlock=" + pbo.getGossipBlockInfo + ",blksize=" + blks.length
                    + ",rheight=" + blk.getHeader.getHeight.intValue());
                }
              } else {
                var startBlock = pbo.getVn.getCurBlock;
                while (startBlock > pbo.getVn.getCurBlock - VConfig.SYNC_SAFE_BLOCK_COUNT && startBlock > 0) {
                  val blks = Daos.chainHelper.listBlockByHeight(startBlock);
                  if (blks != null && blks.length == 1) {
                    psret.setSugguestStartSyncBlockId(startBlock);
                    startBlock = -100;
                  } else {
                    startBlock = startBlock - 1;
                  }
                }
                { //add gossip info
                  val blk = Daos.chainHelper.getBlockByHeight(pbo.getVn.getCurBlock);
                  if (blk != null) {
                    psret.setGossipMinerInfo(GossipMiner.newBuilder().setBcuid(blk.getMiner.getNid)
                      .setCurBlockHash(Daos.enc.bytesToHexStr(blk.getHeader.getHash.toByteArray()))
                      .setBlockExtrData(blk.getMiner.getBits)
                      .setBeaconHash(blk.getMiner.getTerm)
                      .setCurBlock(pbo.getVn.getCurBlock))
                  }
                }
              }
              // log.info("pbo=" + pbo + " msgid=" + pbo.getMessageId);
              if (pbo.getIsQuery) {
                psret.setIsQuery(false);
                network.postMessage("INFVRF", Left(psret.build()), pbo.getMessageId, n._bcuid);
              }
            case _ =>
              {
                log.info("unknown node");
              }
          }
        }
      } catch {
        case e: FBSException => {
          log.error("error:", e);
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

  def addMurCominer(pbo: PSNodeInfo, network: Network): Unit = {
    pbo.getMursList.toList.map(mur => {
      VCtrl.coMinerByUID.getOrElse(mur.getBcuid, null) match {
        case p if p != null =>
          if (p.getCurBlock < mur.getCurBlock) {
            val bb = p.toBuilder();
            bb.setCurBlock(mur.getCurBlock);
            bb.setCurBlockHash(mur.getCurBlockHash);
            VCtrl.addCoMiner(bb.build())
          }
        case _ =>
          network.nodeByBcuid(mur.getBcuid) match {
            case network.noneNode => {
            }
            case pn: PNode if pn != null => {
              VCtrl.addCoMiner(VNode.newBuilder().setBcuid(mur.getBcuid).setCurBlock(mur.getCurBlock).setCurBlockHash(mur.getCurBlockHash).build());
            }
          }
        //                      VCtrl.addCoMiner(mur)
        //                    }
      }
    })
  }
  //  override def getCmds(): Array[String] = Array(PWCommand.LST.name())
  override def cmd: String = PCommand.INF.name();
}
