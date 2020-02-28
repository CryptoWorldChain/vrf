package org.brewchain.vrfblk.msgproc

import java.math.BigInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ CountDownLatch, TimeUnit }

import onight.tfw.async.CallBack
import onight.tfw.otransio.api.beans.FramePacket
import org.apache.commons.lang3.StringUtils
import org.brewchain.mcore.crypto.BitMap
import org.brewchain.bcrand.model.Bcrand.{ PBlockEntryOrBuilder, PRetGetTransaction, PSCoinbase, PSGetTransaction }
import org.brewchain.mcore.model.Block.BlockInfo
import org.brewchain.mcore.model.Transaction.TransactionInfo
import org.brewchain.p22p.action.PMNodeHelper
import org.brewchain.p22p.node.{ Network, Node }
import org.brewchain.p22p.utils.LogHelper
import org.brewchain.vrfblk.Daos
import org.brewchain.vrfblk.tasks._
import org.brewchain.vrfblk.utils.{ BlkTxCalc, RandFunction, VConfig }
import org.brewchain.mcore.bean.BlockSyncMessage

import scala.collection.JavaConverters._
import scala.util.Random
import com.google.protobuf.ByteString
import scala.collection.mutable.Buffer

case class ApplyBlock(pbo: PSCoinbase) extends BlockMessage with PMNodeHelper with BitMap with LogHelper {

  val bestheight = new AtomicLong(0);

  val emptyBlock = new AtomicLong(0);
  //b: PBlockEntryOrBuilder
  def saveBlock(block: BlockInfo.Builder, needBody: Boolean = false): (Int, Int, String) = {
    // val block = BlockEntity.newBuilder().mergeFrom(b.getBlockHeader);
    if (!block.getMiner.getNid.equals(VCtrl.curVN().getBcuid)) {
      val startupApply = System.currentTimeMillis();

      val vres = Daos.blkHelper.syncBlock(block, needBody);
      if (vres.getSyncTxHash != null && vres.getSyncTxHash.size() > 0) {
        log.info("must sync transaction first,losttxcount=" + vres.getSyncTxHash.size() + ",height=" + block.getHeader.getHeight)
        // TODO: Sync Transaction  need Sleep for a while First

        val sleepMs = Math.min(VConfig.SYNC_TX_SLEEP_MAX_MS, getRandomSleepMS(block.getMiner.getNid))
        log.debug(s"sync transaction sleep to reduce press TIME:${sleepMs}")

        //同步交易, 同步完成后, 继续保存applyBlock
        trySyncTransaction(block, needBody, vres, sleepMs)
      } else if (vres.getCurrentHeight > 0) {
        log.debug("checkMiner --> updateBlockHeight::" + vres.getCurrentHeight.intValue() + ",blk.height=" + block.getHeader.getHeight + ",wantNumber=" + vres.getWantHeight.intValue())
        if (vres.getCurrentHeight.intValue() == block.getHeader.getHeight) {
          BlkTxCalc.adjustTx(System.currentTimeMillis() - startupApply)
        }
        val lastBlock = Daos.chainHelper.getLastConnectedBlock
        // 如果lastconnectblock是beaconhash的第一个，就update
        // 如果不是第一个，判断当前是否已经记录了第一个，如果没有记录就update
        
        if (lastBlock != null) {
          VCtrl.instance.updateBlockHeight(VCtrl.getPriorityBlockInBeaconHash(lastBlock));
          // VCtrl.instance.updateBlockHeight(lastBlock.getHeader.getNumber.intValue, b.getSign, lastBlock.getMiner.getBit)
          (vres.getCurrentHeight.intValue(), vres.getWantHeight.intValue(), lastBlock.getMiner.getBits)
        } else {
          VCtrl.instance.updateBlockHeight(vres.getWantHeight.intValue(), "", "", "", 0)
          (vres.getCurrentHeight.intValue(), vres.getWantHeight.intValue(), block.getMiner.getBits)
        }
      } else {
        (vres.getCurrentHeight.intValue(), vres.getWantHeight.intValue(), block.getMiner.getBits)
      }

    } else {
      val lastBlock = Daos.chainHelper.getLastConnectedBlock();
      if (lastBlock != null) {
        VCtrl.instance.updateBlockHeight(VCtrl.getPriorityBlockInBeaconHash(lastBlock));
        (block.getHeader.getHeight.intValue, block.getHeader.getHeight.intValue, lastBlock.getMiner.getBits)
      } else {
        VCtrl.instance.updateBlockHeight(block.build())
        (block.getHeader.getHeight.intValue, block.getHeader.getHeight.intValue, "")
      }
    }
  }

  def tryNotifyState(blockHash: String, blockHeight: Int, beaconHash: String, nodeBit: String) {
    log.info("tryNotifyState height=" + blockHeight + " Blockhash=" + blockHash + " BeaconHash=" + beaconHash + " nodeBit=" + nodeBit)
    val (hash, sign) = RandFunction.genRandHash(blockHash, beaconHash, nodeBit)
    NodeStateSwitcher.offerMessage(new StateChange(sign, hash, beaconHash, nodeBit, blockHeight));
  }

  def proc() {
    val cn = VCtrl.curVN();
    MDCSetBCUID(VCtrl.network())
    if (StringUtils.equals(pbo.getCoAddress, cn.getCoAddress) || pbo.getBlockHeight > cn.getCurBlock) {
      val block = BlockInfo.newBuilder().mergeFrom(pbo.getBlockEntry.getBlockHeader);
      val (acceptHeight, blockWant, nodebit) = saveBlock(block, block.hasBody());
      acceptHeight match {
        case n if n > 0 && n < pbo.getBlockHeight =>
          //                  ret.setResult(CoinbaseResult.CR_PROVEN)
          log.error("applyblock:UU,H=" + pbo.getBlockHeight + ",DB=" + n + ":coadr=" + pbo.getCoAddress
            + ",DN=" + VCtrl.network().directNodeByIdx.size + ",PN=" + VCtrl.network().pendingNodeByBcuid.size
            + ",NB=" + new String(pbo.getVrfCodes.toByteArray())
            + ",VB=" + pbo.getWitnessBits
            + ",MB=" + cn.getCominers
            + ",B=" + pbo.getBlockEntry.getSign
            + ",TX=" + pbo.getTxcount
            + ",CBH=" + VCtrl.curVN().getCurBlock
            + ",QS=" + BlockProcessor.getQueue.size()
            + ",C=" + (System.currentTimeMillis() - BeaconGossip.lastGossipTime)
            + ",BEMS=" + VConfig.BLK_EPOCH_MS);

          // && BlockProcessor.getQueue.size() < 2
          if (pbo.getBlockHeight >= (VCtrl.curVN().getCurBlock - VConfig.BLOCK_DISTANCE_NETBITS)
            && (System.currentTimeMillis() - BeaconGossip.lastGossipTime) >= VConfig.BLK_EPOCH_MS) {
            log.info("cannot apply block, do gossip");
            BeaconGossip.gossipBlocks();
          }
        case n if n > 0 =>
          val vstr =
            if (StringUtils.equals(pbo.getCoAddress, cn.getCoAddress)) {
              "MY"
            } else {
              "OK"
            }
          log.error("applyblock:" + vstr + ",H=" + pbo.getBlockHeight + ",DB=" + n + ":coadr=" + pbo.getCoAddress
            + ",DN=" + VCtrl.network().directNodeByIdx.size + ",PN=" + VCtrl.network().pendingNodeByBcuid.size
            + ",MN=" + VCtrl.coMinerByUID.size
            + ",NB=" + new String(pbo.getVrfCodes.toByteArray())
            + ",MB=" + cn.getCominers
            + ",VB=" + pbo.getWitnessBits
            + ",B=" + pbo.getBlockEntry.getBlockhash
            + ",TX=" + pbo.getTxcount);
          bestheight.set(n);
          val notaBits = mapToBigInt(pbo.getWitnessBits);
          if (notaBits.testBit(cn.getBitIdx)) {
            VCtrl.network().dwallMessage("CBWVRF", Left(pbo.toBuilder().setBcuid(cn.getBcuid).build()), pbo.getMessageId, '9')
          }
          if (pbo.getBlockHeight >= VCtrl.curVN().getCurBlock + VConfig.BLOCK_DISTANCE_NETBITS) {
            log.info(s"block to large,blockh=${pbo.getBlockHeight},curblock=${VCtrl.curVN().getCurBlock},saveoffset=${VConfig.BLOCK_DISTANCE_NETBITS} , need to gossip");
            BeaconGossip.gossipBlocks();
          }

           tryNotifyState(VCtrl.curVN().getCurBlockHash,VCtrl.curVN().getCurBlock,VCtrl.curVN().getBeaconHash, nodebit);
//          tryNotifyState(Daos.enc.bytesToHexStr(block.getHeader.getHash.toByteArray()), block.getHeader.getHeight.intValue, block.getMiner.getTerm, nodebit);
        case n @ _ =>
          log.error("applyblock:NO,H=" + pbo.getBlockHeight + ",DB=" + n + ":coadr=" + pbo.getCoAddress
            + ",DN=" + VCtrl.network().directNodeByIdx.size + ",PN=" + VCtrl.network().pendingNodeByBcuid.size
            + ",NB=" + new String(pbo.getVrfCodes.toByteArray())
            + ",VB=" + pbo.getWitnessBits
            + ",B=" + pbo.getBlockEntry.getSign
            + ",TX=" + pbo.getTxcount);
          if (pbo.getBlockHeight > VCtrl.curVN().getCurBlock - VConfig.BLOCK_DISTANCE_COMINE
            && (System.currentTimeMillis() - BeaconGossip.lastGossipTime) >= VConfig.BLK_EPOCH_MS) {
            log.info("cannot apply block, do gossip");
            BeaconGossip.gossipBlocks();
          }
      }
      //更新PZP节点信息，用于区块浏览器查看块高
      VCtrl.network().root().counter.blocks.set(VCtrl.curVN().getCurBlock)
    }

  }

  def buildReqTx(lackList: Buffer[Array[Byte]]): PSGetTransaction.Builder = {
    val reqTx = PSGetTransaction.newBuilder()
    lackList.map(f => {
      val txHash = Daos.enc.bytesToHexStr(f);
      reqTx.addTxHash(txHash)
    })
    return reqTx;
  }

  def getRandomSleepTime(blockHash: String): Long = {
    val stepBits: BigInteger = new BigInteger(s"${VCtrl.coMinerByUID.size}", 10)
    val ranInt = new BigInteger(blockHash, 16).abs()
    val stepRange = ranInt.mod(BigInteger.valueOf(stepBits.bitCount())).intValue()

    (VCtrl.coMinerByUID.size + stepRange) % stepBits.bitCount() * VConfig.SYNC_TX_SLEEP_MS
  }

  //block: PBlockEntryOrBuilder
  def trySyncTransaction(miner: BlockInfo.Builder, needBody: Boolean = false, res: BlockSyncMessage, sleepMs: Long): (Int, Int, String) = {
    //def trySyncTransaction(block: PBlockEntryOrBuilder, needBody: Boolean = false, res: AddBlockResponse): Unit = {
    //    this.synchronized({
    // val miner = BlockEntity.parseFrom(block.getBlockHeader)
    val network = VCtrl.network()

    var vNetwork = network.directNodeByBcuid.get(miner.getMiner.getNid)
    if (vNetwork.isEmpty) {
      log.info("not find miner in network")
      val miners = VCtrl.coMinerByUID
        .find(p => p._2.getCurBlock > VCtrl.curVN().getCurBlock)
      if (miners.isDefined) {
        vNetwork = network.directNodeByBcuid.get(miners.get._1)
      } else {
        val randomNode = randomNodeInNetwork(network)
        vNetwork = randomNode
      }
    }
    log.debug("pick node=" + vNetwork)

    var sleepw = sleepMs;
    var lackList = res.getSyncTxHash.asScala
    while (sleepw > 100 && lackList.size > 0) {
      //check.
      lackList = lackList.filter(f =>
        {
          val tx = Daos.txHelper.getTransaction(f)
          tx == null
        });
      sleepw -= 100;
      Thread.sleep(100)
    }
    if (lackList.size <= 0) {
      log.info("no need to get tx body:" + res.getSyncTxHash.size() + ",sleep=" + sleepw + "/" + sleepMs);
      return saveBlock(miner, needBody)
    }
    log.info("need to get tx body:" + lackList.size + "/" + res.getSyncTxHash.size() + ",sleep=" + sleepw + "/" + sleepMs);
    val reqTx = buildReqTx(lackList)

    var rspTx = PRetGetTransaction.newBuilder()

    val cdl = new CountDownLatch(1)
    var notSuccess = true
    var counter = 0
    val start = System.currentTimeMillis()
    var trySaveRes: (Int, Int, String) = (res.getCurrentHeight.intValue(), res.getWantHeight.intValue(), "")

    log.info(s"SRTVRF start sync transaction go=${vNetwork.get.uri}")

    while (cdl.getCount > 0 && counter < 6 && notSuccess) {
      try {
        if (counter > 3) {
          vNetwork = randomNodeInNetwork(network)
        }
        network.sendMessage("SRTVRF", reqTx.build(), vNetwork.get, new CallBack[FramePacket] {
          override def onSuccess(v: FramePacket): Unit = {
            try {
              if (notSuccess) {
                rspTx = if (v.getBody != null) {
                  PRetGetTransaction.newBuilder().mergeFrom(v.getBody)
                } else {
                  log.info(s"no transaction find from ${vNetwork.get.bcuid}")
                  null
                }
                if (rspTx != null && rspTx.getTxContentCount > 0) {
                  val startSave = System.currentTimeMillis()
                  val txList = rspTx.getTxContentList.asScala.map(TransactionInfo.newBuilder().mergeFrom(_).build()).toList
                  Daos.txHelper.syncTransactionBatch(txList.asJava, false, new BigInteger("0").setBit(vNetwork.get.node_idx))
                  notSuccess = false
                  log.debug(s"SRTVRF success height:${miner.getHeader.getHeight} total:${System.currentTimeMillis() - start} save:${System.currentTimeMillis() - startSave}")
                  trySaveRes = saveBlock(miner, needBody)
                  cdl.countDown()

                  log.debug("sync tx complete =" + trySaveRes)
                } else {
                  log.error(s"SRTVRF no transaction find from ${vNetwork.get.bcuid}, blockMiner=${miner.getMiner.getNid}, " +
                    s"SRTVRF back${v}, !!!cost:${System.currentTimeMillis() - start}")
                }
              }
            } catch {
              case t: Throwable => log.warn(s"SRTVRF process failed cost:${System.currentTimeMillis() - start}:", t)
            }
          }

          override def onFailed(e: Exception, v: FramePacket): Unit =
            log.error("apply block need sync transaction, sync transaction failed. error::cost=" +
              s"${System.currentTimeMillis() - start}, targetNode=${vNetwork.get.bcuid}:uri=${vNetwork.get.uri}:", e)
        }, '9')

        counter += 1
        cdl.await(40, TimeUnit.SECONDS)
      } catch {
        case t: Throwable => log.error("get Transaction failed:", t)
      }
    }

    return trySaveRes
    // BlockProcessor.offerMessage(new ApplyBlock(pbo));
    // saveBlock(block, needBody)
    //(res.getCurrentNumber.intValue(), res.getWantNumber.intValue(), "")
    //    })

    //1. waiting to sync( get distance to sleep)
    //A. getBlockMiner
    //B. sleep distance clc

    //2. how Much Transaction,witch TxHash waiting for sync
    //block.getBody.getTxsList

    //3.rquest success and save transaction

  }

  def randomNodeInNetwork(network: Network): Option[Node] = {
    val self = VCtrl.curVN()
    val temp: List[Node] = network.directNodes
      .filter(p => !p.bcuid.equals(self.getBcuid))
      .toList
    val indexRange = temp.size;

    if (indexRange < 0 || temp.isEmpty) {
      Option.empty
    } else if (indexRange == 0) {
      Option.apply(temp.asJava.get(0))
    } else {
      Option.apply(temp.asJava.get(Random.nextInt(indexRange)))
    }
  }

  def getRandomSleepMS(minerBcuid: String): Long = {
    var miner: Int = 0
    val self = VCtrl.curVN().getBitIdx

    val indexs = VCtrl.coMinerByUID.map(p => {
      if (p._2.getBcuid.equals(minerBcuid)) {
        miner = p._2.getBitIdx
      }
      p._2.getBitIdx
    }).toList.sorted

    val (min, max) = if (self > miner) {
      (miner, self)
    } else {
      (self, miner)
    }

    var step = 0;

    for (index <- indexs) {
      if (index >= min && index < max) {
        step += 1
      }
    }
    step * VConfig.SYNC_TX_SLEEP_MS
  }

}