package org.csc.vrfblk.msgproc

import java.math.BigInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{CountDownLatch, TimeUnit}

import onight.tfw.async.CallBack
import onight.tfw.otransio.api.beans.FramePacket
import org.apache.commons.lang3.StringUtils
import org.csc.account.gens.Blockimpl.AddBlockResponse
import org.csc.bcapi.crypto.BitMap
import org.csc.ckrand.pbgens.Ckrand.{PBlockEntryOrBuilder, PRetGetTransaction, PSCoinbase, PSGetTransaction}
import org.csc.evmapi.gens.Block.BlockEntity
import org.csc.evmapi.gens.Tx.Transaction
import org.csc.p22p.action.PMNodeHelper
import org.csc.p22p.node.{Network, Node}
import org.csc.p22p.utils.LogHelper
import org.csc.vrfblk.Daos
import org.csc.vrfblk.tasks._
import org.csc.vrfblk.utils.{BlkTxCalc, RandFunction, VConfig}

import scala.collection.JavaConverters._
import scala.util.Random

case class ApplyBlock(pbo: PSCoinbase) extends BlockMessage with PMNodeHelper with BitMap with LogHelper {

  val bestheight = new AtomicLong(0);

  val emptyBlock = new AtomicLong(0);

  def saveBlock(b: PBlockEntryOrBuilder, needBody: Boolean = false): (Int, Int) = {
    val block = BlockEntity.newBuilder().mergeFrom(b.getBlockHeader);

    if (!b.getCoinbaseBcuid.equals(VCtrl.curVN().getBcuid)) {
      val startupApply = System.currentTimeMillis();
      if (VCtrl.blockLock.tryLock()) {
        try {
          val vres = Daos.blkHelper.ApplyBlock(block, needBody);
          if (vres.getTxHashsCount > 0) {
            log.info("must sync transaction first,losttxcount=" + vres.getTxHashsCount + ",height=" + b.getBlockHeight)
            // TODO: Sync Transaction  need Sleep for a while First

            val sleepMs = getRandomSleepMS(block.getMiner.getBcuid)
            log.debug(s"sync transaction sleep to reduce press TIME:${sleepMs}")
            Thread.sleep(sleepMs)
            trySyncTransaction(b, needBody, vres)

            (vres.getCurrentNumber.intValue(), vres.getWantNumber.intValue())
          } else if (vres.getCurrentNumber > 0) {
            log.debug("checkMiner --> updateBlockHeight::" + vres.getCurrentNumber.intValue() + ",blk.height=" + b.getBlockHeight + ",wantNumber=" + vres.getWantNumber.intValue())
            //        VCtrl.instance.updateBlockHeight( vres.getCurrentNumber.intValue(), if (vres.getCurrentNumber.intValue() == b.getBlockHeight) b.getSign else null, block.getHeader.getExtraData)
            if (vres.getCurrentNumber.intValue() == b.getBlockHeight) {
              BlkTxCalc.adjustTx(System.currentTimeMillis() - startupApply)
            }

            // VCtrl.instance.updateBlockHeight(b.getBlockHeight, b.getSign, new String(block.getHeader.getExtData.toByteArray()))
            VCtrl.instance.updateBlockHeight(b.getBlockHeight, b.getSign, block.getMiner.getBit)
            (vres.getCurrentNumber.intValue(), vres.getWantNumber.intValue())
          } else {
            (vres.getCurrentNumber.intValue(), vres.getWantNumber.intValue())
          }
        } finally {
          log.info(s"UNLOCK ApplyBlock time:${System.currentTimeMillis()}")
          VCtrl.blockLock.unlock()
        }
      } else {
        log.info(s"apply block lock failed! blockNum:${block.getHeader.getNumber}")
        VCtrl.instance.updateBlockHeight(b.getBlockHeight, b.getSign, new String(block.getHeader.getExtData.toByteArray()))
        (b.getBlockHeight, b.getBlockHeight)
      }
    } else {
      //      log.debug("checkMiner --> updateBlockHeight::" + b.getBlockHeight)
      // VCtrl.instance.updateBlockHeight(b.getBlockHeight, b.getSign, new String(block.getHeader.getExtData.toByteArray()))
      VCtrl.instance.updateBlockHeight(b.getBlockHeight, b.getSign, block.getMiner.getBit)
      (b.getBlockHeight, b.getBlockHeight)
    }
  }

  def tryNotifyState() {
    //    if(VCtrl.instance.b

    var nodeBit = mapToBigInt(VCtrl.network().node_strBits).bigInteger;

    log.debug("tryNotifyState:: pbo=" + pbo + " node_strBits=" + nodeBit)
    if (nodeBit.bitCount() < VCtrl.coMinerByUID.size) {
      log.debug("netBits must change:: bc=" + nodeBit.bitCount() + " size=" + VCtrl.coMinerByUID.size)
      nodeBit = BigInteger.ZERO
      VCtrl.coMinerByUID.map(f => {
        nodeBit = nodeBit.setBit(f._2.getBitIdx);
      })
    }
    val (hash, sign) = RandFunction.genRandHash(pbo.getBlockEntry.getBlockhash, pbo.getBeaconHash, hexToMapping(nodeBit))
    NodeStateSwitcher.offerMessage(new StateChange(sign, hash, pbo.getBeaconHash, hexToMapping(nodeBit)));

  }

  def proc() {
    val cn = VCtrl.curVN();
    MDCSetBCUID(VCtrl.network())
    if (StringUtils.equals(pbo.getCoAddress, cn.getCoAddress) || pbo.getBlockHeight > cn.getCurBlock) {
      val (acceptHeight, blockWant) = saveBlock(pbo.getBlockEntry);
      acceptHeight match {
        case n if n > 0 && n < pbo.getBlockHeight =>
          //                  ret.setResult(CoinbaseResult.CR_PROVEN)
          log.info("applyblock:UU,H=" + pbo.getBlockHeight + ",DB=" + n + ":coadr=" + pbo.getCoAddress
            + ",DN=" + VCtrl.network().directNodeByIdx.size + ",PN=" + VCtrl.network().pendingNodeByBcuid.size
            + ",NB=" + new String(pbo.getVrfCodes.toByteArray())
            + ",VB=" + pbo.getWitnessBits

            + ",B=" + pbo.getBlockEntry.getSign
            + ",TX=" + pbo.getTxcount);

          BeaconGossip.gossipBlocks();
        case n if n > 0 =>
          val vstr =
            if (StringUtils.equals(pbo.getCoAddress, cn.getCoAddress)) {
              "MY"
            } else {
              "OK"
            }
          log.info("applyblock:" + vstr + ",H=" + pbo.getBlockHeight + ",DB=" + n + ":coadr=" + pbo.getCoAddress
            + ",DN=" + VCtrl.network().directNodeByIdx.size + ",PN=" + VCtrl.network().pendingNodeByBcuid.size
            + ",MN=" + VCtrl.coMinerByUID.size
            + ",NB=" + new String(pbo.getVrfCodes.toByteArray())
            + ",VB=" + pbo.getWitnessBits
            + ",B=" + pbo.getBlockEntry.getBlockhash
            + ",TX=" + pbo.getTxcount);
          bestheight.set(n);
          val notaBits = mapToBigInt(pbo.getWitnessBits);
          if (notaBits.testBit(cn.getBitIdx)) {
            VCtrl.network().dwallMessage("CBWVRF", Left(pbo.toBuilder().setBcuid(cn.getBcuid).build()), pbo.getMessageId, '9')
          }
          tryNotifyState();
        case n@_ =>
          log.info("applyblock:NO,H=" + pbo.getBlockHeight + ",DB=" + n + ":coadr=" + pbo.getCoAddress
            + ",DN=" + VCtrl.network().directNodeByIdx.size + ",PN=" + VCtrl.network().pendingNodeByBcuid.size
            + ",NB=" + new String(pbo.getVrfCodes.toByteArray())
            + ",VB=" + pbo.getWitnessBits
            + ",B=" + pbo.getBlockEntry.getSign
            + ",TX=" + pbo.getTxcount);
      }
    }

  }

  def buildReqTx(res: AddBlockResponse): PSGetTransaction.Builder = {
    val reqTx = PSGetTransaction.newBuilder()
    if (res.getTxHashsCount > 0) {
      for (txHash <- res.getTxHashsList.asScala) {
        if (res.getTxHashsCount < 20) {
          log.info(s"request Hash:${txHash}, blockNum=${res.getCurrentNumber}")
        }
        reqTx.addTxHash(txHash)
      }
      reqTx
    } else {
      log.info("no transaction need sync in BlockResponse")
      null
    }
  }

  def getRandomSleepTime(blockHash: String): Long = {
    val stepBits: BigInteger = new BigInteger(s"${VCtrl.coMinerByUID.size}", 10)
    val ranInt = new BigInteger(blockHash, 16).abs()
    val stepRange = ranInt.mod(BigInteger.valueOf(stepBits.bitCount())).intValue()

    (VCtrl.coMinerByUID.size + stepRange) % stepBits.bitCount() * VConfig.SYNC_TX_SLEEP_MS
  }

  def trySyncTransaction(block: PBlockEntryOrBuilder, needBody: Boolean = false, res: AddBlockResponse): Unit = {
    this.synchronized({
      val miner = BlockEntity.parseFrom(block.getBlockHeader)
      val network = VCtrl.network

      var vNetwork = network.directNodeByBcuid.get(miner.getMiner.getBcuid)
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


      val reqTx = buildReqTx(res)
      if (reqTx == null) {
        // saveBlock(block, needBody)
        return
      }

      var rspTx = PRetGetTransaction.newBuilder()

      val cdl = new CountDownLatch(1)
      var notSuccess = true
      var counter = 0
      val start = System.currentTimeMillis()


      log.info(s"SRTVRF start sync transaction go=${vNetwork.get.uri}")

      while (cdl.getCount > 0 && counter < 6 && notSuccess) {
        try {
          if (counter > 3) {
            vNetwork = randomNodeInNetwork(network)
          }
          network.asendMessage("SRTVRF", reqTx.build(), vNetwork.get, new CallBack[FramePacket] {
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
                    val txList = rspTx.getTxContentList.asScala.map(Transaction.newBuilder().mergeFrom(_)).toList
                    Daos.txHelper.syncTransactionBatch(txList.asJava, false, new BigInteger("0").setBit(vNetwork.get.node_idx))
                    notSuccess = false
                    log.debug(s"SRTVRF success !!!cost:${System.currentTimeMillis() - start}")
                    cdl.countDown()
                  } else {
                    log.error(s"SRTVRF no transaction find from ${vNetwork.get.bcuid}, blockMiner=${miner.getMiner.getBcuid}, " +
                      s"SRTVRF back${v}, !!!cost:${System.currentTimeMillis() - start}")
                  }
                }
              }
              catch {
                case t: Throwable => log.warn(s"SRTVRF process failed cost:${System.currentTimeMillis() - start}:", t)
              }
            }

            override def onFailed(e: Exception, v: FramePacket): Unit =
              log.error("apply block need sync transaction, sync transaction failed. error::cost=" +
                s"${System.currentTimeMillis() - start}, targetNode=${vNetwork.get.bcuid}:uri=${vNetwork.get.uri}:", e)
          }
            , '9')

          counter += 1
          cdl.await(40, TimeUnit.SECONDS)

        }
        catch {
          case t: Throwable => log.error("get Transaction failed:", t)
        }
      }
      BlockProcessor.offerMessage(new ApplyBlock(pbo));
      // saveBlock(block, needBody)
    })


    //1. waiting to sync( get distance to sleep)
    //A. getBlockMiner
    //B. sleep distance clc

    //2. how Much Transaction,witch TxHash waiting for sync
    //block.getBody.getTxsList


    //3.rquest success and save transaction

  }

  def randomNodeInNetwork(network: Network): Option[Node] = {
    val self = VCtrl.curVN()
    val indexRange = network.directNodes.size - 1

    Option.apply(
      network.directNodes
        .filter(p => p.bcuid.equals(self.getBcuid))
        .toList.asJava
        .get(Random.nextInt(indexRange))
    )
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