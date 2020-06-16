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
import org.brewchain.bcrand.model.Bcrand.PSCoinbase.ApplyStatus
import org.brewchain.bcrand.model.Bcrand.VNodeState
import onight.tfw.otransio.api.PacketHelper
import org.brewchain.mcore.model.Block.BlockInfo
import org.brewchain.mcore.model.Block.BlockInfo
import org.brewchain.bcrand.model.Bcrand.PBlockEntry

case class ApplyBlock(pbo: PSCoinbase) extends BlockMessage with PMNodeHelper with BitMap with LogHelper {

  val bestheight = new AtomicLong(0);

  val emptyBlock = new AtomicLong(0);
  //b: PBlockEntryOrBuilder
  def saveBlock(block: BlockInfo.Builder, hasBody: Boolean = false): (Int, Int, String) = {
    // val block = BlockEntity.newBuilder().mergeFrom(b.getBlockHeader);
    if (!block.getMiner.getNid.equals(VCtrl.curVN().getBcuid)) {
      val startupApply = System.currentTimeMillis();

      val vres = Daos.blkHelper.syncBlock(block, hasBody);
      if (vres.getSyncTxHash != null && vres.getSyncTxHash.size() > 0 && !hasBody) {
        log.info("must sync transaction first,losttxcount=" + vres.getSyncTxHash.size() + ",height=" + block.getHeader.getHeight)
        // TODO: Sync Transaction  need Sleep for a while First

        val sleepMs = Math.min(VConfig.SYNC_TX_SLEEP_MAX_MS, getRandomSleepMS(block.getMiner.getNid))
        log.debug(s"sync transaction sleep to reduce press TIME:${sleepMs}")

        //同步交易, 同步完成后, 继续保存applyBlock
        trySyncTransaction(block, hasBody, vres, sleepMs)
      } else if (vres.getCurrentHeight > 0) {
        log.debug("checkMiner --> updateBlockHeight::" + vres.getCurrentHeight.intValue() + ",blk.height=" + block.getHeader.getHeight + ",wantNumber=" + vres.getWantHeight.intValue())
        if (vres.getCurrentHeight.intValue() == block.getHeader.getHeight) {
          BlkTxCalc.adjustTx(System.currentTimeMillis() - startupApply)
        }
        val lastBlock = Daos.chainHelper.getLastConnectedBlock
        // 如果lastconnectblock是beaconhash的第一个，就update
        // 如果不是第一个，判断当前是否已经记录了第一个，如果没有记录就update

        if (lastBlock != null) {
          log.info("last connect block equal c=" + lastBlock.getHeader.getHeight + "==>b=" + block.getHeader.getHeight);
          VCtrl.instance.updateBlockHeight(VCtrl.getPriorityBlockInBeaconHash(lastBlock));
          // VCtrl.instance.updateBlockHeight(lastBlock.getHeader.getNumber.intValue, b.getSign, lastBlock.getMiner.getBit)
          (vres.getCurrentHeight.intValue(), vres.getWantHeight.intValue(), lastBlock.getMiner.getBits)
        } else {
          //          VCtrl.instance.updateBlockHeight(vres.getWantHeight.intValue(), "", "", "", 0)
          (vres.getCurrentHeight.intValue(), vres.getWantHeight.intValue(), block.getMiner.getBits)
        }
      } else {
        VCtrl.instance.updateBlockHeight(block.build());
        (vres.getCurrentHeight.intValue(), vres.getWantHeight.intValue(), block.getMiner.getBits)
      }

    } else {
      //      val lastBlock = Daos.chainHelper.getLastConnectedBlock();
      //      if (lastBlock != null && lastBlock.getHeader.getHeight != block.getHeader.getHeight ) {
      //        VCtrl.instance.updateBlockHeight(VCtrl.getPriorityBlockInBeaconHash(lastBlock));
      //        (block.getHeader.getHeight.intValue, block.getHeader.getHeight.intValue, lastBlock.getMiner.getBits)
      //      } else {
      VCtrl.instance.updateBlockHeight(block.build())
      (block.getHeader.getHeight.intValue, block.getHeader.getHeight.intValue, block.getMiner.getBits)
      //      }
    }
  }

  def tryNotifyState(blockHash: String, blockHeight: Int, beaconHash: String, nodeBit: String) {
    log.info("tryNotifyState height=" + blockHeight + " Blockhash=" + blockHash + " BeaconHash=" + beaconHash + " nodeBit=" + nodeBit)
    val (hash, sign) = RandFunction.genRandHash(blockHash, beaconHash, nodeBit)
    NodeStateSwitcher.offerMessage(new StateChange(sign, hash, blockHash, nodeBit, blockHeight));
  }

  val ApplyBlockFP = PacketHelper.genPack("APPLYBLOCK", "__VRF", "", true, 9);
  def proc() {
    val cn = VCtrl.curVN();
    MDCSetBCUID(VCtrl.network())
    //    if (StringUtils.equals(pbo.getCoAddress, cn.getCoAddress) || pbo.getBlockHeight > cn.getCurBlock) {
    if (Runtime.getRuntime.freeMemory() < VConfig.METRIC_COMINER_MIN_FREE_MEMEORY_MB * 1024 * 1024) {
      VCtrl.instance.lowMemoryCounter.incrementAndGet();
      log.info("local system is low memory:counter=" + VCtrl.instance.lowMemoryCounter.get());
    } else if (VCtrl.instance.lowMemoryCounter.get > 1) {
      VCtrl.instance.lowMemoryCounter.decrementAndGet();
    }
    //    log.debug("current free memory.MB = " + (Runtime.getRuntime.freeMemory() / 1024 / 1024) + ",counter=" + VCtrl.instance.lowMemoryCounter.get);
    if (pbo.getBlockHeight < cn.getCurBlock) {
      log.error("cannot apply lower block:height=" + pbo.getBlockHeight + ",local=" + cn.getCurBlock + ",from=" + pbo.getCoAddress);
    } else {
      val block = BlockInfo.newBuilder().mergeFrom(pbo.getBlockEntry.getBlockHeader);

      val (newhash, sign) = RandFunction.genRandHash(Daos.enc.bytesToHexStr(block.getHeader.getHash.toByteArray()), block.getMiner.getTerm, block.getMiner.getBits);
      //      newhash, prevhash, mapToBigInt(netbits).bigInteger
      val ranInt: Int = new BigInteger(newhash, 16).intValue().abs;
      val newNetBits = mapToBigInt(block.getMiner.getBits).bigInteger;
      val (state, newblockbits, natarybits, sleepMs, firstBlockMakerBitIndex) = RandFunction.chooseGroups(ranInt, newNetBits, cn.getBitIdx);
      val blks = Daos.chainHelper.listBlockByHeight(pbo.getBlockHeight);
      val reject =
        if (state == VNodeState.VN_DUTY_NOTARY) {
          // i m a notary.
          //          val blks = Daos.chainHelper.listBlockByHeight(pbo.getBlockHeight);
          if (blks != null && blks.size >= 1) {
            log.info("get soft fork block. reject this:" + pbo.getBlockHeight + ",from=" + pbo.getCoAddress);
            //            val wallmsg = pbo.toBuilder().clearTxbodies().setBcuid(cn.getBcuid).setApplyStatus(ApplyStatus.APPLY_REJECT).clearBlockEntry();
            //            VCtrl.network().wallMessage("CBWVRF", Left(wallmsg.build()), pbo.getMessageId)

            true;
          } else {
            false
          }
          //
        } else {
          false
        }
      val reject_apply = if (blks != null && blks.size >= 1) {
        val stablehash = Daos.vrfvotedb.get(("stable-" + pbo.getBlockHeight).getBytes).get;
        if (stablehash != null) {
          log.error("cannot apply block for already stable block:" + pbo.getBlockHeight + ",hash=" + Daos.enc.bytesToHexStr(stablehash));
          true
        } else {
          false
        }
      } else {
        false
      }

      if (!reject_apply) {

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
              + ",BEMS=" + Daos.mcore.getBlockEpochMS());

            // && BlockProcessor.getQueue.size() < 2
            //        if (pbo.getBlockHeight >= (VCtrl.curVN().getCurBlock - VConfig.BLOCK_DISTANCE_COMINE)
            //          && (System.currentTimeMillis() - BeaconGossip.lastGossipTime) >= Daos.mcore.getBlockEpochMS()) {
            //          log.info("cannot apply block, do gossip");
            //        } else {

            //          val (newhash, sign) = RandFunction.genRandHash(Daos.enc.bytesToHexStr(block.getHeader.getHash.toByteArray()), block.getMiner.getTerm, block.getMiner.getBits);
            //          //      newhash, prevhash, mapToBigInt(netbits).bigInteger
            //          val ranInt: Int = new BigInteger(newhash, 16).intValue().abs;
            //          val newNetBits = mapToBigInt(block.getMiner.getBits).bigInteger;
            //          val (state, newblockbits, natarybits, sleepMs, firstBlockMakerBitIndex) = RandFunction.chooseGroups(ranInt, newNetBits, cn.getBitIdx);
            if (state == VNodeState.VN_DUTY_BLOCKMAKERS) {
              log.warn("try to notify other nodes because not apply ok,waitms = " + VConfig.MAX_WAITMS_WHEN_LAST_BLOCK_NOT_APPLY);
              val sleepMS = System.currentTimeMillis() + VConfig.MAX_WAITMS_WHEN_LAST_BLOCK_NOT_APPLY;
//              Daos.ddc.executeNow(ApplyBlockFP, new Runnable() {
//                def run() {
//                  do {
//                    //while (sleepMS > 0 && (Daos.chainHelper.getLastBlockNumber() == 0 || Daos.chainHelper.GetConnectBestBlock() == null || blkInfo.preBeaconHash.equals(Daos.chainHelper.GetConnectBestBlock().getMiner.getTermid))) {
//                    Thread.sleep(Math.max(1, Math.min(100, sleepMS - System.currentTimeMillis())));
//                  } while (sleepMS > System.currentTimeMillis() && VCtrl.curVN().getCurBlock < pbo.getBlockHeight);
////                  if (VCtrl.curVN().getCurBlock < pbo.getBlockHeight) {
////                    //              log.error("MPRealCreateBlock:start");
////
////                    val wallmsg = pbo.toBuilder().clearTxbodies().setBlockEntry(PBlockEntry.newBuilder().setBlockhash(pbo.getBlockEntry.getBlockhash)).setBcuid(cn.getBcuid).setApplyStatus(ApplyStatus.APPLY_NOT_CONTINUE);
////                    log.info("ban for create block from=" + pbo.getBlockHeight);
////                    VCtrl.banMinerByUID.put(cn.getBcuid, (pbo.getBlockHeight, System.currentTimeMillis()))
////                    VCtrl.network().wallMessage("CBWVRF", Left(wallmsg.build()), pbo.getMessageId)
////                  } else {
////                    //                    log.info("still try to  create block");
////                    //                    BeaconGossip.tryGossip("");
////                  }
//                }
//              })
            } else {
              log.info("cannot apply block, do gossip");
              BeaconGossip.tryGossip("apply_uu,h=" + pbo.getBlockHeight);
            }
          //        }

          //          在这里启动监听线程，如果每法apply，则放弃打快
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
            //if (notaBits.testBit(cn.getBitIdx)) {
            val wallmsg = pbo.toBuilder().clearTxbodies().setBcuid(cn.getBcuid).setBlockEntry(PBlockEntry.newBuilder().setBlockhash(pbo.getBlockEntry.getBlockhash));
            if (VCtrl.instance.lowMemoryCounter.get > VConfig.METRIC_COMINER_LOW_MEMORY_COUNT) {
              log.error("ban for miner. low memory counter too large " + VCtrl.instance.lowMemoryCounter.get + "==>max=" + VConfig.METRIC_COMINER_LOW_MEMORY_COUNT);
              wallmsg.setApplyStatus(ApplyStatus.APPLY_OK_LOW_MEMORY);
            }
            if ((cn.getState == VNodeState.VN_DUTY_SYNC || cn.getState == VNodeState.VN_DUTY_BLOCKMAKERS || cn.getState == VNodeState.VN_DUTY_NOTARY)
              && cn.getCurBlock + VConfig.BLOCK_DISTANCE_COMINE * 3 >= VCtrl.instance.heightBlkSeen.get) {

              if (state == VNodeState.VN_DUTY_NOTARY) {
                if (reject) {
                  VCtrl.network().wallMessage("CBWVRF", Left(wallmsg.setApplyStatus(ApplyStatus.APPLY_REJECT).build()), pbo.getMessageId)
                } else {
                  VCtrl.network().wallMessage("CBWVRF", Left(wallmsg.setApplyStatus(ApplyStatus.APPLY_NOTARY_OK).build()), pbo.getMessageId)
                }
              } else {
                VCtrl.network().wallMessage("CBWVRF", Left(wallmsg.build()), pbo.getMessageId)
              }

            }
            //}
            if (pbo.getBlockHeight >= VCtrl.curVN().getCurBlock + VConfig.BLOCK_DISTANCE_NETBITS && cn.getState != VNodeState.VN_SYNC_BLOCK) {
              log.info(s"block to large,blockh=${pbo.getBlockHeight},curblock=${VCtrl.curVN().getCurBlock},saveoffset=${VConfig.BLOCK_DISTANCE_NETBITS} , need to gossip");
              //              BeaconGossip.tryGossip("block");
            }

            tryNotifyState(VCtrl.curVN().getCurBlockHash, VCtrl.curVN().getCurBlock, VCtrl.curVN().getBeaconHash, VCtrl.curVN().getVrfRandseeds);
          //          tryNotifyState(Daos.enc.bytesToHexStr(block.getHeader.getHash.toByteArray()), block.getHeader.getHeight.intValue, block.getMiner.getTerm, nodebit);
          case n @ _ =>
            log.error("applyblock:NO,H=" + pbo.getBlockHeight + ",DB=" + n + ":coadr=" + pbo.getCoAddress
              + ",DN=" + VCtrl.network().directNodeByIdx.size + ",PN=" + VCtrl.network().pendingNodeByBcuid.size
              + ",NB=" + new String(pbo.getVrfCodes.toByteArray())
              + ",VB=" + pbo.getWitnessBits
              + ",B=" + pbo.getBlockEntry.getSign
              + ",TX=" + pbo.getTxcount);
            if (pbo.getBlockHeight > VCtrl.curVN().getCurBlock - VConfig.BLOCK_DISTANCE_COMINE
              && (System.currentTimeMillis() - BeaconGossip.lastGossipTime) >= Daos.mcore.getBlockEpochMS()) {
              log.info("cannot apply block, do gossip");
              BeaconGossip.tryGossip("apply_no,h=" + pbo.getBlockHeight);
            }
        }
        //更新PZP节点信息，用于区块浏览器查看块高
        VCtrl.network().root().counter.blocks.set(VCtrl.curVN().getCurBlock)
      }
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
  def trySyncTransaction(miner: BlockInfo.Builder, hasBody: Boolean = false, res: BlockSyncMessage, sleepMs: Long): (Int, Int, String) = {
    //def trySyncTransaction(block: PBlockEntryOrBuilder, needBody: Boolean = false, res: AddBlockResponse): Unit = {
    //    this.synchronized({
    // val miner = BlockEntity.parseFrom(block.getBlockHeader)
    val network = VCtrl.network()

    var fastFromBcuid = miner.getMiner.getNid;

    var vNetwork = network.directNodeByBcuid.get(fastFromBcuid)
    log.info("pick node=" + vNetwork)

    var sleepw = sleepMs;
    var lackList = res.getSyncTxHash.asScala
    while (sleepw > 100 && lackList.size > 0) {
      //check.
      lackList = lackList.filter(f =>
        {
          val tx = Daos.txHelper.getTransaction(f)
          tx == null
        });
      sleepw -= 10;
      Thread.sleep(10)
    }
    if (lackList.size <= 0) {
      log.info("no need to get tx body:" + res.getSyncTxHash.size() + ",sleep=" + sleepw + "/" + sleepMs);
      return saveBlock(miner, true)
    }
    log.info("need to get tx body:" + lackList.size + "/" + res.getSyncTxHash.size() + ",sleep=" + sleepw + "/" + sleepMs);
    val reqTx = buildReqTx(lackList)

    var rspTx = PRetGetTransaction.newBuilder()

    var notSuccess = true
    var counter = 0

    var trySaveRes: (Int, Int, String) = (res.getCurrentHeight.intValue(), res.getWantHeight.intValue(), "")

    while (counter < 6 && notSuccess) {
      try {
        val cdl = new CountDownLatch(1)
        //        if (counter > 3) {
        //          vNetwork = randomNodeInNetwork(network)
        val randList = Buffer.empty[String]
        val randSameLocList = Buffer.empty[String]
        VCtrl.coMinerByUID.filter(f => (!f._1.equals(VCtrl.curVN().getBcuid) && f._2.getCurBlock >= miner.getHeader.getHeight && !fastFromBcuid.equals(f._1))).map(f => {
          val bcuid = f._1;
          val vnode = f._2;
          val locktime = VCtrl.syncMinerErrorByBCUID.get(bcuid).getOrElse(0L)
          if (System.currentTimeMillis() - locktime > VConfig.BLOCK_DISTANCE_WAITMS) {
            if (StringUtils.equals(VCtrl.network().nodeByBcuid(bcuid).loc_gwuris, VCtrl.network().root().loc_gwuris)) {
              //                fastFromBcuid = bcuid;
              randSameLocList.append(bcuid)
            } else {
              randList.append(bcuid);
            }
          }
        })
        if (randSameLocList.size > 0) {
          fastFromBcuid = randSameLocList(counter % randSameLocList.size);
        } else if (randList.size > 0) {
          fastFromBcuid = randList(counter % randList.size)
        }
        vNetwork = network.directNodeByBcuid.get(fastFromBcuid)
        //        }
        val start = System.currentTimeMillis()
        log.info(s"SRTVRF start sync transaction go=${vNetwork.get.uri},fastFromBcuid=" + fastFromBcuid + ",miner=" + miner.getMiner.getNid)
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
                  val startSave = System.currentTimeMillis()
                  val txList = rspTx.getTxContentList.asScala.map(TransactionInfo.newBuilder().mergeFrom(_).build()).toList
                  Daos.txHelper.syncTransactionBatch(txList.asJava, true, new BigInteger("0").setBit(vNetwork.get.node_idx))
                  notSuccess = false
                  log.info(s"SRTVRF success height:${miner.getHeader.getHeight} total:${System.currentTimeMillis() - start} save:${System.currentTimeMillis() - startSave},from=" + fastFromBcuid)

                  if (txList.length == reqTx.getTxHashCount) {
                    log.info("sync tx complete =" + trySaveRes)
                    trySaveRes = saveBlock(miner, true)
                  } else {
                    log.error("sync tx error =" + trySaveRes + ",reqTx.count=" + reqTx.getTxHashCount + ",txlist=" + txList.length)

                  }
                  cdl.countDown()

                } else if (rspTx != null && rspTx.getRetCode == -2) {
                  log.error(s"SRTVRF low memory warning find from ${vNetwork.get.bcuid}, blockMiner=${miner.getMiner.getNid}, " +
                    s"SRTVRF back${v}, !!!cost:${System.currentTimeMillis() - start}")
                  VCtrl.syncMinerErrorByBCUID.put(vNetwork.get.bcuid, System.currentTimeMillis());
                } else {
                  log.error(s"SRTVRF no transaction find from ${vNetwork.get.bcuid}, blockMiner=${miner.getMiner.getNid}, " +
                    s"SRTVRF back${v}, !!!cost:${System.currentTimeMillis() - start}")

                }
              } else {
                log.info(s"success get tx from ${vNetwork.get.bcuid}, blockMiner=${miner.getMiner.getNid}, cost=" + (System.currentTimeMillis() - start));
              }
            } catch {
              case t: Throwable => log.warn(s"SRTVRF process failed cost:${System.currentTimeMillis() - start}:", t)
            } finally {
              cdl.countDown()
            }
          }

          override def onFailed(e: Exception, v: FramePacket): Unit =
            {
              log.error("apply block need sync transaction, sync transaction failed. error::cost=" +
                s"${System.currentTimeMillis() - start}, targetNode=${vNetwork.get.bcuid}:uri=${vNetwork.get.uri}:", e)
              cdl.countDown()
            }
        }, '9')

        counter += 1
        while (!cdl.await(20, TimeUnit.SECONDS) && (System.currentTimeMillis() - start) < 20 * 1000) {
          log.info("still to wait sync result:post=" + (System.currentTimeMillis() - start));
        }
        log.info(s"finished get tx from ${vNetwork.get.bcuid}, blockMiner=${miner.getMiner.getNid}, cost=" + (System.currentTimeMillis() - start));
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