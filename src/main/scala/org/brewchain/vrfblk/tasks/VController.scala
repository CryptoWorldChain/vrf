package org.brewchain.vrfblk.tasks

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock

import com.google.common.cache.{ Cache, CacheBuilder }
import org.apache.commons.lang3.StringUtils
import org.brewchain.bcrand.model.Bcrand.{ PBlockEntry, VNode, VNodeState }
import org.brewchain.p22p.action.PMNodeHelper
import org.brewchain.p22p.node.{ Network, Node }
import org.brewchain.p22p.utils.LogHelper
import org.brewchain.vrfblk.Daos
import org.brewchain.vrfblk.utils.{ RandFunction, VConfig }
import org.brewchain.mcore.model.Block.BlockInfo
import scala.collection.JavaConverters._
import scala.collection.mutable.Map
import org.brewchain.mcore.model.Transaction.TransactionInfo
import com.google.protobuf.ByteString
import org.brewchain.mcore.crypto.BitMap
import scala.collection.JavaConversions._
import java.util.ArrayList
import org.brewchain.p22p.action.PMNodeHelper
import java.math.BigInteger
import org.brewchain.mcore.concurrent.AccountInfoWrapper
import org.brewchain.mcore.actuators.tokencontracts20.TokensContract20.TokenRC20Value
import org.brewchain.mcore.tools.bytes.BytesHelper
import org.brewchain.p22p.exception.FBSException

//投票决定当前的节点
case class VRFController(network: Network) extends PMNodeHelper with LogHelper with BitMap {
  def getName() = "VCtrl"

  val VRF_NODE_DB_KEY = "CURRENT_VRF_KEY";
  var cur_vnode: VNode.Builder = VNode.newBuilder()
  var isStop: Boolean = false;

  var waitCreateBlockBeacon: String = ""

  val heightBlkSeen = new AtomicInteger(0);

  val lowMemoryCounter = new AtomicInteger(0);

  def loadNodeFromDB() = {
    val ov = Daos.vrfpropdb.get(VRF_NODE_DB_KEY.getBytes).get
    val root_node = network.root();
    if (ov == null) {
      cur_vnode.setBcuid(root_node.bcuid)
        .setCurBlock(1).setCoAddress(root_node.v_address)
        .setBitIdx(root_node.node_idx)
      Daos.vrfpropdb.put(
        VRF_NODE_DB_KEY.getBytes,
        cur_vnode.build().toByteArray())
    } else {
      cur_vnode.mergeFrom(ov).setBitIdx(root_node.node_idx)
      if (!StringUtils.equals(cur_vnode.getBcuid, root_node.bcuid)) {
        log.warn("load from dnode info not equals with pzp node:{" + cur_vnode.toString().replaceAll("\n", ",") + "},root=" + root_node)
        cur_vnode.setBcuid(root_node.bcuid);
        syncToDB();
      } else {
        // log.info("load from db:OK:{" + cur_vnode.toString().replaceAll("\n", ", ") + "}")
      }
    }
    if (cur_vnode.getCurBlock != Daos.chainHelper.getLastConnectedBlockHeight.intValue()) {
      //log.info("vrf block Info load from DB:c=" +
      //  cur_vnode.getCurBlock + " ==> a=" + Daos.chainHelper.getLastBlockNumber);

      if (Daos.chainHelper.getLastConnectedBlockHeight.intValue() == -0) {
        cur_vnode.setCurBlock(Daos.chainHelper.getLastConnectedBlockHeight.intValue())
        //读取创世块HASH
        log.error("read genesis block");
        cur_vnode.setCurBlockHash(Daos.enc.bytesToHexStr(Daos.chainHelper.getBlockByHeight(0).getHeader.getHash.toByteArray()));
        cur_vnode.setBeaconHash("")
      } else {
        var blk = Daos.chainHelper.getMaxConnectBlock
        if (blk == null) {
          blk = Daos.chainHelper.getLastConnectedBlock;
        }
        cur_vnode.setCurBlock(blk.getHeader.getHeight.intValue())
        //当前块BlockHASH
        cur_vnode.setCurBlockHash(Daos.enc.bytesToHexStr(blk.getHeader.getHash.toByteArray()));
        log.error("set beacon hash=" + blk.getMiner.getTerm);
        cur_vnode.setBeaconHash(blk.getMiner.getTerm)
      }

      heightBlkSeen.set(cur_vnode.getCurBlock);
      syncToDB();
    }
    if (VConfig.RUN_COMINER == 1) {
      //成为BackUp节点，不参与挖矿
      cur_vnode.setDoMine(true)
    }
  }

  def syncToDB() {
    Daos.vrfpropdb.put(
      VRF_NODE_DB_KEY.getBytes,
      cur_vnode.build().toByteArray())
  }

  def updateBlockHeight(block: BlockInfo): Unit = {
    //    updateBlockHeight(block.getHeader.getHeight.intValue, Daos.enc.bytesToHexStr(block.getHeader.getHash.toByteArray()), block.getMiner.getTerm, block.getMiner.getBits, block.getHeader.getTimestamp)

    Daos.blkHelper.synchronized({
      cur_vnode.setCurBlockRecvTime(System.currentTimeMillis())
      cur_vnode.setCurBlockMakeTime(block.getHeader.getTimestamp)
      cur_vnode.setCurBlock(block.getHeader.getHeight.intValue);
      cur_vnode.setCurBlockHash(Daos.enc.bytesToHexStr(block.getHeader.getHash.toByteArray()))
      cur_vnode.setBeaconHash(block.getMiner.getTerm);
      cur_vnode.setVrfRandseeds(block.getMiner.getBits);
      cur_vnode.setCurBlockMinerCoaddr(Daos.enc.bytesToHexStr(block.getMiner.getAddress.toByteArray()))

      syncToDB()
    })

  }

  def startup() = {
    loadNodeFromDB();
    NodeStateSwitcher.offerMessage(new Initialize())
    //    BeaconGossip.offerMessage(PSNodeInfo.newBuilder().setVn(cur_vnode).build());
  }

}

object VCtrl extends LogHelper with BitMap with PMNodeHelper {
  var instance: VRFController = VRFController(null);

  def network(): Network = instance.network;
  val coMinerByUID: Map[String, VNode] = Map.empty[String, VNode];
  val banMinerByUID: Map[String, (Int, Long)] = Map.empty[String, (Int, Long)];
  val syncMinerErrorByBCUID: Map[String, Long] = Map.empty[String, Long];

  val maxApplyWaitBlockHeight = new AtomicInteger(0);
  
  def curVN(): VNode.Builder = instance.cur_vnode

  def haveEnoughToken(nodeAddress: String) = {
    val acct = Daos.accountHandler.getAccount(Daos.enc.hexStrToBytes(nodeAddress));
    if (acct != null) {
      val account = new AccountInfoWrapper(acct);
      account.loadStorageTrie(Daos.mcore.getStateTrie());
      val tokendata = account.getStorage(Daos.enc.hexStrToBytes(VConfig.AUTH_TOKEN));
      if (tokendata != null) {
        val oTokenRC20Value = TokenRC20Value.parseFrom(tokendata)
        if (BytesHelper.bytesToBigInteger(oTokenRC20Value.getBalance.toByteArray()).compareTo(VConfig.AUTH_TOKEN_MIN) >= 0) {
          true
        }
      }
    }
    false
  }

  def addCoMiner(node: VNode) = {
    coMinerByUID.synchronized({
      val lastnode = coMinerByUID.getOrElse(node.getBcuid, null)
      if (lastnode == null) {
        coMinerByUID.put(node.getBcuid, node.toBuilder().setLastBeginMinerTime(System.currentTimeMillis()).build)
      } else {
        coMinerByUID.put(node.getBcuid, node.toBuilder().setLastBeginMinerTime(lastnode.getLastBeginMinerTime).build);
      }
      var cobits = BigInteger.ZERO;
      coMinerByUID.map(f => if (f._2.getBitIdx > 0) cobits = cobits.setBit(f._2.getBitIdx));
      instance.cur_vnode.setCominers(hexToMapping(cobits))
    })
  }
  def changeCoMinerHeight(bcuid: String, height: Int) = {
    coMinerByUID.synchronized({
      val lastnode = coMinerByUID.getOrElse(bcuid, null)
      if (lastnode != null) {
        coMinerByUID.put(bcuid, lastnode.toBuilder().setCurBlock(height).build);
      }
    })
  }
  def removeCoMiner(bcuid: String) = {
    coMinerByUID.synchronized({
      val existnode = coMinerByUID.remove(bcuid);
      if (existnode != null) {
        var cobits = BigInteger.ZERO;
        coMinerByUID.map(f => cobits = cobits.setBit(f._2.getBitIdx));
        instance.cur_vnode.setCominers(hexToMapping(cobits))
      }
    })
  }
  //防止ApplyBlock时节点Make出相同高度的block,或打出beaconHash错误的block
  val blockLock: ReentrantLock = new ReentrantLock();

  def getFastNode(): String = {
    var fastNode = curVN().build();
    coMinerByUID.map { f =>
      if (f._2.getCurBlock > fastNode.getCurBlock) {
        fastNode = f._2;
      }
    }
    fastNode.getBcuid
  }

  def ensureNode(trybcuid: String): Node = {
    val net = instance.network;
    net.nodeByBcuid(trybcuid) match {
      case net.noneNode =>
        net.directNodeByBcuid.values.toList((Math.abs(Math.random() * 100000) % net.directNodes.size).asInstanceOf[Int]);
      case n: Node =>
        n;
    }
  }

  //  def curTermMiner(): PSDutyTermVoteOrBuilder = instance.term_Miner

  def isReady(): Boolean = {
    instance != null && instance.network != null &&
      instance.cur_vnode != null &&
      instance.cur_vnode.getStateValue > VNodeState.VN_INIT_VALUE
  }

  val recentBlocks: Cache[Int, Iterable[PBlockEntry.Builder]] = CacheBuilder.newBuilder().expireAfterWrite(30, TimeUnit.SECONDS)
    .maximumSize(1000).build().asInstanceOf[Cache[Int, Iterable[PBlockEntry.Builder]]]

  def loadFromBlock(block: Int): Iterable[PBlockEntry.Builder] = {
    loadFromBlock(block, false)
  }

  // 判断这个block是否是当前beacon中的第一个块
  def getPriorityBlockInBeaconHash(blk: BlockInfo): BlockInfo = {
    // 如果已经有更高的高度了，直接返回最高块
    // 如果相同高度的区块只有1个，返回true
    val bestblks = Daos.chainHelper.listConnectBlocksByHeight(blk.getHeader.getHeight);
    
    if (bestblks.length == 1) {
      log.info("bestblks.size=1,ready to update blk=" + blk.getHeader.getHeight + " hash=" + Daos.enc.bytesToHexStr(blk.getHeader.getHash.toByteArray()) + " beacon=" + blk.getMiner.getTerm)
      blk
    } else if (bestblks.length > 1) {
      // 判断是否是beaconhash中更高优先级的块
      // 循环所有相同高度的块，排序sleepMS
      val priorityBlk = bestblks.toList.map(p => {
        //        val prevBlock = Daos.chainHelper.getBlockByHash(blk.getHeader.getParentHash.toByteArray());
        //        val blknode = instance.network.nodeByBcuid(prevBlock.getMiner.getNid);
        log.info("get soft fork block:height=" + p.getHeader.getHeight + ",miner=" + p.getMiner.getNid + ",hash=" + Daos.enc.bytesToHexStr(p.getHeader.getHash.toByteArray()));
        p
      }).sortBy(_.getHeader.getTimestamp).get(0)
      log.info("bestblks.size=" + bestblks.size + ",ready to update blk=" + priorityBlk.getHeader.getHeight + " hash=" + Daos.enc.bytesToHexStr(priorityBlk.getHeader.getHash.toByteArray())
        + ",minerfrom=" + priorityBlk.getMiner.getNid)
      priorityBlk
    } else {
      blk;
    }
  }

  def loadFromBlock(block: Int, needBody: Boolean): Iterable[PBlockEntry.Builder] = {
    if (block > curVN().getCurBlock) {
      null
    } else {
      val cacheblks = recentBlocks.getIfPresent(block);
      if (cacheblks != null) {
        cacheblks
      } else {
        val blks = Daos.chainHelper.listBlockByHeight(block);
        if (blks != null && blks.size > 0) {
          val recents = blks.filter(f => if (block == 0 ||
            block < VConfig.SYNC_SAFE_BLOCK_COUNT || block < VCtrl.curVN().getCurBlock - VConfig.SYNC_SAFE_BLOCK_COUNT) {
            //创世块安全块允许直接广播
            true
          } else {
            // 本地block超出安全高度的是否能校验通过，只有通过的才广播??
            //          log.error("height=" + f.getHeader.getHeight + " hash=" + Daos.enc.bytesToHexStr(f.getHeader.getHash.toByteArray()));
            //          val parentBlock = Daos.chainHelper.getBlockByHash(f.getHeader.getParentHash.toByteArray());
            //          val nodebits = if (f.getHeader.getHeight == 1) "" else parentBlock.getMiner.getBits;
            //          val (hash, sign) = RandFunction.genRandHash(Daos.enc.bytesToHexStr(f.getHeader.getParentHash.toByteArray()), parentBlock.getMiner.getTerm, nodebits);
            //          if (hash.equals(f.getMiner.getTerm) || f.getHeader.getHeight == 1) {
            //            true
            //          } else {
            //            true; //false,,直接通过吧，brew 20190507
            //
            //          }
            true;
          }).map(f => {
            // 本地block是否能校验通过，只有通过的才广播
            if (needBody) {
              val txbodys = f.getBody.toBuilder();
              //如果当前body里面有完整txList, 不再需要在重新构建tx, 否则会造成txBody重复
              if (txbodys.getTxsCount == 0 && f.getHeader.getTxHashsCount > 0) {
                val txlist = new ArrayList[TransactionInfo]();
                f.getHeader.getTxHashsList.map(txHash => {
                  val tx = Daos.txHelper.getTransaction(txHash.toByteArray());
                  if (tx != null) {
                    txlist.add(tx);
                  }else{
                    throw new FBSException("tx not found:"+Daos.enc.bytesToHexStr(txHash.toByteArray)+",height="+block+",cur="+curVN().getCurBlock);
                  }
                })
                txbodys.addAllTxs(txlist);
              }

              val b = PBlockEntry.newBuilder().setBlockHeader(f.toBuilder().setBody(txbodys).build().toByteString()).setBlockHeight(block)
              //            recentBlocks.put(block, b);
              b
            } else {
              val b = PBlockEntry.newBuilder().setBlockHeader(f.toBuilder().clearBody().build().toByteString()).setBlockHeight(block)
              //            recentBlocks.put(block, b);
              b
            }

          })
          recentBlocks.put(block, recents);
          recents
        } else {
          log.error("blk not found in AccountDB:" + block);
          null;
        }
      }
    }
  }
  def isBanforMiner(blockHeight: Int, bcuid: String = VCtrl.curVN().getBcuid): Boolean = {
    if (blockHeight > VConfig.BAN_BLOCKS_FOR_NOT_APPLY + 2) {
      val memrec = VCtrl.banMinerByUID.get(bcuid).getOrElse((0, 0l))
      VCtrl.coMinerByUID.size > 3 && blockHeight - memrec._1 < VConfig.BAN_BLOCKS_FOR_NOT_APPLY && System.currentTimeMillis() - memrec._2 < Daos.mcore.getBlockMineTimeoutMs() * 2
    } else {
      false
    }
  }
  def isSafeForMine(): Boolean = {
    VCtrl.instance.lowMemoryCounter.get < VConfig.METRIC_COMINER_LOW_MEMORY_COUNT
  }
}