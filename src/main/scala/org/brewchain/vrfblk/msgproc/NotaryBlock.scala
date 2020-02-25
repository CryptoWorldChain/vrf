package org.brewchain.vrfblk.msgproc

import org.brewchain.mcore.crypto.BitMap
import org.brewchain.bcrand.model.Bcrand.PSCoinbase
import org.brewchain.p22p.action.PMNodeHelper
import org.brewchain.p22p.utils.LogHelper
import org.brewchain.vrfblk.tasks.BlockMessage
import org.brewchain.vrfblk.tasks.VCtrl
import org.brewchain.vrfblk.Daos
import org.brewchain.p22p.core.Votes

import scala.collection.JavaConverters._
import org.brewchain.p22p.core.Votes.Converge
import org.brewchain.p22p.core.Votes.NotConverge

case class NotaryBlock(pbo: PSCoinbase) extends BlockMessage with PMNodeHelper with BitMap with LogHelper {

  def proc(): Unit = {
    //块确认
    //to notify other.
    MDCSetBCUID(VCtrl.network())
    //log.info("get notaryblock,H=" + pbo.getBlockHeight + ":coadr=" + pbo.getCoAddress + ",DN=" + VCtrl.network().directNodeByIdx.size + ",PN=" + VCtrl.network().pendingNodeByBcuid.size
    //  + ",MN=" + VCtrl.coMinerByUID.size
    //  + ",from=" + pbo.getBcuid
    //  + ",NB=" + new String(pbo.getVrfCodes.toByteArray())
    //  + ",VB=" + pbo.getWitnessBits
    //  + ",VBC=" + mapToHex(pbo.getWitnessBits).bitCount
    //  + ",B=" + pbo.getBlockEntry.getSign
    //  + ",TX=" + pbo.getTxcount);

    //save to db
//    val key = OEntityBuilder.byteKey2OKey(pbo.getBlockEntry.getBlockhashBytes);
//    val value = OEntityBuilder.byteValue2OValue(pbo.getBlockEntry.getBlockMiner.toByteArray()).toBuilder()
//    value.setSecondKey(String.valueOf(pbo.getBlockHeight))
//    value.setInfo(pbo.getBlockEntry.getBlockhash);
//    Daos.vrfvotedb.put(key, value.build());
//    val vs = Daos.vrfvotedb.listBySecondKey(String.valueOf(pbo.getBlockHeight));
//    Votes.vote(vs.get.asScala).PBFTVote(n => {
//      Some(n.getValue.getInfo)
//    }, mapToHex(pbo.getWitnessBits).bitCount) match {
//      case Converge(blockhash) =>
//        //log.info("get merge blockhash :" + blockhash + ",height=" + pbo.getBlockHeight);
//        //Daos.chainHelper.confirmBlock(blockhash.toString());
//      case n: NotConverge =>
//        //log.info("cannot get converge for pbft vote:" + vs.get.size);
//      case n @ _ =>
//        //log.debug("need more results:" + vs.get.size);
//    };

  }
}