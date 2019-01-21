package org.csc.vrfblk

import scala.beans.BeanProperty

import org.apache.felix.ipojo.annotations.Provides
import org.csc.account.api.IBlockHelper
import org.csc.account.api.IChainHelper
import org.csc.account.api.IConfirmTxMap
import org.csc.account.api.ITransactionHelper
import org.csc.bcapi.EncAPI
import org.csc.bcapi.backend.ODBSupport
import org.csc.dposblk.pbgens.Dposblock.PModule
import org.csc.p22p.core.PZPCtrl
import org.fc.zippo.dispatcher.IActorDispatcher

import com.google.protobuf.Message

import onight.oapi.scala.commons.PBUtils
import onight.oapi.scala.commons.SessionModules
import onight.oapi.scala.traits.OLog
import onight.osgi.annotation.NActorProvider
import onight.tfw.ntrans.api.ActorService
import onight.tfw.ntrans.api.annotation.ActorRequire
import onight.tfw.ojpa.api.DomainDaoSupport
import onight.tfw.ojpa.api.annotations.StoreDAO
import onight.tfw.ojpa.api.IJPAClient

abstract class PSMDPoSNet[T <: Message] extends SessionModules[T] with PBUtils with OLog {
  override def getModule: String = PModule.DOB.name()
}

@NActorProvider
@Provides(specifications = Array(classOf[ActorService], classOf[IJPAClient]))
class Daos extends PSMDPoSNet[Message] with ActorService {

  @StoreDAO(target = "bc_bdb", daoClass = classOf[ODSDPoSDao])
  @BeanProperty
  var dposdb: ODBSupport = null

  @StoreDAO(target = "bc_bdb", daoClass = classOf[ODSDPoSVoteDao])
  @BeanProperty
  var dposvotedb: ODBSupport = null

  //  @StoreDAO(target = "bc_bdb", daoClass = classOf[ODSBlkDao])
  //  @BeanProperty
  //  var blkdb: ODBSupport = null

  def setDposdb(daodb: DomainDaoSupport) {
    if (daodb != null && daodb.isInstanceOf[ODBSupport]) {
      dposdb = daodb.asInstanceOf[ODBSupport];
      Daos.dpospropdb = dposdb;
    } else {
      log.warn("cannot set dposdb ODBSupport from:" + daodb);
    }
  }

  def setDposvotedb(daodb: DomainDaoSupport) {
    if (daodb != null && daodb.isInstanceOf[ODBSupport]) {
      dposvotedb = daodb.asInstanceOf[ODBSupport];
      Daos.dposvotedb = dposvotedb;
    } else {
      log.warn("cannot set dposdb ODBSupport from:" + daodb);
    }
  }

  //  def setBlkdb(daodb: DomainDaoSupport) {
  //    if (daodb != null && daodb.isInstanceOf[ODBSupport]) {
  //      blkdb = daodb.asInstanceOf[ODBSupport];
  //      Daos.blkdb = blkdb;
  //    } else {
  //      log.warn("cannot set blkdb ODBSupport from:" + daodb);
  //    }
  //  }

  @ActorRequire(scope = "global", name = "pzpctrl")
  var pzp: PZPCtrl = null;

  @ActorRequire(name = "BlockChain_Helper", scope = "global")
  var bcHelper: IChainHelper = null;

  @ActorRequire(name = "Block_Helper", scope = "global")
  var blkHelper: IBlockHelper = null;

  @ActorRequire(name = "Transaction_Helper", scope = "global")
  var txHelper: ITransactionHelper = null;

  @ActorRequire(name = "bc_encoder", scope = "global") //  @BeanProperty
  var enc: EncAPI = null;

  def setPzp(_pzp: PZPCtrl) = {
    pzp = _pzp;
    Daos.pzp = pzp;
  }
  def getPzp(): PZPCtrl = {
    pzp
  }
  def setBcHelper(_bcHelper: IChainHelper) = {
    bcHelper = _bcHelper;
    Daos.actdb = bcHelper;
  }
  def getBcHelper: IChainHelper = {
    bcHelper
  }

  def setBlkHelper(_blkHelper: IBlockHelper) = {
    blkHelper = _blkHelper;
    Daos.blkHelper = _blkHelper;
  }
  def getBlkHelper: IBlockHelper = {
    blkHelper
  }

  def setTxHelper(_txHelper: ITransactionHelper) = {
    txHelper = _txHelper;
    Daos.txHelper = _txHelper;
  }
  def getTxHelper: ITransactionHelper = {
    txHelper
  }

  def setEnc(_enc: EncAPI) = {
    enc = _enc;
    Daos.enc = _enc;
  }
  def getEnc(): EncAPI = {
    enc;
  }

  @ActorRequire(name = "zippo.ddc", scope = "global")
  var ddc: IActorDispatcher = null;

  def getDdc(): IActorDispatcher = {
    return ddc;
  }

  def setDdc(ddc: IActorDispatcher) = {
    log.info("setDispatcher==" + ddc);
    this.ddc = ddc;
    Daos.ddc = ddc;
  }

  @ActorRequire(name = "ConfirmTxHashDB", scope = "global")
  var confirmMapDB: IConfirmTxMap = null; // 保存待打包block的交易
  
  def getConfirmMapDB(): IConfirmTxMap = {
    return confirmMapDB;
  }

  def setConfirmMapDB(ddc: IConfirmTxMap) = {
    log.info("setconfirmMapDB==" + ddc);
    this.confirmMapDB = ddc;
    Daos.confirmMapDB = ddc;
  }

  
  
}

object Daos extends OLog {
  var dpospropdb: ODBSupport = null
  var dposvotedb: ODBSupport = null
  //  var blkdb: ODBSupport = null
  var pzp: PZPCtrl = null;
  var actdb:IChainHelper = null;
  var blkHelper: IBlockHelper = null;
  var txHelper: ITransactionHelper = null;
  var enc: EncAPI = null;
  var ddc: IActorDispatcher = null;
  var confirmMapDB: IConfirmTxMap  = null; // 保存待打包block的交易

  def isDbReady(): Boolean = {
    dpospropdb != null && dpospropdb.getDaosupport.isInstanceOf[ODBSupport] &&
      dposvotedb != null && dposvotedb.getDaosupport.isInstanceOf[ODBSupport] &&
      blkHelper != null &&
      txHelper != null &&
      ddc != null &&
      confirmMapDB != null &&
      pzp != null && actdb != null;
       
   
  }
}



