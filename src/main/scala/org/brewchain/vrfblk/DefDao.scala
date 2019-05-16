package org.brewchain.vrfblk

import scala.beans.BeanProperty

import org.apache.felix.ipojo.annotations.Provides
import org.brewchain.core.AccountHelper
import org.brewchain.core.BlockChainHelper
import org.brewchain.core.BlockHelper
import org.brewchain.core.TransactionHelper
import org.brewchain.core.cryptoapi.ICryptoHandler;
import org.brewchain.core.dbapi.ODBSupport;
import org.brewchain.p22p.core.PZPCtrl
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
import org.brewchain.bcrand.model.Bcrand.PModule

abstract class PSMVRFNet[T <: Message] extends SessionModules[T] with PBUtils with OLog {
  override def getModule: String = PModule.VRF.name()
}

@NActorProvider
@Provides(specifications = Array(classOf[ActorService], classOf[IJPAClient]))
class Daos extends PSMVRFNet[Message] with ActorService {

  @StoreDAO(target = "bc_db", daoClass = classOf[ODSVRFDao])
  @BeanProperty
  var vrfdb: ODBSupport = null

  @StoreDAO(target = "bc_db", daoClass = classOf[ODSVRFVoteDao])
  @BeanProperty
  var vrfvotedb: ODBSupport = null

  def setVrfdb(daodb: DomainDaoSupport) {
    if (daodb != null && daodb.isInstanceOf[ODBSupport]) {
      vrfdb = daodb.asInstanceOf[ODBSupport];
      Daos.vrfpropdb = vrfdb;
    } else {
      log.warn("cannot set dposdb ODBSupport from:" + daodb);
    }
  }

  def setVrfvotedb(daodb: DomainDaoSupport) {
    if (daodb != null && daodb.isInstanceOf[ODBSupport]) {
      vrfvotedb = daodb.asInstanceOf[ODBSupport];
      Daos.vrfvotedb = vrfvotedb;
    } else {
      log.warn("cannot set dposdb ODBSupport from:" + daodb);
    }
  }

  @ActorRequire(scope = "global", name = "pzpctrl")
  var pzp: PZPCtrl = null;

  @ActorRequire(name = "bc_blockchain_helper", scope = "global")
  var bcHelper: BlockChainHelper = null;

  @ActorRequire(name = "bc_block_helper", scope = "global")
  var blkHelper: BlockHelper = null;

  @ActorRequire(name = "bc_transaction_helper", scope = "global")
  var txHelper: TransactionHelper = null;

  @ActorRequire(name = "bc_crypto", scope = "global") //  @BeanProperty
  var enc: ICryptoHandler = null;

  def setPzp(_pzp: PZPCtrl) = {
    pzp = _pzp;
    Daos.pzp = pzp;
  }
  def getPzp(): PZPCtrl = {
    pzp
  }
  def setBcHelper(_bcHelper: BlockChainHelper) = {
    bcHelper = _bcHelper;
    Daos.chainHelper = bcHelper;
  }
  def getBcHelper: BlockChainHelper = {
    bcHelper
  }

  def setBlkHelper(_blkHelper: BlockHelper) = {
    blkHelper = _blkHelper;
    Daos.blkHelper = _blkHelper;
  }
  def getBlkHelper: BlockHelper = {
    blkHelper
  }

  def setTxHelper(_txHelper: TransactionHelper) = {
    txHelper = _txHelper;
    Daos.txHelper = _txHelper;
  }
  def getTxHelper: TransactionHelper = {
    txHelper
  }

  def setEnc(_enc: ICryptoHandler) = {
    enc = _enc;
    Daos.enc = _enc;
  }
  def getEnc(): ICryptoHandler = {
    enc;
  }

  @ActorRequire(name = "zippo.ddc", scope = "global")
  var ddc: IActorDispatcher = null;

  def getDdc(): IActorDispatcher = {
    return ddc;
  }

  def setDdc(ddc: IActorDispatcher) = {
    //    log.info("setDispatcher==" + ddc);
    this.ddc = ddc;
    Daos.ddc = ddc;
  }
}

object Daos extends OLog {
  var vrfpropdb: ODBSupport = null
  var vrfvotedb: ODBSupport = null
  //  var blkdb: ODBSupport = null
  var pzp: PZPCtrl = null;
  var chainHelper: BlockChainHelper = null; 
  var blkHelper: BlockHelper = null;
  var txHelper: TransactionHelper = null;
  var enc: ICryptoHandler = null;
  var ddc: IActorDispatcher = null;

  def isDbReady(): Boolean = {
    vrfpropdb != null && vrfpropdb.getDaosupport.isInstanceOf[ODBSupport] &&
      vrfvotedb != null && vrfvotedb.getDaosupport.isInstanceOf[ODBSupport] &&
      ddc != null &&
       pzp != null
  }
}



