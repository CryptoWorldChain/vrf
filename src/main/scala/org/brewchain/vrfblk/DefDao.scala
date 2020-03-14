package org.brewchain.vrfblk

import scala.beans.BeanProperty

import org.apache.felix.ipojo.annotations.Provides
import org.brewchain.mcore.handler.AccountHandler
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
import org.brewchain.mcore.api.ODBSupport
import org.brewchain.mcore.handler.ChainHandler
import org.brewchain.mcore.handler.BlockHandler
import org.brewchain.mcore.api.ICryptoHandler
import org.brewchain.mcore.handler.TransactionHandler
import org.brewchain.mcore.handler.MCoreServices

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

  @ActorRequire(name = "bc_account", scope = "global")
  var accountHandler: AccountHandler = null;

  @ActorRequire(name = "bc_transaction", scope = "global")
  var transactionHandler: TransactionHandler = null;

  @ActorRequire(name = "bc_chain", scope = "global")
  var chainHandler: ChainHandler = null;

  @ActorRequire(name = "bc_block", scope = "global")
  var blockHandler: BlockHandler = null;

  @ActorRequire(name = "bc_crypto", scope = "global") //  @BeanProperty
  var enc: ICryptoHandler = null;

  @ActorRequire(name = "MCoreServices", scope = "global")
	var mcore: MCoreServices = null;
  
  def setPzp(_pzp: PZPCtrl) = {
    pzp = _pzp;
    Daos.pzp = pzp;
  }
  def getPzp(): PZPCtrl = {
    pzp
  }

  def setAccountHandler(_accountHandler: AccountHandler) = {
    accountHandler = _accountHandler;
    Daos.accountHandler = _accountHandler;
  }
  def getAccountHandler: AccountHandler = {
    accountHandler
  }

  def setTransactionHandler(_transactionHandler: TransactionHandler) = {
    transactionHandler = _transactionHandler;
    Daos.txHelper = _transactionHandler;
  }
  def getTransactionHandler: TransactionHandler = {
    transactionHandler
  }

  def setChainHandler(_chainHandler: ChainHandler) = {
    chainHandler = _chainHandler;
    Daos.chainHelper = _chainHandler;
  }
  def getChainHandler: ChainHandler = {
    chainHandler
  }

  def setBlockHandler(_blockHanlder: BlockHandler) = {
    blockHandler = _blockHanlder;
    Daos.blkHelper = _blockHanlder;
  }
  def getBlockHandler: BlockHandler = {
    blockHandler
  }

  def setEnc(_enc: ICryptoHandler) = {
    enc = _enc;
    Daos.enc = _enc;
  }
  def getEnc(): ICryptoHandler = {
    enc;
  }
  
  def setMcore(_mcore: MCoreServices) = {
    mcore = _mcore;
    Daos.mcore = _mcore;
  }
  def getMcore(): MCoreServices = {
    mcore;
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
  var chainHelper: ChainHandler = null;
  var blkHelper: BlockHandler = null;
  var txHelper: TransactionHandler = null;
  var enc: ICryptoHandler = null;
  var ddc: IActorDispatcher = null;
  var accountHandler: AccountHandler = null;
  var mcore: MCoreServices = null;

  def isDbReady(): Boolean = {
    vrfpropdb != null && vrfpropdb.getDaosupport.isInstanceOf[ODBSupport] &&
      vrfvotedb != null && vrfvotedb.getDaosupport.isInstanceOf[ODBSupport] &&
      ddc != null &&
      pzp != null && accountHandler != null && txHelper != null && blkHelper != null && chainHelper != null
  }
}



