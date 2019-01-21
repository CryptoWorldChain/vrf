package org.csc.vrfblk


import org.apache.felix.ipojo.annotations.Invalidate
import org.apache.felix.ipojo.annotations.Validate
import org.csc.bcapi.URLHelper
import org.csc.dposblk.tasks.DCtrl
import org.csc.dposblk.tasks.DPosNodeController
import org.csc.dposblk.tasks.TxSync
import org.csc.dposblk.utils.DConfig
import org.csc.p22p.utils.LogHelper

import com.google.protobuf.Message

import onight.osgi.annotation.NActorProvider
import onight.tfw.outils.serialize.UUIDGenerator
import org.csc.dposblk.tasks.TransactionSync
import java.util.concurrent.TimeUnit

@NActorProvider
class DPoSStartup extends PSMDPoSNet[Message] {

  override def getCmds: Array[String] = Array("SSS");

  @Validate
  def init() {

    //    System.setProperty("java.protocol.handler.pkgs", "org.fc.brewchain.url");
    log.debug("startup:");
    new Thread(new DPoSBGLoader()).start()

    log.debug("tasks inited....[OK]");
  }

  @Invalidate
  def destory() {
    DCtrl.instance.isStop = true;
  }

}

class DPoSBGLoader() extends Runnable with LogHelper {
  def run() = {
    URLHelper.init();
    while (!Daos.isDbReady() //        || MessageSender.sockSender.isInstanceOf[NonePackSender]
    ) {
      log.debug("Daos Or sockSender Not Ready..:pzp=" + Daos.pzp+",dbready="+Daos.isDbReady()
          +",daos="+Daos)
      Thread.sleep(1000);
    }

    var dposnet = Daos.pzp.networkByID("dpos")

    while (dposnet == null
      || dposnet.node_bits().bitCount <= 0 || !dposnet.inNetwork()) {
      dposnet = Daos.pzp.networkByID("dpos")
      if (dposnet != null) {
        MDCSetBCUID(dposnet)
      }
      log.debug("dposnet not ready. dposnet=" + dposnet+",ddc="+Daos.ddc)
      Thread.sleep(5000);
    }
    //    RSM.instance = RaftStateManager(raftnet);

    //     Daos.actdb.getNodeAccount();

    while (Daos.actdb.getNodeAccount == null) {
      log.debug("dpos cws account not ready. "+",ddc="+Daos.ddc)
      Thread.sleep(5000);
    }
    val naccount = Daos.actdb.getNodeAccount;
    Daos.actdb.onStart(dposnet.root().bcuid, dposnet.root().v_address, dposnet.root().name)
    UUIDGenerator.setJVM(dposnet.root().bcuid.substring(1))
    dposnet.changeNodeVAddr(naccount);
    log.debug("dposnet.initOK:My Node=" + dposnet.root() + ",CoAddr=" + dposnet.root().v_address
        +",dctrl.tick="+Math.min(DConfig.TICK_DCTRL_MS, DConfig.BLK_EPOCH_MS)) // my node

    DCtrl.instance = DPosNodeController(dposnet);

//    Scheduler.schedulerForDCtrl.scheduleWithFixedDelay(DCtrl.instance, DConfig.INITDELAY_DCTRL_SEC,
//      Math.min(DConfig.TICK_DCTRL_MS, DConfig.BLK_EPOCH_MS), TimeUnit.MILLISECONDS)
    Daos.ddc.scheduleWithFixedDelay(DCtrl.instance, DConfig.INITDELAY_DCTRL_SEC,
      Math.min(DConfig.TICK_DCTRL_MS, DConfig.BLK_EPOCH_MS),TimeUnit.MILLISECONDS)

    TxSync.instance = TransactionSync(dposnet);
//    Scheduler.scheduleWithFixedDelayTx(TxSync.instance, DConfig.INITDELAY_DCTRL_SEC,
//      Math.min(DConfig.TICK_DCTRL_MS_TX, DConfig.TXS_EPOCH_MS), TimeUnit.MILLISECONDS)

    Daos.ddc.scheduleWithFixedDelay(TxSync.instance, DConfig.INITDELAY_DCTRL_SEC,
      Math.min(DConfig.TICK_DCTRL_MS_TX, DConfig.TXS_EPOCH_MS),TimeUnit.MILLISECONDS)

  }
}