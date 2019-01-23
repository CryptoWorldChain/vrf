package org.csc.vrfblk


import java.util.concurrent.TimeUnit

import org.apache.felix.ipojo.annotations.Invalidate
import org.apache.felix.ipojo.annotations.Validate
import org.csc.bcapi.URLHelper
import org.csc.p22p.utils.LogHelper

import com.google.protobuf.Message

import onight.osgi.annotation.NActorProvider
import onight.tfw.outils.serialize.UUIDGenerator
import org.csc.vrfblk.utils.VConfig
import org.csc.vrfblk.tasks.VCtrl
import org.csc.vrfblk.tasks.VRFController

@NActorProvider
class VRFStartup extends PSMVRFNet[Message] {

  override def getCmds: Array[String] = Array("SSS");

  @Validate
  def init() {

    //    System.setProperty("java.protocol.handler.pkgs", "org.fc.brewchain.url");
    log.debug("startup:");
    new Thread(new VRFBGLoader()).start()

    log.debug("tasks inited....[OK]");
  }

  @Invalidate
  def destory() {
//    !!DCtrl.instance.isStop = true;
  }

}

class VRFBGLoader() extends Runnable with LogHelper {
  def run() = {
    URLHelper.init();
    while (!Daos.isDbReady() //        || MessageSender.sockSender.isInstanceOf[NonePackSender]
    ) {
      log.debug("Daos Or sockSender Not Ready..:pzp=" + Daos.pzp+",dbready="+Daos.isDbReady()
          +",daos="+Daos)
      Thread.sleep(1000);
    }

    var vrfnet = Daos.pzp.networkByID("vrf")

    while (vrfnet == null
      || vrfnet.node_bits().bitCount <= 0 || !vrfnet.inNetwork()) {
      vrfnet = Daos.pzp.networkByID("vrf")
      if (vrfnet != null) {
        MDCSetBCUID(vrfnet)
      }
      log.debug("vrf ctrl not ready. vrfnet=" + vrfnet+",ddc="+Daos.ddc)
      Thread.sleep(5000);
    }
    //    RSM.instance = RaftStateManager(raftnet);

    //     Daos.actdb.getNodeAccount();

    while (Daos.chainHelper.getNodeAccount == null) {
      log.debug(" cws account not ready. "+",ddc="+Daos.ddc)
      Thread.sleep(5000);
    }
    val naccount = Daos.chainHelper.getNodeAccount;
    Daos.chainHelper.onStart(vrfnet.root().bcuid, vrfnet.root().v_address, vrfnet.root().name)
    UUIDGenerator.setJVM(vrfnet.root().bcuid.substring(1))
    vrfnet.changeNodeVAddr(naccount);
    log.debug("dposnet.initOK:My Node=" + vrfnet.root() + ",CoAddr=" + vrfnet.root().v_address
        +",vctrl.tick="+Math.min(VConfig.TICK_DCTRL_MS, VConfig.BLK_EPOCH_MS)) // my node

    VCtrl.instance = VRFController(vrfnet);

//    Scheduler.schedulerForDCtrl.scheduleWithFixedDelay(DCtrl.instance, DConfig.INITDELAY_DCTRL_SEC,
//      Math.min(DConfig.TICK_DCTRL_MS, DConfig.BLK_EPOCH_MS), TimeUnit.MILLISECONDS)
    Daos.ddc.scheduleWithFixedDelay(VCtrl.instance, VConfig.INITDELAY_DCTRL_SEC,
      Math.min(VConfig.TICK_DCTRL_MS, VConfig.BLK_EPOCH_MS),TimeUnit.MILLISECONDS)

//!!    TxSync.instance = TransactionSync(dposnet);

//!!    Daos.ddc.scheduleWithFixedDelay(TxSync.instance, DConfig.INITDELAY_DCTRL_SEC,
//!!      Math.min(DConfig.TICK_DCTRL_MS_TX, DConfig.TXS_EPOCH_MS),TimeUnit.MILLISECONDS)

  }
}