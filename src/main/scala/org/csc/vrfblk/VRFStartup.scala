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
import org.csc.vrfblk.tasks.BeaconGossip
import org.csc.vrfblk.tasks.BlockProcessor
import org.csc.vrfblk.tasks.NodeStateSwitcher
import org.csc.vrfblk.tasks.BlockSync
import org.csc.vrfblk.tasks.BeaconTask
import org.csc.ckrand.pbgens.Ckrand.PSNodeGraceShutDown
import org.csc.vrfblk.tasks.TxSync
import org.csc.vrfblk.tasks.TransactionSync

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
      log.debug("Daos Or sockSender Not Ready..:pzp=" + Daos.pzp + ",dbready=" + Daos.isDbReady())
      Thread.sleep(1000);
    }

    var vrfnet = Daos.pzp.networkByID("vrf")

    while (vrfnet == null
      || vrfnet.node_bits().bitCount <= 0 || !vrfnet.inNetwork()) {
      vrfnet = Daos.pzp.networkByID("vrf")
      if (vrfnet != null) {
        MDCSetBCUID(vrfnet)
      }
      log.debug("vrf ctrl not ready. vrfnet=" + vrfnet + ",ddc=" + Daos.ddc)
      Thread.sleep(5000);
    }
    //    RSM.instance = RaftStateManager(raftnet);

    //     Daos.actdb.getNodeAccount();

    while (Daos.chainHelper.getNodeAccount == null) {
      log.debug(" cws account not ready. " + ",ddc=" + Daos.ddc)
      Thread.sleep(5000);
    }
    val naccount = Daos.chainHelper.getNodeAccount;
    Daos.chainHelper.onStart(vrfnet.root().bcuid, vrfnet.root().v_address, vrfnet.root().name)
    UUIDGenerator.setJVM(vrfnet.root().bcuid.substring(1))
    vrfnet.changeNodeVAddr(naccount);
    log.debug("vrfnet.initOK:My Node=" + vrfnet.root() + ",CoAddr=" + vrfnet.root().v_address
      + ",vctrl.tick=" + Math.min(VConfig.TICK_DCTRL_MS, VConfig.BLK_EPOCH_MS)) // my node

    VCtrl.instance = VRFController(vrfnet);
    Array(BeaconGossip, BlockProcessor, NodeStateSwitcher, BlockSync).map(f => {
      f.startup(Daos.ddc.getExecutorService("vrf"));
    })

    //    BeaconGossip.startup(Daos.ddc);
    VCtrl.instance.startup();

    Daos.ddc.scheduleWithFixedDelay(BeaconTask, VConfig.INITDELAY_GOSSIP_SEC,
      VConfig.TICK_GOSSIP_SEC, TimeUnit.SECONDS);
    val messageId = UUIDGenerator.generate();
    val body = PSNodeGraceShutDown.newBuilder().setReason("shutdown").build();

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run() = {
          VCtrl.network().wallMessage("SOSVRF", Left(body), messageId, '9');
      }
    })

    TxSync.instance = TransactionSync(VCtrl.network());
    Daos.ddc.scheduleWithFixedDelay(TxSync.instance, VConfig.INITDELAY_GOSSIP_SEC,
      Math.min(VConfig.TICK_DCTRL_MS_TX, VConfig.TXS_EPOCH_MS),TimeUnit.MILLISECONDS)

    //    Scheduler.schedulerForDCtrl.scheduleWithFixedDelay(DCtrl.instance, DConfig.INITDELAY_DCTRL_SEC,
    //      Math.min(DConfig.TICK_DCTRL_MS, DConfig.BLK_EPOCH_MS), TimeUnit.MILLISECONDS)
    //    Daos.ddc.scheduleWithFixedDelay(VCtrl.instance, VConfig.INITDELAY_DCTRL_SEC,
    //      Math.min(VConfig.TICK_DCTRL_MS, VConfig.BLK_EPOCH_MS),TimeUnit.MILLISECONDS)

    //!!    TxSync.instance = TransactionSync(dposnet);

    //!!    Daos.ddc.scheduleWithFixedDelay(TxSync.instance, DConfig.INITDELAY_DCTRL_SEC,
    //!!      Math.min(DConfig.TICK_DCTRL_MS_TX, DConfig.TXS_EPOCH_MS),TimeUnit.MILLISECONDS)

  }
}