package org.brewchain.vrfblk

import java.util.concurrent.TimeUnit

import org.apache.felix.ipojo.annotations.Invalidate
import org.apache.felix.ipojo.annotations.Validate
import org.brewchain.p22p.utils.LogHelper

import com.google.protobuf.Message

import onight.osgi.annotation.NActorProvider
import onight.tfw.outils.serialize.UUIDGenerator
import org.brewchain.vrfblk.utils.VConfig
import org.brewchain.vrfblk.tasks.VCtrl
import org.brewchain.vrfblk.tasks.VRFController
import org.brewchain.vrfblk.tasks.BeaconGossip
import org.brewchain.vrfblk.tasks.BlockProcessor
import org.brewchain.vrfblk.tasks.NodeStateSwitcher
import org.brewchain.vrfblk.tasks.BlockSync
import org.brewchain.vrfblk.tasks.BeaconTask
import org.brewchain.bcrand.model.Bcrand.PSNodeGraceShutDown
import org.brewchain.vrfblk.tasks.TxSync
import org.brewchain.vrfblk.tasks.TransactionSync
import org.brewchain.tools.url.URLHelper

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
      log.debug("vrf ctrl not ready. vrfnet=" + vrfnet 
        + ",ddc=" + Daos.ddc 
        + ",bit=" + vrfnet.node_bits().bitCount
        + ",innetwork=" + vrfnet.inNetwork())
      Thread.sleep(1000);
    }
    //    RSM.instance = RaftStateManager(raftnet);

    //     Daos.actdb.getNodeAccount();

    while (Daos.chainHelper.getNodeAccountAddress == null) {
      log.debug(" cws account not ready. " + ",ddc=" + Daos.ddc)
      Thread.sleep(5000);
    }
    val naccount = Daos.chainHelper.getNodeAccountAddress;
    Daos.chainHelper.startBlockChain(vrfnet.root().bcuid, vrfnet.root().v_address, vrfnet.root().name)
    UUIDGenerator.setJVM(vrfnet.root().bcuid.substring(1))
    vrfnet.changeNodeVAddr(Daos.enc.bytesToHexStr(naccount));
    log.info("vrfnet.initOK:My Node=" + vrfnet.root() + ",CoAddr=" + vrfnet.root().v_address
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