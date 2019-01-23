package org.csc.vrfblk;

import org.csc.bcapi.backend.ODBDao;

import onight.tfw.ojpa.api.ServiceSpec;

public class ODSVRFBlkDao extends ODBDao {

	public ODSVRFBlkDao(ServiceSpec serviceSpec) {
		super(serviceSpec);
	}

	@Override
	public String getDomainName() {
		return "vrf.blk";
	}

	
}
