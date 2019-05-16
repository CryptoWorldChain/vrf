package org.brewchain.vrfblk;

import org.brewchain.core.dbapi.ODBDao;

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
