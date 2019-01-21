package org.csc.vrfblk;

import org.csc.bcapi.backend.ODBDao;

import onight.tfw.ojpa.api.ServiceSpec;

public class ODSBlkDao extends ODBDao {

	public ODSBlkDao(ServiceSpec serviceSpec) {
		super(serviceSpec);
	}

	@Override
	public String getDomainName() {
		return "dpos.blk";
	}

	
}
