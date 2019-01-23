package org.csc.vrfblk;

import org.csc.bcapi.backend.ODBDao;

import onight.tfw.ojpa.api.ServiceSpec;

public class ODSVRFDao extends ODBDao {

	public ODSVRFDao(ServiceSpec serviceSpec) {
		super(serviceSpec);
	}

	@Override
	public String getDomainName() {
		return "vrf.prop";
	}

	
}
