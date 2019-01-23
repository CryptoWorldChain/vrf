package org.csc.vrfblk;

import org.csc.bcapi.backend.ODBDao;

import onight.tfw.ojpa.api.ServiceSpec;

public class ODSVRFVoteDao extends ODBDao {

	public ODSVRFVoteDao(ServiceSpec serviceSpec) {
		super(serviceSpec);
	}

	@Override
	public String getDomainName() {
		return "vrf.vote";
	}

	
}
