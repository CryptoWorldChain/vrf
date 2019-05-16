package org.brewchain.vrfblk;

import org.brewchain.core.dbapi.ODBDao;

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
