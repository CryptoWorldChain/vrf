package org.csc.vrfblk;

import org.csc.bcapi.backend.ODBDao;

import onight.tfw.ojpa.api.ServiceSpec;

public class ODSDPoSVoteDao extends ODBDao {

	public ODSDPoSVoteDao(ServiceSpec serviceSpec) {
		super(serviceSpec);
	}

	@Override
	public String getDomainName() {
		return "dpos.vote";
	}

	
}
