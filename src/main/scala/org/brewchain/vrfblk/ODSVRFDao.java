package org.brewchain.vrfblk;

import org.brewchain.mcore.odb.ODBDao;

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
