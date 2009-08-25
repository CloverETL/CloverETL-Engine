package org.jetel.component.ws.util.extensions;

import org.jetel.component.ws.util.nsmap.WSDLExtensionNamespace;

public class Policy15ExtensionImpl extends PolicyExtensionBaseImpl {

	public Policy15ExtensionImpl() throws Exception {
		super();
		setPolicyVersion(WSDLExtensionNamespace.NS_URI_WS_POLICY_v1_5);
	}

}
