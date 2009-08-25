package org.jetel.component.ws.util.extensions;

import org.jetel.component.ws.util.nsmap.WSDLExtensionNamespace;

public class Policy12ExtensionImpl extends PolicyExtensionBaseImpl {

	public Policy12ExtensionImpl() throws Exception {
		super();
		setPolicyVersion(WSDLExtensionNamespace.NS_URI_WS_POLICY_v1_2);
	}

}
