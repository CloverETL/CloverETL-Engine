package org.jetel.component.ws.util.extensions;

import javax.xml.namespace.QName;

import org.apache.neethi.Constants;
import org.apache.neethi.PolicyReference;
import org.jetel.component.ws.util.nsmap.WSDLExtensionNamespace;

public class Policy15ReferenceExtensionImpl extends PolicyReferenceExtensionBaseImpl {

	public Policy15ReferenceExtensionImpl() throws Exception {
		super();
		setPolicyVersion(WSDLExtensionNamespace.NS_URI_WS_POLICY_v1_5);
	}

	/**
	 * HACK: Support for WS-Policy 1.5 is planned in Neethi 3.0 version.
	 */
	public PolicyReference getPolicyReference() throws Exception {
    	if (getPolicyVersion() == null) {
    		throw new IllegalArgumentException("The WS-Policy standard version is not specified.");
    	}
    	
        if (getPolicyVersion().equals(getXMLElement().getNamespaceURI())) {

            if (Constants.ELEM_POLICY_REF.equals(getXMLElement().getLocalName())) {
                PolicyReference policyReference = new PolicyReference();
                policyReference.setURI(getXMLElement().getAttribute("URI"));
            	//return PolicyEngine.getPolicyReferene(inputBufferStream);
                return policyReference;
            }
        }

        throw new IllegalArgumentException("Element " + new QName(getPolicyVersion(), Constants.ELEM_POLICY_REF) + " not found.");
    }
}
