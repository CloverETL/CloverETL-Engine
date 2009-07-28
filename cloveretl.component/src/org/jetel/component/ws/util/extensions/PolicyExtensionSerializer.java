
package org.jetel.component.ws.util.extensions;

import java.io.PrintWriter;
import javax.wsdl.Definition;
import javax.wsdl.WSDLException;
import javax.wsdl.extensions.ExtensibilityElement;
import javax.wsdl.extensions.ExtensionRegistry;
import javax.wsdl.extensions.ExtensionSerializer;
import javax.xml.namespace.QName;

/**
 *
 * @author Pavel Pospichal
 */
public class PolicyExtensionSerializer implements ExtensionSerializer{

    public void marshall(Class parentType, QName elementType, ExtensibilityElement extension, PrintWriter pw, Definition definition, ExtensionRegistry registry) throws WSDLException {

        if (!(extension instanceof PolicyExtension)) {
            throw new WSDLException("PolicyExtensionSerializer","Invalid extension element type. Expected PolicyExtension type.");
        }

        PolicyExtension policyExtension = (PolicyExtension) extension;
        try {
            policyExtension.serialize(pw);
        } catch(Exception e) {
            throw new WSDLException("PolicyExtensionSerializer", "Unable to serialize policy.", e);
        }

    }

}
