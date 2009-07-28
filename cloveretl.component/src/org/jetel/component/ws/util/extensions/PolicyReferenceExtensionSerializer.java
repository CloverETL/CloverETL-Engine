
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
public class PolicyReferenceExtensionSerializer implements ExtensionSerializer{

    public void marshall(Class parentType, QName elementType, ExtensibilityElement extension, PrintWriter pw, Definition definition, ExtensionRegistry registry) throws WSDLException {

        if (!(extension instanceof PolicyReferenceExtension)) {
            throw new WSDLException("PolicyReferenceExtensionSerializer","Invalid extension element type. Expected PolicyReferenceExtension type.");
        }

        PolicyReferenceExtension policyRefExtension = (PolicyReferenceExtension) extension;
        try {
            policyRefExtension.serialize(pw);
        } catch(Exception e) {
            throw new WSDLException("PolicyReferenceExtensionSerializer", "Unable to serialize policy reference.", e);
        }
    }

}
