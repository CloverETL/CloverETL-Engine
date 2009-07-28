
package org.jetel.component.ws.util.extensions;

import javax.wsdl.Definition;
import javax.wsdl.WSDLException;
import javax.wsdl.extensions.ExtensibilityElement;
import javax.wsdl.extensions.ExtensionDeserializer;
import javax.wsdl.extensions.ExtensionRegistry;
import javax.xml.namespace.QName;

import org.w3c.dom.Element;

/**
 *
 * @author Pavel Pospichal
 */
public class PolicyExtensionDeserializer implements ExtensionDeserializer {

    public ExtensibilityElement unmarshall(Class parentType, QName extensionType, Element extensionElement, Definition definition, ExtensionRegistry registry) throws WSDLException {
        PolicyExtension extension = (PolicyExtension) registry.createExtension(parentType, extensionType);

        extension.setElementType(extensionType);
        extension.setXMLElement(extensionElement);
        
        return extension;
    }

}
