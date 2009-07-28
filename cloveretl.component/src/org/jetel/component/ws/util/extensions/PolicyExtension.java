
package org.jetel.component.ws.util.extensions;

import java.io.Writer;
import javax.wsdl.extensions.ExtensibilityElement;
import org.apache.neethi.Policy;
import org.w3c.dom.Element;

/**
 *
 * @author Pavel Pospichal
 */
public interface PolicyExtension extends ExtensibilityElement {
         /**
         * Sets the xML element.
         *
         * @param source the new xML element
         */
        public void setXMLElement(Element source);

        /**
         * Gets the xML element.
         *
         * @return the xML element
         */
        public Element getXMLElement();

        /**
         * Gets the policy.
         *
         * @return the policy
         */
        public Policy getPolicy() throws Exception;

        /**
         * 
         * @param policy
         */
        public void setPolicy(Policy policy) throws Exception;

        /**
         * 
         * @param os
         * @throws java.lang.Exception
         */
        public void serialize(Writer os) throws Exception;

}
