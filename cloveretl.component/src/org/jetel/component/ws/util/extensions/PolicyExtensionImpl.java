package org.jetel.component.ws.util.extensions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Writer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import javax.xml.namespace.QName;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.apache.neethi.Assertion;
import org.apache.neethi.Constants;
import org.apache.neethi.Policy;
import org.apache.neethi.PolicyEngine;
import org.w3c.dom.Element;

/**
 *
 * @author Pavel Pospichal
 */
public class PolicyExtensionImpl implements PolicyExtension {

    private static final int INITIAL_TEMPORARY_BUFFER_SIZE = 100;
    private Transformer xmlTransformer = null;

    public PolicyExtensionImpl() throws Exception {
        TransformerFactory tf = TransformerFactory.newInstance();
        xmlTransformer = tf.newTransformer();
    }
    /** The source. */
    private Element policyEl;
    /** The extension type. */
    private QName extensionType;
    /** The required. */
    private Boolean required = Boolean.FALSE;

    private Policy policy;

    public Policy getPolicy() throws Exception {
        if (Constants.URI_POLICY_NS.equals(policyEl.getNamespaceURI())) {

            Source policySource = new DOMSource(policyEl);
            ByteArrayOutputStream outputBufferStream = new ByteArrayOutputStream(INITIAL_TEMPORARY_BUFFER_SIZE);
            Result outputBuffer = new StreamResult(outputBufferStream);

            xmlTransformer.transform(policySource, outputBuffer);

            ByteArrayInputStream inputBufferStream = new ByteArrayInputStream(outputBufferStream.toByteArray());

            if (Constants.ELEM_POLICY.equals(policyEl.getLocalName())) {
                return PolicyEngine.getPolicy(inputBufferStream);
            }
        }

        throw new IllegalArgumentException("Element " + new QName(Constants.URI_POLICY_NS, Constants.ELEM_POLICY) + " not found.");
    }

    public void serialize(Writer os) throws Exception {

        if (policy == null) return;

        XMLStreamWriter writer = XMLOutputFactory.newInstance().createXMLStreamWriter(os);
        policy = (Policy) policy.normalize(false);

        // declare the necessary namespaces
        String wspPrefix = writer.getPrefix(Constants.URI_POLICY_NS);

        if (wspPrefix == null) {
            wspPrefix = Constants.ATTR_WSP;
            writer.setPrefix(wspPrefix, Constants.URI_POLICY_NS);
        }

        String wsuPrefix = writer.getPrefix(Constants.URI_WSU_NS);
        if (wsuPrefix == null) {
            wsuPrefix = Constants.ATTR_WSU;
            writer.setPrefix(wsuPrefix, Constants.URI_WSU_NS);
        }

        writer.writeStartElement(wspPrefix, Constants.ELEM_POLICY,
                Constants.URI_POLICY_NS);

        QName key;

        String prefix = null;
        String namespaceURI = null;
        String localName = null;

        HashMap prefix2ns = new HashMap();

        // process policy attributes
        for (Iterator iterator = policy.getAttributes().keySet().iterator(); iterator.hasNext();) {

            key = (QName) iterator.next();
            localName = key.getLocalPart();

            namespaceURI = key.getNamespaceURI();
            namespaceURI = (namespaceURI == null || namespaceURI.length() == 0) ? null
                    : namespaceURI;

            if (namespaceURI != null) {

                String writerPrefix = writer.getPrefix(namespaceURI);
                writerPrefix = (writerPrefix == null || writerPrefix.length() == 0) ? null : writerPrefix;

                if (writerPrefix == null) {
                    prefix = key.getPrefix();
                    prefix = (prefix == null || prefix.length() == 0) ? null
                            : prefix;

                } else {
                    prefix = writerPrefix;
                }

                if (prefix != null) {
                    writer.writeAttribute(prefix, namespaceURI, localName,
                            policy.getAttribute(key));
                    prefix2ns.put(prefix, key.getNamespaceURI());

                } else {
                    writer.writeAttribute(namespaceURI, localName, policy.getAttribute(key));
                }

            } else {
                writer.writeAttribute(localName, policy.getAttribute(key));
            }

        }

        // writes xmlns:wsp=".."
        writer.writeNamespace(wspPrefix, Constants.URI_POLICY_NS);

        String prefiX;

        for (Iterator iterator = prefix2ns.keySet().iterator(); iterator.hasNext();) {
            prefiX = (String) iterator.next();
            writer.writeNamespace(prefiX, (String) prefix2ns.get(prefiX));
        }

        writer.writeStartElement(Constants.ATTR_WSP,
                Constants.ELEM_EXACTLYONE, Constants.URI_POLICY_NS);
        // write <wsp:ExactlyOne>

        List assertionList;

        for (Iterator iterator = policy.getAlternatives(); iterator.hasNext();) {

            assertionList = (List) iterator.next();

            // write <wsp:All>
            writer.writeStartElement(Constants.ATTR_WSP, Constants.ELEM_ALL,
                    Constants.URI_POLICY_NS);

            Assertion assertion;

            for (Iterator assertions = assertionList.iterator(); assertions.hasNext();) {
                assertion = (Assertion) assertions.next();
                // serialize assertion
                assertion.serialize(writer);
            }

            // write </wsp:All>
            writer.writeEndElement();
        }

        // write </wsp:ExactlyOne>
        writer.writeEndElement();
        // write </wsp:Policy>
        writer.writeEndElement();

        writer.flush();
    }

    public void setPolicy(Policy policy) {
        this.policy = policy;
    }


    public Element getXMLElement() {
        return policyEl;
    }

    public void setXMLElement(Element policyElement) {
        this.policyEl = policyElement;
    }

    public void setElementType(QName extensionType) {
        this.extensionType = extensionType;
    }

    public QName getElementType() {
        return extensionType;
    }

    public void setRequired(Boolean required) {
        this.required = required;
    }

    public Boolean getRequired() {
        return required;
    }
}
