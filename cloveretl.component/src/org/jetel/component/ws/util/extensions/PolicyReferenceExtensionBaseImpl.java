package org.jetel.component.ws.util.extensions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Writer;
import javax.xml.namespace.QName;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.apache.neethi.Constants;
import org.apache.neethi.Policy;
import org.apache.neethi.PolicyEngine;
import org.apache.neethi.PolicyReference;
import org.w3c.dom.Element;

/**
 *
 * @author Pavel Pospichal
 */
public class PolicyReferenceExtensionBaseImpl implements PolicyReferenceExtension {

    private static final int INITIAL_TEMPORARY_BUFFER_SIZE = 100;
    private Transformer xmlTransformer = null;

    public PolicyReferenceExtensionBaseImpl() throws Exception {
        TransformerFactory tf = TransformerFactory.newInstance();
        xmlTransformer = tf.newTransformer();
    }
    /** The source. */
    private Element policyReferenceEl;
    /** The extension type. */
    private QName extensionType;
    /** The required. */
    private Boolean required = Boolean.FALSE;

    private String policyVersion = null;
    
    private Policy policy;

    public PolicyReference getPolicyReference() throws Exception {
    	if (policyVersion == null) {
    		throw new IllegalArgumentException("The WS-Policy standard version is not specified.");
    	}
    	
        if (policyVersion.equals(policyReferenceEl.getNamespaceURI())) {

            Source policyRefSource = new DOMSource(policyReferenceEl);
            ByteArrayOutputStream outputBufferStream = new ByteArrayOutputStream(INITIAL_TEMPORARY_BUFFER_SIZE);
            Result outputBuffer = new StreamResult(outputBufferStream);

            xmlTransformer.transform(policyRefSource, outputBuffer);

            ByteArrayInputStream inputBufferStream = new ByteArrayInputStream(outputBufferStream.toByteArray());

            if (Constants.ELEM_POLICY_REF.equals(policyReferenceEl.getLocalName())) {
                return PolicyEngine.getPolicyReferene(inputBufferStream);
            }
        }

        throw new IllegalArgumentException("Element " + new QName(policyVersion, Constants.ELEM_POLICY_REF) + " not found.");
    }

    public void serialize(Writer writer) throws Exception {

        PolicyReference policyRef = new PolicyReference();

        String policyRefURI = "#" + policy.getId();
        policyRef.setURI(policyRefURI);

        XMLStreamWriter streamWriter = XMLOutputFactory.newInstance().createXMLStreamWriter(writer);
        policyRef.serialize(streamWriter);
        streamWriter.flush();

    }

    public void setPolicy(Policy policy) {
        this.policy = policy;
    }

    public Element getXMLElement() {
        return policyReferenceEl;
    }

    public void setXMLElement(Element policyReferenceEl) {
        this.policyReferenceEl = policyReferenceEl;
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

	protected String getPolicyVersion() {
		return policyVersion;
	}

	protected void setPolicyVersion(String policyVersion) {
		this.policyVersion = policyVersion;
	}
}
