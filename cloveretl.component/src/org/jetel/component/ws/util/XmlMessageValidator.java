
package org.jetel.component.ws.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.xml.XMLConstants;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.impl.builder.StAXOMBuilder;
import org.apache.axiom.om.impl.dom.DOOMAbstractFactory;
import org.apache.axiom.om.impl.dom.ElementImpl;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ws.commons.schema.XmlSchema;
import org.jetel.component.ws.exception.MessageValidationException;
import org.w3c.dom.Element;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

/**
 *
 * @author Pavel Pospichal
 */
public class XmlMessageValidator {

    private final static Log logger = LogFactory.getLog(XmlMessageValidator.class);

    public static void validateMessage(OMElement messageElement, XmlSchema xmlSchema) throws MessageValidationException {
        assert(messageElement != null);
        assert(xmlSchema != null);

        try {
            // validates xml content against W3C XSD schema language
            SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);

            // options for internal transformer; to save space
            // TODO: export to utils if necessary
            Map<String, Object> writeOptions = new HashMap<String, Object>();
            writeOptions.put(OutputKeys.INDENT, "no");

            ByteArrayOutputStream bufferOutStream = new ByteArrayOutputStream();
            xmlSchema.write(bufferOutStream, writeOptions);
            ByteArrayInputStream bufferInStream = new ByteArrayInputStream(bufferOutStream.toByteArray());
            // end TODO:

            MessageErrorHandler errorHandler = new MessageErrorHandler();
            // XML Schema prepared
            Schema schema = schemaFactory.newSchema(new StreamSource(bufferInStream));
            
            Element messageDOMElement = null;
            // to convert AXIOM element to DOM element; there are DOOM and LLOM
            if (messageElement instanceof ElementImpl) {
                messageDOMElement = (Element) messageElement;
            } else {
                messageDOMElement = getDOMElement(messageElement);
            }
            // neither net.sf.saxon.pull.PullSource nor javax.xml.transform.stax.StAXSource
            // can be used for validation
            DOMSource source = new DOMSource(messageDOMElement);

            Validator validator = schema.newValidator();
            validator.setErrorHandler(errorHandler);
            /**
             * UnsupportedOperationException: TODO
             * This was a problem in the Axiom library which was recently fixed in revision
             * 728550. The Axiom library with this fix will be included in Axis 1.5.
             */
            validator.validate(source);

            if (errorHandler.hasErrorOccured()) {
                MessageValidationException mve = new MessageValidationException("Message validity is disrupted.");
                List<ValidationInfo> validationDisruptions = new ArrayList<ValidationInfo>();
                List<ValidationInfo> info = errorHandler.getErrorInfo();
                validationDisruptions.addAll(info);
                info = errorHandler.getFatalErrorInfo();
                validationDisruptions.addAll(info);
                
                mve.setValidationDisruptions(validationDisruptions);
                throw mve;
            }

        } catch (SAXException saxe) {
            throw new MessageValidationException("Unable to establish W3C XSD schema language.",saxe);
        } catch (IOException ioe) {
            throw new MessageValidationException("Unable to validate XML message.",ioe);
        }
    }

    private static Element getDOMElement(OMElement llomElement) {
        Element domElement = null;
        OMElement doomElement = new StAXOMBuilder(DOOMAbstractFactory.getOMFactory(),
                llomElement.getXMLStreamReader()).getDocumentElement();

        domElement = (Element) doomElement;
        return domElement;
    }

    private static class MessageErrorHandler implements ErrorHandler {

        private List<ValidationInfo> warningInfo = new ArrayList<ValidationInfo>();
        private List<ValidationInfo> errorInfo = new ArrayList<ValidationInfo>();
        private List<ValidationInfo> fatalErrorInfo = new ArrayList<ValidationInfo>();
        
        public void warning(SAXParseException saxe) throws SAXException {
            ValidationInfo info = new ValidationInfo(ValidationInfo.SeverityType.WARNING,
                    saxe.getLineNumber(), saxe.getColumnNumber(), saxe.getMessage());
            warningInfo.add(info);
            if (logger.isDebugEnabled()) {
                logger.debug("Message invalid [" + info.severity + "] at " + info.lineNumber + ":" + info.columnNumber + ": " + info.message);
            }
        }

        public void error(SAXParseException saxe) throws SAXException {
            ValidationInfo info = new ValidationInfo(ValidationInfo.SeverityType.ERROR,
                    saxe.getLineNumber(), saxe.getColumnNumber(), saxe.getMessage());
            errorInfo.add(info);
            if (logger.isErrorEnabled()) {
                logger.error("Message invalid [" + info.severity + "] at " + info.lineNumber + ":" + info.columnNumber + ": " + info.message);
            }
        }

        public void fatalError(SAXParseException saxe) throws SAXException {
            ValidationInfo info = new ValidationInfo(ValidationInfo.SeverityType.FATAL_ERROR,
                    saxe.getLineNumber(), saxe.getColumnNumber(), saxe.getMessage());
            fatalErrorInfo.add(info);
            if (logger.isErrorEnabled()) {
                logger.error("Message invalid [" + info.severity + "] at " + info.lineNumber + ":" + info.columnNumber + ": " + info.message);
            }
        }

        public List<ValidationInfo> getErrorInfo() {
            return errorInfo;
        }

        public List<ValidationInfo> getFatalErrorInfo() {
            return fatalErrorInfo;
        }

        public List<ValidationInfo> getWarningInfo() {
            return warningInfo;
        }

        public boolean hasErrorOccured() {
            return (errorInfo.size() != 0) || (fatalErrorInfo.size() != 0);
        }
    }

    public static class ValidationInfo {
        public enum SeverityType {WARNING, ERROR, FATAL_ERROR};

        public final SeverityType severity;
        public final int columnNumber;
        public final int lineNumber;
        public final String message;

        public ValidationInfo(SeverityType severity, int lineNumber, int columnNumber, String message) {
            this.severity = severity;
            this.columnNumber = columnNumber;
            this.lineNumber = lineNumber;
            this.message = message;
        }
    }
}
