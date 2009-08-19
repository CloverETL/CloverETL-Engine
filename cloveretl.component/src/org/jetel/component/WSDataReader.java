/*
 *    jETeL/Clover - Java based ETL application framework.
 *    Copyright (C) 2002-04  David Pavlis <david_pavlis@hotmail.com>
 *
 *    This library is free software; you can redistribute it and/or
 *    modify it under the terms of the GNU Lesser General Public
 *    License as published by the Free Software Foundation; either
 *    version 2.1 of the License, or (at your option) any later version.
 *
 *    This library is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *    Lesser General Public License for more details.
 *
 *    You should have received a copy of the GNU Lesser General Public
 *    License along with this library; if not, write to the Free Software
 *    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 */
package org.jetel.component;

import java.io.File;
import java.io.StringReader;
import java.net.URL;
import javax.xml.namespace.QName;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.parser.XPathParser;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.ParserExceptionHandlerFactory;
import org.jetel.exception.PolicyType;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.component.ws.WSPureXMLReader;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;

/**
 *
 * @author Pavel Pospichal
 */
public class WSDataReader extends Node {

    static Log logger = LogFactory.getLog(WSDataReader.class);

    /**  Description of the Field */
    public final static String COMPONENT_TYPE = "WS_DATA_READER";

    /** XML attribute names */
	private final static String XML_MAPPING_ATTRIBUTE = "mapping";
	private final static String XML_DATAPOLICY_ATTRIBUTE = "dataPolicy";
    private static final String XML_SKIP_ROWS_ATTRIBUTE = "skipRecords";
    private static final String XML_NUMRECORDS_ATTRIBUTE = "numRecords";
    private static final String XML_OPERATION_NAME_ATTRIBUTE = "operationName";
    private static final String XML_WSDL_ADDRESS_ATTRIBUTE = "wsdlAddress";
    private static final String XML_PORT_TYPE_NAME_ATTRIBUTE = "portTypeName";
    private static final String XML_SERVICE_NAME_ATTRIBUTE = "serviceName";
    private static final String XML_REQUEST_MESSAGE_URL_ATTRIBUTE = "requestMessageURL";
    private static final String XML_VALIDATE_ATTRIBUTE = "validate";

    private static final String XML_PROXY_LOCATION_ATTRIBUTE = "proxyLocation";
    private static final String XML_PROXY_PORT_ATTRIBUTE = "proxyPort";

    private static final int PROXY_PORT_DEFAULT = 8080;

    private final static int OUTPUT_PORT = 0;
    private final static int INPUT_PORT = 0;

    private String requestMessageURL;
    private URL wsdlLocation;
    private QName operationQName;
    private QName portTypeQName;
    private QName serviceQName;

    private Source reqMessageSource = null;

    private XPathParser parser;
    private WSPureXMLReader reader;
    private PolicyType policyType;
    private ValidationType validateMessages = ValidationType.NONE;

    private int skipRows = 0; // do not skip rows by default
    private int numRecords = -1;
    private Object[] ports;

    private String proxyHostLocation = null;
    private int proxyHostPort;

    /**
     * Constructor for the WSDataReader object
     * 
     * @param id
     * @param fileURL
     * @param mapping
     */
    public WSDataReader(String id, Document mapping) {
        super(id);
        parser = new XPathParser(mapping);
    }

    /**
     * 
     * @param status
     * @return
     */
    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        super.checkConfig(status);

        if (!checkInputPorts(status, 0, 1) || !checkOutputPorts(status, 1, Integer.MAX_VALUE)) {
            return status;
        }

//        try {
//            init();
//        } catch (ComponentNotReadyException e) {
//            ConfigurationProblem problem = new ConfigurationProblem(e.getMessage(), ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
//            if(!StringUtils.isEmpty(e.getAttributeName())) {
//                problem.setAttributeName(e.getAttributeName());
//            }
//            status.add(problem);
//        } finally {
//        	free();
//        }

        return status;
    }

    /**
     * 
     * @throws org.jetel.exception.ComponentNotReadyException
     */
    @Override
    public void init() throws ComponentNotReadyException {
        if (isInitialized()) {
            return;
        }
        super.init();

        // test that we have at least one output port
        if (outPorts.size() < 1) {
            throw new ComponentNotReadyException(getId() + ": At least one output port has to be defined!");
        }
        
        if (requestMessageURL != null && requestMessageURL.length() != 0) {
            reqMessageSource = new StreamSource(new File(requestMessageURL));
        }

        TransformationGraph graph = getGraph();
        reader = new WSPureXMLReader(parser, wsdlLocation, operationQName);
        switch (validateMessages) {
            case BOTH:
                reader.setValidateMessageOnRequest(true);
                reader.setValidateMessageOnResponse(true);
                break;
            case INPUT:
                reader.setValidateMessageOnRequest(true);
                break;
            case OUTPUT:
                reader.setValidateMessageOnResponse(true);
                break;
        }
        // handler may be configured during runtime
        parser.setExceptionHandler(ParserExceptionHandlerFactory.getHandler(policyType));
        
        parser.setSkip(skipRows);
        parser.setNumRecords(numRecords);
        parser.setGraph(getGraph());
        //reader.setInputPort(getInputPort(INPUT_PORT)); //for port protocol: ReadableChannelIterator reads data
        //reader.setCharset(charset);
        //reader.setDictionary(graph.getDictionary());
        if (proxyHostLocation != null) {
            reader.setProxyHostLocation(proxyHostLocation);
            reader.setProxyHostPort(proxyHostPort);
        }

        reader.init(getOutputPort(OUTPUT_PORT).getMetadata());
        
        
        ports = parser.getPorts().toArray();
    }

    @Override
    public Result execute() throws Exception {
        // we need to create data record - take the metadata from first output port
		DataRecord[] records = new DataRecord[getOutPorts().size()];
		OutputPort outputPort;
		int outputPortNumber;
		for (int i=0; i<ports.length; i++) {
			outputPortNumber = (Integer)ports[i];
			outputPort = getOutputPort(outputPortNumber);
			if (outputPort == null)
				throw new ComponentNotReadyException("Error: output port '"+ outputPortNumber +"' doesn't exist");
			records[outputPortNumber] = new DataRecord(outputPort.getMetadata());
			records[outputPortNumber].init();
			parser.assignRecord(records[outputPortNumber], outputPortNumber);
		}

		try {
            reader.prepareDataSource(reqMessageSource);
            reader.invokeRemoteService();

			Object record;
			while (runIt) {
			    try {
					record = reader.getNext();
                    logger.debug("record: "+record);
					if (record == null) break;
					outputPortNumber = parser.getActualPort();
			        writeRecord(outputPortNumber, records[outputPortNumber].duplicate());
			    } catch(BadDataFormatException bdfe) {
			        if(policyType == PolicyType.STRICT) {
			            throw bdfe;
			        } else {
			            logger.info(bdfe.getMessage());
			        }
			    }
			    SynchronizeUtils.cloverYield();
			}
		} catch (Exception e) {
			throw e;
		}finally{
			//reader.close();
			broadcastEOF();
            reader.close();
		}
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
    }

    @Override
    public String getType() {
        return COMPONENT_TYPE;
    }

    @Override
    public synchronized void reset() throws ComponentNotReadyException {
        super.reset();
        parser.reset();
    }

    /**
     * Creates org.w3c.dom.Document object from the given String.
     *
     * @param inString
     * @return
     * @throws XMLConfigurationException
     */
    public static Document createDocumentFromString(String inString) throws XMLConfigurationException {
        InputSource is = new InputSource(new StringReader(inString));
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        Document doc;
        try {
            doc = dbf.newDocumentBuilder().parse(is);
        } catch (Exception e) {
            throw new XMLConfigurationException("Mapping parameter parse error occur.", e);
        }
        return doc;
    }

    public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException {
		WSDataReader wsDataReader = null;
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);

		try {
			wsDataReader = new WSDataReader(
					xattribs.getString(XML_ID_ATTRIBUTE),
					createDocumentFromString(xattribs.getString(XML_MAPPING_ATTRIBUTE)));
            wsDataReader.setPolicyType(xattribs.getString(XML_DATAPOLICY_ATTRIBUTE, null));
            if (xattribs.exists(XML_REQUEST_MESSAGE_URL_ATTRIBUTE)){
                wsDataReader.setRequestMessageURL(xattribs.getString(XML_REQUEST_MESSAGE_URL_ATTRIBUTE));
            }

            if (xattribs.exists(XML_SKIP_ROWS_ATTRIBUTE)){
                wsDataReader.setSkipRows(xattribs.getInteger(XML_SKIP_ROWS_ATTRIBUTE));
            }
            if (xattribs.exists(XML_NUMRECORDS_ATTRIBUTE)){
                wsDataReader.setNumRecords(xattribs.getInteger(XML_NUMRECORDS_ATTRIBUTE));
            }
            if (xattribs.exists(XML_OPERATION_NAME_ATTRIBUTE)){
                wsDataReader.setOperationQName(xattribs.getQName(XML_OPERATION_NAME_ATTRIBUTE));
            }
            if (xattribs.exists(XML_PORT_TYPE_NAME_ATTRIBUTE)){
                wsDataReader.setPortTypeQName(xattribs.getQName(XML_PORT_TYPE_NAME_ATTRIBUTE));
            }
            if (xattribs.exists(XML_SERVICE_NAME_ATTRIBUTE)){
                wsDataReader.setServiceQName(xattribs.getQName(XML_SERVICE_NAME_ATTRIBUTE));
            }
            if (xattribs.exists(XML_WSDL_ADDRESS_ATTRIBUTE)){
                wsDataReader.setWsdlLocation(xattribs.getURL(XML_WSDL_ADDRESS_ATTRIBUTE));
            }
            if (xattribs.exists(XML_VALIDATE_ATTRIBUTE)){
                ValidationType validateMessage = ValidationType.valueOf(xattribs.getString(XML_VALIDATE_ATTRIBUTE));
                wsDataReader.setValidateMessages(validateMessage);
            }

            if (xattribs.exists(XML_PROXY_LOCATION_ATTRIBUTE)){
                wsDataReader.setProxyHostLocation(xattribs.getString(XML_PROXY_LOCATION_ATTRIBUTE));
            }
            if (xattribs.exists(XML_PROXY_PORT_ATTRIBUTE)){
                wsDataReader.setProxyHostPort(xattribs.getInteger(XML_PROXY_PORT_ATTRIBUTE, PROXY_PORT_DEFAULT));
            }
		} catch (Exception ex) {
	           throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
		}

		return wsDataReader;
	}

    public enum ValidationType {NONE, INPUT, OUTPUT, BOTH};

    @Override
    public synchronized void abort() {
        if (reader != null) {
            reader.abort();
        }
        super.abort();
    }

    public void setPolicyType(String strPolicyType) {
        setPolicyType(PolicyType.valueOfIgnoreCase(strPolicyType));
    }

	/**
	 * Adds BadDataFormatExceptionHandler to behave according to DataPolicy.
	 *
	 * @param  handler
	 */
	public void setPolicyType(PolicyType policyType) {
        this.policyType = policyType;
	}

	/**
	 * Return data checking policy
	 * @return User defined data policy, or null if none was specified
	 * @see org.jetel.exception.BadDataFormatExceptionHandler
	 */
	public PolicyType getPolicyType() {
		return this.parser.getPolicyType();
	}

    public String getRequestMessageURL() {
        return requestMessageURL;
    }

    public void setRequestMessageURL(String requestMessageURL) {
        this.requestMessageURL = requestMessageURL;
    }

    public int getNumRecords() {
        return numRecords;
    }

    public void setNumRecords(int numRecords) {
        this.numRecords = numRecords;
    }

    public QName getOperationQName() {
        return operationQName;
    }

    public void setOperationQName(QName operationQName) {
        this.operationQName = operationQName;
    }

    public QName getPortTypeQName() {
        return portTypeQName;
    }

    public void setPortTypeQName(QName portTypeQName) {
        this.portTypeQName = portTypeQName;
    }

    public QName getServiceQName() {
        return serviceQName;
    }

    public void setServiceQName(QName serviceQName) {
        this.serviceQName = serviceQName;
    }

    public int getSkipRows() {
        return skipRows;
    }

    public void setSkipRows(int skipRows) {
        this.skipRows = skipRows;
    }

    public URL getWsdlLocation() {
        return wsdlLocation;
    }

    public void setWsdlLocation(URL wsdlLocation) {
        this.wsdlLocation = wsdlLocation;
    }

    public ValidationType getValidateMessages() {
        return validateMessages;
    }

    public void setValidateMessages(ValidationType validateMessages) {
        this.validateMessages = validateMessages;
    }

    public void setProxyHostLocation(String proxyHostLocation) {
        this.proxyHostLocation = proxyHostLocation;
    }

    public void setProxyHostPort(int proxyHostPort) {
        this.proxyHostPort = proxyHostPort;
    }

}
