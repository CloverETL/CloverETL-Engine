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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.StringReader;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.xml.namespace.QName;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dom4j.Document;
import org.dom4j.io.SAXReader;
import org.jetel.data.DataRecord;
import org.jetel.data.formatter.XmlTemplateFormatter;
import org.jetel.data.parser.XPathParser;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.ParserExceptionHandlerFactory;
import org.jetel.exception.PolicyType;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.extension.PortDefinition;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.string.StringUtils;
import org.jetel.util.xml.DOM4jUtils;
import org.jetel.component.ws.WSPureXMLReader;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;

/**
 * The component "WS_DATA_DELEGATOR" is part of the cloverETL graph structure responsible for
 * communication with external system based on Web Services standards. Data records received
 * on input ports are passed on to specified external service provider and the result of service
 * processing is sent further through output ports to other components of defined ETL process. 
 * @author Pavel Pospichal
 */
public class WSDataDelegator extends Node {

    private static final Log logger = LogFactory.getLog(WSDataDelegator.class);

    /**  Description of the Field */
    public final static String COMPONENT_TYPE = "WS_DATA_DELEGATOR";

    /** XML attribute names */
	private static final String XML_REQUEST_TEMPLATE_ATTRIBUTE = "requestTemplate";
	private static final String XML_REQUEST_TEMPLATE_URL_ATTRIBUTE = "requestTemplateURL";
	private static final String XML_RESPONSE_MAPPING_ATTRIBUTE = "responseMapping";
    private static final String XML_RESPONSE_MAPPING_URL_ATTRIBUTE = "responseMappingURL";
	// TODO: Fault message parsing and sending error records through output ports
    private static final String XML_FAULT_MAPPING_ATTRIBUTE = "faultMapping";
    private static final String XML_FAULT_MAPPING_URL_ATTRIBUTE = "faultMappingURL";
    
	private final static String XML_DATAPOLICY_ATTRIBUTE = "dataPolicy";
    private static final String XML_SKIP_PARSED_ENTITIES_ATTRIBUTE = "skipParsedEntities";
    private static final String XML_OUTPUT_RECORDS_COUNT_ATTRIBUTE = "outputRecordsCount";
    private static final String XML_OPERATION_NAME_ATTRIBUTE = "operationName";
    private static final String XML_WSDL_ADDRESS_ATTRIBUTE = "wsdlAddress";
    private static final String XML_PORT_TYPE_NAME_ATTRIBUTE = "portTypeName";
    private static final String XML_SERVICE_NAME_ATTRIBUTE = "serviceName";
    private static final String XML_VALIDATE_ATTRIBUTE = "validate";

    private static final String XML_PROXY_LOCATION_ATTRIBUTE = "proxyLocation";
    private static final String XML_PROXY_PORT_ATTRIBUTE = "proxyPort";

    private static final String XML_HTTP_AUTH_USERNAME_ATTRIBUTE = "authUsername";
    private static final String XML_HTTP_AUTH_PASSWORD_ATTRIBUTE = "authPassword";
    private static final String XML_HTTP_AUTH_DOMAIN_ATTRIBUTE = "authDomain";
    private static final String XML_HTTP_AUTH_REALM_ATTRIBUTE = "httpRealm";
    
    private static final String XML_TRUST_JKS_LOCATION_ATTRIBUTE = "trustJKSLocation";
    private static final String XML_TRUST_JKS_PASSWORD_ATTRIBUTE = "trustJKSPassword";
    private static final String XML_CLIENT_JKS_LOCATION_ATTRIBUTE = "clientJKSLocation";
    private static final String XML_CLIENT_JKS_PASSWORD_ATTRIBUTE = "clientJKSPassword";
    
    private static final int PROXY_PORT_DEFAULT = 8080;

    private final static int OUTPUT_PORT = 0;
    private final static int INPUT_PORT = 0;

    /**
     * The WSDL location information and specification of performed service operation
     */
    private URL wsdlLocation;
    private QName operationQName;
    private QName portTypeQName;
    private QName serviceQName;

    /**
     * The definition of mapping data elements from service provider response message (SOAP body payload)
     * into output port specific data records.
     */
    private Document responseMappingDocument;
    /**
	 * The template message (SOAP body payload) with hooks for mapping input port
	 * specific data records into suitable data source for remote service provider.
	 */
    private Document requestTemplateDocument;

    private WSPureXMLReader reader;
    private ValidationType validateMessages = ValidationType.NONE;
    private XPathParser parser;
    private PolicyType policyType;

    /**
     * Component processing statistics
     */
    private int skipParsedEntities = 0; // do not skip rows by default
    private int outputRecordsCount = -1;

    /**
     * Proxy information
     */
    private String proxyHostLocation = null;
    private int proxyHostPort;
    
    private Object[] outputPorts;
    
    /**
	 * Map of portIndex => PortDefinition
	 * It's read from XML during initialization.
	 */
	private Map<Integer, PortDefinition> allPortDefinitionMap;
	
	// HTTP Authentication
    String httpAuthUsername = null;
    String httpAuthPassword = null;
    String httpAuthDomain = null;
    String httpAuthRealm = null;
    
    // JKS
    URL trustJKSLocation = null;
    String trustJKSPassword = null;
    URL clientJKSLocation = null;
    String clientJKSPassword = null;
    
    /**
     * Constructor for the WSDataReader object
     * 
     * @param id
     * @param fileURL
     * @param mapping
     */
    public WSDataDelegator(String id) {
        super(id);
    }

    /**
     * 
     * @param status
     * @return
     */
    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        super.checkConfig(status);

        checkInputPorts(status, 1, Integer.MAX_VALUE);
        checkOutputPorts(status, 1, Integer.MAX_VALUE);

        if (wsdlLocation == null || StringUtils.isEmpty(wsdlLocation.getHost())) {
        	status.add("Invalid URL location of WSDL document.", Severity.ERROR, this, Priority.NORMAL, "wsdlLocation");
        }
        // for rigours control validate received document against supported WSDL specifications
        // and ensure that enough information is known for the unambiguous operation determination
        
        if (operationQName == null || StringUtils.isEmpty(operationQName.getLocalPart())) {
        	status.add("Invalid qualified name of service operation defined by WSDL document.", Severity.ERROR, this, Priority.NORMAL, "operationQName");
        }
        
        if (responseMappingDocument == null || responseMappingDocument.nodeCount() == 0) {
        	status.add("The definition of mapping the response XML document into output port specific data records is not specified.", Severity.ERROR, this, Priority.NORMAL, "responseMappingDocument");
        }
        
        if (requestTemplateDocument == null || requestTemplateDocument.nodeCount() == 0) {
        	status.add("The definition of formatting the input port specific data records into the request XML document is not specified.", Severity.ERROR, this, Priority.NORMAL, "requestTemplateDocument");
        }
        
        // the rigours control is performed in init phase
        
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
        
        // test that we have at least one input port and one output
        if (inPorts.size() < 1) {
            throw new ComponentNotReadyException(getId() + ": At least one input port has to be defined!");
        }
        if (outPorts.size() < 1) {
            throw new ComponentNotReadyException(getId() + ": At least one output port has to be defined!");
        }
        
        XmlTemplateFormatter formatter = null;
        try {
        	allPortDefinitionMap = new HashMap<Integer, PortDefinition>();
            for (int portIndex = 0; portIndex < inPorts.size(); portIndex++) {
                PortDefinition portData = new PortDefinition();
                portData.portIndex = portIndex;
                allPortDefinitionMap.put(portIndex, portData);
            }
            
            formatter = new XmlTemplateFormatter(requestTemplateDocument);
            formatter.setUseRootElement(false);
        } catch(Exception e) {
			throw new ComponentNotReadyException(getId()
					+ ": Unable to initialize formatter: " + e.getMessage());
        }

        try {
        	parser = new XPathParser(DOM4jUtils.convertToW3CDOM(responseMappingDocument));
            
            //handler may be configured during runtime
            parser.setExceptionHandler(ParserExceptionHandlerFactory.getHandler(policyType));
            
            parser.setSkip(skipParsedEntities);
            parser.setNumRecords(outputRecordsCount);
            parser.setGraph(getGraph());
            parser.setDataModel(XPathParser.SupportedDataModels.W3C_XSD);
        } catch(Exception e) {
        	throw new ComponentNotReadyException(getId()
					+ ": Unable to initialize parser: " + e.getMessage());
        }
        
        reader = new WSPureXMLReader(parser, formatter, wsdlLocation, operationQName);
        reader.setLogger(logger);
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
        
        if (proxyHostLocation != null) {
            reader.setProxyHostLocation(proxyHostLocation);
            reader.setProxyHostPort(proxyHostPort);
        }
        
        reader.setHttpAuthUsername(httpAuthUsername);
        reader.setHttpAuthPassword(httpAuthPassword);
        reader.setHttpAuthRealm(httpAuthRealm);
        reader.setHttpAuthDomain(httpAuthDomain);
        
        try {
        	if (trustJKSLocation != null) {
            	reader.setTrustJKSLocation(new File(trustJKSLocation.toURI()));
            }
            reader.setTrustJKSPassword(trustJKSPassword);
            if (clientJKSLocation != null) {
            	reader.setJKSLocation(new File(clientJKSLocation.toURI()));
            }
            reader.setJKSPassword(clientJKSPassword);
        } catch(URISyntaxException e) {
        	throw new ComponentNotReadyException("The URL of java key store is invalid.", e);
        }
        
        reader.init(getOutputPort(OUTPUT_PORT).getMetadata());
        
        outputPorts = parser.getPorts().toArray();
    }

    @Override
    public Result execute() throws Exception {
        // we need to create data record - take the metadata from first output port
		DataRecord[] records = new DataRecord[getOutPorts().size()];
		OutputPort outputPort;
		int outputPortNumber;
		for (int i=0; i<outputPorts.length; i++) {
			outputPortNumber = (Integer)outputPorts[i];
			outputPort = getOutputPort(outputPortNumber);
			if (outputPort == null)
				throw new ComponentNotReadyException("Error: output port '"+ outputPortNumber +"' doesn't exist");
			records[outputPortNumber] = new DataRecord(outputPort.getMetadata());
			records[outputPortNumber].init();
			parser.assignRecord(records[outputPortNumber], outputPortNumber);
		}
		
		// TODO: use thread manager instead of fix amount of threads
        InputReader[] portReaders = new InputReader[inPorts.size()];

        // read slave ports in separate threads
        for (int idx = 0; idx < inPorts.size(); idx++) {
            if (allPortDefinitionMap.get(idx) == null) {
                throw new IllegalStateException("Input port " + idx + " is connected, but isn't defined in mapping attribute.");
            }
            portReaders[idx] = new InputReader(idx, allPortDefinitionMap.get(idx));
            //portReaders[idx].setSkip(recordsSkip);
            //portReaders[idx].setNumRecords(recordsCount);
            portReaders[idx].start();
        }
        
        // wait for slave input threads to finish their job
        boolean killIt = false;
        for (int idx = 0; idx < inPorts.size(); idx++) {
            while (portReaders[idx].getState() != Thread.State.TERMINATED) {
                if (killIt) {
                    portReaders[idx].interrupt();
                    break;
                }
                killIt = !runIt;
                try {
                    portReaders[idx].join(1000);
                } catch (InterruptedException e) {
                    logger.warn(getId() + "thread interrupted, it will interrupt child threads", e);
                    killIt = true;
                }
            }
        }

		try {
			if (logger.isDebugEnabled()) {
                Set<Map.Entry<Integer, PortDefinition>> portDefinitionPerPortSet =
                        allPortDefinitionMap.entrySet();
                for (Map.Entry<Integer, PortDefinition> portDefinitionPerPort : portDefinitionPerPortSet) {
                    logger.debug("Number of records received for port[" + portDefinitionPerPort.getKey() + "]: "
                            + portDefinitionPerPort.getValue().dataRecords.size());
                }
            }
			
            reader.prepareDataSource(allPortDefinitionMap.values());
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
        for (PortDefinition def : allPortDefinitionMap.values()){
			def.reset();
		}
    }

    /**
     * Creates org.w3c.dom.Document object from the given String.
     *
     * @param inString
     * @return
     * @throws XMLConfigurationException
     */
    private static Document createDocumentFromString(String inString) throws XMLConfigurationException {
        InputSource is = new InputSource(new StringReader(inString));
        SAXReader reader = new SAXReader();
        reader.setIgnoreComments(true);
        reader.setIncludeExternalDTDDeclarations(false);
        reader.setMergeAdjacentText(false);
        reader.setStripWhitespaceText(true);
        
        Document doc;
        try {
            doc = reader.read(is);
        } catch (Exception e) {
            throw new XMLConfigurationException("Mapping parameter parse error occur.", e);
        }
        return doc;
    }

    private static Document createDocumentFromURL(URL xmlLocation) throws XMLConfigurationException {
        Document doc;

        try {
            InputSource is = new InputSource(new BufferedInputStream(xmlLocation.openStream()));
            SAXReader reader = new SAXReader();
            reader.setIgnoreComments(true);
            reader.setIncludeExternalDTDDeclarations(false);
            reader.setMergeAdjacentText(false);
            reader.setStripWhitespaceText(true);
            doc = reader.read(is);
        } catch (Exception e) {
            throw new XMLConfigurationException("Mapping parameter parse error occur.", e);
        }
        return doc;
    }

    public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException {
		WSDataDelegator wsDataDelegator = null;
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);

		try {
			Document requestTemplateDocument = null;
	        if(xattribs.exists(XML_REQUEST_TEMPLATE_ATTRIBUTE)) {
	            //read template document with mapping from string in attribute 'mapping'
	            String requestTemplate = xattribs.getString(XML_REQUEST_TEMPLATE_ATTRIBUTE);
	            requestTemplateDocument = createDocumentFromString(requestTemplate);
	        } else if (xattribs.exists(XML_REQUEST_TEMPLATE_URL_ATTRIBUTE)) {
                // read template document with mapping from URL location
                URL requestTemplateLocation = xattribs.getURL(XML_REQUEST_TEMPLATE_URL_ATTRIBUTE);
                requestTemplateDocument = createDocumentFromURL(requestTemplateLocation);
            } else {
                throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," no template definition specified") + ":");
            }
	        
	        Document responseMappingDocument = null;
	        if(xattribs.exists(XML_RESPONSE_MAPPING_ATTRIBUTE)) {
	            //read mapping document from string in attribute 'mapping'
	            String responseMapping = xattribs.getString(XML_RESPONSE_MAPPING_ATTRIBUTE);
	            responseMappingDocument = createDocumentFromString(responseMapping);
	        } else if (xattribs.exists(XML_RESPONSE_MAPPING_URL_ATTRIBUTE)) {
                // read mapping document from URL location
                URL requestTemplateLocation = xattribs.getURL(XML_RESPONSE_MAPPING_URL_ATTRIBUTE);
                responseMappingDocument = createDocumentFromURL(requestTemplateLocation);
            } else {
                throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," no mapping definition specified") + ":");
            }
			
			wsDataDelegator = new WSDataDelegator(xattribs.getString(XML_ID_ATTRIBUTE));
            wsDataDelegator.setPolicyType(xattribs.getString(XML_DATAPOLICY_ATTRIBUTE, null));
            wsDataDelegator.setRequestTemplateDocument(requestTemplateDocument);
            wsDataDelegator.setResponseMappingDocument(responseMappingDocument);
            
            if (xattribs.exists(XML_SKIP_PARSED_ENTITIES_ATTRIBUTE)){
                wsDataDelegator.setSkipParsedEntities(xattribs.getInteger(XML_SKIP_PARSED_ENTITIES_ATTRIBUTE));
            }
            if (xattribs.exists(XML_OUTPUT_RECORDS_COUNT_ATTRIBUTE)){
                wsDataDelegator.setOutputRecordsCount(xattribs.getInteger(XML_OUTPUT_RECORDS_COUNT_ATTRIBUTE));
            }
            if (xattribs.exists(XML_OPERATION_NAME_ATTRIBUTE)){
                wsDataDelegator.setOperationQName(xattribs.getQName(XML_OPERATION_NAME_ATTRIBUTE));
            }
            if (xattribs.exists(XML_PORT_TYPE_NAME_ATTRIBUTE)){
                wsDataDelegator.setPortTypeQName(xattribs.getQName(XML_PORT_TYPE_NAME_ATTRIBUTE));
            }
            if (xattribs.exists(XML_SERVICE_NAME_ATTRIBUTE)){
                wsDataDelegator.setServiceQName(xattribs.getQName(XML_SERVICE_NAME_ATTRIBUTE));
            }
            if (xattribs.exists(XML_WSDL_ADDRESS_ATTRIBUTE)){
                wsDataDelegator.setWsdlLocation(xattribs.getURL(XML_WSDL_ADDRESS_ATTRIBUTE));
            }
            if (xattribs.exists(XML_VALIDATE_ATTRIBUTE)){
                ValidationType validateMessage = ValidationType.valueOf(xattribs.getString(XML_VALIDATE_ATTRIBUTE));
                wsDataDelegator.setValidateMessages(validateMessage);
            }

            if (xattribs.exists(XML_PROXY_LOCATION_ATTRIBUTE)){
                wsDataDelegator.setProxyHostLocation(xattribs.getString(XML_PROXY_LOCATION_ATTRIBUTE));
            }
            if (xattribs.exists(XML_PROXY_PORT_ATTRIBUTE)){
                wsDataDelegator.setProxyHostPort(xattribs.getInteger(XML_PROXY_PORT_ATTRIBUTE, PROXY_PORT_DEFAULT));
            }
            
            if (xattribs.exists(XML_HTTP_AUTH_USERNAME_ATTRIBUTE)){
                wsDataDelegator.setHttpAuthUsername(xattribs.getString(XML_HTTP_AUTH_USERNAME_ATTRIBUTE));
            }
            if (xattribs.exists(XML_HTTP_AUTH_PASSWORD_ATTRIBUTE)){
                wsDataDelegator.setHttpAuthPassword(xattribs.getString(XML_HTTP_AUTH_PASSWORD_ATTRIBUTE));
            }
            if (xattribs.exists(XML_HTTP_AUTH_DOMAIN_ATTRIBUTE)){
                wsDataDelegator.setHttpAuthDomain(xattribs.getString(XML_HTTP_AUTH_DOMAIN_ATTRIBUTE));
            }
            if (xattribs.exists(XML_HTTP_AUTH_REALM_ATTRIBUTE)){
                wsDataDelegator.setHttpAuthRealm(xattribs.getString(XML_HTTP_AUTH_REALM_ATTRIBUTE));
            }
            
            if (xattribs.exists(XML_TRUST_JKS_LOCATION_ATTRIBUTE)){
                wsDataDelegator.setTrustJKSLocation(xattribs.getURL(XML_TRUST_JKS_LOCATION_ATTRIBUTE));
            }
            if (xattribs.exists(XML_TRUST_JKS_PASSWORD_ATTRIBUTE)){
                wsDataDelegator.setTrustJKSPassword(xattribs.getString(XML_TRUST_JKS_PASSWORD_ATTRIBUTE));
            }
            if (xattribs.exists(XML_CLIENT_JKS_LOCATION_ATTRIBUTE)){
                wsDataDelegator.setClientJKSLocation(xattribs.getURL(XML_CLIENT_JKS_LOCATION_ATTRIBUTE));
            }
            if (xattribs.exists(XML_CLIENT_JKS_PASSWORD_ATTRIBUTE)){
                wsDataDelegator.setClientJKSPassword(xattribs.getString(XML_CLIENT_JKS_PASSWORD_ATTRIBUTE));
            }
		} catch (Exception ex) {
	           throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
		}

		return wsDataDelegator;
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
    
	public Document getResponseMappingDocument() {
		return responseMappingDocument;
	}

	public void setResponseMappingDocument(Document responseMappingDocument) {
		this.responseMappingDocument = responseMappingDocument;
	}

	public Document getRequestTemplateDocument() {
		return requestTemplateDocument;
	}

	public void setRequestTemplateDocument(Document requestTemplateDocument) {
		this.requestTemplateDocument = requestTemplateDocument;
	}

	public int getSkipParsedEntities() {
		return skipParsedEntities;
	}

	public void setSkipParsedEntities(int skipParsedEntities) {
		this.skipParsedEntities = skipParsedEntities;
	}

	public int getOutputRecordsCount() {
		return outputRecordsCount;
	}

	public void setOutputRecordsCount(int outputRecordsCount) {
		this.outputRecordsCount = outputRecordsCount;
	}

	public void setHttpAuthUsername(String httpAuthUsername) {
		this.httpAuthUsername = httpAuthUsername;
	}

	public void setHttpAuthPassword(String httpAuthPassword) {
		this.httpAuthPassword = httpAuthPassword;
	}

	public void setHttpAuthDomain(String httpAuthDomain) {
		this.httpAuthDomain = httpAuthDomain;
	}

	public void setHttpAuthRealm(String httpAuthRealm) {
		this.httpAuthRealm = httpAuthRealm;
	}

	public void setTrustJKSLocation(URL trustJKSLocation) {
		this.trustJKSLocation = trustJKSLocation;
	}

	public void setTrustJKSPassword(String trustJKSPassword) {
		this.trustJKSPassword = trustJKSPassword;
	}

	public void setClientJKSLocation(URL clientJKSLocation) {
		this.clientJKSLocation = clientJKSLocation;
	}

	public void setClientJKSPassword(String clientJKSPassword) {
		this.clientJKSPassword = clientJKSPassword;
	}

	/**
	 * This thread reads records from one input port and stores them to appropriate data structure.
	 * @author Martin Varecha <martin.varecha@javlinconsulting.cz>
	 * (c) JavlinConsulting s.r.o.
	 * www.javlinconsulting.cz
	 * @created Dec 11, 2007
	 */
	private class InputReader extends Thread {
		private InputPort inPort;
		/** Port definition of input port which this thread reads from. */
		private PortDefinition portDefinition;
		DataRecordMetadata metadata;

        /** counter which decreases with each skipped record */
        private int skip = 0;
        /** fixed value */
        private int numRecords = 0;
        private int recordCounter = 0;

		public InputReader(int index, PortDefinition portDefinition) {
			super(Thread.currentThread().getName() + ".InputThread#" + index);
			this.portDefinition = portDefinition;
			runIt = true;
			inPort = getInputPort(index);
			metadata = inPort.getMetadata();
			portDefinition.setMetadata(metadata);
		}

		public void run() {
			while (runIt) {
				try {
					DataRecord record = new DataRecord(metadata);
					record.init();
					if (inPort.readRecord(record) == null) // no more input data
						return;

					//portDefinition.dataMap.put(key, item);

                    if (numRecords > 0 && numRecords == recordCounter) {
                        return;
                    }

                    // shall i skip some records?
                    if (skip > 0) {
                        skip--;
                        return;
                    }

                    // we only store received records, additional processing
                    // is performed based on these records in other parts of the process
                    portDefinition.dataRecords.add(record);
                    recordCounter++;
				} catch (InterruptedException e) {
					logger.error(getId() + ": thread forcibly aborted", e);
					return;
				} catch (Exception e) {
					logger.error(getId() + ": thread failed", e);
					return;
				}
			}
		}
		
        public void setNumRecords(int numRecords) {
            this.numRecords = numRecords;
        }

        public void setSkip(int skip) {
            this.skip = skip;
        }  
	}
}
