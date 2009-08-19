/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2006 Javlin Consulting <info@javlinconsulting>
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
import java.io.StringReader;
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
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.extension.PortDefinition;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.component.ws.WSPureBatchXMLWriter;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;

/**
 * In the message template more than one port definition may be "root" 
 * (mapping of port data that is not encapsulated in another mapping).
 * The behaviour of "root" mapping is different from nested mapping
 * in way that all accumulated records are presented at the extension point.
 * Accumulated records in the nested mapping are presented only in case
 * of superior-mapping key agreement.
 *
 * @author Pavel Pospichal
 */
public class WSTemplateDataWriter extends Node {
    static Log logger = LogFactory.getLog(WSTemplateDataWriter.class);

    private static final String XML_MAPPING_ATTRIBUTE = "mapping";
    private static final String XML_MAPPING_URL_ATTRIBUTE = "mappingURL";

    private static final String XML_RECORDS_SKIP_ATTRIBUTE = "recordSkip";
	private static final String XML_RECORDS_COUNT_ATTRIBUTE = "recordCount";

    private static final String XML_OPERATION_NAME_ATTRIBUTE = "operationName";
    private static final String XML_WSDL_ADDRESS_ATTRIBUTE = "wsdlAddress";
    private static final String XML_PORT_TYPE_NAME_ATTRIBUTE = "portTypeName";
    private static final String XML_SERVICE_NAME_ATTRIBUTE = "serviceName";

    private static final String XML_PROXY_LOCATION_ATTRIBUTE = "proxyLocation";
    private static final String XML_PROXY_PORT_ATTRIBUTE = "proxyPort";

    private static final int PROXY_PORT_DEFAULT = 8080;

    protected int initialCapacity = 50;
    
	public final static String COMPONENT_TYPE = "WS_TEMPLATE_DATA_WRITER";
	private final static int OUTPUT_PORT = 0;

    /**
	 * Map of portIndex => PortDefinition
	 * It's read from XML during initialization.
	 */
	private Map<Integer, PortDefinition> allPortDefinitionMap;
	/**
	 * The message (SOAP body payload) template with port dat mapping definitions.
	 */
	private Document messageTemplate;
	protected int portsCnt;

    /**
     * The URL address where WSDL document is located.
     */
    private URL wsdlLocation;

    /*
     * Service, port type and operation qualified names specifies
     * the service operation used for processing the constructed
     * XML document.
     */
    private QName operationQName;
    private QName portTypeQName;
    private QName serviceQName;

    private WSPureBatchXMLWriter writer;

    /**
     * applied on all attached ports
     */
    private int recordsSkip = 0;
	private int recordsCount = 0;

    private String proxyHostLocation = null;
    private int proxyHostPort;

    /**
	 * Constructor. Other necessary attributes are set with injection.
	 */
	public WSTemplateDataWriter(String id) {
		super(id);
	}

    /* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#checkConfig(org.jetel.exception.ConfigurationStatus)
	 */
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);

		if(!checkInputPorts(status, 1, Integer.MAX_VALUE)
				|| !checkOutputPorts(status, 0, 1)) {
			return status;
		}

		//...

        return status;
	}

    /* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#init()
	 */
	public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();
		TransformationGraph graph = getGraph();

		portsCnt = inPorts.size();

        // test that we have at least one input port
        if (portsCnt < 1) {
            throw new ComponentNotReadyException(getId() + ": At least one input port has to be defined!");
        }

        allPortDefinitionMap = new HashMap<Integer, PortDefinition>();
        for (int portIndex = 0; portIndex < portsCnt; portIndex++) {
            PortDefinition portData = new PortDefinition();
            portData.portIndex = portIndex;
            allPortDefinitionMap.put(portIndex, portData);
        }

        XmlTemplateFormatter formatter = new XmlTemplateFormatter(messageTemplate);
        //formatter.setComponentId(getId());
        Map<String, String> ns = new HashMap<String, String>();
        ns.put("xsi", "http://xsi");
        formatter.setNamespaces(ns);
        //formatter.setGraphName(this.getGraph().getName());
        //formatter.setRootDefaultNamespace(rootDefaultNamespace);
        //formatter.setRootElement(rootElement);
        formatter.setUseRootElement(false);

        writer = new WSPureBatchXMLWriter(formatter, wsdlLocation, operationQName);
        writer.setLogger(logger);
        if (proxyHostLocation != null) {
            writer.setProxyHostLocation(proxyHostLocation);
            writer.setProxyHostPort(proxyHostPort);
        }
        writer.init(null);
	}

    @Override
    public Result execute() throws Exception {
        // TODO: use thread manager instead of fix amount of threads
        InputReader[] portReaders = new InputReader[portsCnt];

        // read slave ports in separate threads
        for (int idx = 0; idx < portsCnt; idx++) {
            if (allPortDefinitionMap.get(idx) == null) {
                throw new IllegalStateException("Input port " + idx + " is connected, but isn't defined in mapping attribute.");
            }
            portReaders[idx] = new InputReader(idx, allPortDefinitionMap.get(idx));
            portReaders[idx].setSkip(recordsSkip);
            portReaders[idx].setNumRecords(recordsCount);
            portReaders[idx].start();
        }
        
        // wait for slave input threads to finish their job
        boolean killIt = false;
        for (int idx = 0; idx < portsCnt; idx++) {
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

            writer.write(allPortDefinitionMap.values());
            
            writer.finish();
        //flushXmlSax();
        } catch (Exception e) {
            logger.error("Error during creating XML file", e);
            throw e;
        } finally {
            writer.close();
        }
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
    }

    /*
	 * (non-Javadoc)
	 * @see org.jetel.graph.Node#reset()
	 */
	@Override
	public synchronized void reset() throws ComponentNotReadyException {
		super.reset();
		this.writer.reset();
		for (PortDefinition def : allPortDefinitionMap.values()){
			def.reset();
		}
	}

    @Override
	public synchronized void free() {
		super.free();
		if (writer != null)
			writer.close();
	}

    @Override
    public synchronized void abort() {
        if (writer != null) {
            writer.abort();
        }
        super.abort();
    }

    public void setProxyHostLocation(String proxyHostLocation) {
        this.proxyHostLocation = proxyHostLocation;
    }

    public void setProxyHostPort(int proxyHostPort) {
        this.proxyHostPort = proxyHostPort;
    }


    /* (non-Javadoc)
	 * @see org.jetel.graph.Node#getType()
	 */
	public String getType() {
		return COMPONENT_TYPE;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#toXML(org.w3c.dom.Element)
	 */
	public void toXML(Element xmlElement) {
		super.toXML(xmlElement);

		xmlElement.setAttribute(XML_ID_ATTRIBUTE, getId());
	}

    /**
	 * Creates an instance according to XML specification.
	 * @param graph
	 * @param xmlElement
	 * @return
	 * @throws XMLConfigurationException
	 * @throws ComponentNotReadyException
	 */
	public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException {
		WSTemplateDataWriter wsDataWriter = null;
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);

		Document template;
        try {
	        if(xattribs.exists(XML_MAPPING_ATTRIBUTE)) {
	            //read template document with mapping from string in attribute 'mapping'
	            String mapping = xattribs.getString(XML_MAPPING_ATTRIBUTE);
	            template = createDocumentFromString(mapping);
	        } else if (xattribs.exists(XML_MAPPING_URL_ATTRIBUTE)) {
                // read template document with mapping from URL location
                URL xmlLocation = new URL(xattribs.getString(XML_MAPPING_URL_ATTRIBUTE));
                template = createDocumentFromURL(xmlLocation);
            } else {
                throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," no mapping definition specified") + ":");
            }
		} catch (Exception e) {
			logger.error("cannot instantiate node from XML", e);
			throw new XMLConfigurationException(e.getMessage(), e);
		}

		try {
			
            wsDataWriter = new WSTemplateDataWriter(xattribs.getString(XML_ID_ATTRIBUTE));

            if (xattribs.exists(XML_OPERATION_NAME_ATTRIBUTE)){
                wsDataWriter.setOperationQName(xattribs.getQName(XML_OPERATION_NAME_ATTRIBUTE));
            }
            if (xattribs.exists(XML_PORT_TYPE_NAME_ATTRIBUTE)){
                wsDataWriter.setPortTypeQName(xattribs.getQName(XML_PORT_TYPE_NAME_ATTRIBUTE));
            }
            if (xattribs.exists(XML_SERVICE_NAME_ATTRIBUTE)){
                wsDataWriter.setServiceQName(xattribs.getQName(XML_SERVICE_NAME_ATTRIBUTE));
            }
            if (xattribs.exists(XML_WSDL_ADDRESS_ATTRIBUTE)){
                wsDataWriter.setWsdlLocation(xattribs.getURL(XML_WSDL_ADDRESS_ATTRIBUTE));
            }
            if (xattribs.exists(XML_PROXY_LOCATION_ATTRIBUTE)){
                wsDataWriter.setProxyHostLocation(xattribs.getString(XML_PROXY_LOCATION_ATTRIBUTE));
            }
            if (xattribs.exists(XML_PROXY_PORT_ATTRIBUTE)){
                wsDataWriter.setProxyHostPort(xattribs.getInteger(XML_PROXY_PORT_ATTRIBUTE, PROXY_PORT_DEFAULT));
            }

			int recordsSkip = xattribs.getInteger(XML_RECORDS_SKIP_ATTRIBUTE, 0);
			int recordsCount = xattribs.getInteger(XML_RECORDS_COUNT_ATTRIBUTE, 0);

            wsDataWriter.setMessageTemplate(template);
			wsDataWriter.setRecordsCount(recordsCount);
            wsDataWriter.setRecordsSkip(recordsSkip);
            
		} catch (Exception ex) {
	           throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
		}
		return wsDataWriter;
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

    public void setOperationQName(QName operationQName) {
        this.operationQName = operationQName;
    }

    public void setPortTypeQName(QName portTypeQName) {
        this.portTypeQName = portTypeQName;
    }

    public void setPortsCnt(int portsCnt) {
        this.portsCnt = portsCnt;
    }

    public void setRecordsCount(int recordsCount) {
        this.recordsCount = recordsCount;
    }

    public void setRecordsSkip(int recordsSkip) {
        this.recordsSkip = recordsSkip;
    }

    public void setServiceQName(QName serviceQName) {
        this.serviceQName = serviceQName;
    }

    public void setWsdlLocation(URL wsdlLocation) {
        this.wsdlLocation = wsdlLocation;
    }

    public void setMessageTemplate(Document messageTemplate) {
        this.messageTemplate = messageTemplate;
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
