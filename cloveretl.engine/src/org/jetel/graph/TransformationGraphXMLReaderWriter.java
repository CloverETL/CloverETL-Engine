/*
 * jETeL/CloverETL - Java based ETL application framework.
 * Copyright (c) Javlin, a.s. (info@cloveretl.com)
 *  
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.jetel.graph;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.component.ComponentFactory;
import org.jetel.data.lookup.LookupTable;
import org.jetel.data.lookup.LookupTableFactory;
import org.jetel.data.sequence.Sequence;
import org.jetel.data.sequence.SequenceFactory;
import org.jetel.database.ConnectionFactory;
import org.jetel.database.IConnection;
import org.jetel.enums.EdgeDebugMode;
import org.jetel.enums.EdgeTypeEnum;
import org.jetel.enums.EnabledEnum;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.GraphConfigurationException;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.ContextProvider.Context;
import org.jetel.graph.dictionary.Dictionary;
import org.jetel.graph.dictionary.UnsupportedDictionaryOperation;
import org.jetel.graph.rest.jaxb.EndpointSettings;
import org.jetel.graph.rest.jaxb.RestJobResponseStatus;
import org.jetel.graph.runtime.ExecutionType;
import org.jetel.graph.runtime.GraphRuntimeContext;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.DataRecordMetadataStub;
import org.jetel.metadata.DataRecordMetadataXMLReaderWriter;
import org.jetel.metadata.MetadataFactory;
import org.jetel.util.JAXBContextProvider;
import org.jetel.util.ReferenceState;
import org.jetel.util.RestJobUtils;
import org.jetel.util.XmlParserFactory;
import org.jetel.util.XmlParserFactory.DocumentBuilderProvider;
import org.jetel.util.file.FileUtils;
import org.jetel.util.primitive.TypedProperties;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.PropertyRefResolver;
import org.jetel.util.property.RefResFlag;
import org.jetel.util.string.StringUtils;
import org.jetel.util.string.UnicodeBlanks;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXParseException;

/**
 *  Helper class which reads transformation graph definition from XML data
 *
 * The XML DTD describing the internal structure is as follows:
 *
 * <pre>
 * &lt;!ELEMENT Graph (Global , Phase+)&gt;
 * &lt;!ATTLIST Graph
 *		name ID #REQUIRED 
 *      debugMode NMTOKEN (true | false) #IMPLIED
 *      debugDirectory CDATE #IMPLIED&gt;
 *
 * &lt;!ELEMENT Global (Property*, Metadata+, Connection*, Sequence*, LookupTable*)&gt;
 *
 * &lt;!ELEMENT Property (#PCDATA)&gt;
 * &lt;!ATTLIST Property
 *           	name CDATA #IMPLIED
 * 				value CDATA #IMPLIED
 *				fileURL CDATA #IMPLIED&gt;
 *
 * &lt;!ELEMENT Metadata (#PCDATA)&gt;
 * &lt;!ATTLIST Metadata
 *           	id ID #REQUIRED
 *				fileURL CDATA #IMPLIED
 *              connection CDATA #IMPLIED
 *              sqlQuery CDATA #IMPLIED&gt;
 *
 * &lt;!ELEMENT Connection (#PCDATA)&gt;
 * &lt;!ATTLIST Connection
 *           	id ID #REQUIRED
 *              type NMTOKEN #REQUIRED
 *
 * &lt;!ELEMENT Sequence (#PCDATA)&gt;
 * &lt;!ATTLIST Sequence
 *          	id ID #REQUIRED 
 *              type NMTOKEN #REQUIRED &gt;
 *
 * &lt;!ELEMENT LookupTable (#PCDATA)&gt;
 * &lt;!ATTLIST LookupTable
 *          	id ID #REQUIRED 
 *              type NMTOKEN #REQUIRED &gt;
 *
 * &lt;!ELEMENT Phase (Node+ , Edge+)&gt;
 * &lt;!ATTLIST Phase
 *		number NMTOKEN #REQUIRED&gt;
 *
 * &lt;!ELEMENT Node (#PCDATA)&gt;
 * &lt;!ATTLIST Node
 *		id ID #REQUIRED 
 *      type NMTOKEN #REQUIRED
 *      enabled NMTOKEN (enabled | disabled) #IMPLIED 
 *      passThroughOutputPort NMTOKEN #IMPLIED 
 *      passThroughInputPort NMTOKEN #IMPLIED &gt;
 *
 * &lt;!ELEMENT Edge (#PCDATA)&gt;
 * &lt;!ATTLIST Edge
 *		id ID #REQUIRED
 *		metadata NMTOKEN #REQUIRED
 *		fromNode NMTOKEN #REQUIRED
 *		toNode	NMTOKEN #REQUIRED
 *      debugMode NMTOKEN (true | false) #IMPLIED 
 *      fastPropagate NMTOKEN (true | false) #IMPLIED&gt;
 *
 *
 * </pre>
 * Node & port specified in Edge element (fromNode & toNode attributes) must comply with following pattern: <br>
 *
 * <i>&lt;Node ID&gt;</i><b>:</b><i>&lt;Port Number&gt;</i><br><br>
 *
 * Example:
 * <pre>
 * &lt;Graph name="MyTransformation"&gt;
 * &lt;Global&gt;
 * &lt;Metadata id="DataTypeA" fileURL="$HOME/myMetadata/dataTypeA.xml"/&gt;
 * &lt;Metadata id="DataTypeB" fileURL="$HOME/myMetadata/dataTypeB.xml"/&gt;
 * &lt;/Global&gt;
 * &lt;Phase number="0"&gt;
 * &lt;Node id="INPUT" type="DELIMITED_DATA_READER" fileURL="c:\projects\jetel\pins.ftdglacc.dat" /&gt;
 * &lt;Node id="COPY" type="SIMPLE_COPY"/&gt;
 * &lt;Node id="OUTPUT" type="DELIMITED_DATA_WRITER" append="false" fileURL="c:\projects\jetel\pins.ftdglacc.dat.out" /&gt;
 * &lt;Edge id="INEDGE" fromNode="INPUT:0" toNode="COPY:0" metadata="InMetadata"/&gt;
 * &lt;Edge id="OUTEDGE" fromNode="COPY:0" toNode="OUTPUT:0" metadata="InMetadata"/&gt;
 * &lt;/Phase&gt;
 * &lt;/Graph&gt;
 * </pre>
 *
 * @author     dpavlis
 * @since    May 21, 2002
 */
public class TransformationGraphXMLReaderWriter {
	
	/**
	 * 
	 */
	public static final String JOBFLOW_NATURE = "jobflow";
	private final static String GRAPH_ELEMENT = "Graph";
	//unused private final static String GLOBAL_ELEMENT = "Global";
	public final static String NODE_ELEMENT = "Node";
	private final static String EDGE_ELEMENT = "Edge";
	private final static String METADATA_ELEMENT = "Metadata";
	public final static String METADATA_GROUP_ELEMENT = "MetadataGroup";
	private final static String PHASE_ELEMENT = "Phase";
	public final static String CONNECTION_ELEMENT = "Connection";
	public final static String SEQUENCE_ELEMENT = "Sequence";
	public final static String LOOKUP_TABLE_ELEMENT = "LookupTable";
	private final static String METADATA_RECORD_ELEMENT = "Record";
	private final static String PROPERTY_ELEMENT = "Property"; //old-fashion graph parameters
	private final static String GRAPH_PARAMETER_ELEMENT = "GraphParameter"; //new graph parameters
	private final static String GRAPH_PARAMETERS_ELEMENT = "GraphParameters";
	private final static String GRAPH_PARAMETER_FILE_ELEMENT = "GraphParameterFile";
	
	public final static String SUBGRAPH_INPUT_PORTS_ELEMENT = "inputPorts";
	public final static String SUBGRAPH_OUTPUT_PORTS_ELEMENT = "outputPorts";
	public final static String SUBGRAPH_SINGLE_PORT_ELEMENT= "singlePort";
	public final static String SUBGRAPH_PORT_NAME_ATTRIBUTE = "name";
	public final static String SUBGRAPH_PORT_REQUIRED_ATTRIBUTE = "required";
	public final static String SUBGRAPH_PORT_KEEP_EDGE_ATTRIBUTE = "keepEdge";
	public final static String SUBGRAPH_PORT_CONNECTED_ATTRIBUTE = "connected";
	
	public final static String ENDPOINT_SETTINGS_ELEMENT = "EndpointSettings";
	public final static String ENDPOINT_SETTINGS_URL_PATH_ELEMENT = "UrlPath";
	public final static String ENDPOINT_SETTINGS_DESCRIPTION_ELEMENT = "Description";
	public final static String ENDPOINT_SETTINGS_ENDPOINT_NAME_ELEMENT = "EndpointName";
	public final static String ENDPOINT_SETTINGS_EXAMPLE_OUTPUT_ELEMENT = "ExampleOutput";
	public final static String ENDPOINT_SETTINGS_METHOD_ELEMENT = "RequestMethod";
	public final static String ENDPOINT_SETTINGS_METHOD_NAME_ATTR = "name";
	public final static String ENDPOINT_SETTINGS_PARAM_ELEMENT = "RequestParameter";
	public final static String ENDPOINT_SETTINGS_PARAM_NAME_ATTR = "name";
	public final static String ENDPOINT_SETTINGS_PARAM_TYPE_ATTR = "type";
	public final static String ENDPOINT_SETTINGS_PARAM_FORMAT_ATTR = "format";
	public final static String ENDPOINT_SETTINGS_PARAM_REQUIRED_ATTR = "required";
	public final static String ENDPOINT_SETTINGS_PARAM_DESCRIPTION_ATTR = "description";
	
	public final static String REST_JOB_RESPONSE_STATUS = "RestJobResponseStatus";
	
	private final static String DICTIONARY_ELEMENT = "Dictionary";
	private final static String DICTIONARY_ENTRY_ELEMENT = "Entry";
	private final static String DICTIONARY_ENTRY_ID = "id";
	private final static String DICTIONARY_ENTRY_NAME = "name";
	private final static String DICTIONARY_ENTRY_TYPE = "type";
	private final static String DICTIONARY_ENTRY_INPUT = "input";
	private final static String DICTIONARY_ENTRY_OUTPUT = "output";
	private final static String DICTIONARY_ENTRY_REQUIRED = "required";
	private final static String DICTIONARY_ENTRY_CONTENT_TYPE = "contentType";
	
	public final static String ID_ATTRIBUTE = "id";
	public final static String NAME_ATTRIBUTE = "name";
	public final static String AUTHOR_ATTRIBUTE = "author";
	public final static String CREATED_ATTRIBUTE = "created";
	public final static String LICENSE_CODE_ATTRIBUTE = "licenseCode";
	public final static String GUI_VERSION_ATTRIBUTE = "guiVersion";
	public final static String DESCRIPTION_ATTRIBUTE = "description";
	public final static String EXECUTION_LABEL_ATTRIBUTE = "executionLabel";
	public final static String CATEGORY_ATTRIBUTE = "category";
	public final static String SMALL_ICON_PATH_ATTRIBUTE = "smallIconPath";
	public final static String MEDIUM_ICON_PATH_ATTRIBUTE = "mediumIconPath";
	public final static String LARGE_ICON_PATH_ATTRIBUTE = "largeIconPath";
	public final static String JOB_TYPE_ATTRIBUTE = "nature";
	public final static String SHOW_COMPONENT_DETAILS_ATTRIBUTE = "showComponentDetails";
	
	private final static int ALLOCATE_MAP_SIZE=64;
	/**
	 * Default parser name.
	 *
	 * @since    May 21, 2002
	 */
	//not needed any more private final static String DEFAULT_PARSER_NAME = "";

	private static final boolean validation = false;
	private static final boolean ignoreComments = true;
	private static final boolean ignoreWhitespaces = true;
	private static final boolean putCDATAIntoText = true;
	private static final boolean createEntityRefs = false;
	
	private static Log logger = LogFactory.getLog(TransformationGraphXMLReaderWriter.class);
	
	private static DocumentBuilderFactory documentBuilderFactory;

	private Document outputXMLDocument = null;
	
    private TransformationGraph graph;
    
    private GraphRuntimeContext runtimeContext;
    
    /**
     * This is already deprecated way to set strict or lenient graph parsing.
     * It is recommended to used {@link GraphRuntimeContext#setStrictGraphFactorization(boolean)} instead.
     */
    @Deprecated
    private Boolean strictParsing;

    /** Should be metadata automatically propagated? */
    private boolean metadataPropagation = true;
    
    /**
     * Extract only graph parameters and dictionary.
     */
    private boolean onlyParamsAndDict = false;
    
    private final Marshaller graphParameterMarshaller;
    private final Unmarshaller graphParameterUnmarshaller;
    
    /**  List of exceptions, which were suppressed with lenient parsing */
    private List<Throwable> suppressedExceptions = new ArrayList<>();
    
    /**
     * Instantiates transformation graph from a given input stream and presets a given properties.
     * @param graphStream graph in XML form stored in character stream
	 * @param runtimeContext is used as source of additional properties and context URL and is also preset as an intial runtime context for the new graph
     * @return transformation graph
     * @throws XMLConfigurationException deserialization from XML fails for any reason.
     * @throws GraphConfigurationException misconfigured graph
     */
	public static TransformationGraph loadGraph(InputStream graphStream, GraphRuntimeContext runtimeContext)
	throws XMLConfigurationException, GraphConfigurationException {
        TransformationGraphXMLReaderWriter graphReader = new TransformationGraphXMLReaderWriter(runtimeContext);
        return graphReader.read(graphStream);
    }

	/**
	 *Constructor for the TransformationGraphXMLReaderWriter object
	 *
	 * @since    May 24, 2002
	 */
	public TransformationGraphXMLReaderWriter(GraphRuntimeContext runtimeContext) {
		this.runtimeContext = runtimeContext;
		
		try {
			graphParameterMarshaller = JAXBContextProvider.getInstance().getContext(GraphParameters.class).createMarshaller();
			graphParameterMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
			
			graphParameterUnmarshaller = JAXBContextProvider.getInstance().getContext(GraphParameter.class).createUnmarshaller();
		}
		catch (JAXBException e) {
			throw new JetelRuntimeException(e);
		}
	}

	private static synchronized DocumentBuilderProvider getDocumentBuilderProvider() {
		if (documentBuilderFactory == null) {
			documentBuilderFactory = DocumentBuilderFactory.newInstance();
			 
			documentBuilderFactory.setNamespaceAware(true);
	
			// Optional: set various configuration options
			documentBuilderFactory.setValidating(validation);
			documentBuilderFactory.setIgnoringComments(ignoreComments);
			documentBuilderFactory.setIgnoringElementContentWhitespace(ignoreWhitespaces);
			documentBuilderFactory.setCoalescing(putCDATAIntoText);
			documentBuilderFactory.setExpandEntityReferences(!createEntityRefs);
		}

		return XmlParserFactory.getDocumentBuilder(documentBuilderFactory);
	}
	
	private static Document prepareDocument(InputStream stream) throws XMLConfigurationException {
		InputSource inputSource = (stream != null) ? new InputSource(new BufferedInputStream(stream)) : null;
		return prepareDocument(inputSource);
	}
	
	private static Document prepareDocument(Reader reader) throws XMLConfigurationException {
		InputSource inputSource = (reader != null) ? new InputSource(new BufferedReader(reader)) : null;
		return prepareDocument(inputSource);
	}
	
	private static Document prepareDocument(InputSource in) throws XMLConfigurationException {
		Document document;
		DocumentBuilderProvider documentBuilderProvider = null;
		try {
			documentBuilderProvider = getDocumentBuilderProvider();
			if (in != null) {
				document = documentBuilderProvider.getDocumentBuilder().parse(in);
				document.normalize();
			}else{
				document = documentBuilderProvider.getDocumentBuilder().newDocument();
			}
		}catch(SAXParseException ex){
			throw new XMLConfigurationException("Error when parsing graph's XML definition  --> on line "+ex.getLineNumber()+" row "+ex.getColumnNumber(), ex);
        }catch (Exception ex) {
            throw new XMLConfigurationException("Error when parsing graph's XML definition", ex);
		} finally {
			XmlParserFactory.releaseDocumentBuilder(documentBuilderProvider);
		}
		
		return document;
	}
	
	/**
	 * Reads graph identifier from the given graph in XML form.
	 * @param in
	 * @return
	 * @throws XMLConfigurationException
	 */
	public String readId(InputStream in) throws XMLConfigurationException {
//		Document document = prepareDocument(in);
//
//		NodeList graphElement = document.getElementsByTagName(GRAPH_ELEMENT);
//		String id = ((Element)graphElement.item(0)).getAttribute("id");
//		
//		if(StringUtils.isEmpty(id)) {
//			id = ((Element)graphElement.item(0)).getAttribute("name");
//		}
//		
//		return id;
		try {
			return readIdStax(in);
		} catch (XMLStreamException e) {
			throw new XMLConfigurationException(e);
		}
	}
	
	private String readIdStax(InputStream in) throws XMLStreamException {
		
		XMLStreamReader reader = XMLInputFactory.newFactory().createXMLStreamReader(in);
		try {
			while (reader.hasNext()) {
				int event = reader.next();
				if (event == XMLEvent.START_ELEMENT && GRAPH_ELEMENT.equals(reader.getLocalName())) {
					final int count = reader.getAttributeCount();
					for (int i = 0; i < count; ++i) {
						if (ID_ATTRIBUTE.equals(reader.getAttributeLocalName(i))) {
							String id = reader.getAttributeValue(i);
							if (!StringUtils.isEmpty(id)) {
								return id;
							}
						}
					}
					for (int i = 0; i < count; ++i) {
						if (NAME_ATTRIBUTE.equals(reader.getAttributeLocalName(i))) {
							return reader.getAttributeValue(i);
						}
					}
				}
			}
			return null;
		} finally {
			reader.close();
		}
	}
	
	/**
	 * Reads graph XML as a string.
	 * 
	 * @param graphXml - content of the graph file
	 * @return {@link TransformationGraph} built from the XML
	 * 
	 * @throws XMLConfigurationException
	 * @throws GraphConfigurationException
	 */
	public TransformationGraph read(String graphXml) throws XMLConfigurationException, GraphConfigurationException {
		return read(new StringReader(graphXml));
	}
	
	/**
	 * Builds {@link TransformationGraph} from the provided character stream.
	 * 
	 * @param reader - character stream containing graph XML 
	 * @return {@link TransformationGraph} built from the reader
	 * 
	 * @throws XMLConfigurationException
	 * @throws GraphConfigurationException
	 */
	public TransformationGraph read(Reader reader) throws XMLConfigurationException, GraphConfigurationException {
		Document document = prepareDocument(reader);
		return readDocument(document);
	}

	/**
	 * Builds {@link TransformationGraph} from the provided byte stream.
	 * The charset is auto-detected from the XML header.
	 * 
	 * @param is - byte stream containing graph XML
	 * @return {@link TransformationGraph} built from the input stream
	 * @throws XMLConfigurationException
	 * @throws GraphConfigurationException
	 */
	public TransformationGraph read(InputStream is) throws XMLConfigurationException, GraphConfigurationException {
		Document document = prepareDocument(is);
		return readDocument(document);
	}
	
	private TransformationGraph readDocument(Document document)
			throws XMLConfigurationException, GraphConfigurationException {
		try {
			read(document);
		} catch (XMLConfigurationException e) {
			if (isStrictParsing()) {
				throw e;
			}
		} catch (GraphConfigurationException e) {
			if (isStrictParsing()) {
				throw e;
			}
		}
		
		return graph;
	}
	
	public EndpointSettings readEndpointSettings(InputStream stream) throws IOException, XMLConfigurationException, GraphConfigurationException {
		
		Document doc = prepareDocument(stream);
		Element global = getGlobalElement(doc);
		NodeList nodes = global.getElementsByTagName(ENDPOINT_SETTINGS_ELEMENT);
		if (nodes.getLength() > 0) {
			return instantiateEndpointSettings(nodes.item(0));
		}
		return null;
	}
	
	public String readOutputFormat(InputStream stream) throws XMLConfigurationException {
		return readOutputFormat(prepareDocument(stream));
	}
	
	public String readOutputFormat(Document doc) throws XMLConfigurationException {
		NodeList nodes = doc.getElementsByTagName(NODE_ELEMENT);
		for (int i = 0; i < nodes.getLength(); i++) {
			if (nodes.item(i).getAttributes().getNamedItem(IGraphElement.XML_TYPE_ATTRIBUTE).getNodeValue()
					.equals(RestJobUtils.REST_JOB_OUTPUT_TYPE)) {
				return nodes.item(i).getAttributes().getNamedItem("responseFormat").getNodeValue();
			}
		}
		return null;
	}
	
	public RestJobResponseStatus readResponseStatuses(InputStream stream) throws XMLConfigurationException, GraphConfigurationException {
		Document doc = prepareDocument(stream);
		Element global = getGlobalElement(doc);
		NodeList nodes = global.getElementsByTagName(REST_JOB_RESPONSE_STATUS);
		if (nodes.getLength() > 0) {
			return instantiateRestJobResponseStatus(nodes.item(0));
		}
		return null;
	}
	
	private void checkInterrupted() {
		if (Thread.currentThread().isInterrupted()) {
			throw new JetelRuntimeException(new InterruptedException());
		}
	}

	/**
	 *Constructor for the read object
	 *
	 * @param  in     Description of Parameter
	 * @param  graph  Description of Parameter
	 * @return        Description of the Returned Value
	 * @since         May 21, 2002
	 */
	public TransformationGraph read(Document document) throws XMLConfigurationException,GraphConfigurationException {
		checkInterrupted();
		Context c = null;
		try {
			Map<String, Object> metadata = new HashMap<String, Object>(ALLOCATE_MAP_SIZE);
			graph = null;
			
			// process document
			NodeList graphElement = document.getElementsByTagName(GRAPH_ELEMENT);
			String id = ((Element)graphElement.item(0)).getAttribute("id");
	        //get graph id
			graph = new TransformationGraph(id);

			//it is necessary for correct edge factorisation in EdgeFactory (maybe will be useful even somewhere else)
			c = ContextProvider.registerGraph(graph);
			
			//deprecated strictParsing flag is stored into runtimeContext, which is preferable way
			if (strictParsing != null) {
				runtimeContext.setStrictGraphFactorization(strictParsing);
			}
			graph.setInitialRuntimeContext(runtimeContext);
			
			// get graph name
			ComponentXMLAttributes grfAttributes=new ComponentXMLAttributes((Element)graphElement.item(0), graph);
			try {
				graph.setName(grfAttributes.getString("name"));
			} catch (AttributeNotFoundException ex) {
				throwXMLConfigurationException("Name attribute at Graph node is missing.", ex);
			}
	        
	        grfAttributes.setResolveReferences(false);
	        //get debug mode
	        graph.setEdgeDebugging(grfAttributes.getString("debugMode", "true"));
	        //get debugMaxRecords
	        graph.setDebugMaxRecords(grfAttributes.getLong("debugMaxRecords", 0));
	        
	        graph.setAuthor(grfAttributes.getString(AUTHOR_ATTRIBUTE, null));
	        graph.setCreated(grfAttributes.getString(CREATED_ATTRIBUTE, null));
	        graph.setLicenseCode(grfAttributes.getString(LICENSE_CODE_ATTRIBUTE, null));
	        graph.setGuiVersion(grfAttributes.getString(GUI_VERSION_ATTRIBUTE, null));
	        graph.setDescription(grfAttributes.getString(DESCRIPTION_ATTRIBUTE, null));
	        graph.setExecutionLabel(grfAttributes.getString(EXECUTION_LABEL_ATTRIBUTE, null));
	        graph.setCategory(grfAttributes.getString(CATEGORY_ATTRIBUTE, null));
	        graph.setSmallIconPath(grfAttributes.getString(SMALL_ICON_PATH_ATTRIBUTE, null));
	        graph.setMediumIconPath(grfAttributes.getString(MEDIUM_ICON_PATH_ATTRIBUTE, null));
	        graph.setLargeIconPath(grfAttributes.getString(LARGE_ICON_PATH_ATTRIBUTE, null));
	        graph.setStaticJobType(JobType.fromString(grfAttributes.getString(JOB_TYPE_ATTRIBUTE, null)));
	
	        final Element global = getGlobalElement(document);
	        //read subgraph input ports
			List<Element> subgraphInputPortsElements = getChildElements(global, SUBGRAPH_INPUT_PORTS_ELEMENT);
	        instantiateSubgraphPorts(true, graph.getSubgraphInputPorts(), subgraphInputPortsElements);

	        //read subgraph output ports
			List<Element> subgraphOutputPortsElements = getChildElements(global, SUBGRAPH_OUTPUT_PORTS_ELEMENT);
	        instantiateSubgraphPorts(false, graph.getSubgraphOutputPorts(), subgraphOutputPortsElements);

			// handle all defined graph parameters - old-fashion
			NodeList PropertyElements = document.getElementsByTagName(PROPERTY_ELEMENT);
			instantiateProperties(PropertyElements);

			// handle all defined graph parameters - new-fashion
			List<Element> graphParametersElements = getChildElements(global, GRAPH_PARAMETERS_ELEMENT);
			instantiateGraphParameters(graph.getGraphParameters(), graphParametersElements);

			//additional graph parameters are loaded after all build-in parameters are already loaded
			//moreover, additional parameters are used in regular loading of build-in parameters (see #instantiateGraphParameter)
			//to ensure correct value of parameters is used when path parameter file is parametrised by
			//parameter overridden by additional parameters.
			graph.getGraphParameters().addPropertiesOverride(runtimeContext.getAdditionalProperties(), !runtimeContext.getJobType().isSubJob());
			
			// read endpoint settings (if any)
			NodeList endpoint = global.getElementsByTagName(ENDPOINT_SETTINGS_ELEMENT);
			if (endpoint.getLength() > 0) {
				EndpointSettings settings = instantiateEndpointSettings(endpoint.item(0));
				graph.setEndpointSettings(settings);
			}

			NodeList responseStatuses = global.getElementsByTagName(REST_JOB_RESPONSE_STATUS);
			if (responseStatuses.getLength() > 0) {
				RestJobResponseStatus status = instantiateRestJobResponseStatus(responseStatuses.item(0));
				graph.setRestJobResponseStatus(status);
			}
			
			graph.setOutputFormat(readOutputFormat(document));
			
			// handle dictionary
			NodeList dictionaryElements = global.getElementsByTagName(DICTIONARY_ELEMENT);
			instantiateDictionary(dictionaryElements);
			
			if (!onlyParamsAndDict) {
				// handle all defined DB connections
				NodeList dbConnectionElements = global.getElementsByTagName(CONNECTION_ELEMENT);
				instantiateDBConnections(dbConnectionElements);
		
				// handle all defined DB connections
				NodeList sequenceElements = global.getElementsByTagName(SEQUENCE_ELEMENT);
				instantiateSequences(sequenceElements);
				
				//create metadata
				//NodeList metadataElements = document.getElementsByTagName(METADATA_ELEMENT);
				instantiateMetadata(global, metadata);
		
				// register all metadata (DataRecordMetadata) within transformation graph
				graph.addDataRecordMetadata(metadata);
		
				// handle all defined lookup tables
				NodeList lookupsElements = global.getElementsByTagName(LOOKUP_TABLE_ELEMENT);
				instantiateLookupTables(lookupsElements);
		
				NodeList phaseElements = document.getElementsByTagName(PHASE_ELEMENT);
				instantiatePhases(phaseElements);
		
				NodeList edgeElements = document.getElementsByTagName(EDGE_ELEMENT);
				instantiateEdges(edgeElements, metadata, graph.getDebugMaxRecords());

				//finally analyse the graph
				try {
					TransformationGraphAnalyzer.analyseGraph(graph, runtimeContext, metadataPropagation);
				} catch (Exception e) {
					throwXMLConfigurationException("Graph analysis failed.", e);
				}
			}

	        return graph;
		} finally {
			ContextProvider.unregister(c);
		}
	}

	private EndpointSettings instantiateEndpointSettings(org.w3c.dom.Node element) throws GraphConfigurationException {
		try {
			JAXBContext ctx = JAXBContextProvider.getInstance().getContext(EndpointSettings.class);
			Unmarshaller unmarshaller = ctx.createUnmarshaller();
			return (EndpointSettings)unmarshaller.unmarshal(element);
		} catch (Exception e) {
			throw new GraphConfigurationException("Could not parse endpoint settings: " + e.getMessage());
		}
	}
	
	private RestJobResponseStatus instantiateRestJobResponseStatus(org.w3c.dom.Node element) throws GraphConfigurationException {
		try {
			JAXBContext ctx = JAXBContextProvider.getInstance().getContext(RestJobResponseStatus.class);
			Unmarshaller unmarshaller = ctx.createUnmarshaller();
			return (RestJobResponseStatus)unmarshaller.unmarshal(element);
		} catch (Exception e) {
			throw new GraphConfigurationException("Could not parse REST job response status: " + e.getMessage());
		}
	}

	/**
	 * Return 'Global' XML element, direct child of 'Graph' element.
	 */
	private Element getGlobalElement(Document document) throws XMLConfigurationException {
		List<Element> global = getChildElements(document.getDocumentElement(), "Global");
		if (global.size() == 0) {
			return null;
		} else if (global.size() == 1) {
			return global.get(0);
		} else {
			throwXMLConfigurationException("Multiple Global XML element.");
			return global.get(0);
		}
	}

	private void instantiateMetadata(Element metadataRoot, Map<String, Object> metadata) throws XMLConfigurationException {
		List<Element> metadataElements = getChildElements(metadataRoot, METADATA_ELEMENT);
		instantiateMetadata(metadataElements, metadata);
		
		List<Element> metadataGroupElements = getChildElements(metadataRoot, METADATA_GROUP_ELEMENT);
		for (Element metadataGroupElement : metadataGroupElements) {
			instantiateMetadata(metadataGroupElement, metadata);
		}
	}

	private void instantiateMetadata(List<Element> metadataElements, Map<String, Object> metadata) throws XMLConfigurationException {
		String metadataID = null;
		String fileURL=null;
		Object recordMetadata = null;
		//PropertyRefResolver refResolver=new PropertyRefResolver();

		// loop through all Metadata elements & create appropriate Metadata objects
		for (int i = 0; i < metadataElements.size(); i++) {
			checkInterrupted();
			ComponentXMLAttributes attributes = new ComponentXMLAttributes(metadataElements.get(i), graph);
			recordMetadata = null;
			try {
				// process metadata element attributes "id" & "fileURL"
				metadataID = attributes.getString("id");
				
				// process metadata from file
				if (attributes.exists("fileURL")){
					fileURL = attributes.getStringEx("fileURL", RefResFlag.SPEC_CHARACTERS_OFF);
					try {
					    recordMetadata=MetadataFactory.fromFile(graph, fileURL);
	                } catch (IOException ex) {
	                	throwXMLConfigurationException("Cannot parse metadata '" + metadataID + "'. Error when reading/parsing record metadata definition file '" + fileURL +"'.", ex);
	                }
				}// metadata from analyzing DB table (JDBC) - will be resolved
				// later during Edge init - just put stub now.
				else if (attributes.exists(DataRecordMetadataXMLReaderWriter.CONNECTION_ATTR)){
					IConnection connection = graph.getConnection(attributes.getString(DataRecordMetadataXMLReaderWriter.CONNECTION_ATTR));
					if(connection == null) {
						throwXMLConfigurationException("Cannot find Connection id - " + attributes.getString(DataRecordMetadataXMLReaderWriter.CONNECTION_ATTR) + ".");
					} else {
						recordMetadata = new DataRecordMetadataStub(connection, attributes.attributes2Properties(null));
					}
				} // probably metadata inserted directly into graph
				else {
					recordMetadata=MetadataFactory.fromXML(graph, attributes.getChildNode(metadataElements.get(i),METADATA_RECORD_ELEMENT));
				}
			} catch (AttributeNotFoundException ex) {
				throwXMLConfigurationException("Metadata - Attributes missing (id='" + metadataID + "').", ex);
			} catch (Exception e) {
				throwXMLConfigurationException("Metadata cannot be instantiated (id='" + metadataID + "').", e);
			}
			//set metadataId
			if (recordMetadata != null) {
				if (recordMetadata instanceof DataRecordMetadata) {
					((DataRecordMetadata) recordMetadata).setId(metadataID);
				} else {
					((DataRecordMetadataStub) recordMetadata).setId(metadataID);
				}
				// register metadata object
				if (metadataID != null && metadata.put(metadataID, recordMetadata) != null) {
					throwXMLConfigurationException("Metadata '" + metadataID + "' already defined - duplicate ID detected!");
				}
			}
		}
		// we successfully instantiated all metadata
	}

	private void instantiatePhases(NodeList phaseElements) throws XMLConfigurationException,GraphConfigurationException{
		org.jetel.graph.Phase phase;
		int phaseNum;
		NodeList nodeElements;
		
		// loop through all Node elements & create appropriate Metadata objects
		for (int i = 0; i < phaseElements.getLength(); i++) {
			ComponentXMLAttributes attributes = new ComponentXMLAttributes((Element)phaseElements.item(i), graph);
			// process Phase element attribute "number"
			try {
				phaseNum = attributes.getInteger("number");
				if ((phase = graph.getPhase(phaseNum)) == null) {
					phase=new Phase(phaseNum);
					graph.addPhase(phase);
				}
				// get all nodes defined in this phase and instantiate them
				// we expect that all children of phase are Nodes
				//phaseElements.item(i).normalize();
				nodeElements=phaseElements.item(i).getChildNodes();
				instantiateNodes(phase,nodeElements);
			}catch(AttributeNotFoundException ex) {
				throwXMLConfigurationException("Attribute is missing for phase.", ex);
			}catch(NumberFormatException ex1){
				throwXMLConfigurationException("Phase attribute number is not a valid integer.", ex1);
			} catch (Exception e) {
				throwXMLConfigurationException(null, e);
			}
		}
	}


	/**
	 *  Description of the Method
	 *
	 * @param  nodeElements  Description of Parameter
	 * @param  nodes         Description of Parameter
	 * @since                May 24, 2002
	 */
	private void instantiateNodes(Phase phase, NodeList nodeElements) throws XMLConfigurationException,GraphConfigurationException {
		Node graphNode;
		String nodeType;
		String nodeID="unknown";
		String nodeEnabled;
        int nodePassThroughInputPort;
        int nodePassThroughOutputPort;
        
		// loop through all Node elements & create appropriate Metadata objects
		for (int i = 0; i < nodeElements.getLength(); i++) {
			checkInterrupted();
			if (NODE_ELEMENT.compareToIgnoreCase(nodeElements.item(i)
					.getNodeName()) != 0) {
				continue;
			}
			ComponentXMLAttributes attributes = new ComponentXMLAttributes((Element)nodeElements.item(i), graph);

			// process Node element attributes "id" & "type"
			try {
				nodeID = attributes.getString(IGraphElement.XML_ID_ATTRIBUTE);
				nodeType = attributes.getString(IGraphElement.XML_TYPE_ATTRIBUTE);
                nodeEnabled = attributes.getString(Node.XML_ENABLED_ATTRIBUTE, EnabledEnum.DEFAULT_VALUE.toString());
                nodePassThroughInputPort = attributes.getInteger("passThroughInputPort", 0);
                nodePassThroughOutputPort = attributes.getInteger("passThroughOutputPort", 0);
                if (EnabledEnum.fromString(nodeEnabled, EnabledEnum.DEFAULT_VALUE).isEnabled()) {
					graphNode = ComponentFactory.createComponent(graph, nodeType, nodeElements.item(i));
                } else {
                    graphNode = ComponentFactory.createDummyComponent(graph, nodeType, null, nodeElements.item(i));
                }
				if (graphNode != null) {
                    phase.addNode(graphNode);
                    graphNode.setEnabled(nodeEnabled);
                    graphNode.setPassThroughInputPort(nodePassThroughInputPort);
                    graphNode.setPassThroughOutputPort(nodePassThroughOutputPort);
                    persistRawComponentEnabledAttribute(attributes, graphNode);
				} else {
					throwXMLConfigurationException("Error when creating Component type '" + nodeType + "'.");
				}
			} catch (AttributeNotFoundException ex) {
				throwXMLConfigurationException("Missing attribute at node '" + nodeID + "'.", ex);
			} catch (Exception e) {
				throwXMLConfigurationException(e);
			}
		}
	}

	/** This method persists raw value of enabled attribute of given component. */
	private void persistRawComponentEnabledAttribute(ComponentXMLAttributes attributes, Node graphNode) {
        attributes.setResolveReferences(false);
        try {
        	graph.getRawComponentEnabledAttribute().put(graphNode, attributes.getStringEx(Node.XML_ENABLED_ATTRIBUTE, null, RefResFlag.ALL_OFF));
        } finally {
        	attributes.setResolveReferences(true);
        }
	}

	/**
	 *  Description of the Method
	 *
	 * @param  edgeElements  Description of Parameter
	 * @param  edges         Description of Parameter
	 * @param  metadata      Description of Parameter
	 * @param  nodes         Description of Parameter
	 * @since                May 24, 2002
	 */
	private void instantiateEdges(NodeList edgeElements, Map<String, Object> metadata, long graphDebugMaxRecords) throws XMLConfigurationException,GraphConfigurationException {
		String edgeID="unknown";
		String edgeMetadataID;
		String fromNodeAttr;
		String toNodeAttr;
		String edgeType = null;
        EdgeDebugMode debugMode;
        String debugFilterExpression = null;
        long debugMaxRecords = 0;
        boolean debugLastRecords = true;
        boolean debugSampleData = false;
        boolean fastPropagate = false;
		String[] specNodePort;
		int fromPort;
		int toPort;
		String metadataRef;
		ReferenceState metadataRefState;
		org.jetel.graph.Edge graphEdge;
		org.jetel.graph.Node writerNode;
		org.jetel.graph.Node readerNode;

		// loop through all Node elements & create appropriate Metadata objects
		for (int i = 0; i < edgeElements.getLength(); i++) {
			checkInterrupted();
			ComponentXMLAttributes attributes = new ComponentXMLAttributes((Element)edgeElements.item(i), graph);

			// process edge element attributes "id" & "fileURL"
			try{
				edgeID = attributes.getString(IGraphElement.XML_ID_ATTRIBUTE);
				edgeMetadataID = attributes.getString("metadata", null); //metadata paramater on the edge can be empty for disabled edges
				fromNodeAttr = attributes.getString("fromNode");
				toNodeAttr = attributes.getString("toNode");
				edgeType = attributes.getString("edgeType", null);
			}catch(AttributeNotFoundException ex){
				throwXMLConfigurationException("Missing attribute at edge '" + edgeID + "'.", ex);
				continue;
			}
			debugMode = EdgeDebugMode.fromString(attributes.getString("debugMode", null));
            
            if (graphDebugMaxRecords == 0) { // if this value isn't defined for whole graph 
            	debugMaxRecords = attributes.getLong("debugMaxRecords", 0);
            } else {
            	debugMaxRecords = graphDebugMaxRecords;
            }
            debugLastRecords = attributes.getBoolean("debugLastRecords", true);
            debugFilterExpression = attributes.getString("debugFilterExpression", null);
            debugSampleData = attributes.getBoolean("debugSampleData", false);
            fastPropagate = attributes.getBoolean("fastPropagate", false);
            metadataRef = attributes.getString("metadataRef", null);
            metadataRefState = ReferenceState.fromString(attributes.getString("metadataRefState", ReferenceState.VALID_REFERENCE.toString()));
            
			Object metadataObj = edgeMetadataID != null ? metadata.get(edgeMetadataID) : null;
			if (metadataObj == null && edgeMetadataID != null) {
				throwXMLConfigurationException("Cannot find metadata ID '" + edgeMetadataID + "'.");
			}
			// do we have real metadata or stub only ??
			if (metadataObj instanceof DataRecordMetadata){
				// real
				graphEdge = EdgeFactory.newEdge(edgeID, (DataRecordMetadata) metadataObj);
			}else{ 
				// stub
				try {
					graphEdge = EdgeFactory.newEdge(edgeID, (DataRecordMetadataStub) metadataObj);
				} catch (Exception | LinkageError e) {
					// LinkageErrors are caught because errors in jdbc driver loading should respect "strict" graph reading modifier
					throwXMLConfigurationException("Edge '" + edgeID + "' cannot be created.", e);
					graphEdge = EdgeFactory.newEdge(edgeID, (DataRecordMetadata) null);
				}
			}
			graphEdge.setDebugMode(debugMode);
			graphEdge.setDebugMaxRecords(debugMaxRecords);
			graphEdge.setDebugLastRecords(debugLastRecords);
			graphEdge.setFilterExpression(debugFilterExpression);
			graphEdge.setDebugSampleData(debugSampleData);
			graphEdge.setMetadataRef(metadataRef);
			graphEdge.setMetadataReferenceState(metadataRefState);
			// set edge type
			if (runtimeContext.getExecutionType() == ExecutionType.SINGLE_THREAD_EXECUTION) {
				//in single thread execution all edges are buffered
				graphEdge.setEdgeType(EdgeTypeEnum.BUFFERED);
			} else {
				EdgeTypeEnum edgeTypeEnum = EdgeTypeEnum.valueOfIgnoreCase(edgeType);
				if (edgeTypeEnum != null) graphEdge.setEdgeType(edgeTypeEnum);
				else if (fastPropagate) graphEdge.setEdgeType(EdgeTypeEnum.DIRECT_FAST_PROPAGATE);
			}
            
            // assign edge to fromNode
			specNodePort = fromNodeAttr.split(":");
			if (specNodePort.length!=2){
				throwXMLConfigurationException("Wrong definition of \"fromNode\" ["+fromNodeAttr+"] <Node>:<Port> at "+edgeID+" edge !");
				continue;
			}
			writerNode = graph.getNodes().get(specNodePort[0]);
			if (writerNode == null) {
				throwXMLConfigurationException("Cannot find node with ID: " + fromNodeAttr);
				continue;
			}
            try{
                fromPort=Integer.parseInt(specNodePort[1]);
            }catch(NumberFormatException ex){
                throwXMLConfigurationException("Cannot parse \"fromNode\"  port number value at edge "+edgeID+" : "+specNodePort[1]);
                continue;
            }
            
			// check whether port isn't already assigned
			if (writerNode.getOutputPort(fromPort)!=null){
				throwXMLConfigurationException("Output port ["+fromPort+"] of " + writerNode.getId()+" already assigned !");
				continue;
			}
			writerNode.addOutputPort(fromPort, graphEdge);
			// assign edge to toNode
			specNodePort = toNodeAttr.split(":");
			if (specNodePort.length!=2){
				throw new XMLConfigurationException("Wrong definition of \"toNode\" ["+toNodeAttr+"] <Node>:<Port> at edge: "+edgeID+" !");
			}
			// Node & port specified in form of: <nodeID>:<portNum>
			readerNode = graph.getNodes().get(specNodePort[0]);
			if (readerNode == null) {
				throwXMLConfigurationException("Cannot find node ID: " + toNodeAttr);
				continue;
			}
            try{
                toPort=Integer.parseInt(specNodePort[1]);
            }catch(NumberFormatException ex){
                throwXMLConfigurationException("Cannot parse \"toNode\" number value at edge "+edgeID+" : "+specNodePort[1]);
                continue;
            }
			// check whether port isn't already assigned
			if (readerNode.getInputPort(toPort)!=null){
				throwXMLConfigurationException("Input port ["+toPort+"] of " + readerNode.getId()+" already assigned !");
				continue;
			}
			readerNode.addInputPort(toPort, graphEdge);
			
            // register edge within graph
            graph.addEdge(graphEdge);
			
		}
	}

	private void instantiateSubgraphPorts(boolean inputPorts, SubgraphPorts subgraphPorts, List<Element> subgraphPortsElements) throws XMLConfigurationException {
		if (subgraphPortsElements.isEmpty()) {
			return;
		}
		if (subgraphPortsElements.size() > 1) {
			throw new JetelRuntimeException("XML element 'input/outputPorts' has max occurences 1, but is " + subgraphPortsElements.size());
		}

		List<Element> subgraphPortElements = getChildElements(subgraphPortsElements.get(0), SUBGRAPH_SINGLE_PORT_ELEMENT);
		for (Element subgraphPortElement : subgraphPortElements) {
            ComponentXMLAttributes attributes = new ComponentXMLAttributes(subgraphPortElement, graph);
            int index;
            try {
                index = attributes.getInteger(SUBGRAPH_PORT_NAME_ATTRIBUTE);
            } catch (AttributeNotFoundException ex) {
                throwXMLConfigurationException("Attribute name at singlePort is missing.", ex);
                continue;
            }
            boolean required = attributes.getBoolean(SUBGRAPH_PORT_REQUIRED_ATTRIBUTE, true);
            boolean keepEdge = attributes.getBoolean(SUBGRAPH_PORT_KEEP_EDGE_ATTRIBUTE, false);
            boolean connected = attributes.getBoolean(SUBGRAPH_PORT_CONNECTED_ATTRIBUTE, true);
            SubgraphPort subgraphPort;
            if (inputPorts) {
            	subgraphPort = new SubgraphInputPort(subgraphPorts, index, required, keepEdge, connected);
            } else {
            	subgraphPort = new SubgraphOutputPort(subgraphPorts, index, required, keepEdge, connected);
            }
            subgraphPorts.getPorts().add(subgraphPort);
		}
	}

	/**
	 *  Description of the Method
	 *
	 * @param  dbConnectionElements  Description of Parameter
	 * @param  graph                 Description of Parameter
	 * @since                        October 1, 2002
	 */
	private void instantiateDBConnections(NodeList connectionElements) throws XMLConfigurationException{
        IConnection connection;
        String connectionType;
        
        for (int i = 0; i < connectionElements.getLength(); i++) {
			checkInterrupted();
        	try {
	            Element connectionElement = (Element) connectionElements.item(i);
	            ComponentXMLAttributes attributes = new ComponentXMLAttributes(connectionElement, graph);
	
	            // process IConnection element attributes "id" & "type"
	            try {
	                connectionType = attributes.getString("type");
	            } catch (AttributeNotFoundException ex) {
	                throwXMLConfigurationException("Attribute type at Connection is missing.", ex);
	                continue;
	            }
	
	            //create connection
	            connection = ConnectionFactory.createConnection(graph, connectionType, connectionElement);
	            if (connection != null) {
	                //register connection in transformation graph
	                graph.addConnection(connection);
	            }
        	} catch (Exception e) {
        		throwXMLConfigurationException("Connection cannot be instantiated.", e);
        	}
        }
	}

	/**
	 *  Description of the Method
	 *
	 * @param  dbConnectionElements  Description of Parameter
	 * @param  graph                 Description of Parameter
	 * @since                        October 1, 2002
	 */
	private void instantiateSequences(NodeList sequenceElements) throws XMLConfigurationException {
		Sequence seq;
        String sequenceType;
        
		for (int i = 0; i < sequenceElements.getLength(); i++) {
			checkInterrupted();
			try {
	            Element sequenceElement = (Element) sequenceElements.item(i);
	            ComponentXMLAttributes attributes = new ComponentXMLAttributes(sequenceElement, graph);
	
	            // process Sequence element attributes "id" & "type"
	            try {
	                sequenceType = attributes.getString("type");
	            } catch (AttributeNotFoundException ex) {
	                throwXMLConfigurationException("Attribute type at Sequence is missing.", ex);
	                continue;
	            }
	            
	            //create sequence
	            seq = SequenceFactory.createSequence(graph, sequenceType, sequenceElement);
				if (seq != null) {
	                //register sequence in transformation graph
					graph.addSequence(seq);
				}
        	} catch (Exception e) {
        		throwXMLConfigurationException("Sequence cannot be instantiated.", e);
        	}
		}
	}

	/**
	 *  Description of the Method
	 *
	 * @param  dbConnectionElements  Description of Parameter
	 * @param  graph                 Description of Parameter
	 * @since                        October 1, 2002
	 */
	private void instantiateLookupTables(NodeList lookupElements) throws XMLConfigurationException {
        LookupTable lookup = null;
        
        for (int i = 0; i < lookupElements.getLength(); i++) {
			checkInterrupted();
        	try {
	            Element lookupElement = (Element) lookupElements.item(i);
	            lookup = LookupTableFactory.createLookupTable(graph, lookupElement);
	            if(lookup != null) {
	                //register lookup table in transformation graph
	                graph.addLookupTable(lookup);
	            }
        	} catch (Exception e) {
        		throwXMLConfigurationException("Lookup table cannot be instantiated.", e);
        	}
        }
	}

	/**
	 * Load single graph parameter.
	 * @throws XMLConfigurationException 
	 */
	private void instantiateGraphParameter(GraphParameters graphParameters, Element graphParameter) throws XMLConfigurationException {
		try {
			GraphParameter gp = (GraphParameter) graphParameterUnmarshaller.unmarshal(graphParameter);
			overrideParameterValue(gp);
		    graphParameters.addGraphParameter(gp);
		} catch (Exception e) {
			throw new JetelRuntimeException("Deserialisation of graph parameters failed.", e);
		}
	}

	private void overrideParameterValue(GraphParameter gp) {
		Properties additionalProperties = runtimeContext.getAdditionalProperties();
		if (additionalProperties.containsKey(gp.getName())) {
			gp.setValue(additionalProperties.getProperty(gp.getName()));
			gp.setCanBeResolved(!runtimeContext.getJobType().isSubJob());
			String value = runtimeContext.getAdditionalProperties().getProperty(gp.getName());
			if (gp.isSecure() && !UnicodeBlanks.isBlank(value) && !runtimeContext.getAuthorityProxy().isSecureParameterValue(value)) {
				logger.warn("Parameter " + gp.getName() + " is marked as 'Secure' but has not an encrypted value");
			} else if (!gp.isSecure() && !UnicodeBlanks.isBlank(value) && runtimeContext.getAuthorityProxy().isSecureParameterValue(value)) {
				logger.warn("Parameter " + gp.getName() + " is not marked as 'Secure' but has an encrypted value");
			}
		} else {
			// gp.setValue(gp.getValue());
		}
	}
	
	/**
	 * Load parameter file.
	 */
	private boolean instantiateGraphParametersFile(GraphParameters graphParameters, Element graphParameterFile) throws XMLConfigurationException {
		if (!graphParameterFile.hasAttribute("fileURL")) {
			throwXMLConfigurationException("A graph parameter file does not specify fileURL attribute.");
			return true;
		}
    	String fileURL = graphParameterFile.getAttribute("fileURL");
    	String resolvedFileURL = new PropertyRefResolver(graphParameters).resolveRef(fileURL);
    	
		if (!PropertyRefResolver.containsProperty(resolvedFileURL)) {
			instantiateGraphParametersFile(graphParameters, resolvedFileURL);
	    	return true;
		} else {
			return false;
		}
	}

	/**
	 * Load parameter file.
	 */
	public void instantiateGraphParametersFile(GraphParameters graphParameters, String resolvedFileURL) throws XMLConfigurationException {
		try (
				InputStream is = FileUtils.getInputStream(runtimeContext.getContextURL(), resolvedFileURL)
		) {
        	Document document = prepareDocument(is);
        	instantiateGraphParameters(graphParameters, Arrays.asList(document.getDocumentElement()));
        } catch (Exception e) {
        	try {
        		graphParameters.addProperties(loadGraphProperties(resolvedFileURL));
        	} catch(IOException ex) {
        		throwXMLConfigurationException("Cannot load property definition from " + resolvedFileURL, ex);
        	}
        }
	}
	
	/**
	 * Graph parameters loading.
	 * @throws XMLConfigurationException 
	 */
	private void instantiateGraphParameters(GraphParameters graphParameters, List<Element> graphParametersElements) throws XMLConfigurationException {
		if (graphParametersElements.isEmpty()) {
			return;
		}
		if (graphParametersElements.size() > 1) {
			throw new JetelRuntimeException("XML element GraphParameters has max occurences 1, but is " + graphParametersElements.size());
		}

		List<Element> unresolvedGraphParametersFiles = new ArrayList<Element>();
		List<Element> graphParameterElements = getChildElements(graphParametersElements.get(0), null);
		for (Element graphParameterElement : graphParameterElements) {
			checkInterrupted();
			if (graphParameterElement.getNodeName().equals(GRAPH_PARAMETER_ELEMENT)) {
				instantiateGraphParameter(graphParameters, graphParameterElement);
			} else if (graphParameterElement.getNodeName().equals(GRAPH_PARAMETER_FILE_ELEMENT)) {
				if (!instantiateGraphParametersFile(graphParameters, graphParameterElement)) {
					unresolvedGraphParametersFiles.add(graphParameterElement);
				}
			} else {
				throwXMLConfigurationException("Unexpected XML element " + graphParameterElement.getNodeName());
			}
		}

	    // now try to resolve parameters from file which have fileURL with property reference
		boolean progress = true;
		while (!unresolvedGraphParametersFiles.isEmpty() && progress) {
			progress = false;
			List<Element> resolvedGraphParametersFiles = new ArrayList<Element>();
			for (Element graphParameterFile : unresolvedGraphParametersFiles) {
				if (instantiateGraphParametersFile(graphParameters, graphParameterFile)) {
					progress = true;
					resolvedGraphParametersFiles.add(graphParameterFile);
				}
			}
			unresolvedGraphParametersFiles.removeAll(resolvedGraphParametersFiles);
			if (!progress) {
		    	throwXMLConfigurationException("Failed to resolve following parameter file URL: " + unresolvedGraphParametersFiles.get(0).getAttribute("fileURL"));
			}
		}
	}
	
	/**
	 * Old-fashion graph parameters loading.
	 */
	private void instantiateProperties(NodeList propertyElements) throws  XMLConfigurationException {
		List<String> unresolvedUrls = new ArrayList<String>();
	    // loop through all property elements & create appropriate properties
	    for (int i = 0; i < propertyElements.getLength(); i++) {
	        Element propertyElement = (Element) propertyElements.item(i);
	        // process property from file, if fileURL contains property reference, skip it for now
	        if (propertyElement.hasAttribute("fileURL")) {
	        	String fileURL = propertyElement.getAttribute("fileURL");
        		if (PropertyRefResolver.containsProperty(fileURL)) {
        			unresolvedUrls.add(fileURL);
        			continue;
        		}
        		instantiateGraphParametersFile(graph.getGraphParameters(), fileURL);
	        } else if (propertyElement.hasAttribute("name")) {
	        	String name = propertyElement.getAttribute("name");
	        	if (isValidGraphParameterName(name)) { //obsolete parameters with invalid names are ignored
	        		graph.getGraphParameters().addGraphParameter(name, propertyElement.getAttribute("value"));
	        	}
	        } else {
	        	throwXMLConfigurationException("Invalid property definition :" + propertyElement);
	        }
	    }
	    
	    if (unresolvedUrls.size() == 0) {
	    	return;
	    }
	    
	    // now try to resolve properties from file which have fileURL with property reference
	    while (!unresolvedUrls.isEmpty()) {
	    	PropertyRefResolver propertiesRefResolver = new PropertyRefResolver(graph.getGraphParameters());
	    	List<String> stillUnresolvedUrls = new ArrayList<String>();
		    for (String url : unresolvedUrls) {
		    	String resolvedUrl = propertiesRefResolver.resolveRef(url);
		    	if (PropertyRefResolver.containsProperty(resolvedUrl)) {
		    		stillUnresolvedUrls.add(resolvedUrl);
		    	} else {
		    		instantiateGraphParametersFile(graph.getGraphParameters(), resolvedUrl);
		    	}
		    }
		    
		    if (unresolvedUrls.size() == stillUnresolvedUrls.size()) {
		    	throwXMLConfigurationException("Failed to resolve following propertis file URL(s): " + StringUtils.stringArraytoString(stillUnresolvedUrls.toArray(new String[0]), ", "));
		    	break;
		    }
		    unresolvedUrls = stillUnresolvedUrls;
	    }
	}
	
    private TypedProperties loadGraphProperties(String fileURL) throws IOException, XMLConfigurationException {
		TypedProperties graphProperties = new TypedProperties();
        InputStream inStream = null;
        try {
        	inStream = FileUtils.getInputStream(runtimeContext.getContextURL(), fileURL);
            graphProperties.load(inStream);
            TypedProperties result = new TypedProperties();
            for (String name : graphProperties.stringPropertyNames()) {
            	if (isValidGraphParameterName(name)) { //obsolete parameters with invalid names are ignored
            		result.setProperty(name, graphProperties.getProperty(name));
            	}
            }
            return result;
        } catch(MalformedURLException e) {
        	throwXMLConfigurationException("Wrong URL/filename of file specified: " + fileURL, e);
        } finally {
        	IOUtils.closeQuietly(inStream);
        }
    	return null;
    }

	/**
	 * Validation of obsolete graph parameters. Obsolete graph parameters
	 * should be validated by this method and invalid ones shouldn't be used at all.
	 */
	public static boolean isValidGraphParameterName(String name) {
		return !StringUtils.isEmpty(name) && StringUtils.isValidObjectName(name);
	}

	private void instantiateDictionary(NodeList dictionaryElements) throws  XMLConfigurationException {
		final Dictionary dictionary = graph.getDictionary();
		
	    for (int i = 0; i < dictionaryElements.getLength(); i++) {
			checkInterrupted();
	    	NodeList dicEntryElements = dictionaryElements.item(i).getChildNodes();
		    for (int j = 0; j < dicEntryElements.getLength(); j++) {
		    	if(dicEntryElements.item(j).getNodeName().equals(DICTIONARY_ENTRY_ELEMENT)) {
			        ComponentXMLAttributes attributes = new ComponentXMLAttributes((Element) dicEntryElements.item(j), graph);
			        try {
			        	// get basic parameters
			        	String type = attributes.getString(DICTIONARY_ENTRY_TYPE);
			        	String name = attributes.getString(DICTIONARY_ENTRY_NAME);
			        	//check entry name
			        	if (StringUtils.isEmpty(name)) {
			        		throw new ComponentNotReadyException("Empty dictionary entry name!");
			        	}
			        	
			        	// get properties
			        	final Properties entryProperties = attributes.attributes2Properties(null);
			        	entryProperties.remove(DICTIONARY_ENTRY_ID);
			        	entryProperties.remove(DICTIONARY_ENTRY_TYPE);
			        	entryProperties.remove(DICTIONARY_ENTRY_NAME);
			        	entryProperties.remove(DICTIONARY_ENTRY_INPUT);
			        	entryProperties.remove(DICTIONARY_ENTRY_OUTPUT);
			        	entryProperties.remove(DICTIONARY_ENTRY_REQUIRED);
			        	entryProperties.remove(DICTIONARY_ENTRY_CONTENT_TYPE);
			        	String prefix = Dictionary.DICTIONARY_VALUE_NAMESPACE;
			        	for(Object key : entryProperties.keySet().toArray()){
			        		if(key.toString().startsWith(prefix)){
			        			Object value = entryProperties.get(key);
			        			entryProperties.remove(key);
			        			entryProperties.put(key.toString().substring(prefix.length()), value);
			        		}
			        	}

			        	// create entry 
			        	if (!dictionary.hasEntry(name)) {
							if (!entryProperties.isEmpty()) {
								try {
									dictionary.setValueFromProperties(name, type, entryProperties);
								} catch (UnsupportedDictionaryOperation e) {
									// probably only if the dictionary type does not support initialization from
									// Properties and an ID attribute or others was passed
									// so just create dictionary entry without value
									dictionary.setValue(name, type, null);
								}
							} else {
								if (dictionary.getEntry(name) != null) {
									throw new ComponentNotReadyException("Duplicate dictionary entry name: " + name);
								}
								dictionary.setValue(name, type, null);
							}
						}
			        	
			        	if (attributes.exists(DICTIONARY_ENTRY_INPUT) && attributes.getBoolean(DICTIONARY_ENTRY_INPUT)) {
			        		dictionary.setAsInput(name);
			        	}
			        	if (attributes.exists(DICTIONARY_ENTRY_OUTPUT) && attributes.getBoolean(DICTIONARY_ENTRY_OUTPUT)) {
			        		dictionary.setAsOuput(name);
			        	}
			        	if (attributes.exists(DICTIONARY_ENTRY_REQUIRED) && attributes.getBoolean(DICTIONARY_ENTRY_REQUIRED)) {
			        		dictionary.setAsRequired(name);
			        	}
			        	if (attributes.exists(DICTIONARY_ENTRY_CONTENT_TYPE) && !StringUtils.isEmpty(attributes.getString(DICTIONARY_ENTRY_CONTENT_TYPE))) {
			        		dictionary.setContentType(name, attributes.getString(DICTIONARY_ENTRY_CONTENT_TYPE));
			        	}
			        } catch(AttributeNotFoundException ex){
			            throwXMLConfigurationException("Dictionary - Attributes missing.", ex);
			        } catch (ComponentNotReadyException e) {
			            throwXMLConfigurationException("Dictionary initialization problem.", e);
					} catch (Exception e) {
			            throwXMLConfigurationException("Dictionary entry cannot be instantiated.", e);
					}
		    	}
		    }
	    }
	}

	/**
	 * @param parent parent element
	 * @param elementName name of requested child elements, if null, all child elements are returned
	 * @return list of direct child elements
	 */
	private List<Element> getChildElements(Element parent, String elementName) {
		List<Element> result = new ArrayList<Element>();
		if (parent != null) {
			NodeList children = parent.getChildNodes();
			for (int i = 0; i < children.getLength(); i++) {
				if (children.item(i) instanceof Element &&
						(elementName == null || elementName.equals(children.item(i).getNodeName()))) {
					result.add((Element) children.item(i));
				}
			}
		}
		return result;
	}
	
	private void throwXMLConfigurationException(String message) throws XMLConfigurationException {
		throwXMLConfigurationException(message, null);
	}

	private void throwXMLConfigurationException(Throwable cause) throws XMLConfigurationException {
		throwXMLConfigurationException(null, cause);
	}

	private void throwXMLConfigurationException(String message, Throwable cause) throws XMLConfigurationException {
		XMLConfigurationException e = new XMLConfigurationException(message, cause);
		if (isStrictParsing()) {
			throw e;
		} else {
			//strict mode is off, so exception is logged only on debug level
			logger.debug("Graph factorization failed (strictMode = false)", e);
			suppressedExceptions.add(e);
		}
	}
	
	/**
	 * @return the strictParsing
	 * @deprecated use {@link GraphRuntimeContext#isStrictGraphFactorization()} instead
	 */
	@Deprecated
	public boolean isStrictParsing() {
		if (strictParsing != null) {
			return strictParsing;
		} else {
			return runtimeContext.isStrictGraphFactorization();
		}
	}

	/**
	 * Strict mode of graph building can be turned off. So the transformation graph
	 * is assembled with maximum effort. For example, missing external metadata
	 * does not	cause failure of graph reading.
	 * 
	 * @param strictParsing the strictParsing to set
	 * @deprecated use {@link GraphRuntimeContext#setStrictGraphFactorization(boolean)} instead
	 */
	@Deprecated
	public void setStrictParsing(boolean strictParsing) {
		this.strictParsing = strictParsing;
	}

	/**
	 * Metadata propagation can be turned off by this method.
	 */
	public void setMetadataPropagation(boolean metadataPropagation) {
		this.metadataPropagation = metadataPropagation;
	}

	/**
	 * @param onlyParamsAndDict true if only graph parameters and dictionary should be extracted
	 */
	public void setOnlyParamsAndDict(boolean onlyParamsAndDict) {
		this.onlyParamsAndDict = onlyParamsAndDict;
	}
	
	/**
	 * @return list of exceptions, which were suppressed with lenient parsing
	 * @see #setStrictParsing(boolean) 
	 */
	public List<Throwable> getSuppressedExceptions() {
		return suppressedExceptions;
	}
	
	@Deprecated
	public Document getOutputXMLDocumentReference() {
		return(this.outputXMLDocument);
	}
	
	@Deprecated
	public boolean write(Document outputDocument) {
		// store reference to allow usage of getOutputXMLDocument() function
		this.outputXMLDocument = outputDocument;
		return(write());
	}
	
	/**
	 * Tests whether current output XML document contains element with specified name and attributes
	 * with given values.
	 * 
	 * @param elementName 			XML tag to be tested
	 * @param requiredAttributes 	required attribute name-valu pairs to match. If element should contain
	 * 								no attributes, use empty map. Insert "*" as attribute name to match
	 * 								element containing any attributes.
	 * @return element satisfying requirements or null if none was find
	 */
	private Node xmlElementInDocument(String elementName, Map<String, String> requiredAttributes) {
		NodeList xmlElements = this.outputXMLDocument.getElementsByTagName(elementName);
		// check required attribute/value pairs on candidates
		Element candidate = null;
		boolean matchAnyAtts = requiredAttributes.containsKey("*");
		for (int i=0; i < xmlElements.getLength(); i++) {
			candidate = (Element)xmlElements.item(i);
			
			if (matchAnyAtts) {
				// element can have any attributes ("*")
				return((Node)candidate);
			} 
			
			if (requiredAttributes.isEmpty() && candidate.hasAttributes() == false) {
				//	element without attributes required
				return((Node)candidate);			
			
			} 
			
			if (requiredAttributes.isEmpty() == false) {
				// check required name-value pairs
				for(Entry<String, String> entry : requiredAttributes.entrySet()) {
					final String requiredName = entry.getKey();
					final String requiredValue = entry.getValue();
					if (!candidate.hasAttribute(requiredName) ||
						!candidate.getAttribute(requiredName).equals(requiredValue)) {
						// candidate is missing attribute/value
						break;
					} 
				}
			}
		}
		
		return(null);
	}
	
	public void writeGraphParameters(GraphParameters graphParameters, OutputStream os) {
		try {
			graphParameterMarshaller.marshal(graphParameters, os);
		} catch (JAXBException e) {
			throw new JetelRuntimeException("Serialisation of graph parameters failed.", e);
		}
	}
	
	@SuppressWarnings("deprecation")
	private boolean write() {
		// initialize document to which all element will belong to
		try {
			Element rootElement = outputXMLDocument.getDocumentElement();
			if (rootElement == null) {
				rootElement = outputXMLDocument.createElement(GRAPH_ELEMENT);
				outputXMLDocument.appendChild(rootElement);
			}
			// pass through all nodes in graph, serializing them to XML
			Phase[] phases = graph.getPhases();
			
			for (int i=0; i<phases.length; i++) {
				// find element for this phase
				HashMap<String, String> phaseAtts = new HashMap<String, String>();
				phaseAtts.put("number",String.valueOf(i));
				Element phaseElement = (Element)xmlElementInDocument(PHASE_ELEMENT,phaseAtts);
				if (phaseElement == null) {
					phaseElement = outputXMLDocument.createElement(PHASE_ELEMENT);
					phaseElement.setAttribute("number",String.valueOf(i));
					rootElement.appendChild(phaseElement);
				}
				
				Collection<Node> nodes = phases[i].getNodes().values();
				Iterator<Node> iter = nodes.iterator();
				while (iter.hasNext()) {
					Node graphNode = iter.next();
					HashMap<String, String> nodeAtts = new HashMap<String, String>();
					nodeAtts.put(Node.XML_ID_ATTRIBUTE, graphNode.getId());
					nodeAtts.put(Node.XML_TYPE_ATTRIBUTE, graphNode.getType());
					Element xmlElement = (Element)xmlElementInDocument(NODE_ELEMENT,nodeAtts); 
					if (xmlElement == null) {
						xmlElement = outputXMLDocument.createElement(NODE_ELEMENT);
					}
					graphNode.toXML(xmlElement);
					phaseElement.appendChild(xmlElement);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			return(false);
		}
		return(true);
	}

}

