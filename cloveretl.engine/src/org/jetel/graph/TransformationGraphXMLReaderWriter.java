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
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.component.ComponentFactory;
import org.jetel.data.lookup.LookupTable;
import org.jetel.data.lookup.LookupTableFactory;
import org.jetel.data.sequence.Sequence;
import org.jetel.data.sequence.SequenceFactory;
import org.jetel.database.ConnectionFactory;
import org.jetel.database.IConnection;
import org.jetel.enums.EdgeTypeEnum;
import org.jetel.enums.EnabledEnum;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.GraphConfigurationException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.dictionary.Dictionary;
import org.jetel.graph.dictionary.UnsupportedDictionaryOperation;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.DataRecordMetadataStub;
import org.jetel.metadata.DataRecordMetadataXMLReaderWriter;
import org.jetel.metadata.MetadataFactory;
import org.jetel.util.file.FileUtils;
import org.jetel.util.primitive.TypedProperties;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
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
	private final static String GRAPH_ELEMENT = "Graph";
	//unused private final static String GLOBAL_ELEMENT = "Global";
	private final static String NODE_ELEMENT = "Node";
	private final static String EDGE_ELEMENT = "Edge";
	private final static String METADATA_ELEMENT = "Metadata";
	private final static String PHASE_ELEMENT = "Phase";
	private final static String CONNECTION_ELEMENT = "Connection";
	private final static String SEQUENCE_ELEMENT = "Sequence";
	private final static String LOOKUP_TABLE_ELEMENT = "LookupTable";
	private final static String METADATA_RECORD_ELEMENT = "Record";
	private final static String PROPERTY_ELEMENT = "Property";
	
	private final static String DICTIONARY_ELEMENT = "Dictionary";
	private final static String DICTIONARY_ENTRY_ELEMENT = "Entry";
	private final static String DICTIONARY_ENTRY_ID = "id";
	private final static String DICTIONARY_ENTRY_NAME = "name";
	private final static String DICTIONARY_ENTRY_TYPE = "type";
	private final static String DICTIONARY_ENTRY_INPUT = "input";
	private final static String DICTIONARY_ENTRY_OUTPUT = "output";
	private final static String DICTIONARY_ENTRY_REQUIRED = "required";
	private final static String DICTIONARY_ENTRY_CONTENT_TYPE = "contentType";
	
	
	
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
	
	private Document outputXMLDocument = null;
	
    private TransformationGraph graph;
    
    private Properties additionalProperties;
 
    /**
     * Instantiates transformation graph from a given input stream and presets a given properties.
     * @param graphStream graph in XML form stored in character stream
     * @param properties additional properties
     * @return transformation graph
     * @throws XMLConfigurationException deserialization from XML fails for any reason.
     * @throws GraphConfigurationException misconfigured graph
     */
	public static TransformationGraph loadGraph(InputStream graphStream, Properties properties)
	throws XMLConfigurationException, GraphConfigurationException {
        TransformationGraphXMLReaderWriter graphReader = new TransformationGraphXMLReaderWriter(properties);
        return graphReader.read(graphStream);
    }

	/**
	 *Constructor for the TransformationGraphXMLReaderWriter object
	 *
	 * @since    May 24, 2002
	 */
	public TransformationGraphXMLReaderWriter(Properties properties) {
		this.additionalProperties = (properties != null) ? properties : new Properties();
	}

	private static Document prepareDocument(InputStream in) throws XMLConfigurationException {
		Document document;
		try {
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
 
 			dbf.setNamespaceAware(true);

			// Optional: set various configuration options
			dbf.setValidating(validation);
			dbf.setIgnoringComments(ignoreComments);
			dbf.setIgnoringElementContentWhitespace(ignoreWhitespaces);
			dbf.setCoalescing(putCDATAIntoText);
			dbf.setExpandEntityReferences(!createEntityRefs);
			
			DocumentBuilder db = dbf.newDocumentBuilder();
			
			if (in != null) {
				document = db.parse(new BufferedInputStream(in));
				document.normalize();
			}else{
				document = db.newDocument();
			}

		}catch(SAXParseException ex){
			logger.error("Error when parsing graph's XML definition  --> on line "+ex.getLineNumber()+" row "+ex.getColumnNumber(),ex); 
			throw new XMLConfigurationException(ex);
        }catch (ParserConfigurationException ex) {
			logger.error("Error when parsing graph's XML definition",ex);
            throw new XMLConfigurationException(ex);
		}catch (Exception ex) {
            logger.error("Error when parsing graph's XML definition",ex);
            throw new XMLConfigurationException(ex);
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
		Document document = prepareDocument(in);

		NodeList graphElement = document.getElementsByTagName(GRAPH_ELEMENT);
		String id = ((Element)graphElement.item(0)).getAttribute("id");
		
		if(StringUtils.isEmpty(id)) {
			id = ((Element)graphElement.item(0)).getAttribute("name");
		}
		
		return id;
	}
	
	public TransformationGraph read(InputStream in) throws XMLConfigurationException,GraphConfigurationException {
		Document document = prepareDocument(in);

		return read(document);
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
		Map metadata = new HashMap(ALLOCATE_MAP_SIZE);
	//	List<Phase> phases = new LinkedList<Phase>();
	//	Map<String,Node> allNodes = new LinkedHashMap<String,Node>(ALLOCATE_MAP_SIZE);
	//	Map<String,Edge> edges = new HashMap<String,Edge>(ALLOCATE_MAP_SIZE);

		// delete all Nodes & Edges possibly held by TransformationGraph
		//graph.free();
		graph = null;
		
		// process document
		NodeList graphElement = document.getElementsByTagName(GRAPH_ELEMENT);
		String id = ((Element)graphElement.item(0)).getAttribute("id");
        //get graph id
		graph = new TransformationGraph(id);
		graph.loadGraphProperties(additionalProperties);
		// get graph name
		ComponentXMLAttributes grfAttributes=new ComponentXMLAttributes((Element)graphElement.item(0), graph);
		try{
			graph.setName(grfAttributes.getString("name"));
		}catch(AttributeNotFoundException ex){
			throw new XMLConfigurationException("Attribute at Graph node is missing - "+ex.getMessage());
		}
        
        grfAttributes.setResolveReferences(false);
        //get debug mode
        graph.setDebugMode(grfAttributes.getString("debugMode", "true"));
//        //get debug directory
//        graph.setDebugDirectory(grfAttributes.getString("debugDirectory", null));
        //get debugMaxRecords
        graph.setDebugMaxRecords(grfAttributes.getInteger("debugMaxRecords", 0));
        
		// handle all defined Properties
		NodeList PropertyElements = document.getElementsByTagName(PROPERTY_ELEMENT);
		instantiateProperties(PropertyElements);

		// handle dictionary
		NodeList dictionaryElements = document.getElementsByTagName(DICTIONARY_ELEMENT);
		instantiateDictionary(dictionaryElements);
		
		// handle all defined DB connections
		NodeList dbConnectionElements = document.getElementsByTagName(CONNECTION_ELEMENT);
		instantiateDBConnections(dbConnectionElements);

		// handle all defined DB connections
		NodeList sequenceElements = document.getElementsByTagName(SEQUENCE_ELEMENT);
		instantiateSequences(sequenceElements);
		
		//create metadata
		NodeList metadataElements = document.getElementsByTagName(METADATA_ELEMENT);
		instantiateMetadata(metadataElements, metadata);

		// register all metadata (DataRecordMetadata) within transformation graph
		graph.addDataRecordMetadata(metadata);

		// handle all defined lookup tables
		NodeList lookupsElements = document.getElementsByTagName(LOOKUP_TABLE_ELEMENT);
		instantiateLookupTables(lookupsElements);

		NodeList phaseElements = document.getElementsByTagName(PHASE_ELEMENT);
		instantiatePhases(phaseElements);

		NodeList edgeElements = document.getElementsByTagName(EDGE_ELEMENT);
		instantiateEdges(edgeElements, metadata, graph.isDebugMode(), graph.getDebugMaxRecords());

        return graph;
	}


	/**
	 *  Description of the Method
	 *
	 * @param  metadataElements  Description of Parameter
	 * @param  metadata          Description of Parameter
	 * @exception  IOException   Description of Exception
	 * @since                    May 24, 2002
	 */
	private void instantiateMetadata(NodeList metadataElements,Map metadata) throws XMLConfigurationException {
		String metadataID;
		String fileURL=null;
		Object recordMetadata;
		//PropertyRefResolver refResolver=new PropertyRefResolver();

		// loop through all Metadata elements & create appropriate Metadata objects
		for (int i = 0; i < metadataElements.getLength(); i++) {
			ComponentXMLAttributes attributes = new ComponentXMLAttributes((Element)metadataElements.item(i), graph);
			try{
			// process metadata element attributes "id" & "fileURL"
			metadataID = attributes.getString("id");
			
			// process metadata from file
			if (attributes.exists("fileURL")){
				fileURL = attributes.getString("fileURL");
				try{
				    recordMetadata=MetadataFactory.fromFile(graph, fileURL);
                }catch(IOException ex){
                    logger.error("Error when reading/parsing record metadata definition file: "+fileURL);
                    throw new XMLConfigurationException("Can't parse metadata: "+metadataID,ex);
                }
			}// metadata from analyzing DB table (JDBC) - will be resolved
			// later during Edge init - just put stub now.
			else if (attributes.exists(DataRecordMetadataXMLReaderWriter.CONNECTION_ATTR)){
				IConnection connection = graph.getConnection(attributes.getString(DataRecordMetadataXMLReaderWriter.CONNECTION_ATTR));
				if(connection == null) {
					throw new XMLConfigurationException("Can't find Connection id - " + attributes.getString(DataRecordMetadataXMLReaderWriter.CONNECTION_ATTR) + ".");
				}
				recordMetadata = new DataRecordMetadataStub(connection, attributes.attributes2Properties(null));
			} // probably metadata inserted directly into graph
			else {
				recordMetadata=MetadataFactory.fromXML(graph, attributes.getChildNode(metadataElements.item(i),METADATA_RECORD_ELEMENT));
			}
			}catch(AttributeNotFoundException ex){
				throw new XMLConfigurationException("Metadata - Attributes missing "+ex.getMessage());
			}
			// register metadata object
			if (metadata.put(metadataID, recordMetadata)!=null){
				throw new XMLConfigurationException("Metadata "+metadataID+" already defined - duplicate ID detected!");
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
			try{
			phaseNum = attributes.getInteger("number");
			phase=new Phase(phaseNum);
			graph.addPhase(phase);
			}catch(AttributeNotFoundException ex) {
				throw new XMLConfigurationException("Attribute is missing for phase - "+ex.getMessage());
			}catch(NumberFormatException ex1){
				throw new XMLConfigurationException("Phase attribute number is not a valid integer !",ex1);
			}
			// get all nodes defined in this phase and instantiate them
			// we expect that all childern of phase are Nodes
			//phaseElements.item(i).normalize();
			nodeElements=phaseElements.item(i).getChildNodes();
			instantiateNodes(phase,nodeElements);
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
		org.jetel.graph.Node graphNode;
		String nodeType;
		String nodeID="unknown";
		String nodeEnabled;
        int nodePassThroughInputPort;
        int nodePassThroughOutputPort;
        
		// loop through all Node elements & create appropriate Metadata objects
		for (int i = 0; i < nodeElements.getLength(); i++) {
			if (NODE_ELEMENT.compareToIgnoreCase(nodeElements.item(i)
					.getNodeName()) != 0) {
				continue;
			}
			ComponentXMLAttributes attributes = new ComponentXMLAttributes((Element)nodeElements.item(i), graph);

			// process Node element attributes "id" & "type"
			try {
				nodeID = attributes.getString(IGraphElement.XML_ID_ATTRIBUTE);
				nodeType = attributes.getString(IGraphElement.XML_TYPE_ATTRIBUTE);
                nodeEnabled = attributes.getString(Node.XML_ENABLED_ATTRIBUTE, EnabledEnum.ENABLED.toString());
                nodePassThroughInputPort = attributes.getInteger("passThroughInputPort", 0);
                nodePassThroughOutputPort = attributes.getInteger("passThroughOutputPort", 0);
				if(!nodeEnabled.equalsIgnoreCase(EnabledEnum.DISABLED.toString()) 
                        && !nodeEnabled.equalsIgnoreCase(EnabledEnum.PASS_THROUGH.toString())) {
				    graphNode = ComponentFactory.createComponent(graph, nodeType, nodeElements.item(i));
                } else {
                    graphNode = new SimpleNode(nodeID);
                }
				if (graphNode != null) {
                    phase.addNode(graphNode);
                    graphNode.setEnabled(nodeEnabled);
                    graphNode.setPassThroughInputPort(nodePassThroughInputPort);
                    graphNode.setPassThroughOutputPort(nodePassThroughOutputPort);
				} else {
					throw new XMLConfigurationException(
							"Error when creating Component type :" + nodeType);
				}
			} catch (AttributeNotFoundException ex) {
				throw new XMLConfigurationException("Missing attribute at node "+nodeID+" - "
						+ ex.getMessage());
			}
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
	private void instantiateEdges(NodeList edgeElements, Map metadata, boolean graphDebugMode, int graphDebugMaxRecords) throws XMLConfigurationException,GraphConfigurationException {
		String edgeID="unknown";
		String edgeMetadataID;
		String fromNodeAttr;
		String toNodeAttr;
		String edgeType = null;
        boolean debugMode = false;
        String debugFilterExpression = null;
        int debugMaxRecords = 0;
        boolean debugLastRecords = true;
        boolean debugSampleData = false;
        boolean fastPropagate = false;
		String[] specNodePort;
		int fromPort;
		int toPort;
		org.jetel.graph.Edge graphEdge;
		org.jetel.graph.Node graphNode;

		// loop through all Node elements & create appropriate Metadata objects
		for (int i = 0; i < edgeElements.getLength(); i++) {
			ComponentXMLAttributes attributes = new ComponentXMLAttributes((Element)edgeElements.item(i), graph);

			// process edge element attributes "id" & "fileURL"
			try{
			edgeID = attributes.getString(IGraphElement.XML_ID_ATTRIBUTE);
			edgeMetadataID = attributes.getString("metadata", null); //metadata paramater on the edge can be empty for disabled edges
			fromNodeAttr = attributes.getString("fromNode");
			toNodeAttr = attributes.getString("toNode");
			edgeType = attributes.getString("edgeType", null);
			}catch(AttributeNotFoundException ex){
				throw new XMLConfigurationException("Missing attribute at edge "+edgeID+" - "+ex.getMessage());
			}
			debugMode = attributes.getBoolean("debugMode", false);
            
            if (graphDebugMaxRecords == 0) { // if this value isn't defined for whole graph 
            	debugMaxRecords = attributes.getInteger("debugMaxRecords", 0);
            } else {
            	debugMaxRecords = graphDebugMaxRecords;
            }
            debugLastRecords = attributes.getBoolean("debugLastRecords", true);
            debugFilterExpression = attributes.getString("debugFilterExpression", null);
            debugSampleData = attributes.getBoolean("debugSampleData", false);
            
            fastPropagate = attributes.getBoolean("fastPropagate", false);
			Object metadataObj = edgeMetadataID != null ? metadata.get(edgeMetadataID) : null;
			if (metadataObj == null && edgeMetadataID != null) {
				throw new XMLConfigurationException("Can't find metadata ID: " + edgeMetadataID);
			}
			// do we have real metadata or stub only ??
			if (metadataObj instanceof DataRecordMetadata){
				// real
				graphEdge = new Edge(edgeID, (DataRecordMetadata) metadataObj);
			}else{ 
				// stub
				graphEdge = new Edge(edgeID, (DataRecordMetadataStub) metadataObj);
			}
			graphEdge.setDebugMode(debugMode);
			graphEdge.setDebugMaxRecords(debugMaxRecords);
			graphEdge.setDebugLastRecords(debugLastRecords);
			graphEdge.setFilterExpression(debugFilterExpression);
			graphEdge.setDebugSampleData(debugSampleData);
			// set edge type
			EdgeTypeEnum edgeTypeEnum = EdgeTypeEnum.valueOfIgnoreCase(edgeType);
			if (edgeTypeEnum != null) graphEdge.setEdgeType(edgeTypeEnum);
			else if (fastPropagate) graphEdge.setEdgeType(EdgeTypeEnum.DIRECT_FAST_PROPAGATE);
            
            // assign edge to fromNode
			specNodePort = fromNodeAttr.split(":");
			if (specNodePort.length!=2){
				throw new XMLConfigurationException("Wrong definition of \"fromNode\" ["+fromNodeAttr+"] <Node>:<Port> at "+edgeID+" edge !");
			}
			graphNode = graph.getNodes().get(specNodePort[0]);
			if (graphNode == null) {
				throw new XMLConfigurationException("Can't find node with ID: " + fromNodeAttr);
			}
            try{
                fromPort=Integer.parseInt(specNodePort[1]);
            }catch(NumberFormatException ex){
                throw new XMLConfigurationException("Can't parse \"fromNode\"  port number value at edge "+edgeID+" : "+specNodePort[1]);
            }
            
			// check whether port isn't already assigned
			if (graphNode.getOutputPort(fromPort)!=null){
				throw new XMLConfigurationException("Output port ["+fromPort+"] of "+graphNode.getId()+" already assigned !");
			}
			graphNode.addOutputPort(fromPort, graphEdge);
			// assign edge to toNode
			specNodePort = toNodeAttr.split(":");
			if (specNodePort.length!=2){
				throw new XMLConfigurationException("Wrong definition of \"toNode\" ["+toNodeAttr+"] <Node>:<Port> at edge: "+edgeID+" !");
			}
			// Node & port specified in form of: <nodeID>:<portNum>
			graphNode = graph.getNodes().get(specNodePort[0]);
			if (graphNode == null) {
				throw new XMLConfigurationException("Can't find node ID: " + toNodeAttr);
			}
            try{
                toPort=Integer.parseInt(specNodePort[1]);
            }catch(NumberFormatException ex){
                throw new XMLConfigurationException("Can't parse \"toNode\" number value at edge "+edgeID+" : "+specNodePort[1]);
            }
			// check whether port isn't already assigned
			if (graphNode.getInputPort(toPort)!=null){
				throw new XMLConfigurationException("Input port ["+toPort+"] of "+graphNode.getId()+" already assigned !");
			}
			graphNode.addInputPort(toPort, graphEdge);

            // register edge within graph
            graph.addEdge(graphEdge);
			
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
            Element connectionElement = (Element) connectionElements.item(i);
            ComponentXMLAttributes attributes = new ComponentXMLAttributes(connectionElement, graph);

            // process IConnection element attributes "id" & "type"
            try {
                connectionType = attributes.getString("type");
            } catch (AttributeNotFoundException ex) {
                throw new XMLConfigurationException("Attribute type at Connection is missing - " + ex.getMessage());
            }

            //create connection
            connection = ConnectionFactory.createConnection(graph, connectionType, connectionElement);
            if (connection != null) {
                //register connection in transformation graph
                graph.addConnection(connection);
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
            Element sequenceElement = (Element) sequenceElements.item(i);
            ComponentXMLAttributes attributes = new ComponentXMLAttributes(sequenceElement, graph);

            // process Sequence element attributes "id" & "type"
            try {
                sequenceType = attributes.getString("type");
            } catch (AttributeNotFoundException ex) {
                throw new XMLConfigurationException("Attribute type at Sequence is missing - " + ex.getMessage());
            }
            
            //create sequence
            seq = SequenceFactory.createSequence(graph, sequenceType, sequenceElement);
			if (seq != null) {
                //register sequence in transformation graph
				graph.addSequence(seq);
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
        String lookupTableType;
        
        for (int i = 0; i < lookupElements.getLength(); i++) {
            Element lookupElement = (Element) lookupElements.item(i);
            ComponentXMLAttributes attributes = new ComponentXMLAttributes(lookupElement, graph);

            if (attributes.exists("lookupConfig")) {
            	String fileURL = null;
				try {
					fileURL = attributes.getString("lookupConfig");
				} catch (AttributeNotFoundException e) {
					// can't happen: we've checked it
				}
            	TypedProperties lookupProperties = new TypedProperties(null,graph);
            	try {
                	lookupProperties.setProperty(IGraphElement.XML_ID_ATTRIBUTE, attributes.getString(IGraphElement.XML_ID_ATTRIBUTE));
					lookupProperties.load(FileUtils.getInputStream(graph.getProjectURL(), fileURL));
					String metadata = lookupProperties.getStringProperty("metadata");
					if (metadata != null && ! metadata.trim().equals("")) {
						DataRecordMetadata lookupMetadata = MetadataFactory.fromFile(graph, lookupProperties.getStringProperty("metadata"));
						lookupProperties.setProperty(LookupTable.XML_METADATA_ID, 
								graph.addDataRecordMetadata(TransformationGraph.DEFAULT_METADATA_ID, lookupMetadata));
					}
					if (lookupProperties.containsKey(LookupTable.XML_DBCONNECTION)) {
						IConnection connection = ConnectionFactory.createConnection(graph, "JDBC", 
								new Object[]{graph.getUniqueConnectionId() , lookupProperties.getStringProperty("dbConnection")},
								new Class[]{String.class, String.class});
						graph.addConnection(connection);
						lookupProperties.setProperty(LookupTable.XML_DBCONNECTION, connection.getId());
					}
					if (!lookupProperties.containsKey(IGraphElement.XML_TYPE_ATTRIBUTE)) {
						throw new XMLConfigurationException("Attribute type at Lookup table is missing");
					}
				} catch (Exception e) {
	                throw new XMLConfigurationException("Lokkup table is not configured properly: " + e.getMessage());
				}
	            lookup = LookupTableFactory.createLookupTable(lookupProperties);
            }else{
	            // process Lookup table element attributes "id" & "type"
	            try {
	                lookupTableType = attributes.getString(IGraphElement.XML_TYPE_ATTRIBUTE);
	            } catch (AttributeNotFoundException ex) {
	                throw new XMLConfigurationException("Attribute type at Lookup table is missing - " + ex.getMessage());
	            }
	
	            //create lookup table
	            lookup = LookupTableFactory.createLookupTable(graph, lookupTableType, lookupElement);
            }
            if(lookup != null) {
                //register lookup table in transformation graph
                graph.addLookupTable(lookup);
            }
        }
	}

	private void instantiateProperties(NodeList propertyElements) throws  XMLConfigurationException {
	    // loop through all property elements & create appropriate properties
	    for (int i = 0; i < propertyElements.getLength(); i++) {
	        Element propertyElement = (Element) propertyElements.item(i);
	        // process property from file
	        if (propertyElement.hasAttribute("fileURL")) {
	        	String fileURL = propertyElement.getAttribute("fileURL");
	        	try {
	        		graph.loadGraphPropertiesSafe(fileURL);
	        	} catch(IOException ex) {
	        		throw new XMLConfigurationException("Can't load property definition from " + fileURL, ex); 
	        	}
	        } else if (propertyElement.hasAttribute("name")) {
	        	graph.getGraphProperties().setPropertySafe(propertyElement.getAttribute("name"), propertyElement.getAttribute("value"));
	        } else {
	        	throw new XMLConfigurationException("Invalid property definition :" + propertyElement);
	        }
	    }
	}

	private void instantiateDictionary(NodeList dictionaryElements) throws  XMLConfigurationException {
		final Dictionary dictionary = graph.getDictionary();
		
	    for (int i = 0; i < dictionaryElements.getLength(); i++) {
	    	NodeList dicEntryElements = dictionaryElements.item(i).getChildNodes();
		    for (int j = 0; j < dicEntryElements.getLength(); j++) {
		    	if(dicEntryElements.item(j).getNodeName().equals(DICTIONARY_ENTRY_ELEMENT)) {
			        ComponentXMLAttributes attributes = new ComponentXMLAttributes((Element) dicEntryElements.item(j), graph);
			        try {
			        	// get basic parameters
			        	String type = attributes.getString(DICTIONARY_ENTRY_TYPE);
			        	String name = attributes.getString(DICTIONARY_ENTRY_NAME);
			        	
			        	// get properties
			        	final Properties entryProperties = attributes.attributes2Properties(null);
			        	entryProperties.remove(DICTIONARY_ENTRY_ID);
			        	entryProperties.remove(DICTIONARY_ENTRY_TYPE);
			        	entryProperties.remove(DICTIONARY_ENTRY_NAME);
			        	entryProperties.remove(DICTIONARY_ENTRY_INPUT);
			        	entryProperties.remove(DICTIONARY_ENTRY_OUTPUT);
			        	entryProperties.remove(DICTIONARY_ENTRY_REQUIRED);
			        	entryProperties.remove(DICTIONARY_ENTRY_CONTENT_TYPE);
			        	String prefix = "ss.";
			        	for(Object key : entryProperties.keySet().toArray()){
			        		if(key.toString().startsWith(prefix)){
			        			Object value = entryProperties.get(key);
			        			entryProperties.remove(key);
			        			entryProperties.put(key.toString().substring(prefix.length()), value);
			        		}
			        	}

			        	// create entry 
			        	if (!entryProperties.isEmpty()) {
				        	try {
								dictionary.setValueFromProperties(name, type, entryProperties);
							} catch (UnsupportedDictionaryOperation e) {
								//probably only if the dictionary type does not support initialization from Properties and an ID attribute or others was passed
								//so just create dictionary entry without value
								dictionary.setValue(name, type, null);
							}
						} else {
							dictionary.setValue(name, type, null);
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
			            throw new XMLConfigurationException("Dictionary - Attributes missing " + ex.getMessage());
			        } catch (ComponentNotReadyException e) {
			            throw new XMLConfigurationException("Dictionary initialization problem.", e);
					}
		    	}
		    }
	    }
	}

	public Document getOutputXMLDocumentReference() {
		return(this.outputXMLDocument);
	}
	
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
	private Node xmlElementInDocument(String elementName, Map requiredAttributes) {
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
				for( Object entryObject:requiredAttributes.entrySet()){
					final Map.Entry entry = (Entry) entryObject;
					final String requiredName = (String)entry.getKey();
					final String requiredValue = (String)entry.getValue();
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
				HashMap phaseAtts = new HashMap();
				phaseAtts.put("number",String.valueOf(i));
				Element phaseElement = (Element)xmlElementInDocument(PHASE_ELEMENT,phaseAtts);
				if (phaseElement == null) {
					phaseElement = outputXMLDocument.createElement(PHASE_ELEMENT);
					phaseElement.setAttribute("number",String.valueOf(i));
					rootElement.appendChild(phaseElement);
				}
				
				Collection<Node> nodes = phases[i].getNodes().values();
				Iterator iter = nodes.iterator();
				while (iter.hasNext()) {
					Node graphNode = (Node)iter.next();
					HashMap nodeAtts = new HashMap();
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

    /**
     * Simple implementation of Node, used for "disabled" and "pass through" nodes 
     * by reading graph from xml. In next graph processing will be this nodes removed from graph.
     */
    static class SimpleNode extends Node {

        public SimpleNode(String id) {
            super(id);
        }

        public String getType() { return null; }

        @Override
        public ConfigurationStatus checkConfig(ConfigurationStatus status) { return status; }

        public Result execute() { return Result.FINISHED_OK; }

        public void init() throws ComponentNotReadyException { }

        public void free() {
            
        }
    }
}

