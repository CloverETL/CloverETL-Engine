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
package org.jetel.graph;

import java.io.*;
import java.util.*;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import javax.xml.parsers.*;
import org.xml.sax.SAXParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.DataRecordMetadataJDBCStub;
import org.jetel.metadata.MetadataFactory;
import org.jetel.component.ComponentFactory;
import org.jetel.data.sequence.Sequence;
import org.jetel.data.sequence.SimpleSequence;
import org.jetel.database.DBConnection;
import org.jetel.exception.NotFoundException;
import org.jetel.util.ComponentXMLAttributes;

/**
 *  Helper class which reads transformation graph definition from XML data
 *
 * The XML DTD describing the internal structure is as follows:
 *
 * <pre>
 * &lt;!ELEMENT Graph (Global , Phase+)&gt;
 * &lt;!ATTLIST Graph
 *		name ID #REQUIRED &gt;
 *
 * &lt;!ELEMENT Global (Property*, Metadata+, DBConnection*, Sequence*, Lookup*)&gt;
 *
 * &lt;!ELEMENT Property (#PCDATA)&gt;
 * &lt;!ATTLIST Property
 *           	name CDATA #IMPLIED
 * 				value CDATA #IMPLIED
 *				fileURL CDATA #IMPLIED &gt;
 *
 * &lt;!ELEMENT Metadata (#PCDATA)&gt;
 * &lt;!ATTLIST Metadata
 *           	id ID #REQUIRED
 * 				type CDATA #REQUIRED
 *				fileURL CDATA 
 *				sqlQuery CDATA 
 *				dbConnection CDATA &gt;
 *
 * &lt;!ELEMENT DBConnection (#PCDATA)&gt;
 * &lt;!ATTLIST DBConnection
 *           	id ID #REQUIRED
 *		        dbDriver CDATA #REQUIRED
 *		        dbURL CDATA #REQUIRED
 *		        dbConfig CDATA #IMPLIED
 *		        driverLibrary CDATA #IMPLIED
 *              user CDATA #IMPLIED
 *		        password CDATA #IMPLIED
 *              transactionIsolation (READ_UNCOMMITTED | READ_COMMITTED |
 *                                 REPEATABLE_READ | SERIALIZABLE ) #IMPLIED&gt;
 *
 * &lt;!ELEMENT Sequence (#PCDATA)&gt;
 * &lt;!ATTLIST Sequence
 *          	id ID #REQUIRED &gt;
 *
 * &lt;!ELEMENT Lookup (#PCDATA)&gt;
 * &lt;!ATTLIST Lookup
 *          	id ID #REQUIRED &gt;
 *
 * &lt;!ELEMENT Phase (Node+ , Edge+)&gt;
 * &lt;!ATTLIST Phase
 *		number NMTOKEN #REQUIRED&gt;
 *
 * &lt;!ELEMENT Node (#PCDATA)&gt;
 * &lt;!ATTLIST Node
 *		type NMTOKEN #REQUIRED
 *		id ID #REQUIRED&gt;
 *
 * &lt;!ELEMENT Edge (#PCDATA)&gt;
 * &lt;!ATTLIST Edge
 *		id ID #REQUIRED
 *		metadata NMTOKEN #REQUIRED
 *		fromNode NMTOKEN #REQUIRED
 *		toNode	NMTOKEN #REQUIRED&gt;
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
	private final static String GLOBAL_ELEMENT = "Global";
	private final static String NODE_ELEMENT = "Node";
	private final static String EDGE_ELEMENT = "Edge";
	private final static String METADATA_ELEMENT = "Metadata";
	private final static String PHASE_ELEMENT = "Phase";
	private final static String DBCONNECTION_ELEMENT = "DBConnection";
	private final static String SEQUENCE_ELEMENT = "Sequence";
	private final static String METADATA_RECORD_ELEMENT = "Record";
	private final static String PROPERTY_ELEMENT = "Property";

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
	private static final boolean putCDATAIntoText = false;
	private static final boolean createEntityRefs = false;
	
	private static TransformationGraphXMLReaderWriter graphXMLReaderWriter = new TransformationGraphXMLReaderWriter();

	private static Log logger = LogFactory.getLog(TransformationGraphXMLReaderWriter.class);
	
	private Document outputXMLDocument = null;
	
	/**
	 *Constructor for the TransformationGraphXMLReaderWriter object
	 *
	 * @since    May 24, 2002
	 */
	private TransformationGraphXMLReaderWriter() { }


	/**
	 *Constructor for the read object
	 *
	 * @param  in     Description of Parameter
	 * @param  graph  Description of Parameter
	 * @return        Description of the Returned Value
	 * @since         May 21, 2002
	 */
	public boolean read(TransformationGraph graph, InputStream in) {
		Document document;
		Iterator colIterator;
		Map metadata = new HashMap(ALLOCATE_MAP_SIZE);
		List phases = new LinkedList();
		Map allNodes = new LinkedHashMap(ALLOCATE_MAP_SIZE);
		Map edges = new HashMap(ALLOCATE_MAP_SIZE);

		// delete all Nodes & Edges possibly held by TransformationGraph
		graph.deleteEdges();
		graph.deleteNodes();
		graph.deletePhases();
		graph.deleteDBConnections();
		graph.deleteSequences();

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
			
			document =  db.parse(new BufferedInputStream(in)); 
			document.normalize();

		}catch(SAXParseException ex){
			System.err.print(ex.getMessage());
			System.err.println(" --> on line "+ex.getLineNumber()+" row "+ex.getColumnNumber()); 
			return false;
		}catch (ParserConfigurationException ex) {
			System.err.println(ex.getMessage());
			return false;
		}catch (Exception ex) {
			System.err.println(ex.getMessage());
			return false;
		}

		try {

			// process document
			// get graph name
			NodeList graphElement = document.getElementsByTagName(GRAPH_ELEMENT);
			ComponentXMLAttributes grfAttributes=new ComponentXMLAttributes(graphElement.item(0));
			try{
				graph.setName(grfAttributes.getString("name"));
			}catch(NotFoundException ex){
				throw new RuntimeException("Attribute at Graph node is missing - "+ex.getMessage());
			}

			// handle all defined Properties
			NodeList PropertyElements = document.getElementsByTagName(PROPERTY_ELEMENT);
			instantiateProperties(PropertyElements, graph);
			
			// handle all defined DB connections
			NodeList dbConnectionElements = document.getElementsByTagName(DBCONNECTION_ELEMENT);
			instantiateDBConnections(dbConnectionElements, graph);

			// handle all defined DB connections
			NodeList sequenceElements = document.getElementsByTagName(SEQUENCE_ELEMENT);
			instantiateSequences(sequenceElements, graph);
			
			//create metadata
			NodeList metadataElements = document.getElementsByTagName(METADATA_ELEMENT);
			instantiateMetadata(metadataElements, metadata);

			NodeList phaseElements = document.getElementsByTagName(PHASE_ELEMENT);
			instantiatePhases(phaseElements, phases,allNodes);

			NodeList edgeElements = document.getElementsByTagName(EDGE_ELEMENT);
			instantiateEdges(edgeElements, edges, metadata, allNodes);
		}
		catch (Exception ex) {
			throw new RuntimeException(ex);
		}
		// register all PHASEs, NODEs & EDGEs within transformation graph;
		colIterator = phases.iterator();
		while (colIterator.hasNext()) {
			graph.addPhase((org.jetel.graph.Phase) colIterator.next());
		}
		colIterator = allNodes.values().iterator();
		while (colIterator.hasNext()) {
			Node node=(org.jetel.graph.Node) colIterator.next();
			graph.addNode(node,node.getPhase());
		}
		colIterator = edges.values().iterator();
		while (colIterator.hasNext()) {
			graph.addEdge((org.jetel.graph.Edge) colIterator.next());
		}

		// register metadata (DataRecordMetadata) within transformation graph
		graph.addDataRecordMetadata(metadata);
		
		return true;
	}


	/**
	 *  Description of the Method
	 *
	 * @param  metadataElements  Description of Parameter
	 * @param  metadata          Description of Parameter
	 * @exception  IOException   Description of Exception
	 * @since                    May 24, 2002
	 */
	private void instantiateMetadata(NodeList metadataElements, Map metadata) throws IOException {
		String metadataID;
		String fileURL=null;
		Object recordMetadata;
		//PropertyRefResolver refResolver=new PropertyRefResolver();

		// loop through all Metadata elements & create appropriate Metadata objects
		for (int i = 0; i < metadataElements.getLength(); i++) {
			ComponentXMLAttributes attributes = new ComponentXMLAttributes(metadataElements.item(i));
			try{
			// process metadata element attributes "id" & "fileURL"
			metadataID = attributes.getString("id");
			
			// process metadata from file
			if (attributes.exists("fileURL")){
				fileURL = attributes.getString("fileURL");
				 recordMetadata=MetadataFactory.fromFile(fileURL);
					if (recordMetadata==null){
						logger.fatal("Error when reading/parsing record metadata definition file: "+fileURL);
						throw new RuntimeException("Can't parse metadata: "+metadataID);
					}
			}// metadata from analyzing DB table (JDBC) - will be resolved
			// later during Edge init - just put stub now.
			else if (attributes.exists("sqlQuery")){
				DBConnection dbConnection=TransformationGraph.getReference().getDBConnection(attributes.getString("dbConnection"));
				if (dbConnection==null){
					throw new RuntimeException("Can't find DBConnection "+attributes.getString("dbConnection"));
				}
				recordMetadata=new DataRecordMetadataJDBCStub(dbConnection,attributes.getString("sqlQuery"));
			} // probably metadata inserted directly into graph
			else {
				recordMetadata=MetadataFactory.fromXML(attributes.getChildNode(metadataElements.item(i),METADATA_RECORD_ELEMENT));
			}
			}catch(NotFoundException ex){
				throw new RuntimeException("Metadata - Attributes missing "+ex.getMessage());
			}
			// register metadata object with Transformation graph
			if (metadata.put(metadataID, recordMetadata)!=null){
				throw new RuntimeException("Metadata "+metadataID+" already defined - duplicate ID detected!");
			}
		}
		// we successfully instantiated all metadata
	}

	
	private void instantiatePhases(NodeList phaseElements, List phases,Map allNodes) {
		org.jetel.graph.Phase phase;
		int phaseNum;
		NodeList nodeElements;
		
		// loop through all Node elements & create appropriate Metadata objects
		for (int i = 0; i < phaseElements.getLength(); i++) {
			ComponentXMLAttributes attributes = new ComponentXMLAttributes(phaseElements.item(i));
			// process Phase element attribute "number"
			try{
			phaseNum = attributes.getInteger("number");
			phase=new Phase(phaseNum);
			phases.add(phase);
			}catch(NotFoundException ex) {
				throw new RuntimeException("Attribute is missing for phase -"+ex.getMessage());
			}catch(NumberFormatException ex1){
				throw new RuntimeException("Phase attribute number is not a valid integer !");
			}
			// get all nodes defined in this phase and instantiate them
			// we expect that all childern of phase are Nodes
			//phaseElements.item(i).normalize();
			nodeElements=phaseElements.item(i).getChildNodes();
			instantiateNodes(phase.getPhaseNum(),nodeElements,allNodes);
		}
	}


	/**
	 *  Description of the Method
	 *
	 * @param  nodeElements  Description of Parameter
	 * @param  nodes         Description of Parameter
	 * @since                May 24, 2002
	 */
	private void instantiateNodes(int phaseNum, NodeList nodeElements, Map nodes) {
		org.jetel.graph.Node graphNode;
		String nodeType;
		String nodeID="";

		// loop through all Node elements & create appropriate Metadata objects
		for (int i = 0; i < nodeElements.getLength(); i++) {
			if (NODE_ELEMENT.compareToIgnoreCase(nodeElements.item(i)
					.getNodeName()) != 0) {
				continue;
			}
			ComponentXMLAttributes attributes = new ComponentXMLAttributes(
					nodeElements.item(i));

			// process Node element attributes "id" & "type"
			try {
				nodeID = attributes.getString("id");
				nodeType = attributes.getString("type");
				graphNode = ComponentFactory.createComponent(nodeType,
						nodeElements.item(i));
				graphNode.setPhase(phaseNum);
				if (graphNode != null) {
					if (nodes.put(nodeID, graphNode) != null) {
						throw new RuntimeException(
								"Duplicate NodeID detected: " + nodeID);
					}
				} else {
					throw new RuntimeException(
							"Error when creating Component type :" + nodeType);
				}
			} catch (NotFoundException ex) {
				throw new RuntimeException("Attribute at Node "+nodeID+" is missing - "
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
	private void instantiateEdges(NodeList edgeElements, Map edges, Map metadata, Map nodes) {
		String edgeID="unknown";
		String edgeMetadataID;
		String fromNodeAttr;
		String toNodeAttr;
		String[] specNodePort;
		int fromPort;
		int toPort;
		org.jetel.graph.Edge graphEdge;
		org.jetel.graph.Node graphNode;

		// loop through all Node elements & create appropriate Metadata objects
		for (int i = 0; i < edgeElements.getLength(); i++) {
			ComponentXMLAttributes attributes = new ComponentXMLAttributes(edgeElements.item(i));

			// process edge element attributes "id" & "fileURL"
			try{
			edgeID = attributes.getString("id");
			edgeMetadataID = attributes.getString("metadata");
			fromNodeAttr = attributes.getString("fromNode");
			toNodeAttr = attributes.getString("toNode");
			}catch(NotFoundException ex){
				throw new RuntimeException("Attribute missing at edge "+edgeID+" - "+ex.getMessage());
			}

			Object metadataObj=metadata.get(edgeMetadataID);
			if (metadataObj == null) {
				throw new RuntimeException("Can't find metadata ID: " + edgeMetadataID);
			}
			// do we have real metadata or stub only ??
			if (metadataObj instanceof DataRecordMetadata){
				// real
				graphEdge = new Edge(edgeID, (DataRecordMetadata) metadataObj);
			}else{ 
				// stub
				graphEdge = new Edge(edgeID, (DataRecordMetadataJDBCStub) metadataObj, null);
			}
			if (edges.put(edgeID, graphEdge)!=null){
				throw new RuntimeException("Duplicate EdgeID detected: "+edgeID);
			}
			// assign edge to fromNode
			specNodePort = fromNodeAttr.split(":");
			if (specNodePort.length!=2){
				throw new RuntimeException("Wrong definition of \"fromNode\" <Node>:<Port> at "+edgeID+" edge!");
			}
			graphNode = (org.jetel.graph.Node) nodes.get(specNodePort[0]);
			fromPort=Integer.parseInt(specNodePort[1]);
			if (graphNode == null) {
				throw new RuntimeException("Can't find node ID: " + fromNodeAttr);
			}
			// check whether port isn't already assigned
			if (graphNode.getOutputPort(fromPort)!=null){
				throw new RuntimeException("Output port ["+fromPort+"] of "+graphNode.getID()+" already assigned !");
			}
			graphNode.addOutputPort(fromPort, graphEdge);
			// assign edge to toNode
			specNodePort = toNodeAttr.split(":");
			if (specNodePort.length!=2){
				throw new RuntimeException("Wrong definition of \"toNode\" <Node>:<Port> at edge: "+edgeID+" !");
			}
			// Node & port specified in form of: <nodeID>:<portNum>
			graphNode = (org.jetel.graph.Node) nodes.get(specNodePort[0]);
			toPort=Integer.parseInt(specNodePort[1]);
			if (graphNode == null) {
				throw new RuntimeException("Can't find node ID: " + fromNodeAttr);
			}
			// check whether port isn't already assigned
			if (graphNode.getInputPort(toPort)!=null){
				throw new RuntimeException("Input port ["+toPort+"] of "+graphNode.getID()+" already assigned !");
			}
			graphNode.addInputPort(toPort, graphEdge);

			
		}
	}


	/**
	 *  Description of the Method
	 *
	 * @param  dbConnectionElements  Description of Parameter
	 * @param  graph                 Description of Parameter
	 * @since                        October 1, 2002
	 */
	private void instantiateDBConnections(NodeList dbConnectionElements, TransformationGraph graph) {
		DBConnection dbConnection;
		for (int i = 0; i < dbConnectionElements.getLength(); i++) {
			ComponentXMLAttributes attributes=new ComponentXMLAttributes(dbConnectionElements.item(i));
			try{
				dbConnection = DBConnection.fromXML(dbConnectionElements.item(i));
			if (dbConnection != null) {
				graph.addDBConnection(attributes.getString("id"), dbConnection);
			}
			}catch(NotFoundException ex1){
				throw new RuntimeException("Attribute is missing at DBConnection -"+ex1.getMessage());
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
	private void instantiateSequences(NodeList sequenceElements, TransformationGraph graph) {
		Sequence seq;
		for (int i = 0; i < sequenceElements.getLength(); i++) {
			seq = SimpleSequence.fromXML(sequenceElements.item(i));
			if (seq != null) {
				graph.addSequence(((Element) sequenceElements.item(i)).getAttribute("id"), seq);
			}
		}
	}

	private void instantiateProperties(NodeList propertyElements, TransformationGraph graph) throws IOException {
	    
	    // loop through all property elements & create appropriate properties
	    for (int i = 0; i < propertyElements.getLength(); i++) {
	        ComponentXMLAttributes attributes = new ComponentXMLAttributes(propertyElements.item(i));
	        try{
	            // process property from file
	            if (attributes.exists("fileURL")){
	                String fileURL = attributes.getString("fileURL");
	                graph.loadGraphProperties(fileURL);
	                
	            }else if (attributes.exists("name")){
	                graph.getGraphProperties().setProperty(attributes.getString("name"),
	                        attributes.resloveReferences(attributes.getString("value")));
	            }else{
	                throw new RuntimeException("Invalid property definition :"+propertyElements.item(i));
	            }
	        }catch(NotFoundException ex){
	            throw new RuntimeException("Property - Attributes missing "+ex.getMessage());
	        }
	        
	    }
	    // we successfully instantiated all properties
	}
	
	public Document getOutputXMLDocumentReference() {
		return(this.outputXMLDocument);
	}
	
	/**
	 *  Gets the reference to GraphXMLReaderWriter static object
	 *
	 * @return    object reference
	 * @since     May 28, 2002
	 */
	public static TransformationGraphXMLReaderWriter getReference() {
		return graphXMLReaderWriter;
	}
	
	
	public boolean write(TransformationGraph graph, Document outputDocument) {
		// store reference to allow usage of getOutputXMLDocument() function
		this.outputXMLDocument = outputDocument;
		return(write(graph));
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
				Iterator nameIterator = requiredAttributes.keySet().iterator();
				while (nameIterator.hasNext()) {
					String requiredName = (String)nameIterator.next();
					String requiredValue = (String)requiredAttributes.get(requiredName);
					if (candidate.hasAttribute(requiredName) == false ||
						candidate.getAttribute(requiredName) != requiredValue) {
						// candidate is missing attribute/value
						break;
					} 
	 						
				}
			}
			
		}
		
		return(null);
	}
	
	
	private boolean write(TransformationGraph graph) {
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
				
				List nodes = phases[i].getNodes();
				Iterator iter = nodes.iterator();
				while (iter.hasNext()) {
					Node graphNode = (Node)iter.next();
					HashMap nodeAtts = new HashMap();
					nodeAtts.put(Node.XML_ID_ATTRIBUTE, graphNode.getID());
					nodeAtts.put(Node.XML_TYPE_ATTRIBUTE, graphNode.getType());
					Element xmlElement = (Element)xmlElementInDocument(NODE_ELEMENT,nodeAtts); 
					if (xmlElement == null) {
						xmlElement = outputXMLDocument.createElement(NODE_ELEMENT);
					}
					System.out.println("writing: " + graphNode.getName());
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

