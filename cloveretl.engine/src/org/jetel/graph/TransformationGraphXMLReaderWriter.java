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
import org.w3c.dom.NodeList;
import javax.xml.parsers.*;
import org.xml.sax.SAXParseException;
import org.jetel.util.FileUtils;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.DataRecordMetadataXMLReaderWriter;
import org.jetel.component.ComponentFactory;
import org.jetel.database.DBConnection;
import org.jetel.util.PropertyRefResolver;
import java.util.logging.Logger;

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
 * &lt;!ELEMENT Global (Metadata+, DBConnection*)&gt;
 *
 * &lt;!ELEMENT Metadata (#PCDATA)&gt;
 * &lt;!ATTLIST Metadata
 *           	id ID #REQUIRED
 *		fileURL CDATA #REQUIRED &gt;
 *
 * &lt;!ELEMENT DBConnection (#PCDATA)&gt;
 * &lt;!ATTLIST DBConnection
 *           	id ID #REQUIRED
 *		dbDriver CDATA #REQUIRED
 *		dbURL CDATA #REQUIRED
 *		user CDATA
 *		password CDATA &gt;
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

	private static Logger logger = Logger.getLogger("org.jetel.graph");
	
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
			org.w3c.dom.NamedNodeMap grfAttributes = graphElement.item(0).getAttributes();
			graph.setName(grfAttributes.getNamedItem("name").getNodeValue());

			//create metadata
			NodeList metadataElements = document.getElementsByTagName(METADATA_ELEMENT);
			instantiateMetadata(metadataElements, metadata);

			NodeList phaseElements = document.getElementsByTagName(PHASE_ELEMENT);
			instantiatePhases(phaseElements, phases,allNodes);

			NodeList edgeElements = document.getElementsByTagName(EDGE_ELEMENT);
			instantiateEdges(edgeElements, edges, metadata, allNodes);

			NodeList dbConnectionElements = document.getElementsByTagName(DBCONNECTION_ELEMENT);
			instantiateDBConnections(dbConnectionElements, graph);

		}
		catch (Exception ex) {
			ex.printStackTrace(System.err);
			return false;
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
		org.w3c.dom.NamedNodeMap attributes;
		String metadataID;
		String fileURL;
		DataRecordMetadataXMLReaderWriter metadataXMLRW = new DataRecordMetadataXMLReaderWriter();
		DataRecordMetadata recordMetadata;
		PropertyRefResolver refResolver=new PropertyRefResolver();

		// loop through all Metadata elements & create appropriate Metadata objects
		for (int i = 0; i < metadataElements.getLength(); i++) {
			attributes = metadataElements.item(i).getAttributes();

			// process metadata element attributes "id" & "fileURL"
			metadataID = attributes.getNamedItem("id").getNodeValue();
			fileURL = refResolver.resolveRef(attributes.getNamedItem("fileURL").getNodeValue());

			if ((metadataID != null) && (fileURL != null)) {
					recordMetadata=metadataXMLRW.read(
						new BufferedInputStream(new FileInputStream(FileUtils.getFullPath(fileURL))));
					if (recordMetadata==null){
						logger.severe("Error when reading/parsing record metadata definition file: "+fileURL);
						throw new RuntimeException("Can't parse metadata: "+metadataID);
					}else{
						if (metadata.put(metadataID, recordMetadata)!=null){
							throw new RuntimeException("Metadata "+metadataID+" already defined - duplicate ID detected!");
						}
					}
			} else {
				throw new RuntimeException("Attributes missing");
			}
		}
		// we successfully instantiated all metadata
	}

	
	private void instantiatePhases(NodeList phaseElements, List phases,Map allNodes) {
		org.w3c.dom.NamedNodeMap attributes;
		org.jetel.graph.Phase phase;
		String phaseNum;
		NodeList nodeElements;
		
		// loop through all Node elements & create appropriate Metadata objects
		for (int i = 0; i < phaseElements.getLength(); i++) {
			attributes = phaseElements.item(i).getAttributes();
			// process Phase element attribute "number"
			phaseNum = attributes.getNamedItem("number").getNodeValue();

			if (phaseNum != null){
				phase=new Phase(Integer.parseInt(phaseNum));
				phases.add(phase);
			} else {
				throw new RuntimeException("Attribute \"number\" missing for phase");
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
	private void instantiateNodes(int phaseNum,NodeList nodeElements, Map nodes) {
		org.w3c.dom.NamedNodeMap attributes;
		org.jetel.graph.Node graphNode;
		String nodeType;
		String nodeID;
		
		// loop through all Node elements & create appropriate Metadata objects
		for (int i = 0; i < nodeElements.getLength(); i++) {
			if (NODE_ELEMENT.compareToIgnoreCase(nodeElements.item(i).getNodeName())!=0){
				continue;
			}
			attributes = nodeElements.item(i).getAttributes();
			
			// process Node element attributes "id" & "type"
			nodeID = attributes.getNamedItem("id").getNodeValue();
			nodeType = attributes.getNamedItem("type").getNodeValue();
			
			if ((nodeID != null) && (nodeType != null)) {

				
				graphNode = ComponentFactory.createComponent(nodeType, nodeElements.item(i));
				graphNode.setPhase(phaseNum);
				if (graphNode != null) {
					if (nodes.put(nodeID, graphNode)!=null){
						throw new RuntimeException("Duplicate NodeID detected: "+nodeID);
					}
				} else {
					throw new RuntimeException("Error when creating Component type :" + nodeType);
				}

			} else {
				throw new RuntimeException("Attribute missing");
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
		org.w3c.dom.NamedNodeMap attributes;
		String edgeID="unknown";
		String edgeMetadataID;
		String fromNodeAttr;
		String toNodeAttr;
		String[] specNodePort;
		int fromPort;
		int toPort;
		DataRecordMetadata edgeMetadata;
		org.jetel.graph.Edge graphEdge;
		org.jetel.graph.Node graphNode;

		// loop through all Node elements & create appropriate Metadata objects
		for (int i = 0; i < edgeElements.getLength(); i++) {
			attributes = edgeElements.item(i).getAttributes();

			// process edge element attributes "id" & "fileURL"
			try{
			edgeID = attributes.getNamedItem("id").getNodeValue();
			edgeMetadataID = attributes.getNamedItem("metadata").getNodeValue();
			fromNodeAttr = attributes.getNamedItem("fromNode").getNodeValue();
			toNodeAttr = attributes.getNamedItem("toNode").getNodeValue();
			}catch(Exception ex){
				throw new RuntimeException("Attribute missing at edge "+edgeID+" (one of id,metadata,fromNode,toNode)!");
			}

			

			edgeMetadata = (DataRecordMetadata) metadata.get(edgeMetadataID);
			if (edgeMetadata == null) {
				throw new RuntimeException("Can't find metadata ID: " + edgeMetadataID);
			}
			
			graphEdge = new Edge(edgeID, edgeMetadata);
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
		String connectionID;
		DBConnection dbConnection;
		for (int i = 0; i < dbConnectionElements.getLength(); i++) {
			connectionID = dbConnectionElements.item(i).getAttributes().getNamedItem("id").getNodeValue();
			dbConnection = DBConnection.fromXML(dbConnectionElements.item(i));
			if (dbConnection != null && connectionID != null) {
				graph.addDBConnection(connectionID, dbConnection);
			}
		}
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
}

