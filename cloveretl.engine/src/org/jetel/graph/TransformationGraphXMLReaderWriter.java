/*
 *  jETeL/Clover - Java based ETL application framework.
 *  Copyright (C) 2002  David Pavlis
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
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
	private final static String GLOBAL_ELEMENT = "Global";
	private final static String NODE_ELEMENT = "Node";
	private final static String EDGE_ELEMENT = "Edge";
	private final static String METADATA_ELEMENT = "Metadata";
	private final static String PHASE_ELEMENT = "Phase";
	private final static String DBCONNECTION_ELEMENT = "DBConnection";

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
		Map metadata = new HashMap();
		Map nodes = new HashMap();
		Map edges = new HashMap();

		// delete all Nodes & Edges possibly held by TransformationGraph
		graph.deleteEdges();
		graph.deleteNodes();
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

			// process document - create metadata
			NodeList metadataElements = document.getElementsByTagName(METADATA_ELEMENT);
			instantiateMetadata(metadataElements, metadata);

			NodeList nodeElements = document.getElementsByTagName(NODE_ELEMENT);
			instantiateNodes(nodeElements, nodes);

			NodeList edgeElements = document.getElementsByTagName(EDGE_ELEMENT);
			instantiateEdges(edgeElements, edges, metadata, nodes);

			NodeList dbConnectionElements = document.getElementsByTagName(DBCONNECTION_ELEMENT);
			instantiateDBConnections(dbConnectionElements, graph);

		}
		catch (Exception ex) {
			ex.printStackTrace(System.err);
			return false;
		}
		// register all NODEs & EDGEs within transformation graph;
		colIterator = nodes.values().iterator();
		while (colIterator.hasNext()) {
			graph.addNode((org.jetel.graph.Node) colIterator.next());
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

		// loop through all Metadata elements & create appropriate Metadata objects
		for (int i = 0; i < metadataElements.getLength(); i++) {
			attributes = metadataElements.item(i).getAttributes();

			// process metadata element attributes "id" & "fileURL"
			metadataID = attributes.getNamedItem("id").getNodeValue();
			fileURL = attributes.getNamedItem("fileURL").getNodeValue();

			if ((metadataID != null) && (fileURL != null)) {
					recordMetadata=metadataXMLRW.read(
						new BufferedInputStream(new FileInputStream(FileUtils.getFullPath(fileURL))));
					if (recordMetadata==null){
						logger.severe("Error when reading/parsing record metadata definition file: "+fileURL);
						throw new RuntimeException("Can't parse metadata: "+metadataID);
					}else{
						metadata.put(metadataID, recordMetadata);
					}
			} else {
				throw new RuntimeException("Attributes missing");
			}
		}
		// we successfully instantiated all metadata
	}


	/**
	 *  Description of the Method
	 *
	 * @param  nodeElements  Description of Parameter
	 * @param  nodes         Description of Parameter
	 * @since                May 24, 2002
	 */
	private void instantiateNodes(NodeList nodeElements, Map nodes) {
		org.w3c.dom.NamedNodeMap attributes;
		org.jetel.graph.Node graphNode;
		String nodeType;
		String nodeID;

		// loop through all Node elements & create appropriate Metadata objects
		for (int i = 0; i < nodeElements.getLength(); i++) {
			attributes = nodeElements.item(i).getAttributes();

			// process Node element attributes "id" & "type"
			nodeID = attributes.getNamedItem("id").getNodeValue();
			nodeType = attributes.getNamedItem("type").getNodeValue();

			if ((nodeID != null) && (nodeType != null)) {

				// TODO: here, call some node factory which will create appropriate Node type
				graphNode = ComponentFactory.createComponent(nodeType, nodeElements.item(i));
				if (graphNode != null) {
					nodes.put(nodeID, graphNode);
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
		String edgeID;
		String edgeMetadataID;
		String fromNodeAttr;
		String toNodeAttr;
		String[] specNodePort;
		String fromNode;
		int fromPort;
		String toNode;
		int toPort;
		DataRecordMetadata edgeMetadata;
		org.jetel.graph.Edge graphEdge;
		org.jetel.graph.Node graphNode;

		// loop through all Node elements & create appropriate Metadata objects
		for (int i = 0; i < edgeElements.getLength(); i++) {
			attributes = edgeElements.item(i).getAttributes();

			// process edge element attributes "id" & "fileURL"
			edgeID = attributes.getNamedItem("id").getNodeValue();
			edgeMetadataID = attributes.getNamedItem("metadata").getNodeValue();
			fromNodeAttr = attributes.getNamedItem("fromNode").getNodeValue();
			toNodeAttr = attributes.getNamedItem("toNode").getNodeValue();

			if ((edgeID != null) && (edgeMetadataID != null) && (fromNodeAttr != null) && (toNodeAttr != null)) {

				edgeMetadata = (DataRecordMetadata) metadata.get(edgeMetadataID);
				if (edgeMetadata == null) {
					throw new RuntimeException("Can't find metadata ID: " + edgeMetadataID);
				}

				graphEdge = new Edge(edgeID, edgeMetadata);
				edges.put(edgeID, graphEdge);
				// assign edge to fromNode
				specNodePort = fromNodeAttr.split(":");
				graphNode = (org.jetel.graph.Node) nodes.get(specNodePort[0]);
				if (graphNode == null) {
					throw new RuntimeException("Can't find node ID: " + fromNodeAttr);
				}
				graphNode.addOutputPort(Integer.parseInt(specNodePort[1]), graphEdge);
				// assign edge to toNode
				specNodePort = toNodeAttr.split(":");
				// Node & port specified in form of: <nodeID>:<portNum>
				graphNode = (org.jetel.graph.Node) nodes.get(specNodePort[0]);
				if (graphNode == null) {
					throw new RuntimeException("Can't find node ID: " + fromNodeAttr);
				}
				graphNode.addInputPort(Integer.parseInt(specNodePort[1]), graphEdge);

			} else {
				// TODO : some error reporting should take place here
				// attribute missing should be handled by validating parser
				throw new RuntimeException("Attribute missing");
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

