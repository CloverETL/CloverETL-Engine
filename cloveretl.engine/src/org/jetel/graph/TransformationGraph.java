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
// FILE: c:/projects/jetel/org/jetel/graph/TransformationGraph.java

package org.jetel.graph;
import java.io.*;
import java.text.DateFormat;
import java.util.*;

import java.util.logging.Logger;
import org.jetel.data.Defaults;
import org.jetel.database.DBConnection;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.GraphConfigurationException;
import org.jetel.util.StringUtils;
/*
 *  import org.apache.log4j.Logger;
 *  import org.apache.log4j.BasicConfigurator;
 */
/**
 * A class that represents Transformation Graph - all the Nodes and connecting Edges
 *
 * @author      D.Pavlis
 * @since       April 2, 2002
 * @revision    $Revision$
 * @see         org.jetel.graph.WatchDog
 */

public final class TransformationGraph {

	private List phases;

	private List nodes;

	private Node[] nodesArray;
	private Phase[] phasesArray;
	private List edges;

	private Map dbConnections;

	private Map lookupTables;

	private String name;

	private Phase currentPhase;

	private static TransformationGraph graph = new TransformationGraph("");

	static Logger logger = Logger.getLogger("org.jetel");

	static PrintStream log = System.out;// default info messages to stdout

	private WatchDog watchDog;

	private Properties graphProperties;


	/**
	 *Constructor for the TransformationGraph object
	 *
	 * @param  _name  Name of the graph
	 * @since         April 2, 2002
	 */
	private TransformationGraph(String _name) {
		this.name = new String(_name);
		phases = new LinkedList();
		nodes = new LinkedList();
		edges = new LinkedList();
		dbConnections = new HashMap();
		lookupTables = new HashMap();
		// initialize logger - just basic
		//BasicConfigurator.configure();
		currentPhase = null;
		graphProperties = null;
	}


	/**
	 *  Sets the Name attribute of the TransformationGraph object
	 *
	 * @param  _name  The new Name value
	 * @since         April 10, 2002
	 */
	public void setName(String _name) {
		this.name = new String(_name);
	}


	/**
	 *  Gets the Name attribute of the TransformationGraph object
	 *
	 * @return    The Name value
	 * @since     April 10, 2002
	 */
	public String getName() {
		return name;
	}


	/**
	 *  Gets the DBConnection object asssociated with the name provided
	 *
	 * @param  name  Description of Parameter
	 * @return       The DBConnection object (if found) or null
	 * @since        October 1, 2002
	 */
	public DBConnection getDBConnection(String name) {
		return (DBConnection) dbConnections.get(name);
	}


	/**
	 * An operation that starts execution of graph
	 *
	 * @return    True if all nodes successfully started, otherwise False
	 * @since     April 2, 2002
	 */
	public boolean run() {
		long timestamp = System.currentTimeMillis();
		watchDog = new WatchDog(this, phasesArray, log, Defaults.WatchDog.DEFAULT_WATCHDOG_TRACKING_INTERVAL);

		log.println("[Clover] starting WatchDog thread ...");
		watchDog.start();
		try {
			watchDog.join();
		} catch (InterruptedException ex) {
			logger.severe(ex.getMessage());
			return false;
		}
		log.print("[Clover] WatchDog thread finished - total execution time: ");
		log.print((System.currentTimeMillis() - timestamp) / 1000);
		log.println(" (sec)");

		freeResources();

		if (watchDog.getStatus() == WatchDog.WATCH_DOG_STATUS_FINISHED_OK) {
			log.println("[Clover] Graph execution finished successfully");
			return true;
		} else {
			log.println("[Clover] !!! Graph execution finished with errors !!!");
			return false;
		}

	}



	/**
	 * An operation that aborts execution of graph
	 *
	 * @since    April 2, 2002
	 */
	public void abort() {
		if (watchDog != null) {
			watchDog.abort();
		}
	}


	/**
	 *  initializes graph (must be called prior attemting to run graph)
	 *
	 * @param  out  OutputStream - if defined, info messages are printed thereDescription of Parameter
	 * @return      returns TRUE if succeeded or FALSE if some Node or Edge failed initialization
	 * @since       April 10, 2002
	 */
	public boolean init(OutputStream out) {
		Iterator iterator;
		DBConnection dbCon;
		int i = 0;

		if (out != null) {
			log = new PrintStream(out);
		}
		// initialize DB Connections
		// iterate through all dbConnection(s) and initialize them - try to connect to db
		iterator = dbConnections.values().iterator();
		while (iterator.hasNext()) {
			log.print("Initializing DB connection: ");
			try {
				dbCon = (DBConnection) iterator.next();
				log.print(dbCon);
				dbCon.connect();
				log.println(" ... OK");
			} catch (Exception ex) {
				log.println(" ... !!! ERROR !!!");
				logger.severe("Can't connect to database: " + ex.getMessage());
				return false;
			}
		}
		// initialize phases
		i = 0;
		phasesArray = new Phase[phases.size()];
		iterator = phases.iterator();
		while (iterator.hasNext()) {
			phasesArray[i++] = (Phase) iterator.next();
		}
		try {
			nodesArray = TransformationGraphAnalyzer.enumerateNodes(nodes);
		} catch (GraphConfigurationException ex) {
			logger.severe(ex.getMessage());
			return false;
		}
		TransformationGraphAnalyzer.distributeNodes2Phases(phasesArray, nodesArray, edges);
		// remove reference of nodes and edges from graph - it is now
		// held by phases
		deleteEdges();
		deleteNodes();
		nodesArray = null;// delete references to nodes (only phases contain them)
		// sort phases (the array containing phases) so we
		// can next execute them in proper order (by WatchDog)
		Arrays.sort(phasesArray);
		// initialized OK
		return true;
	}


	/**  Free all allocated resources which need special care */
	private void freeResources() {
		// Free (close) all opened db connections
		// some JDBC drivers start up thread which monitors opened connection
		// this thread sometimes won't die when the main thread is finished - hence
		// this code
		Iterator iterator;
		DBConnection dbCon;
		iterator = dbConnections.values().iterator();
		while (iterator.hasNext()) {
			try {
				dbCon = (DBConnection) iterator.next();
				dbCon.close();
			} catch (Exception ex) {
				log.println(ex.getMessage());
			}
		}
		// any other deinitialization shoud go here
		//
	}


	/**
	 * An operation that registers Phase within current graph
	 *
	 * @param  phase  The phase reference
	 * @since         August 3, 2003
	 */
	public void addPhase(Phase phase) {
		phases.add(phase);
		phase.setGraph(this);
		currentPhase = phase;
		// assign this graph referenco to Phase
	}


	/**
	 *  Adds a feature to the Node attribute of the TransformationGraph object
	 *
	 * @param  node   The feature to be added to the Node attribute
	 * @deprecated
	 */
	public void addNode(Node node) {
		nodes.add(node);
		node.setGraph(this);
		if (currentPhase == null) {
			addPhase(new Phase(0));// default phase (at least one must be created) - number is 0)
		}
		node.setPhase(currentPhase.getPhaseNum());
	}


	/**
	 *  Adds a feature to the Node attribute of the TransformationGraph object
	 *
	 * @param  node   The feature to be added to the Node attribute
	 * @param  phase  The feature to be added to the Node attribute
	 */
	public void addNode(Node node, int phase) {
		nodes.add(node);
		node.setGraph(this);
		node.setPhase(phase);
	}


	/**  Description of the Method */
	public void deleteNodes() {
		nodes.clear();
	}


	/**
	 * An operation that registeres Edge within current graph
	 *
	 * @param  edge  The feature to be added to the Edge attribute
	 * @since        April 2, 2002
	 */
	public void addEdge(Edge edge) {
		edges.add(edge);
		edge.setGraph(this);
		// assign this graph reference to Edge
	}


	/**
	 *  Removes all Edges from graph
	 *
	 * @since    April 2, 2002
	 */
	public void deleteEdges() {
		edges.clear();
	}


	/**
	 *  Removes all Nodes from graph
	 *
	 * @since    April 2, 2002
	 */
	public void deletePhases() {
		phases.clear();
	}


	/**
	 *  Description of the Method
	 *
	 * @param  phaseNo  Description of the Parameter
	 * @return          Description of the Return Value
	 */
	public boolean deletePhase(int phaseNo) {
		Iterator iterator = phases.iterator();
		while (iterator.hasNext()) {
			if (((Phase) (iterator.next())).getPhaseNum() == phaseNo) {
				iterator.remove();
				return true;
			}
		}
		return false;
	}


	/**
	 *  Adds a feature to the DBConnection attribute of the TransformationGraph object
	 *
	 * @param  name        Name(ID) under which the DBConnection is registered
	 * @param  connection  DBConnection object to associate with ID
	 * @since              October 1, 2002
	 */
	public void addDBConnection(String name, DBConnection connection) {
		dbConnections.put(name, connection);
	}


	/**
	 *  Adds a feature to the LookupTable attribute of the TransformationGraph object
	 *
	 * @param  name         The feature to be added to the LookupTable attribute
	 * @param  lookupTable  The feature to be added to the LookupTable attribute
	 */
	public void addLookupTable(String name, Object lookupTable) {
		lookupTables.put(name, lookupTable);
	}


	/**
	 *  Removes all DBConnection objects from Map
	 *
	 * @since    October 1, 2002
	 */
	public void deleteDBConnections() {
		dbConnections.clear();
	}


	/**  Remove all lookup table definitions */
	public void deleteLookupTables() {
		lookupTables.clear();
	}



	/**
	 *  Gets the reference to the TransformationGraph class
	 *
	 * @return    The Reference value
	 * @since     April 10, 2002
	 */
	public static TransformationGraph getReference() {
		return graph;
	}


	/**
	 * Good for debugging. Prints out all defined phases and nodes assigned to phases. Has to be
	 * called after init()
	 */
	public void dumpGraphConfiguration() {
		Iterator iterator;
		Node node;
		Edge edge;
		log.println("*** TRANSFORMATION GRAPH CONFIGURATION ***\n");
		for (int i = 0; i < phasesArray.length; i++) {
			log.println("--- Phase [" + phasesArray[i].getPhaseNum() + "] ---");
			log.println("\t... nodes ...");
			iterator = phasesArray[i].getNodes().iterator();
			while (iterator.hasNext()) {
				node = (Node) iterator.next();
				log.println("\t" + node.getID() + " : " + node.getName() + " phase: " + node.getPhase());
			}
			log.println("\t... edges ...");
			iterator = phasesArray[i].getEdges().iterator();
			while (iterator.hasNext()) {
				edge = (Edge) iterator.next();
				log.print("\t" + edge.getID() + " type: ");
				log.println(edge.getType() == edge.EDGE_TYPE_BUFFERED ? "buffered" : "direct");
			}
			log.println("--- end phase ---");
		}
		log.println("*** END OF GRAPH LIST ***");
	}


	/**
	 *  Gets the graphProperties attribute of the TransformationGraph object
	 *
	 * @return    The graphProperties value
	 */
	public Properties getGraphProperties() {
		return graphProperties;
	}


	/**
	 *  Sets the graphProperties attribute of the TransformationGraph object
	 *
	 * @param  properties  The new graphProperties value
	 */
	public void setGraphProperties(Properties properties) {
		this.graphProperties = properties;
	}


	/**
	 *  Loads global graph properties from specified file. These properties
	 *  can later be used throughout graph elements (Nodes, Edges, etc).
	 *
	 * @param  filename  Description of the Parameter
	 */
	public void loadGraphProperties(String filename) {
		if (graphProperties == null) {
			graphProperties = new Properties();
		}
		try {
			InputStream inStream = new BufferedInputStream(new FileInputStream(filename));
			graphProperties.load(inStream);
		} catch (IOException ex) {
			logger.severe(ex.getMessage());
		}
	}


	/**
	 *  Populates global graph properties from specified property
	 *  object. <br> Can be used for "loading-in" system properties, etc.
	 *
	 * @param  properties  Description of the Parameter
	 */
	public void loadGraphProperties(Properties properties) {
		if (graphProperties == null) {
			graphProperties = new Properties();
		}
		graphProperties.putAll(properties);
	}

}
/*
 *  end class TransformationGraph
 */

