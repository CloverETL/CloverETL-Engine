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
// FILE: c:/projects/jetel/org/jetel/graph/TransformationGraph.java

package org.jetel.graph;
import java.util.*;
import java.io.*;
import java.text.DateFormat;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.database.DBConnection;
import org.jetel.util.StringUtils;
import org.jetel.data.Defaults;
import org.jetel.exception.GraphConfigurationException;

import java.util.logging.Logger;
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
 * @see         OtherClasses
 */

/*
 *  TODO: enumerateNodes called too many times. The result should be preserved somewhere. It is
 *  not a big problem now, but with increasing complexity of graph, the time needed to complete
 *  this task will grow. However, it affects only initialization phase.
 */
public final class TransformationGraph {

	// Attributes

	// Associations
	/**
	 * @since    April 2, 2002
	 */
	private List phases;

	private List nodes;

	private Node[] nodesArray;
	private Phase[] phasesArray;
	/**
	 * @since    April 2, 2002
	 */
	private List edges;

	private Map dbConnections;

	private Map lookupTables;

	private String name;

	private Phase currentPhase;

	private static TransformationGraph graph = new TransformationGraph("");

	static Logger logger = Logger.getLogger("org.jetel");

	static PrintStream log = System.out;// default info messages to stdout


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
		WatchDog watchDog;
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
		log.print((System.currentTimeMillis()-timestamp)/1000);
		log.println(" (sec)");
		if (watchDog.getStatus() == WatchDog.WATCH_DOG_STATUS_FINISHED_OK){
			log.println("[Clover] Graph execution finished successfully");
			return true;
		}else{
			log.println("[Clover] !!! Graph execution finished with errors !!!");
			return false;
		}
	}



	/**
	 * An operation that aborts execution of graph
	 *
	 * @param  out  OutputStream - if defined, info messages are printed there
	 * @since       April 2, 2002
	 */
	public void abort(OutputStream out) {

	}


	/**
	 *  An operation that ends execution of graph
	 *
	 * @param  out  OutputStream - if defined, info messages are printed there
	 * @since       April 10, 2002
	 */
	public void end(OutputStream out) {

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
		int i = 0;
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


	/**
	 * An operation that registers Phase within current graph
	 *
	 * @param  phase  The phase reference
	 * @since         August 3, 2003
	 * 
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
	 * @param  node  The feature to be added to the Node attribute
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


	/**  Description of the Method */
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
		PrintStream log = System.out;
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
			iterator=phasesArray[i].getEdges().iterator();
			while(iterator.hasNext()){
			edge=(Edge)iterator.next();
			log.print("\t"+edge.getID()+" type: ");
			log.println(edge.getType()==edge.EDGE_TYPE_BUFFERED ? "buffered" : "direct");
		}
			log.println("--- end phase ---");
		}
		log.println("*** END OF GRAPH LIST ***");
	}

}
/*
 *  end class TransformationGraph
 */

