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

import java.util.logging.Logger;
/*
import org.apache.log4j.Logger;
import org.apache.log4j.BasicConfigurator;
*/
/**
 * A class that represents Transformation Graph - all the Nodes and connecting Edges
 *
 * @author     D.Pavlis
 * @since    April 2, 2002
 * @see        OtherClasses
 */
 
 /*
 	TODO: enumerateNodes called too many times. The result should be preserved somewhere. It is
	not a big problem now, but with increasing complexity of graph, the time needed to complete
	this task will grow. However, it affects only initialization phase.
 */
public final class TransformationGraph {

	// Attributes

	// Associations
	/**
	 * @since    April 2, 2002
	 */
	private List nodes;
	/**
	 * @since    April 2, 2002
	 */
	private List edges;

	private Map dbConnections;

	private String name;

	private static TransformationGraph graph = new TransformationGraph("");

	static Logger logger=Logger.getLogger("org.jetel");

	static PrintStream log=System.out; // default info messages to stdout
	
	// Operations

	/**
	 *Constructor for the TransformationGraph object
	 *
	 * @param  _name  Name of the graph
	 * @since         April 2, 2002
	 */
	private TransformationGraph(String _name) {
		this.name = new String(_name);
		nodes = new ArrayList();
		edges = new ArrayList();
		dbConnections = new HashMap();
		// initialize logger - just basic
		//BasicConfigurator.configure();
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
		return (DBConnection)dbConnections.get(name);
	}


	/**
	 * An operation that starts execution of graph
	 *
	 * @param  out  OutputStream - if defined, info messages are printed there
	 * @return      True if all nodes successfully started, otherwise False
	 * @since       April 2, 2002
	 */
	public boolean run() {
		WatchDog watchDog;
		
		watchDog = new WatchDog(log, WatchDog.DEFAULT_WATCHDOG_TRACKING_INTERVAL);
		
		log.println("[Clover] starting WatchDog thread ...");
		watchDog.start();
		try {
			watchDog.join();
		}
		catch (InterruptedException ex) {
			logger.severe(ex.getMessage());
			return false;
		}
		log.println("[Clover] WatchDog thread finished");
		return watchDog.getStatus() == 0 ? true : false;
	}



	/**
	 *  Returns linked list of all Nodes. The order of Nodes listed is such that
	 *  any parent Node is guaranteed to be listed befor child Node.
	 *  The circular references between nodes should be detected.
	 *
	 * @return    Description of the Returned Value
	 * @since     July 29, 2002
	 */
	public Iterator enumerateNodes() {
		Set set1 = new HashSet();
		Set set2 = new HashSet();
		Set actualSet;
		Set enumerationOfNodes = new LinkedHashSet();
		int totalNodesEncountered = 0;
		Node node;
		Iterator iterator;

		// initial populating of set1 - with root Nodes only
		iterator = nodes.iterator();
		while (iterator.hasNext()) {
			node = (Node) iterator.next();
			if (node.isRoot()) {
				set1.add(node);
			}
		}

		if (set1.isEmpty()) {
			logger.severe("No root Nodes detected!");
			throw new RuntimeException();
		}

		actualSet = set1;
		// initialize - actualSet is set1 for the very first run
		while (!actualSet.isEmpty()) {
			totalNodesEncountered += actualSet.size();
			// if there is some Node from actualSet already in the "global" list, it will
			// be removed which indicates circular reference
			if (enumerationOfNodes.removeAll(actualSet)) {
				logger.severe("Circular reference found in graph !");
				throw new RuntimeException();
			}
			// did we process already more nodes than we have in total ??
			// that indicates we have circular graph
			if (totalNodesEncountered > nodes.size()) {
				logger.severe("Circular reference found in graph !");
				throw new RuntimeException();
			}
			// add individual nodes from set
			enumerationOfNodes.addAll(actualSet);

			// find successors , switch actualSet
			if (actualSet == set1) {
				findNodesSuccessors(set1, set2);
				actualSet = set2;
			} else {
				findNodesSuccessors(set2, set1);
				actualSet = set1;
			}
		}
		return enumerationOfNodes.iterator();
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
	 *  Description of the Method
	 *
	 * @param  out  OutputStream - if defined, info messages are printed thereDescription of Parameter
	 * @return      returns TRUE if succeeded or FALSE if some Node or Edge failed initialization
	 * @since       April 10, 2002
	 */
	public boolean init(OutputStream out) {
		Iterator nodeIterator;
		ListIterator edgeIterator = edges.listIterator();
		Iterator dbConnectionIterator=dbConnections.values().iterator();
		Node node;
		Edge edge;
		// if the output stream is specified, create logging possibility information
		if (out != null) {
			log = new PrintStream(out);
		}
		// iterate through all dbConnection(s) and initialize them - try to connect to db
		while(dbConnectionIterator.hasNext()){
			try{
				((DBConnection)dbConnectionIterator.next()).connect();
			}catch(Exception ex){
				logger.severe("Can't connect to database: "+ex.getMessage());
				return false;
			}
		}
		// iterate through all nodes and initialize them
		try {
			nodeIterator = enumerateNodes();
		}
		catch (RuntimeException ex) {
			logger.severe(ex.getMessage());
			return false;
		}
		while (nodeIterator.hasNext()) {
			try {
				node = (Node) nodeIterator.next();
				node.init();
			}
			catch (ComponentNotReadyException ex) {
				logger.severe(ex.getMessage());
				return false;
			}
			// if logger exists, print some out information
			if (log != null) {
				log.print("[Clover] Initialized node: ");
				log.println(node.getID());
			}
		}
		// iterate through all edges and initialize them
		while (edgeIterator.hasNext()) {
			try {
				edge = (Edge) edgeIterator.next();
				edge.init();
			}
			catch (IOException ex) {
				ex.printStackTrace();
				return false;
			}
			// if logger exists, print some out information
			if (log != null) {
				log.print("[Clover] Initialized edge: ");
				log.println(edge.getID());
			}
		}
		return true;
		// initialized OK
	}


	/**
	 * An operation that registers Node within current graph
	 *
	 * @param  node  The feature to be added to the Node attribute
	 * @since        April 2, 2002
	 */
	public void addNode(Node node) {
		nodes.add(node);
		node.setGraph(this);
		// assign this graph referenco to Node
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
	public void deleteNodes() {
		nodes.clear();
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
	 *  Removes all DBConnection objects from Map
	 *
	 * @since    October 1, 2002
	 */
	public void deleteDBConnections() {
		dbConnections.clear();
	}


	/**
	 *  Finds all the successors of Nodes from source Set
	 *
	 * @param  source       Set of source Nodes
	 * @param  destination  Set of all immediate successors of Nodes from <source> set
	 * @since               April 18, 2002
	 */
	protected void findNodesSuccessors(Set source, Set destination) {
		Iterator nodeIterator = source.iterator();
		Iterator portIterator;
		OutputPort outPort;
		// remove all previous items from dest.
		destination.clear();
		// iterate through all source nodes
		while (nodeIterator.hasNext()) {
			portIterator = ((Node) nodeIterator.next()).getOutPorts().iterator();
			// iterate through all output ports
			// some other node is perhaps connected to these ports
			while (portIterator.hasNext()) {
				outPort = (OutputPort) portIterator.next();
				// is some Node reading data produced by our source node ?
				if (outPort.getReader() != null) {
					destination.add(outPort.getReader());
				}
			}
		}
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
	 *  Description of the Class
	 *
	 * @author     dpavlis
	 * @since    July 29, 2002
	 */
	class WatchDog extends Thread {

		PrintStream log;
		int trackingInterval;
		int watchDogStatus;

		/**
		 *  Description of the Field
		 *
		 * @since    July 30, 2002
		 */
		public final static int WATCHDOG_SLEEP_INTERVAL = 200;
		//milliseconds
		/**
		 *  Description of the Field
		 *
		 * @since    July 30, 2002
		 */
		public final static int DEFAULT_WATCHDOG_TRACKING_INTERVAL = 30000;

		/**
		 *  Description of the Field
		 *
		 * @since    October 1, 2002
		 */
		public final static int NUMBER_OF_TICKS_BETWEEN_STATUS_CHECKS = 5;


		//milliseconds

		/**
		 *Constructor for the WatchDog object
		 *
		 * @since    July 29, 2002
		 */
		WatchDog() {
			super("WatchDog");
			log = System.out;
			trackingInterval = DEFAULT_WATCHDOG_TRACKING_INTERVAL;
			setDaemon(true);
		}


		/**
		 *Constructor for the WatchDog object
		 *
		 * @param  out       Description of Parameter
		 * @param  tracking  Description of Parameter
		 * @since            July 29, 2002
		 */
		WatchDog(PrintStream out, int tracking) {
			super("WatchDog");
			log = out;
			trackingInterval = tracking;
			setDaemon(true);
		}


		/**
		 * Execute transformation - start-up all Nodes & watch them running
		 *
		 * @since    July 29, 2002
		 */
		public void run() {
			List leafNodesList = new LinkedList();
			List orderedNodes = new LinkedList(); // nodes in order the are in graph
			ListIterator leafNodesIterator;
			ListIterator nodesIterator;
			Node node;
			int ticker = NUMBER_OF_TICKS_BETWEEN_STATUS_CHECKS;
			long lastTimestamp;
			long currentTimestamp;
			long startTimestamp;

			// first, start-up all Nodes & build list of leaf nodes
			log.println("[WatchDog] Thread started.");
			startUpNodes(enumerateNodes(), leafNodesList, orderedNodes);

			startTimestamp = lastTimestamp = System.currentTimeMillis();
			// entering the loop awaiting completion of work by all leaf nodes
			while (true) {
				if (leafNodesList.isEmpty()) {
					watchDogStatus = 0;
					log.print("[WatchDog] Execution sucessfully finished - elapsed time(sec): ");
					log.println((System.currentTimeMillis() - startTimestamp) / 1000);
					printProcessingStatus(orderedNodes.listIterator());
					return;
					// nothing else to do
				}
				leafNodesIterator = leafNodesList.listIterator();
				while (leafNodesIterator.hasNext()) {
					node = (Node) leafNodesIterator.next();
					// is this Node still alive - ? doing something
					if (!node.isAlive()) {
						leafNodesIterator.remove();
					}
				}
				// check that no Node finished with some fatal error
				if ((ticker--) == 0) {
					ticker = NUMBER_OF_TICKS_BETWEEN_STATUS_CHECKS;
					nodesIterator = orderedNodes.listIterator();
					while (nodesIterator.hasNext()) {
						node = (Node) nodesIterator.next();
						if ((!node.isAlive()) && (node.getResultCode() != Node.RESULT_OK)) {
							log.println("[WatchDog] !!! Fatal Error !!! - graph execution is aborting");
							logger.severe("Node " + node.getID() + " finished with fatal error: "+node.getResultMsg());
							abort();
							printProcessingStatus(orderedNodes.listIterator());
							return;
						}
					}
				}
				// Display processing status, if it is time
				currentTimestamp = System.currentTimeMillis();
				if ((currentTimestamp - lastTimestamp) >= DEFAULT_WATCHDOG_TRACKING_INTERVAL) {
					printProcessingStatus(orderedNodes.listIterator());
					lastTimestamp = currentTimestamp;
				}
				// rest for some time
				try {
					sleep(WATCHDOG_SLEEP_INTERVAL);
				}
				catch (InterruptedException ex) {
					watchDogStatus = -1;
					return;
				}
			}

		}


		/**
		 *  Gets the Status of the WatchDog
		 *
		 * @return    0 if successful, -1 otherwise
		 * @since     July 30, 2002
		 */
		int getStatus() {
			return watchDogStatus;
		}


		/**
		 *  Outputs basic LOG information about graph processing
		 *
		 * @param  iterator  Description of Parameter
		 * @since            July 30, 2002
		 */
		void printProcessingStatus(ListIterator iterator) {
			int i;
			int recCount;
			Node node;
			StringBuffer stringBuf;
			stringBuf = new StringBuffer(90);
			log.println("----------------------------** Start of tracking Log **--------------------------");
			log.print("Time: ");
			// France is here just to get 24hour time format
			log.println(DateFormat.getDateTimeInstance(DateFormat.SHORT,DateFormat.MEDIUM,Locale.FRANCE).
					format(Calendar.getInstance().getTime()));
			log.println("Node                        Status         Port                          #Records");
			log.println("---------------------------------------------------------------------------------");
			while (iterator.hasNext()) {
				node = (Node) iterator.next();
				Object nodeInfo[]={node.getID(),node.getStatus()};
				int nodeSizes[]={-28,-15};
				log.println(StringUtils.formatString(nodeInfo,nodeSizes));
				//in ports
				i=0;
				while((recCount=node.getRecordCount(Node.INPUT_PORT,i))!=-1) {
					Object portInfo[]={"In:",Integer.toString(i),Integer.toString(recCount)};
					int portSizes[]={47,-2,32};
					log.println(StringUtils.formatString(portInfo,portSizes));
					i++;
				}
				//out ports
				i=0;
				while((recCount=node.getRecordCount(Node.OUTPUT_PORT,i))!=-1) {
					Object portInfo[]={"Out:",Integer.toString(i),Integer.toString(recCount)};
					int portSizes[]={47,-2,32};
					log.println(StringUtils.formatString(portInfo,portSizes));
					i++;
				}
			}
			log.println("---------------------------------** End of Log **--------------------------------");
			log.flush();
		}


		/**
		 *Constructor for the abort object
		 *
		 * @since    July 29, 2002
		 */
		void abort() {
			ListIterator iterator = nodes.listIterator();
			Node node;

			// iterate through all the nodes and start them
			while (iterator.hasNext()) {
				node = (Node) iterator.next();
				node.abort();
				log.print("[WatchDog] Interrupted node: ");
				log.println(node.getID());
				log.flush();
			}
		}


		/**
		 *  Description of the Method
		 *
		 * @param  nodesIterator  Description of Parameter
		 * @param  leafNodesList  Description of Parameter
		 * @since                 July 31, 2002
		 */
		private void startUpNodes(Iterator nodesIterator, List leafNodesList, List orderedNodes) {
			Node node;
			log.println("[WatchDog] Starting up all Nodes:");
			while (nodesIterator.hasNext()) {
				node = (Node) nodesIterator.next();
				orderedNodes.add(node);
				node.start();
				if (node.isLeaf()) {
					leafNodesList.add(node);
				}
				log.print("[WatchDog] started node: ");
				log.println(node.getID());
				log.flush();
			}
			log.println("[WatchDog] Sucessfully started All Nodes !");
		}
	}

}
/*
 *  end class TransformationGraph
 */

