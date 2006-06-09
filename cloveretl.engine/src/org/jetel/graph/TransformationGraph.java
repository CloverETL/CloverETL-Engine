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
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.Defaults;
import org.jetel.data.lookup.LookupTable;
import org.jetel.data.sequence.Sequence;
import org.jetel.database.DBConnection;
import org.jetel.exception.GraphConfigurationException;
import org.jetel.exception.NotFoundException;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.PropertyRefResolver;
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

	private Phase[] phasesArray;
	private List edges;

	private Map dbConnections;

	private Map sequences;

	private Map lookupTables;
	
	private Map dataRecordMetadata;

	private String name;
    
    private boolean debugMode = true;
    
    private String debugModeStr;
    
    private String debugDirectory;
    
	private Phase currentPhase;

	static Log logger = LogFactory.getLog(TransformationGraph.class);

	private WatchDog watchDog;

	private Properties graphProperties;
	
	private int trackingInterval = Defaults.WatchDog.DEFAULT_WATCHDOG_TRACKING_INTERVAL;


    public TransformationGraph() {
        this(null);
    }

	/**
	 *Constructor for the TransformationGraph object
	 *
	 * @param  _name  Name of the graph
	 * @since         April 2, 2002
	 */
	public TransformationGraph(String _name) {
		this.name = _name;
		phases = new LinkedList();
		nodes = new LinkedList();
		edges = new LinkedList();
		dbConnections = new HashMap();
		sequences = new HashMap();
		lookupTables = new HashMap();
		dataRecordMetadata = new HashMap();
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
		this.name = _name;
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
     * Sets debug mode on the edges.
	 * @param debug
	 */
    private boolean isDebugModeResolved;
    
	public void setDebugMode(boolean debugMode) {
	    this.debugMode = debugMode;
        isDebugModeResolved = true;
    }

    public void setDebugMode(String debugModeStr) {
        if(debugModeStr == null || debugModeStr.length() == 0) {
            isDebugModeResolved = true;
        } else {
            this.debugModeStr = debugModeStr;
            isDebugModeResolved = false;
        }
    }

    /**
     * @param debug
     * @return <b>true</b> if is debug on; else <b>false</b>
     */
    public boolean isDebugMode() {
        if(!isDebugModeResolved) {
            PropertyRefResolver prr = new PropertyRefResolver(getGraphProperties());
            try {
                debugMode = Boolean.valueOf(prr.resolveRef(debugModeStr)).booleanValue();
            } catch (Exception ex) {
                throw new NotFoundException("Attribute debug mode not found!");
            }
            isDebugModeResolved = true;
        }
        return debugMode;
    }
    
    /**
     * Sets debug directory. Default is System.getProperty("java.io.tmpdir").
     * @param debugDirectory
     */
    private boolean isDebugDirectoryResolved;

    public void setDebugDirectory(String debugDirectory) {
        if(debugDirectory == null || debugDirectory.length() == 0) {
            this.debugDirectory = null;
            isDebugDirectoryResolved = true;
        } else {
            this.debugDirectory = debugDirectory;
            isDebugDirectoryResolved = false;
        }
    }
    
    /**
     * @return debug directory. Default is System.getProperty("java.io.tmpdir").
     */
    public String getDebugDirectory() {
        if(!isDebugDirectoryResolved) {
            PropertyRefResolver prr = new PropertyRefResolver(getGraphProperties());
            try {
                debugDirectory = prr.resolveRef(debugDirectory);
            } catch (Exception ex) {
                throw new NotFoundException("Attribute debug directory not found!");
            }
            isDebugDirectoryResolved = true;
        }
        if(debugDirectory == null) {
            return System.getProperty("java.io.tmpdir");
        } else {
            return debugDirectory;
        }
    }
    
	/**
	 *  Gets the DBConnection object asssociated with the name provided
	 *
	 * @param  name  The DBConnection name under which the connection was registered.
	 * @return       The DBConnection object (if found) or null
	 * @since        October 1, 2002
	 */
	public DBConnection getDBConnection(String name) {
		return (DBConnection) dbConnections.get(name);
	}

	/**
	 *  Gets the sequence object asssociated with the name provided
	 *
	 * @param  name  The sequence name under which the connection was registered.
	 * @return       The sequence object (if found) or null
	 */
	public Sequence getSequence(String name) {
		return (Sequence) sequences.get(name);
	}

	/**
	 * Gets the Iterator which can be used to go through all DBConnection objects
	 * (their names) registered with graph.<br>
	 * 
	 * @return	Iterator with DBConnections names
	 * @see		getDBConnection
	 */
	public Iterator getDBConnections(){
	    return dbConnections.keySet().iterator();
	}
	
	/**
	 * Gets the Iterator which can be used to go through all sequence objects
	 * (their names) registered with graph.<br>
	 * 
	 * @return	Iterator with sequence names
	 * @see		getSequence
	 */
	public Iterator getSequences(){
	    return sequences.keySet().iterator();
	}
	
	/**
	 * Gets the LookupTable object asssociated with the name provided
	 * 
	 * @param name The LookupTable name under which the connection was registered.
	 * @return The LookupTable object (if found) or null
	 */
	public LookupTable getLookupTable(String name){
	    return (LookupTable)lookupTables.get(name);
	}

	/**
	 * Gets the Iterator which can be used to go through all DBConnection objects
	 * (their names) registered with graph.<br>
	 * 
	 * @return Iterator with LookupTables names
	 */
	public Iterator getLookupTables(){
	    return lookupTables.keySet().iterator();
	}
	
	/**
	 * Gets the dataRecordMetadata registered under given name (ID)
	 * 
	 * @param name name (the ID) under which dataRecordMetadata has been registered with graph
	 * @return
	 */
	public DataRecordMetadata getDataRecordMetadata(String name){
	    return (DataRecordMetadata)dataRecordMetadata.get(name);
	}
	
	/**
	 * Gets the Iterator which can be used to go through all DataRecordMetadata objects
	 * (their names) registered with graph.<br>
	 * 
	 * @return Iterator with Metadata names (IDs)
	 */
	public Iterator getDataRecordMetadata(){
	    return dataRecordMetadata.keySet().iterator();
	}
	
	/**
	 * @return Returns the Phases array.
	 */
	public Phase[] getPhases() {
		return phasesArray;
	}
	/**
	 * An operation that starts execution of graph
	 *
	 * @return    True if all nodes successfully started, otherwise False
	 * @since     April 2, 2002
	 */
	public boolean run() {
		long timestamp = System.currentTimeMillis();
		watchDog = new WatchDog(this, phasesArray, trackingInterval);

		logger.info("Starting WatchDog thread ...");
		watchDog.start();
		try {
			watchDog.join();
		} catch (InterruptedException ex) {
			logger.error(ex);
			return false;
		}
		logger.info("WatchDog thread finished - total execution time: " 
				+ (System.currentTimeMillis() - timestamp) / 1000
				+ " (sec)");

		freeResources();

		if (watchDog.getStatus() == WatchDog.WATCH_DOG_STATUS_FINISHED_OK) {
			logger.info("Graph execution finished successfully");
			return true;
		} else {
			logger.error("!!! Graph execution finished with errors !!!");
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
	 * @param out The output stream to log to
	 * @return      returns TRUE if succeeded or FALSE if some Node or Edge failed initialization
	 * @since       April 10, 2002
	 * @deprecated The OutputStream is now ignored, please call init().  The logging is
	 * sent to commons-logging.
	 */
	public boolean init(OutputStream out) {
		return init();
	}
	
	/**
	 *  initializes graph (must be called prior attemting to run graph)
	 *
	 * @return      returns TRUE if succeeded or FALSE if some Node or Edge failed initialization
	 * @since       Sept. 16, 2005
	 */
	public boolean init() {
		Iterator iterator;
		DBConnection dbCon = null;
		Sequence seq = null;
		
		// initialize DB Connections
		// iterate through all dbConnection(s) and initialize them - try to connect to db
		iterator = dbConnections.values().iterator();
		while (iterator.hasNext()) {
			logger.info("Initializing DB connection: ");
			try {
				dbCon = (DBConnection) iterator.next();
				dbCon.connect();
				logger.info(dbCon + " ... OK");
			} catch (Exception ex) {
				logger.info(dbCon + " ... !!! ERROR !!!");
				logger.error("Can't connect to database: " + ex.getMessage(),ex);
				return false;
			}
		}
		// initialize sequences
		// iterate through all sequences and initialize them
		iterator = sequences.values().iterator();
		while (iterator.hasNext()) {
			logger.info("Initializing sequence: ");
			try {
				seq = (Sequence) iterator.next();
				seq.init();
				logger.info(seq + " ... OK");
			} catch (Exception ex) {
				logger.info(seq + " ... !!! ERROR !!!");
				logger.error("Can't initialize sequence: " + ex.getMessage(),ex);
				return false;
			}
		}
        
        //remove disabled components and their edges
        TransformationGraphAnalyzer.disableComponents(nodes);
        
		// initialize phases
        phasesArray = (Phase[]) phases.toArray(new Phase[phases.size()]);
        Node[] nodesArray;
        try {
			nodesArray = TransformationGraphAnalyzer.enumerateNodes(nodes);
		} catch (GraphConfigurationException ex) {
			logger.fatal(ex.getMessage(),ex);
			return false;
		}
		TransformationGraphAnalyzer.distributeNodes2Phases(phasesArray, nodesArray, edges);
		TransformationGraphAnalyzer.analyzeMultipleFeeds(nodes);
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
			dbCon = (DBConnection) iterator.next();

			try {
				dbCon.free();
			} catch (Exception ex) {
			    logger.warn("Can't free DBConnection", ex);
			}
		}
		// free all lookup tables
		iterator = lookupTables.values().iterator();
		while (iterator.hasNext()) {
			try {
				((LookupTable)iterator.next()).free();
			} catch (Exception ex) {
			    logger.warn("Can't free LookupTable", ex);
			}
		}
		// free all sequences
		iterator = sequences.values().iterator();
		while (iterator.hasNext()) {
			try {
				((Sequence)iterator.next()).free();
			} catch (Exception ex) {
			    logger.warn("Can't free Sequence", ex);
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
		//node.setPhase(phase); //relict code
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
	 *  Adds a feature to the sequence attribute of the TransformationGraph object
	 *
	 * @param  name        Name(ID) under which the DBConnection is registered
	 * @param  connection  DBConnection object to associate with ID
	 * @since              October 1, 2002
	 */
	public void addSequence(String name, Sequence seq) {
		sequences.put(name, seq);
	}

	/**
	 * Registers lookup table within TransformationGraph. It can be later
	 * retrieved by calling getLookupTable().
	 *
	 * @param  lookupTable  The lookup table object to be registered
	 */
	public void addLookupTable(String name, LookupTable lookupTable) {
		lookupTables.put(name, lookupTable);
	}

	/**
	 * Registers record metadata object within TransformationGraph. It can be later
	 * retrieved by calling getDataRecordMetadata().
	 *
	 * @param name  name/ID under which metadata object is registered
	 * @param metadata reference to metadata object
	 */
	public void addDataRecordMetadata(String name, DataRecordMetadata metadata) {
		this.dataRecordMetadata.put(name, metadata);
	}
	
	/**
	 * Bulk registration of metadata objects. Mainly used by TransformationGraphXMLReaderWriter
	 * 
	 * @param metadata	Map object containing metadata IDs and metadata objects
	 */
	public void addDataRecordMetadata(Map metadata){
	    dataRecordMetadata.putAll(metadata);
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
	 *  Removes all sequences objects from Map
	 *
	 * @since    October 1, 2002
	 */
	public void deleteSequences() {
		sequences.clear();
	}

	/**  Remove all lookup table definitions */
	public void deleteLookupTables() {
		lookupTables.clear();
	}

	/**
	 *  Removes all metadata registrations. However, the metadata
	 * objects may still survive as all Edges contain reference to them
	 */
	public void deleteDataRecordMetadata(){
	    dataRecordMetadata.clear();
	}
	

	/**
	 * Good for debugging. Prints out all defined phases and nodes assigned to phases. Has to be
	 * called after init()
	 */
	public void dumpGraphConfiguration() {
		Iterator iterator;
		Node node;
		Edge edge;
		logger.info("*** TRANSFORMATION GRAPH CONFIGURATION ***\n");
		for (int i = 0; i < phasesArray.length; i++) {
			logger.info("--- Phase [" + phasesArray[i].getPhaseNum() + "] ---");
			logger.info("\t... nodes ...");
			iterator = phasesArray[i].getNodes().iterator();
			while (iterator.hasNext()) {
				node = (Node) iterator.next();
				logger.info("\t" + node.getId() + " : " + node.getName() + " phase: " + node.getPhase());
			}
			logger.info("\t... edges ...");
			iterator = phasesArray[i].getEdges().iterator();
			while (iterator.hasNext()) {
				edge = (Edge) iterator.next();
				logger.info("\t" + edge.getID() + " type: "
					+ (edge.getType() == Edge.EDGE_TYPE_BUFFERED ? "buffered" : "direct"));
			}
			logger.info("--- end phase ---");
		}
		logger.info("*** END OF GRAPH LIST ***");
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
	public void loadGraphProperties(String fileURL) {
		if (graphProperties == null) {
			graphProperties = new Properties();
		}
		URL url=null;
        try{
            url = new URL(fileURL); 
        }catch(MalformedURLException e){
            // try to patch the url
            try {
                url=new URL("file:"+fileURL);
            }catch(MalformedURLException ex){
                logger.error("Wrong URL/filename of file specified: "+fileURL,ex);
                throw new RuntimeException("Wrong URL/filename of property file specified: "+fileURL,ex);
            }
        }
		try {
		    InputStream inStream = new BufferedInputStream(url.openStream());
			graphProperties.load(inStream);
		} catch (IOException ex) {
			logger.error(ex.getMessage(),ex);
            throw new RuntimeException("Can't read property file !",ex);
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

	/**
	 * If graph is running (WatchDog thread is running) then
	 * it returns which phase (number) is currently beeing executed.
	 * Otherwise it returns -1;
	 * 
	 * @return Phase number or -1 is no phase is beeing executed
	 */
	public int getRunningPhaseNum(){
		if (watchDog!=null){
			return watchDog.getCurrentPhaseNum();
		}else{
			return -1;
		}
	}
	/**
	 * @param trackingInterval Sets the tracking interval. How often is the processing status printed  (in milliseconds).
	 */
	public void setTrackingInterval(int trackingInterval) {
		this.trackingInterval = trackingInterval;
	}
}
/*
 *  end class TransformationGraph
 */

