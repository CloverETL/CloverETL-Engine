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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.Defaults;
import org.jetel.data.lookup.LookupTable;
import org.jetel.data.sequence.Sequence;
import org.jetel.database.IConnection;
import org.jetel.exception.GraphConfigurationException;
import org.jetel.exception.AttributeNotFoundException;
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

	private Map <Integer,Phase> phases;

	private Map <String,Node> nodes;
    private Map <String,Edge> edges;
    
    // auxiliary variables
	private Phase[] phasesArray;
    private List <Node> nodesList;
    private List <Edge> edgesList;

	private Map <String,IConnection> connections;

	private Map <String, Sequence> sequences;

	private Map <String, LookupTable> lookupTables;
	
	private Map <String, DataRecordMetadata> dataRecordMetadata;

	private String name;
    
    private boolean debugMode = true;
    
    private String debugModeStr;
    
    private String debugDirectory;
    
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
		phases = new HashMap<Integer,Phase>();
		nodes = new HashMap<String,Node>();
		edges = new HashMap<String,Edge>();
		connections = new HashMap <String,IConnection> ();
		sequences = new HashMap<String,Sequence> ();
		lookupTables = new HashMap<String,LookupTable> ();
		dataRecordMetadata = new HashMap<String,DataRecordMetadata> ();
		// initialize logger - just basic
		//BasicConfigurator.configure();
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
                debugMode = Boolean.valueOf(prr.resolveRef(debugModeStr)).booleanValue();
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
                debugDirectory = prr.resolveRef(debugDirectory);
            isDebugDirectoryResolved = true;
        }
        if(debugDirectory == null) {
            return System.getProperty("java.io.tmpdir");
        } else {
            return debugDirectory;
        }
    }
    
	/**
	 *  Gets the IConnection object asssociated with the name provided
	 *
	 * @param  name  The IConnection name under which the connection was registered.
	 * @return       The IConnection object (if found) or null
	 * @since        October 1, 2002
	 */
	public IConnection getConnection(String name) {
		return  connections.get(name);
	}

	/**
	 *  Gets the sequence object asssociated with the name provided
	 *
	 * @param  name  The sequence name under which the connection was registered.
	 * @return       The sequence object (if found) or null
	 */
	public Sequence getSequence(String name) {
		return  sequences.get(name);
	}

	/**
	 * Gets the Iterator which can be used to go through all IConnection objects
	 * (their names) registered with graph.<br>
	 * 
	 * @return	Iterator with IConnections names
	 * @see		getConnection
	 */
	public Iterator getConnections(){
	    return connections.keySet().iterator();
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
	    return lookupTables.get(name);
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
	    return dataRecordMetadata.get(name);
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
     * Return array of Phases defined within graph sorted (ascentially)
     * according phase numbers.
     * 
	 * @return Returns the Phases array.
	 */
	public Phase[] getPhases() {
        Phase[] array=phases.values().toArray(new Phase[0]);
        Arrays.sort(array);
		return array; 
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
	@Deprecated public boolean init(OutputStream out) {
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
		IConnection dbCon = null;
		Sequence seq = null;
		
		// initialize DB Connections
		// iterate through all dbConnection(s) and initialize them - try to connect to db
		iterator = connections.values().iterator();
		while (iterator.hasNext()) {
			logger.info("Initializing DB connection: ");
			try {
				dbCon = (IConnection) iterator.next();
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
		
        // initialize phases array (it is sorted according to phase number)
        phasesArray = getPhases();
        
        //remove disabled components and their edges
        TransformationGraphAnalyzer.disableNodesInPhases(phasesArray);
       
        // assemble new list of all Nodes (after disabling some)
        nodesList = new ArrayList<Node> (nodes.size());
        for(int i=0;i<phasesArray.length;i++){
            nodesList.addAll(phasesArray[i].getNodes());
        }
        // list of edges - so they can be further analyzed
        edgesList = new ArrayList<Edge>(edges.size());
        edgesList.addAll(edges.values());  //.toArray(new Edge[0]));
     
        // analyze graph's topology
        try {
            nodesList = TransformationGraphAnalyzer.analyzeGraphTopology(nodesList);
		} catch (GraphConfigurationException ex) {
			logger.error(ex.getMessage(),ex);
			return false;
		}
		TransformationGraphAnalyzer.analyzeEdges(edgesList);
		TransformationGraphAnalyzer.analyzeMultipleFeeds(nodesList);
		
        edgesList=null;
        nodesList=null;
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
		IConnection dbCon;
		iterator = connections.values().iterator();
		while (iterator.hasNext()) {
		    dbCon = (IConnection) iterator.next();

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
	public void addPhase(Phase phase) throws GraphConfigurationException {
		if (phases.put(phase.getPhaseNum(),phase)!=null){
		    throw new GraphConfigurationException("Phase already exists in graph "+phase);
        }
		phase.setGraph(this);
        getPhases(); // update array of phases
	}


	/**
	 *  Adds Node to transformation graph - just for registration.<br>
     *  This method should never be called directly - only from Phase class as
     *  a result of phase.addNode() !.
	 *
	 * @param  node   The Node to be registered within graph attribute
     * @param  phase    The Phase to which node belongs
	 */
	void addNode(Node node,Phase phase) throws GraphConfigurationException {
		if (phase==null){
		    throw new IllegalArgumentException("Phase parameter can NOT be null !");
        }
        if (nodes.put(node.getId(),node)!=null){
            throw new GraphConfigurationException("Node already exists in graph "+node);
        }
		node.setGraph(this);
        node.setPhase(phase);
	}
    
    


	/**  Description of the Method */
	public void deleteNodes() {
		nodes.clear();
	}
    
    public void deleteNode(Node node){
        nodes.remove(node);
    }

	/**
	 * An operation that registeres Edge within current graph.
	 *
	 * @param  edge  The feature to be added to the Edge attribute
	 * @since        April 2, 2002
	 */
	public void addEdge(Edge edge) throws GraphConfigurationException  {
		if (edges.put(edge.getId(),edge)!=null){
            throw new GraphConfigurationException("Edge already exists in graph "+edge); 
        }
		edge.setGraph(this); // assign this graph reference to Edge
	}


	/**
	 *  Removes all Edges from graph
	 *
	 * @since    April 2, 2002
	 */
	public void deleteEdges() {
		edges.clear();
	}

    public void deleteEdge(Edge edge){
        edges.remove(edge);
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
		return phases.remove(phaseNo)!=null ? true : false;
	}


	/**
	 *  Adds a feature to the IConnection attribute of the TransformationGraph object
	 *
	 * @param  name        Name(ID) under which the IConnection is registered
	 * @param  connection  IConnection object to associate with ID
	 * @since              October 1, 2002
	 */
	public void addConnection(String name, IConnection connection) {
		connections.put(name, connection);
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
		connections.clear();
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
				logger.info("\t" + node.getId() + " : " + (node.getName() !=null ? node.getName() : "") + " phase: " + node.getPhase().getPhaseNum());
			}
			logger.info("\t... edges ...");
			iterator = phasesArray[i].getEdgesInPhase().iterator();
			while (iterator.hasNext()) {
				edge = (Edge) iterator.next();
				logger.info("\t" + edge.getId() + " type: "
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
	public void loadGraphProperties(String fileURL) throws IOException {
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
                throw new IOException(ex.getMessage());
            }
        }
        InputStream inStream = new BufferedInputStream(url.openStream());
		graphProperties.load(inStream);
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
    
    
    /**
     * Clears/removes all registered objects (Edges,Nodes,Phases,etc.)
     * 
     */
    public void clear(){
        freeResources();
        deleteEdges();
        deleteNodes();
        deletePhases();
        deleteDBConnections();
        deleteSequences();
        deleteLookupTables();
        deleteDataRecordMetadata();
    }

    public Map<String, Node> getNodes() {
        return nodes;
    }

    public Map<String, Edge> getEdges() {
        return edges;
    }
}
/*
 *  end class TransformationGraph
 */

