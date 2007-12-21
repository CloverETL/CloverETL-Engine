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
import java.awt.event.ComponentEvent;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.lookup.LookupTable;
import org.jetel.data.sequence.Sequence;
import org.jetel.database.IConnection;
import org.jetel.enums.EdgeTypeEnum;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.GraphConfigurationException;
import org.jetel.graph.runtime.CloverRuntime;
import org.jetel.graph.runtime.GraphRuntimeContext;
import org.jetel.graph.runtime.WatchDog;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.crypto.Enigma;
import org.jetel.util.file.FileUtils;
import org.jetel.util.primitive.TypedProperties;
import org.jetel.util.property.PropertyRefResolver;
import org.jetel.util.string.StringUtils;
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
 * @see         org.jetel.graph.runtime.WatchDog
 */

public final class TransformationGraph extends GraphElement {

	public static final String DEFAULT_GRAPH_ID = "DEFAULT_GRAPH_ID";
	
	public static final String PROJECT_DIR_PROPERTY = "PROJECT_DIR";

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

	private String password = null;
	
	private Enigma enigma = null;
	
    private boolean debugMode = true;
    
    private String debugModeStr;
    
    private String debugDirectory;
    
    private int debugMaxRecords = 0;
    
    private String debugMaxRecordsStr;
    
	static Log logger = LogFactory.getLog(TransformationGraph.class);

	private WatchDog watchDog;

	private TypedProperties graphProperties;

	public TransformationGraph() {
		this(DEFAULT_GRAPH_ID);
	}

	/**
	 *Constructor for the TransformationGraph object
	 *
	 * @param  _name  Name of the graph
	 * @since         April 2, 2002
	 */
	public TransformationGraph(String id) {
		super(id);
		
		phases = new HashMap<Integer,Phase>();
		nodes = new HashMap<String,Node>();
		edges = new HashMap<String,Edge>();
		connections = new HashMap <String,IConnection> ();
		sequences = new HashMap<String,Sequence> ();
		lookupTables = new HashMap<String,LookupTable> ();
		dataRecordMetadata = new HashMap<String,DataRecordMetadata> ();
		// initialize logger - just basic
		//BasicConfigurator.configure();
		graphProperties = new TypedProperties();
	}

	private static boolean firstCallProjectURL = true; //have to be reset
	private static URL projectURL = null;
    public URL getProjectURL() {
        if(firstCallProjectURL) {
            firstCallProjectURL = false;
            String projectURLStr = null;
            if(getGraphProperties().containsKey(PROJECT_DIR_PROPERTY)) {
                projectURLStr = getGraphProperties().getStringProperty(PROJECT_DIR_PROPERTY);
            } else {
            	//maybe any other default project directory
            }
            
            if(projectURLStr != null) {
                try {
                    projectURL = FileUtils.getFileURL(null, projectURLStr);
                } catch (MalformedURLException e) {
                    getLogger().warn("Home project dir is not in valid URL format - " + projectURLStr);
                }
            }
        }

        return projectURL;
    }

    public void setPassword(String password) {
    	this.password = password;
    }
    
    public Enigma getEnigma() {
    	if(enigma == null && !StringUtils.isEmpty(password)) {
    		enigma = new Enigma(password);
    	}
    	return enigma;
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
     * Sets maximum debugged records on the edges.
     * @param debugMaxRecords
     */
    public void setDebugMaxRecords(int debugMaxRecords) {
    	this.debugMaxRecords = debugMaxRecords;
    }
    
    /**
     * @return maximum debugged records on the edges.
     */
    public int getDebugMaxRecords() {
        return debugMaxRecords;
    }

//    /**
//     * Returns URL from PROJECT_DIR graph property value.
//     * It is used as context URL for conversion from relative to absolute path.
//     * @return 
//     */
//    private boolean firstCallprojectURL = true;
//    public URL getProjectURL() {
//        if(firstCallprojectURL) {
//            firstCallprojectURL = false;
//            String projectURLStr = getGraphProperties().getStringProperty(PROJECT_DIR_PROPERTY);
//            
//            if(projectURLStr != null) {
//                try {
//                    projectURL = FileUtils.getFileURL(null, projectURLStr);
//                } catch (MalformedURLException e) {
//                    logger.warn("Home project dir is not in valid URL format - " + projectURLStr);
//                }
//            }
//        }
//
//        return projectURL;
//    }
    
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
	public Iterator<String> getConnections(){
	    return connections.keySet().iterator();
	}
	
	/**
	 * Gets the Iterator which can be used to go through all sequence objects
	 * (their names) registered with graph.<br>
	 * 
	 * @return	Iterator with sequence names
	 * @see		getSequence
	 */
	public Iterator<String> getSequences(){
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
	public Iterator<String> getLookupTables(){
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
	public Iterator<String> getDataRecordMetadata(){
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
     * Return phase with specified phase number.<br>
     * <i>Note:Phases in graph can go out-of-sequence !</i>
     * 
     * @param phaseNum
     * @return
     * @since 26.2.2007
     */
    public Phase getPhase(int phaseNum) {
        return phases.get(new Integer(phaseNum));
    }
    
//	/**
//	 * An operation that aborts execution of graph
//	 *
//	 * @since    April 2, 2002
//	 */
//	public void abort() {
//		if (watchDog != null) {
//			watchDog.abort();
//		}
//	}


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
		try {
			init();
		} catch (ComponentNotReadyException e) {
			return false;
		}
		return true;
	}
	
	/**
	 *  initializes graph (must be called prior attemting to run graph)
	 *
	 * @return      returns TRUE if succeeded or FALSE if some Node or Edge failed initialization
	 * @since       Sept. 16, 2005
	 */
	public void init() throws ComponentNotReadyException {
		Iterator iterator;
		IConnection dbCon = null;
		Sequence seq = null;
		
		// initialize Connections
		// iterate through all Connection(s) and initialize them - try to connect to db
		iterator = connections.values().iterator();
		while (iterator.hasNext()) {
			logger.info("Initializing connection: ");
			try {
				dbCon = (IConnection) iterator.next();
				dbCon.init();
                dbCon.free();
				logger.info(dbCon + " ... OK");
			} catch (Exception ex) {
				logger.info(dbCon + " ... !!! ERROR !!!");
				logger.error("Can't init connection", ex);
				throw new ComponentNotReadyException("Can't connect to database", ex);
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
				logger.error("Can't initialize sequence", ex);
				throw new ComponentNotReadyException("Can't initialize sequence", ex);
			}
		}
		
        // initialize phases array (it is sorted according to phase number)
        phasesArray = getPhases();
        
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
			throw new ComponentNotReadyException("Graph topology analyze failed: " + ex.getMessage(), ex);
		}
		TransformationGraphAnalyzer.analyzeEdges(edgesList);
		TransformationGraphAnalyzer.analyzeMultipleFeeds(nodesList);
		
        edgesList=null;
        nodesList=null;
		// initialized OK
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
        nodes.remove(node.getId());
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
        edges.remove(edge.getId());
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
        connection.setGraph(this);
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
        seq.setGraph(this);
	}

	/**
	 * Registers lookup table within TransformationGraph. It can be later
	 * retrieved by calling getLookupTable().
	 *
	 * @param  lookupTable  The lookup table object to be registered
	 */
	public void addLookupTable(String name, LookupTable lookupTable) {
		lookupTables.put(name, lookupTable);
        lookupTable.setGraph(this);
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
					+ (edge.getEdgeType() == EdgeTypeEnum.BUFFERED ? "buffered" : "direct"));
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
	public TypedProperties getGraphProperties() {
		return graphProperties;
	}


	/**
	 *  Sets the graphProperties attribute of the TransformationGraph object
	 *
	 * @param  properties  The new graphProperties value
	 */
	public void setGraphProperties(Properties properties) {
		this.graphProperties = new TypedProperties(properties);
	}


	/**
	 *  Loads global graph properties from specified file. These properties
	 *  can later be used throughout graph elements (Nodes, Edges, etc).
	 *
	 * @param  filename  Description of the Parameter
	 */
	public void loadGraphProperties(String fileURL) throws IOException {
		if (graphProperties == null) {
			graphProperties = new TypedProperties();
		}
		InputStream inStream;
        try {
        	inStream = Channels.newInputStream(FileUtils.getReadableChannel(getProjectURL(), fileURL));
        } catch(MalformedURLException e) {
            logger.error("Wrong URL/filename of file specified: " + fileURL);
            throw e;
        }
		graphProperties.load(inStream);
	}

    /**
     * Same as TransformationGraph.loadGraphProperties() method.
     * Existing properties are not overwritten.
     * @param fileURL
     * @throws IOException
     */
    public void loadGraphPropertiesSafe(String fileURL) throws IOException {
        if (graphProperties == null) {
            graphProperties = new TypedProperties();
        }
        InputStream inStream;
        try {
        	inStream = Channels.newInputStream(FileUtils.getReadableChannel(getProjectURL(), fileURL));
        } catch(MalformedURLException e) {
            logger.error("Wrong URL/filename of file specified: " + fileURL);
            throw e;
        }
        graphProperties.loadSafe(inStream);
    }

	/**
	 *  Populates global graph properties from specified property
	 *  object. <br> Can be used for "loading-in" system properties, etc.
	 *
	 * @param  properties  Description of the Parameter
	 */
	public void loadGraphProperties(Properties properties) {
		if (graphProperties == null) {
			graphProperties = new TypedProperties();
		}
		graphProperties.putAll(properties);
	}

//	/**
//	 * If graph is running (WatchDog thread is running) then
//	 * it returns which phase (number) is currently beeing executed.
//	 * Otherwise it returns -1;
//	 * 
//	 * @return Phase number or -1 is no phase is beeing executed
//	 */
//	public int getRunningPhaseNum(){
//		if (watchDog!=null){
//			return watchDog.getCurrentPhaseNum();
//		}else{
//			return -1;
//		}
//	}
	/**
	 * @param trackingInterval Sets the tracking interval. How often is the processing status printed  (in milliseconds).
	 */
	@Deprecated public void setTrackingInterval(int trackingInterval) {
		// do nothing - obsolete
	}
    
    
    /**
     * Clears/removes all registered objects (Edges,Nodes,Phases,etc.)
     * @deprecated use TransformationGraph.free() method instead.
     */
    @Deprecated
    public void clear(){
        free();
    }

    /**
     * Clears/removes all registered objects (Edges,Nodes,Phases,etc.)
     */
    public void free() {
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
    
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        if(status == null) {
            status = new ConfigurationStatus();
        }
        
        //check connections configuration
        for(IConnection connection : connections.values()) {
            connection.checkConfig(status);
        }

        //check lookup tables configuration
        for(LookupTable lookupTable : lookupTables.values()) {
            lookupTable.checkConfig(status);
        }

        //check sequences configuration
        for(Sequence sequence : sequences.values()) {
            sequence.checkConfig(status);
        }
        
        //check edges configuration
        for(Edge edge : getEdges().values()) {
            edge.checkConfig(status);
        }

        //check phases configuration
        for(Phase phase : getPhases()) {
            phase.checkConfig(status);
        }
        
        return status;
    }
    
    public CloverRuntime getRuntime(){
        return watchDog;
    }

    /**
     * Returns reference to WatchDog - a thread
     * which orchestrates all phases/nodes at runtime.<br>
     * It also contains detailed information about errors which
     * caused stop of transformation processing.
     * 
     * @return the watchDog
     * @since 10.1.2007
     */
    public WatchDog getWatchDog() {
        return watchDog;
    }

    public void setWatchDog(WatchDog watchDog) {
        this.watchDog = watchDog;
    }

	public Log getLogger() {
		return TransformationGraph.logger;
	}
    
}
/*
 *  end class TransformationGraph
 */

