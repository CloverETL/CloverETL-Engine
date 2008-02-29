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
import java.util.LinkedHashMap;
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
import org.jetel.graph.dictionary.Dictionary;
import org.jetel.graph.dictionary.DictionaryValue;
import org.jetel.graph.runtime.CloverPost;
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

    private Map <String,IConnection> connections;

	private Map <String, Sequence> sequences;

	private Map <String, LookupTable> lookupTables;
	
	private Map <String, DataRecordMetadata> dataRecordMetadata;

	private Dictionary dictionary;
	
	private String password = null;
	
	private Enigma enigma = null;
	
    private boolean debugMode = true;
    
    private String debugModeStr;
    
    private String debugDirectory;
    
    private int debugMaxRecords = 0;
    
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
		connections = new HashMap <String,IConnection> ();
		sequences = new HashMap<String,Sequence> ();
		lookupTables = new HashMap<String,LookupTable> ();
		dataRecordMetadata = new HashMap<String,DataRecordMetadata> ();
		// initialize logger - just basic
		//BasicConfigurator.configure();
		graphProperties = new TypedProperties();
		dictionary = new Dictionary(this);
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
                    projectURL = FileUtils.getFileURL(projectURLStr);
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
		Phase[] ret;
		
		ret = phases.values().toArray(new Phase[0]);
		Arrays.sort(ret);
		
		return ret; 
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
        if(isInitialized()) return;
		super.init();

		//initialize dictionary
		dictionary.init();
		
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
                //dbCon.free();
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
		
        // analyze graph's topology
		List<Node> orderedNodeList;
        try {
            orderedNodeList = TransformationGraphAnalyzer.analyzeGraphTopology(new ArrayList<Node>(getNodes().values()));
    		TransformationGraphAnalyzer.analyzeEdges(new ArrayList<Edge>(getEdges().values()));
    		TransformationGraphAnalyzer.analyzeMultipleFeeds(orderedNodeList);
		} catch (GraphConfigurationException ex) {
			logger.error(ex.getMessage(),ex);
			throw new ComponentNotReadyException("Graph topology analyze failed: " + ex.getMessage(), ex);
		}

        //initialization of all phases
		//phases have to be initialized separately and immediately before is run - in runtime after previous phase is finished
		//temporarily solution
//        for(Phase phase : phases.values()) {
//        	phase.init();
//        }
		
		// initialized OK
	}

	@Override
	public synchronized void reset() throws ComponentNotReadyException {
		super.reset();
		
		setWatchDog(null);
		
		//reset dictionary
		dictionary.reset();
		
		//reset all phases
		for(Phase phase : phases.values()) {
			phase.reset();
		}

		//reset all connections
		for(IConnection connection : connections.values()) {
			connection.reset();
		}
		
		//reset all lookup tables
		for(LookupTable lookupTable : lookupTables.values()) {
			lookupTable.reset();
		}
		
		//reset all sequences
		for(Sequence sequence: sequences.values()) {
			sequence.reset();
		}
		
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
		
		// free all phases
		for(Phase phase : phases.values()) {
			phase.free();
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
	}

	/**
	 *  Adds a feature to the IConnection attribute of the TransformationGraph object
	 *
	 * @param  connection  IConnection object to associate with ID
	 * @since              October 1, 2002
	 */
	public void addConnection(IConnection connection) {
		connections.put(connection.getId(), connection);
        connection.setGraph(this);
	}

	/**
	 *  Adds a feature to the sequence attribute of the TransformationGraph object
	 *
	 * @param  connection  DBConnection object to associate with ID
	 * @since              October 1, 2002
	 */
	public void addSequence(Sequence seq) {
		sequences.put(seq.getId(), seq);
        seq.setGraph(this);
	}

	/**
	 * Registers lookup table within TransformationGraph. It can be later
	 * retrieved by calling getLookupTable().
	 *
	 * @param  lookupTable  The lookup table object to be registered
	 */
	public void addLookupTable(LookupTable lookupTable) {
		lookupTables.put(lookupTable.getId(), lookupTable);
        lookupTable.setGraph(this);
	}

	/**
	 * Registers record metadata object within TransformationGraph. It can be later
	 * retrieved by calling getDataRecordMetadata().
	 *
	 * @param metadata reference to metadata object
	 */
	public void addDataRecordMetadata(DataRecordMetadata metadata) {
		this.dataRecordMetadata.put(metadata.getName(), metadata);
	}
	
	/**
	 * Bulk registration of metadata objects. Mainly used by TransformationGraphXMLReaderWriter
	 * 
	 * @param metadata	Map object containing metadata IDs and metadata objects
	 */
	public void addDataRecordMetadata(Map<String, DataRecordMetadata> metadata){
	    dataRecordMetadata.putAll(metadata);
	}
	
	/**
	 * Bulk registration of metadata objects.
	 * 
	 * @param metadata
	 */
	public void addDataRecordMetadata(DataRecordMetadata... metadata){
		for(DataRecordMetadata md : metadata) {
			addDataRecordMetadata(md);
		}
	}
	
	/**
	 * Good for debugging. Prints out all defined phases and nodes assigned to phases. Has to be
	 * called after init()
	 */
	public void dumpGraphConfiguration() {
		logger.info("*** TRANSFORMATION GRAPH CONFIGURATION ***\n");
		for (Phase phase : getPhases()) {
			logger.info("--- Phase [" + phase.getPhaseNum() + "] ---");
			logger.info("\t... nodes ...");
			for(Node node : phase.getNodes().values()) {
				logger.info("\t" + node.getId() + " : " + (node.getName() !=null ? node.getName() : "") + " phase: " + node.getPhase().getPhaseNum());
			}
			logger.info("\t... edges ...");
			for(Edge edge : phase.getEdges().values()) {
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
    	//free dictionary
    	dictionary.free();
    	
    	setWatchDog(null);
    	
        freeResources();
    }
    
    /**
     * @return list of all nodes in all phases of this graph
     */
    public Map<String, Node> getNodes() {
		Map<String, Node> ret = new LinkedHashMap<String, Node>();
		
		for(Phase phase : phases.values()) {
			ret.putAll(phase.getNodes());
		}

		return ret;
    }

    /**
     * @return unmodifiable view to local <id, edge> map
     */
    public Map<String, Edge> getEdges() {
		Map<String, Edge> ret = new LinkedHashMap<String, Edge>();
		
		for(Phase phase : phases.values()) {
			ret.putAll(phase.getEdges());
		}

		return ret;
    }
    
    /**
     * Adds given edge into the appropriate phase.
     * @param edge
     * @throws GraphConfigurationException 
     */
    public void addEdge(Edge edge) throws GraphConfigurationException {
    	Node writer = edge.getWriter();
    	if(writer == null) {
    		throw new GraphConfigurationException("An edge without source node cannot be added into the graph.");
    	}
    	Phase phase = writer.getPhase();
    	if(phase == null) {
    		throw new GraphConfigurationException("An edge without strict phase definition cannot be added into the graph.");
    	}
    	if(!phases.containsValue(phase)) {
    		throw new GraphConfigurationException("An edge cannot be added into the graph - phase does not exist.");
    	}
    	phase.addEdge(edge);
    }
    
    /**
     * Bulk adding edges into appropriate phases
     * 
     * @param edges
     * @throws GraphConfigurationException
     */
    public void addEdge(Edge... edges) throws GraphConfigurationException{
    	for(Edge edge : edges) {
    		addEdge(edge);
    	}
    }
    
    /**
     * Deletes given edge from the appropriate phase.
     * @param edge
     * @throws GraphConfigurationException 
     */
    public void deleteEdge(Edge edge) throws GraphConfigurationException {
    	Node writer = edge.getWriter();
    	if(writer == null) {
    		throw new GraphConfigurationException("An edge without source node cannot be removed from the graph.");
    	}
    	Phase phase = writer.getPhase();
    	if(phase == null) {
    		throw new GraphConfigurationException("An edge without strict phase definition cannot be removed from the graph.");
    	}
    	if(!phases.containsValue(phase)) {
    		throw new GraphConfigurationException("An edge cannot be removed from the graph - phase does not exist.");
    	}
    	phase.deleteEdge(edge);
    }
    
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);

    	if(status == null) {
            status = new ConfigurationStatus();
        }
        
        //check dictionary
        dictionary.checkConfig(status);
        
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
        
        //check phases configuration
        for(Phase phase : getPhases()) {
            phase.checkConfig(status);
        }
        
        return status;
    }
    
    public CloverPost getPost(){
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
    
	public DictionaryValue<?> getDictionaryValue(String key) {
		return dictionary.get(key);
	}
	
	public void setDictionaryEntry(String key, DictionaryValue<?> value) {
		dictionary.put(key, value);
	}
	
}
/*
 *  end class TransformationGraph
 */

