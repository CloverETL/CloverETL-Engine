/*
 * jETeL/CloverETL - Java based ETL application framework.
 * Copyright (c) Javlin, a.s. (info@cloveretl.com)
 *  
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.jetel.graph;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.lookup.LookupTable;
import org.jetel.data.sequence.Sequence;
import org.jetel.database.IConnection;
import org.jetel.enums.EdgeTypeEnum;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.exception.GraphConfigurationException;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.graph.ContextProvider.Context;
import org.jetel.graph.dictionary.Dictionary;
import org.jetel.graph.modelview.impl.MetadataPropagationResolver;
import org.jetel.graph.runtime.CloverPost;
import org.jetel.graph.runtime.GraphRuntimeContext;
import org.jetel.graph.runtime.IAuthorityProxy;
import org.jetel.graph.runtime.WatchDog;
import org.jetel.graph.runtime.tracker.TokenTracker;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.DataRecordMetadataStub;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.SubgraphUtils;
import org.jetel.util.bytes.MemoryTracker;
import org.jetel.util.crypto.Enigma;
import org.jetel.util.file.FileUtils;
import org.jetel.util.file.TrueZipVFSEntries;
import org.jetel.util.primitive.TypedProperties;
import org.jetel.util.property.PropertyRefResolver;
import org.jetel.util.string.StringUtils;

/**
 * A class that represents Transformation Graph - all the Nodes and connecting Edges
 *
 * @author      D.Pavlis
 * @since       April 2, 2002
 * @see         org.jetel.graph.runtime.WatchDog
 */
public final class TransformationGraph extends GraphElement {

	public static final String DEFAULT_GRAPH_ID = "DEFAULT_GRAPH_ID";
	
    private static final int MAX_ALLOWED_OBJ_IDX = 1000000;

    private Map <Integer,Phase> phases;

    private Map <String,IConnection> connections;

	private Map <String, Sequence> sequences;

	private Map <String, LookupTable> lookupTables;
	
	private Map <String, Object> dataRecordMetadata;
	
	final static String DEFAULT_CONNECTION_ID = "Connection0";
	final static String DEFAULT_SEQUENCE_ID = "Sequence0";
	final static String DEFAULT_LOOKUP_ID = "LookupTable0";
	final static public String DEFAULT_METADATA_ID = "Metadata0";

	private Dictionary dictionary;
	
	private String password = null;
	
	private Enigma enigma = null;
	
    private boolean debugMode = true;
    
    private String debugModeStr;
    
    private long debugMaxRecords = 0;
    
	static Log logger = LogFactory.getLog(TransformationGraph.class);

	/** Time stamp of instance creation time. */
	private long instanceCreated = System.currentTimeMillis();
	
	private WatchDog watchDog;

	private GraphParameters graphParameters;

	/**
	 * Memory tracker associated with this graph.
	 */
	private MemoryTracker memoryTracker;
	
	private TrueZipVFSEntries vfsEntries;
	
	/**
	 * Set of variables describing this graph instance. All information are retrieved from graph xml file.
	 */
	private String author;
	private String revision;
	private String created;
	private String modified;
	private String modifiedBy;
	private String licenseType;
	private String licenseCode;
	private String guiVersion;
	
	/**
	 * This runtime context is necessary to be given in the initialization time.
	 * A lot of components need to have a runtimeContext instance for their initialization.
	 * While the graph is running the runtime context from current watchdog is taken to account. 
	 */
	private GraphRuntimeContext initialRuntimeContext;
	
	/**
	 * Job type of the parent transformation graph of this graph element - {@link JobType#ETL_GRAPH} or {@link JobType#JOBFLOW}.
	 */
	private JobType jobType = JobType.ETL_GRAPH;

	/**
	 * This is result of automatic metadata propagation. Now it is cached only for designer purpose.
	 * For example information about "no metadata" is stored in this resolver.
	 */
	private MetadataPropagationResolver metadataPropagationResolver;
	
	/**
	 * Flag which indicates the graph has been already analysed by {@link TransformationGraphAnalyzer#analyseGraph(TransformationGraph, GraphRuntimeContext, boolean)}
	 */
	private boolean isAnalysed = false;
	
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
		
		//graph is graph for itself
		setGraph(this);
		
		phases = new LinkedHashMap<Integer,Phase>();
		connections = new LinkedHashMap <String,IConnection> ();
		sequences = new LinkedHashMap<String,Sequence> ();
		lookupTables = new LinkedHashMap<String,LookupTable> ();
		dataRecordMetadata = new LinkedHashMap<String,Object> ();
		graphParameters = new GraphParameters();
		dictionary = new Dictionary(this);
		memoryTracker = new MemoryTracker();
		initialRuntimeContext = new GraphRuntimeContext();
		vfsEntries = new TrueZipVFSEntries();
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
    private boolean isDebugModeResolved = true;
    
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
            PropertyRefResolver prr = new PropertyRefResolver(getGraphParameters());
            debugMode = Boolean.valueOf(prr.resolveRef(debugModeStr)).booleanValue();
            isDebugModeResolved = true;
        }
        
        if (debugMode) {
	        if (watchDog != null) {
	        	return watchDog.getGraphRuntimeContext().isDebugMode();
	        } else {
	            return true;
	        }
        } else {
        	return false;
        }
    }
    
    /**
     * Sets static JobType for this transformation graph.
     * Static JobType is a JobType derived from type of file from which
     * the graph has been created. This static JobType can be different from
     * runtime JobType in {@link GraphRuntimeContext#getJobType()}.
     * For example subgraph (*.sgrf) executed from a jobflow has static JobType
     * {@link JobType#SUBGRAPH} but runtime JobType is {@link JobType#SUBJOBFLOW}.
     */
    public void setStaticJobType(JobType jobType) {
    	this.jobType = jobType;
    }
    
    /**
     * Returns runtime job type. Can be different from static JobType ({@link #getStaticJobType()}).
     */
    @Override
    public JobType getJobType() {
    	GraphRuntimeContext runtimeContext = getRuntimeContext();
    	if (runtimeContext != null) {
    		return runtimeContext.getJobType();
    	} else {
    		return jobType;
    	}
    }

    /**
     * Gets static JobType for this transformation graph.
     * Static JobType is a JobType derived from type of file from which
     * the graph has been created. This static JobType can be different from
     * runtime JobType in {@link GraphRuntimeContext#getJobType()}.
     * For example subgraph (*.sgrf) executed from a jobflow has static JobType
     * {@link JobType#SUBGRAPH} but runtime JobType is {@link JobType#SUBJOBFLOW}.
     */
    public JobType getStaticJobType() {
    	return jobType;
    }
    
    /**
     * Sets maximum debugged records on the edges.
     * @param debugMaxRecords
     */
    public void setDebugMaxRecords(long debugMaxRecords) {
    	this.debugMaxRecords = debugMaxRecords;
    }
    
    /**
     * @return maximum debugged records on the edges.
     */
    public long getDebugMaxRecords() {
        return debugMaxRecords;
    }

	/**
	 *  Gets the IConnection object asssociated with the name provided
	 *
	 * @param  id  The IConnection ID under which the connection was registered.
	 * @return       The IConnection object (if found) or null
	 * @since        October 1, 2002
	 */
	public IConnection getConnection(String id) {
		return  connections.get(id);
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
	public DataRecordMetadata getDataRecordMetadata(String name) {
		return getDataRecordMetadata(name, true);
	}
	
	public DataRecordMetadata getDataRecordMetadata(String name, boolean forceFromStub) {
		Object metadata = dataRecordMetadata.get(name);
		if (metadata != null && metadata instanceof DataRecordMetadataStub) {
			if (forceFromStub) {
				try {
					metadata = ((DataRecordMetadataStub)metadata).createMetadata();
					dataRecordMetadata.put(name, (DataRecordMetadata) metadata);
				} catch (UnsupportedOperationException e) {
					throw new JetelRuntimeException("Creating metadata '" + name + "' from stub not defined for this connection: ", e);
				} catch (Exception e) {
					throw new JetelRuntimeException("Creating metadata '" + name + "' from stub failed: ", e);
				}
			} else {
				metadata = null;
			}
		}
	    return (DataRecordMetadata) metadata;
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
	 * Returns metadata with given name.
	 * WARNING: DataRecordMetadataStub is ignored
	 */
	public String getDataRecordMetadataByName(String name) {
		for (Entry<String, Object> metadataEntry : dataRecordMetadata.entrySet()) {
			Object metadata = metadataEntry.getValue();
			if (metadata instanceof DataRecordMetadata && ((DataRecordMetadata) metadata).getName().equals(name)) {
				return metadataEntry.getKey();
			}
		}
		return null;
	}
	
	/**
     * Return array of Phases defined within graph sorted (ascentially)
     * according phase numbers.
     * 
	 * @return Returns the Phases array.
	 */
	public Phase[] getPhases() {
		
		final Collection<Phase> retList = phases.values();
		final Phase[] ret = retList.toArray(new Phase[retList.size()]);
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
        return phases.get(Integer.valueOf(phaseNum));
    }
    
	/**
	 *  initializes graph (must be called prior attemting to run graph)
	 *
	 * @return      returns TRUE if succeeded or FALSE if some Node or Edge failed initialization
	 * @since       Sept. 16, 2005
	 */
	@Override
	public void init() throws ComponentNotReadyException {
		//register current thread in ContextProvider - it is necessary to static approach to transformation graph
		Context c = ContextProvider.registerGraph(this);
		try {
	        if(isInitialized()) return;
			super.init();
	
			//analyse the graph if necessary - usually the graph is analysed already in TransformationGraphXMLReaderWriter
			if (!isAnalysed()) {
				TransformationGraphAnalyzer.analyseGraph(this, getRuntimeContext(), true);
			}

			//initialize dictionary
			dictionary.init();
			
			// initialize connections
			for (IConnection connection : connections.values()) {
				logger.info("Initializing connection:");
				ClassLoader formerClassLoader = Thread.currentThread().getContextClassLoader();
				try {
					Thread.currentThread().setContextClassLoader(connection.getClass().getClassLoader());
					connection.init();
					logger.info(connection + " ... OK");
				} catch (Exception e) {
					throw new ComponentNotReadyException(connection, "Can't initialize connection " + connection + ".", e);
				} finally {
					Thread.currentThread().setContextClassLoader(formerClassLoader);
				}
			}

			// initialize sequences
			for (Sequence sequence : sequences.values()) {
				logger.info("Initializing sequence:");
				ClassLoader formerClassLoader = Thread.currentThread().getContextClassLoader();
				try {
					Thread.currentThread().setContextClassLoader(sequence.getClass().getClassLoader());
					if (sequence.isShared()) {
						sequence = getAuthorityProxy().getSharedSequence(sequence);
						sequences.put(sequence.getId(), sequence);
					}
					sequence.init();
					logger.info(sequence + " ... OK");
				} catch (Exception e) {
					throw new ComponentNotReadyException(sequence, "Can't initialize sequence " + sequence + ".", e);
				} finally {
					Thread.currentThread().setContextClassLoader(formerClassLoader);
				}
			}

			// initialize lookup tables
			for (LookupTable lookupTable : lookupTables.values()) {
				logger.info("Initializing lookup table:");
				ClassLoader formerClassLoader = Thread.currentThread().getContextClassLoader();
				try {
					Thread.currentThread().setContextClassLoader(lookupTable.getClass().getClassLoader());
					lookupTable.init();
					logger.info(lookupTable + " ... OK");
				} catch (Exception e) {
					throw new ComponentNotReadyException(lookupTable, "Can't initialize lookup table " + lookupTable + ".", e);
				} finally {
					Thread.currentThread().setContextClassLoader(formerClassLoader);
				}
			}

			//initialization of all phases
			//it is no more true --> phases have to be initialized separately and immediately before is run - in runtime after previous phase is finished
			for (Phase phase : phases.values()) {
				phase.init();
			}
		} finally {
			//unregister current thread from ContextProvider
			ContextProvider.unregister(c);
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#preExecute()
	 */
	@Override
	public synchronized void preExecute() throws ComponentNotReadyException {
		super.preExecute();

		//print out types of all edges
		printEdgesInfo();
		
		//pre-execute initialization of dictionary
		dictionary.preExecute();
		
		//pre-execute initialization of connections
		for (IConnection connection : connections.values()) {
			logger.info("Pre-execute initialization of connection:");
			try {
				connection.preExecute();
				logger.info(connection + " ... OK");
			} catch (Exception e) {
				throw new ComponentNotReadyException(connection, "Pre-Execution of connection " + connection + "failed.", e);
			}
		}

		//pre-execute initialization of sequences
		for (Sequence sequence : sequences.values()) {
			logger.info("Pre-execute initialization of sequence:");
			try {
				sequence.preExecute();
				logger.info(sequence + " ... OK");
			} catch (Exception e) {
				throw new ComponentNotReadyException(sequence, "Pre-Execution of sequence " + sequence + "failed.", e);
			}
		}

		//pre-execute initialization of lookup tables
		for (LookupTable lookupTable : lookupTables.values()) {
			logger.info("Pre-execute initialization of lookup table:");
			try {
				lookupTable.preExecute();
				logger.info(lookupTable + " ... OK");
			} catch (Exception e) {
				throw new ComponentNotReadyException(lookupTable, "Pre-Execution of lookup table " + lookupTable + "failed.", e);
			}
		}
	}
	
	@Override
	@Deprecated
	public synchronized void reset() throws ComponentNotReadyException {
		super.reset();
		
		//reset dictionary
		dictionary.reset();
		
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

		//reset all phases
		for(Phase phase : phases.values()) {
			phase.reset();
		}

	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#postExecute(org.jetel.graph.TransactionMethod)
	 */
	@Override
	public void postExecute() throws ComponentNotReadyException {
		super.postExecute();
		
		//post-execute initialization of dictionary
		dictionary.postExecute();
		
		//post-execute finalization of connections
		for (IConnection connection : connections.values()) {
			logger.info("Post-execute finalization of connection:");
			try {
				connection.postExecute();
				logger.info(connection + " ... OK");
			} catch (Exception e) {
				throw new ComponentNotReadyException(connection, "Can't finalize connection " + connection + ".", e);
			}
		}

		//post-execute finalization of sequences
		for (Sequence sequence : sequences.values()) {
			logger.info("Post-execute finalization of sequence:");
			try {
				sequence.postExecute();
				logger.info(sequence + " ... OK");
			} catch (Exception e) {
				throw new ComponentNotReadyException(sequence, "Can't finalize sequence " + sequence + ".", e);
			}
		}

		//post-execute finalization of lookup tables
		for (LookupTable lookupTable : lookupTables.values()) {
			logger.info("Post-execute finalization of lookup table:");
			try {
				lookupTable.postExecute();
				logger.info(lookupTable + " ... OK");
			} catch (Exception e) {
				throw new ComponentNotReadyException(lookupTable, "Can't finalize lookup table " + lookupTable + ".", e);
			}
		}
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#commit()
	 */
	@Override
	public void commit() {
		super.commit();
		
		//commit dictionary
		dictionary.commit();
		
		//commit all phases
		for (Phase phase : phases.values()) {
			phase.commit();
		}

		//commit connections
		for (IConnection connection : connections.values()) {
			connection.commit();
		}

		//commit sequences
		for (Sequence sequence : sequences.values()) {
			sequence.commit();
		}

		//commit lookup tables
		for (LookupTable lookupTable : lookupTables.values()) {
			lookupTable.commit();
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#rollback()
	 */
	@Override
	public void rollback() {
		super.rollback();
		
		//rollback dictionary
		dictionary.rollback();
		
		//rollback all phases
		for (Phase phase : phases.values()) {
			phase.rollback();
		}

		//rollback connections
		for (IConnection connection : connections.values()) {
			connection.rollback();
		}

		//rollback sequences
		for (Sequence sequence : sequences.values()) {
			sequence.rollback();
		}

		//rollback lookup tables
		for (LookupTable lookupTable : lookupTables.values()) {
			lookupTable.rollback();
		}
	}
	
	/**  Free all allocated resources which need special care */
	private void freeResources() {
		// Free (close) all opened db connections
		// some JDBC drivers start up thread which monitors opened connection
		// this thread sometimes won't die when the main thread is finished - hence
		// this code
		Iterator<?> iterator;
		IConnection dbCon;
		
		// free all phases
		for(Phase phase : phases.values()) {
			phase.free();
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
		//

		// free all sequences
		iterator = sequences.values().iterator();
		while (iterator.hasNext()) {
			try {
				final Sequence seq = (Sequence) iterator.next();
				if (seq.isShared()) {
					IAuthorityProxy ap = getAuthorityProxy();
					if (ap == null) { // CLD-3693: graph runs on the server
						// try with the default authority proxy instead - should be a ServerAuthorityProxy
						ap = IAuthorityProxy.getDefaultProxy();
					}
					ap.freeSharedSequence(seq);
				} else {
					seq.free();
				}
			} catch (Exception ex) {
			    logger.warn("Can't free Sequence", ex);
			}
		}

		iterator = connections.values().iterator();
		while (iterator.hasNext()) {
		    dbCon = (IConnection) iterator.next();

			try {
				dbCon.free();
			} catch (Exception ex) {
			    logger.warn("Can't free DBConnection", ex);
			}
		}
		// any other deinitialization shoud go here
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
	 * Removes given phase from the transformation graph.
	 * @param phase
	 * @throws GraphConfigurationException 
	 */
	public void removePhase(Phase phase) throws GraphConfigurationException {
		if (phase.equals(phases.get(phase.getPhaseNum()))) {
			phases.remove(phase.getPhaseNum());
		} else {
			throw new GraphConfigurationException("Phase is not a part of the graph (" + phase + ").");
		}
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
		metadata.setGraph(this);
	}
	
	public String addDataRecordMetadata(String id, DataRecordMetadata metadata) {
		String newId = getUniqueId(id, dataRecordMetadata);
		this.dataRecordMetadata.put(newId, metadata);
		metadata.setGraph(this);
		return newId;
	}

	/**
	 * Bulk registration of metadata objects. Mainly used by TransformationGraphXMLReaderWriter.
	 * <p>
	 * <b>Important!</b><br />
	 * Map values are usually {@link DataRecordMetadata}, 
	 * but may contain other classes, specifically {@link DataRecordMetadataStub}.
	 * </p> 
	 * 
	 * @param metadata	Map object containing metadata IDs and metadata objects.
	 */
	public void addDataRecordMetadata(Map<String, ?> metadata){
	    dataRecordMetadata.putAll(metadata);
	    for (Object md : metadata.values()) {
	    	if (md instanceof DataRecordMetadata) { // prevents a ClassCastException - can also be DataRecordMetadataStub
	    		((DataRecordMetadata) md).setGraph(this);
	    	}
	    }
	}
	
	/**
	 * Bulk registration of metadata objects.
	 * 
	 * @param metadata
	 */
	public void addDataRecordMetadata(DataRecordMetadata... metadata){
		for (DataRecordMetadata md : metadata) {
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
			for (Node node : phase.getNodes().values()) {
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
	 * @return graph parameters container for this transformation graph
	 */
	public GraphParameters getGraphParameters() {
		return graphParameters;
	}
	
	/**
	 * Gets the graphProperties attribute of the TransformationGraph object
	 * NOTE: backward compatibility issue introduced in rel-3-5 - returned graph parameters
	 * are only copy of real graph parameters, no changes on returned object are reflected in real graph parameters
	 *
	 * @return    The graphProperties value
	 * @deprecated use getGraphParameters().asProperties() instead
	 * @see CLO-2002
	 */
	@Deprecated
	public TypedProperties getGraphProperties() {
		return getGraphParameters().asProperties();
	}


	/**
	 *  Sets the graphProperties attribute of the TransformationGraph object
	 *
	 * @param  properties  The new graphProperties value
	 * @deprecated use getGraphParameters().addProperties(properties) instead
	 */
	@Deprecated
	public void setGraphProperties(Properties properties) {
		getGraphParameters().setProperties(properties);
	}


	/**
	 *  Loads global graph properties from specified file. These properties
	 *  can later be used throughout graph elements (Nodes, Edges, etc).
	 *
	 * @param  filename  Description of the Parameter
	 * @deprecated simple {@link Properties} format is no more supported by clover engine
	 */
	@Deprecated
	public void loadGraphProperties(String fileURL) throws IOException {
		TypedProperties graphProperties = new TypedProperties();
		InputStream inStream = null;
        try {
        	inStream = FileUtils.getInputStream(getRuntimeContext().getContextURL(), fileURL);
        } catch(MalformedURLException e) {
            logger.error("Wrong URL/filename of file specified: " + fileURL);
            throw e;
        } finally {
        	if (inStream != null) {
        		try {
        			inStream.close();
        		} catch (IOException e) {
        			//DO NOTHING
        		}
        	}
        }
		graphProperties.load(inStream);
		
		getGraphParameters().addProperties(graphProperties);
	}

    /**
     * Same as TransformationGraph.loadGraphProperties() method.
     * Existing properties are not overwritten.
     * @param fileURL
     * @throws IOException
	 * @deprecated simple {@link Properties} format is no more supported by clover engine
     */
	@Deprecated
    public void loadGraphPropertiesSafe(String fileURL) throws IOException {
		TypedProperties graphProperties = new TypedProperties();

		InputStream inStream = null;
        try {
        	inStream = FileUtils.getInputStream(getRuntimeContext().getContextURL(), fileURL);
            graphProperties.loadSafe(inStream);
    		getGraphParameters().addProperties(graphProperties);
        } catch(MalformedURLException e) {
            logger.error("Wrong URL/filename of file specified: " + fileURL);
            throw e;
        } finally {
        	if (inStream != null) {
        		try {
        			inStream.close();
        		} catch (IOException e) {
        			//DO NOTHING
        		}
        	}
        }
    }

	/**
	 *  Populates global graph properties from specified property
	 *  object. <br> Can be used for "loading-in" system properties, etc.
	 *
	 * @param  properties  Description of the Parameter
	 * @deprecated use getGraphParameters().addProperties(properties) instead
	 */
	@Deprecated
	public void loadGraphProperties(Properties properties) {
		getGraphParameters().addProperties(properties);
	}

	/**
	 * @param trackingInterval Sets the tracking interval. How often is the processing status printed  (in milliseconds).
	 */
	@Deprecated
	public void setTrackingInterval(int trackingInterval) {
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
    @Override
	public void free() {
		//register current thread in ContextProvider - it is necessary to static approach to transformation graph
		Context c = ContextProvider.registerGraph(this);
		try {
	        freeResources();
	    	
	    	//free dictionary /some readers use dictionary in the free method for the incremental reading
	    	dictionary.free();
	    	
	    	setWatchDog(null);
		} finally {
			//unregister current thread from ContextProvider
			ContextProvider.unregister(c);
		}
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
     * Bulk adding edges into appropriate phases
     * 
     * @param edges
     * @throws GraphConfigurationException
     */
    public void addAllEdges(Collection<Edge> edges) throws GraphConfigurationException{
    	for (Edge edge : edges) {
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
    
    @Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);

		//analyse the graph if necessary - usually the graph is analysed already in TransformationGraphXMLReaderWriter
		if (!isAnalysed()) {
			TransformationGraphAnalyzer.analyseGraph(this, getRuntimeContext(), true);
		}

		//register current thread in ContextProvider - it is necessary to static approach to transformation graph
		Context c = ContextProvider.registerGraph(this);
		try {
	    	if(status == null) {
	            status = new ConfigurationStatus();
	        }
	    	
	    	graphParameters.checkConfig(status);
	        
	        //check dictionary
	        dictionary.checkConfig(status);
	        
	        //check connections configuration
	        for(IConnection connection : connections.values()) {
	        	try {
	        		connection.checkConfig(status);
	        	} catch (Exception e) {
	        		ConfigurationProblem problem = new ConfigurationProblem(ExceptionUtils.getMessage(e), Severity.ERROR, connection, Priority.HIGH);
	        		problem.setCauseException(e);
	        		status.add(problem);
	        	}
	        }
	
	        //check lookup tables configuration
	        for(LookupTable lookupTable : lookupTables.values()) {
	        	try {
	        		lookupTable.checkConfig(status);
	        	} catch (Exception e) {
	        		ConfigurationProblem problem = new ConfigurationProblem(ExceptionUtils.getMessage(e), Severity.ERROR, lookupTable, Priority.HIGH);
	        		problem.setCauseException(e);
	        		status.add(problem);
	        	}
	        }
	
	        //check sequences configuration
	        for(Sequence sequence : sequences.values()) {
	        	try {
	        		sequence.checkConfig(status);
	        	} catch (Exception e) {
	        		ConfigurationProblem problem = new ConfigurationProblem(ExceptionUtils.getMessage(e), Severity.ERROR, sequence, Priority.HIGH);
	        		problem.setCauseException(e);
	        		status.add(problem);
	        	}
	        }
	
	        //check metadatas configuration
	        for(Object oDataRecordMetadata : dataRecordMetadata.values()) {
	            if (oDataRecordMetadata instanceof DataRecordMetadata) ((DataRecordMetadata)oDataRecordMetadata).checkConfig(status);
	        }
	
	        //check phases configuration
	        for(Phase phase : getPhases()) {
	            phase.checkConfig(status);
	        }
	        
	        //SubgraphInput and SubgraphOutput components can be present only one instance in the graph
	        boolean hasSubgraphInput = false;
	        boolean hasSubgraphOutput = false;
	        for (Node component : getNodes().values()) {
	        	if (SubgraphUtils.isSubJobInputComponent(component.getType())) {
	        		if (hasSubgraphInput) {
	        			status.add("Multiple SubgraphInput component detected in the graph.", Severity.ERROR, component, Priority.NORMAL);
	        		} else {
	        			hasSubgraphInput = true;
	        		}
	        	}
	        	if (SubgraphUtils.isSubJobOutputComponent(component.getType())) {
	        		if (hasSubgraphOutput) {
	        			status.add("Multiple SubgraphOutput component detected in the graph.", Severity.ERROR, component, Priority.NORMAL);
	        		} else {
	        			hasSubgraphOutput = true;
	        		}
	        	}
	        }
	        
	        return status;
		} finally {
			//unregister current thread from ContextProvider
			ContextProvider.unregister(c);
		}
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

    /**
     * @return runtime context of watchdog if is available or initialization runtime context instance
     */
    public GraphRuntimeContext getRuntimeContext() {
    	if (watchDog == null) {
    		return initialRuntimeContext;
    	} else {
    		return watchDog.getGraphRuntimeContext();
    	}
    }
    
    /**
     * @return token tracker for this graph provided by {@link WatchDog}
     */
    public TokenTracker getTokenTracker() {
    	if (watchDog != null) {
    		return watchDog.getTokenTracker();
    	} else {
    		return null;
    	}
    }
    
    /**
     * Sets an instance of runtime context which is used during initialization time.
     * @param initialRuntimeContext
     */
    public void setInitialRuntimeContext(GraphRuntimeContext initialRuntimeContext) {
    	this.initialRuntimeContext = initialRuntimeContext;
    }
    
	public Log getLogger() {
		return TransformationGraph.logger;
	}
    
	/**
	 * @return time when this class instance was created
	 */
	public long getInstanceCreated() {
		return instanceCreated;
	}
	
	public Dictionary getDictionary() {
		return dictionary;
	}
	
    public IAuthorityProxy getAuthorityProxy() {
    	return getRuntimeContext().getAuthorityProxy();
    }

    /**
     * @return memory tracker associated with this graph
     */
    public MemoryTracker getMemoryTracker() {
    	return memoryTracker;
    }
    
	public TrueZipVFSEntries getVfsEntries() {
		return vfsEntries;
	}

    public String getUniqueConnectionId(){
    	return getUniqueId(null, connections);
    }
    
    private String getUniqueId(String id, Map<String, ?> elements) {

    	if (id == null) {
			if (elements == dataRecordMetadata) {
				id = DEFAULT_METADATA_ID;
			} else if (elements == connections) {
				id = DEFAULT_CONNECTION_ID;
			} else if (elements == lookupTables) {
				id = DEFAULT_LOOKUP_ID;
			} else if (elements == sequences) {
				id = DEFAULT_SEQUENCE_ID;
			} else {
				throw new IllegalArgumentException("Unknown graph element");
			}
		}
    	
		if (!elements.containsKey(id)) return id;
        
        int idx = id.length();
        char c = 0;
        int num = 0;
        int exp = 1;
        while (--idx >= 0 && Character.isDigit(c = id.charAt(idx))); {
            num += exp * (c - '0');
            exp *= 10;
        }
        final String prefix = (idx < id.length() - 1) ? id.substring(0, idx + 1) : id;
        
        num = 0; // try to fill free gaps between 0 and num
        
        int i = num;
        do {
            i = (i + 1) % MAX_ALLOWED_OBJ_IDX;
            final String newid = prefix + i; 
            if (!elements.containsKey(newid)) return newid;
        } while (i != num);
        return null;
    }

	private void printEdgesInfo() {
		if (logger.isDebugEnabled()) {
			for (Edge edge : getEdges().values()) {
				StringBuilder edgeLabel = new StringBuilder();
				if (edge instanceof JobflowEdge) {
					edgeLabel.append("JobflowEdge");
				} else {
					edgeLabel.append("GraphEdge");
				}
				edgeLabel.append(" [" + edge.getId() + "] : ");
				if (edge.isSharedEdgeBase()) {
					edgeLabel.append("shared " + EdgeTypeEnum.valueOf(edge.getEdgeBase()) + " [" + edge.getEdgeBase().getProxy().getId() + "]");
				} else {
					edgeLabel.append(edge.getEdgeType());
				}
				logger.debug(edgeLabel);
			}
		}
	}

	public String getAuthor() {
		return author;
	}

	public void setAuthor(String author) {
		this.author = author;
	}

	public String getRevision() {
		return revision;
	}

	public void setRevision(String revision) {
		this.revision = revision;
	}

	public String getCreated() {
		return created;
	}

	public void setCreated(String created) {
		this.created = created;
	}

	public String getModifiedBy() {
		return modifiedBy;
	}

	public void setModifiedBy(String modifiedBy) {
		this.modifiedBy = modifiedBy;
	}

	public String getModified() {
		return modified;
	}

	public void setModified(String modified) {
		this.modified = modified;
	}

	public String getLicenseType() {
		return licenseType;
	}

	public void setLicenseType(String licenseType) {
		this.licenseType = licenseType;
	}

	public String getLicenseCode() {
		return licenseCode;
	}

	public void setLicenseCode(String licenseCode) {
		this.licenseCode = licenseCode;
	}

	public String getGuiVersion() {
		return guiVersion;
	}

	public void setGuiVersion(String guiVersion) {
		this.guiVersion = guiVersion;
	}

	/**
	 * @return result of automatic metadata propagation
	 */
	public MetadataPropagationResolver getMetadataPropagationResolver() {
		return metadataPropagationResolver;
	}

	/**
	 * Sets result of automatic metadata propagation.
	 * @param metadataPropagationResolver result object of automatic metadata propagation
	 */
	public void setMetadataPropagationResolver(MetadataPropagationResolver metadataPropagationResolver) {
		this.metadataPropagationResolver = metadataPropagationResolver;
	}

	/**
	 * @return true if the graph has been already analysed by {@link TransformationGraphAnalyzer#analyseGraph(TransformationGraph, GraphRuntimeContext, boolean)}
	 */
	public boolean isAnalysed() {
		return isAnalysed;
	}

	/**
	 * Sets flag which indicates the graph has been already analysed by {@link TransformationGraphAnalyzer#analyseGraph(TransformationGraph, GraphRuntimeContext, boolean)}
	 */
	public void setAnalysed(boolean isAnalysed) {
		this.isAnalysed = isAnalysed;
	}

	@Override
	public String toString() {
		return getId() + ":" + getRuntimeContext().getRunId();
	}
	
}
