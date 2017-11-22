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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.ctl.debug.DebugJMX;
import org.jetel.data.lookup.LookupTable;
import org.jetel.data.sequence.Sequence;
import org.jetel.database.IConnection;
import org.jetel.database.sql.DBConnection;
import org.jetel.enums.EdgeTypeEnum;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.GraphConfigurationException;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.exception.RecursiveSubgraphException;
import org.jetel.graph.ContextProvider.Context;
import org.jetel.graph.dictionary.Dictionary;
import org.jetel.graph.modelview.impl.MetadataPropagationResult;
import org.jetel.graph.rest.jaxb.EndpointSettings;
import org.jetel.graph.rest.jaxb.RestJobResponseStatus;
import org.jetel.graph.runtime.CloverPost;
import org.jetel.graph.runtime.GraphRuntimeContext;
import org.jetel.graph.runtime.IAuthorityProxy;
import org.jetel.graph.runtime.WatchDog;
import org.jetel.graph.runtime.tracker.TokenTracker;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.DataRecordMetadataStub;
import org.jetel.util.CloverPublicAPI;
import org.jetel.util.RestJobUtils;
import org.jetel.util.SubgraphUtils;
import org.jetel.util.crypto.Enigma;
import org.jetel.util.file.FileUtils;
import org.jetel.util.file.TrueZipVFSEntries;
import org.jetel.util.primitive.TypedProperties;
import org.jetel.util.property.PropertyRefResolver;
import org.jetel.util.property.RefResFlag;
import org.jetel.util.string.StringUtils;

/**
 * A class that represents Transformation Graph - all the Nodes and connecting Edges
 *
 * @author      D.Pavlis
 * @since       April 2, 2002
 * @see         org.jetel.graph.runtime.WatchDog
 */
@CloverPublicAPI
public final class TransformationGraph extends GraphElement {

	public static final String DEFAULT_GRAPH_ID = "DEFAULT_GRAPH_ID";
	
    private static final int MAX_ALLOWED_OBJ_IDX = 1000000;

    private Map <Integer, Phase> phases;

    private Map <String,IConnection> connections;

	private Map <String, Sequence> sequences;

	private Map <String, LookupTable> lookupTables;
	
	private Map <String, Object> dataRecordMetadata;

	private SubgraphPorts subgraphInputPorts = new SubgraphPorts(this);
	
	private SubgraphPorts subgraphOutputPorts = new SubgraphPorts(this);

    private Map<String, Node> nodesCache = null;

	final static String DEFAULT_CONNECTION_ID = "Connection0";
	final static String DEFAULT_SEQUENCE_ID = "Sequence0";
	final static String DEFAULT_LOOKUP_ID = "LookupTable0";
	final static public String DEFAULT_METADATA_ID = "Metadata0";
	final static String DEFAULT_EDGE_ID = "Edge0";

	private Dictionary dictionary;
	
	private String password = null;
	
	private Enigma enigma = null;
	
    private boolean edgeDebugging = true;
    
    private String edgeDebuggingStr;
    
    private long debugMaxRecords = 0;
    
	static Log logger = LogFactory.getLog(TransformationGraph.class);

	/** Time stamp of instance creation time. */
	private final long instanceCreated = System.nanoTime();
	
	private WatchDog watchDog;
	
	private DebugJMX debugJMX;

	private GraphParameters graphParameters;

	private TrueZipVFSEntries vfsEntries;
	
	/**
	 * Set of variables describing this graph instance. All information are retrieved from graph xml file.
	 */
	private String author;
	private String created;
	private String licenseCode;
	private String guiVersion;
	private String description;
	
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
	 * This is result of automatic metadata propagation.
	 */
	private MetadataPropagationResult metadataPropagationResult;
	
	/**
	 * Flag which indicates the graph has been already analysed by {@link TransformationGraphAnalyzer#analyseGraph(TransformationGraph, GraphRuntimeContext, boolean)}
	 */
	private boolean isAnalysed = false;
	
	/**
	 * Execution label is human-readable text which can describe execution of this graph.
	 * This text is specified directly in grf file, but can be parametrised by public graph parameters.
	 * This is default for runtime equivalent {@link GraphRuntimeContext#getExecutionLabel()}.
	 */
	private String executionLabel;

	/**
	 * Component category - readers, writers, joiners, ...
	 * This information is not used in runtime, but is necessary
	 * for SubgraphComponentDynamization in designer, where TranformationGraph is
	 * used as model for various Subgraph component modifications.
	 */
	private String category;

	/**
	 * Path to icons, which are used only by subgraphs. It is of course useless
	 * for runtime, but it is necessary for SubgraphComponentDynamization
	 * in designer, where TranformationGraph is used as model for various
	 * Subgraph component modifications.
	 */
	private String smallIconPath;
	private String mediumIconPath;
	private String largeIconPath;

	/**
	 * This checkConfig status is populated in graph factorisation.
	 * Once the real {@link #checkConfig(ConfigurationStatus)} method is
	 * executed this preliminary issues are copied to the final result.
	 * @see TransformationGraphAnalyzer
	 */
	private ConfigurationStatus preCheckConfigStatus = new ConfigurationStatus();
	
	/**
	 * Reference to SubgraphInput component or null if the graph is not subjob.
	 * Lazy initialised variable.
	 */
	private Node subgraphInputComponent = null;

	/**
	 * Reference to SubgraphOutput component or null if the graph is not subjob.
	 * Lazy initialised variable.
	 */
	private Node subgraphOutputComponent = null;

	/**
	 * This map contains all raw values of enabled attributes of all components
	 * (disabled components are included). This value cannot be persisted in {@link Node}
	 * class, since disabled components are not available in {@link TransformationGraph}.
	 * This cache is used for logging purpose, see {@link WatchDog#printComponentsEnabledStatus()}.
	 * Moreover, this cache is used also for check component configuration, see {@link Node#checkConfig(ConfigurationStatus)}. 
	 */
	private Map<Node, String> rawComponentEnabledAttribute = new HashMap<>();
	
	/**
	 * This map contains set of blocked components for each blocker component. This map is used for displaying info about blockers
	 * in GUI tooltips. Contains nodes of original components - i.e. not the Trashifiers that are used as replacements.
	 */
	private Map<Node, Set<Node>> blockingComponents = new HashMap<>();
	
	/**
	 * Set of components that are blocked but are kept in the graph so they can still accept records.
	 * Contains nodes of original components - i.e. not the Trashifiers that are used as replacements.
	 */
	private Set<Node> keptBlocked = new HashSet<Node>();
	
	/**
	 * Endpoint settings if the graph represents a REST job
	 */
	private EndpointSettings endpointSettings;
	
	private RestJobResponseStatus responseStatus;
	
	private String outputFormat;
	
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
		graphParameters = new GraphParameters(this);
		dictionary = new Dictionary(this);
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
    private boolean isEdgeDebuggingResolved = true;
    
	public void setDebugMode(boolean edgeDebugging) {
	    this.edgeDebugging = edgeDebugging;
        isEdgeDebuggingResolved = true;
    }

    public void setEdgeDebugging(String edgeDebuggingStr) {
        if(edgeDebuggingStr == null || edgeDebuggingStr.length() == 0) {
            isEdgeDebuggingResolved = true;
        } else {
            this.edgeDebuggingStr = edgeDebuggingStr;
            isEdgeDebuggingResolved = false;
        }
    }

    /**
     * @param debug
     * @return <b>true</b> if is debug on; else <b>false</b>
     */
    public boolean isEdgeDebugging() {
        if(!isEdgeDebuggingResolved) {
            PropertyRefResolver prr = new PropertyRefResolver(getGraphParameters());
            edgeDebugging = Boolean.valueOf(prr.resolveRef(edgeDebuggingStr)).booleanValue();
            isEdgeDebuggingResolved = true;
        }
        
        if (edgeDebugging) {
	        if (watchDog != null) {
	        	//edge debugging is disabled for non-persistent graphs
	        	return watchDog.getGraphRuntimeContext().getRunId() > 0
	        			&& watchDog.getGraphRuntimeContext().isEdgeDebugging();
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
     * For example *.sgrf file executed as root job, has static job type {@link JobType#SUBGRAPH},
     * but runtime jobtype is {@link JobType#ETL_GRAPH}.
     */
    @Override
    public JobType getRuntimeJobType() {
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
	 * @param metadataId The ID under which dataRecordMetadata has been registered with graph
	 * @return
	 */
	public DataRecordMetadata getDataRecordMetadata(String metadataId) {
		return getDataRecordMetadata(metadataId, true);
	}
	
	public DataRecordMetadata getDataRecordMetadata(String metadataId, boolean forceFromStub) {
		Object metadata = dataRecordMetadata.get(metadataId);
		if (metadata != null && metadata instanceof DataRecordMetadataStub) {
			if (forceFromStub) {
				try {
					metadata = ((DataRecordMetadataStub)metadata).createMetadata();
					dataRecordMetadata.put(metadataId, (DataRecordMetadata) metadata);
				} catch (UnsupportedOperationException e) {
					throw new JetelRuntimeException("Creating metadata '" + metadataId + "' from stub not defined for this connection: ", e);
				} catch (Exception e) {
					throw new JetelRuntimeException("Creating metadata '" + metadataId + "' from stub failed: ", e);
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
	 * Returns ID of metadata with given name.
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
     * Return array of Phases defined within graph sorted (ascending)
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
			// emit init event for debugger
			if (getDebugJMX() != null) {
				getDebugJMX().notifyInit();
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
					//Lookup JNDI connection in original classloader
					if (connection instanceof DBConnection) {
						((DBConnection)connection).lookupJndiConnection();
					}
				}
			}

			// initialize sequences
			for (Sequence sequence : sequences.values()) {
				logger.info("Initializing sequence:");
				ClassLoader formerClassLoader = Thread.currentThread().getContextClassLoader();
				try {
					Thread.currentThread().setContextClassLoader(sequence.getClass().getClassLoader());
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

		//check all required graph parameters whether the values have been passed from executor
		validateRequiredGraphParameters();
		
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
		
		//output edges from SubgraphInput component and input edges to SubgraphOutput component
		//can be shared with parent graph if possible
		shareSubgraphInputEdges();
		shareSubgraphOutputEdges();

		//print out types of all edges
		printEdgesInfo();
	}
	
	/**
	 * This method tries to detect whether output edges from SubgraphInput component
	 * can be shared with parent graph.
	 */
	private void shareSubgraphInputEdges() {
		//update output edges of SubgraphInput component - can we share edge base from parent graph with local output edge?
		if (getGraph().getRuntimeJobType().isSubJob()) {
			Node subgraphInput = SubgraphUtils.getSubgraphInput(this);
			if (subgraphInput != null) { //subgraph input component can be null for clustered subgraphs, where only one partition has SGI and SGO components
				for (OutputPort outputPort : subgraphInput.getOutPorts()) {
					//edge from this graph
					Edge outputEdge = outputPort.getEdge();
					//corresponding edge from parent graph
					Edge parentEdge = getGraph().getAuthorityProxy().getParentGraphSourceEdge(outputPort.getOutputPortNumber());
					//is it possible to share edge base between these two edges?
					if (parentEdge != null && SubgraphUtils.isSubgraphInputEdgeShared(outputEdge, parentEdge)) {
						//let's share the edge base
						outputEdge.setEdge(parentEdge.getEdgeBase());
						outputEdge.setSharedEdgeBaseFromWriter(true);
					}
				}
			}
		}
	}

	/**
	 * This method tries to detect whether input edges from SubgraphOutput component
	 * can be shared with parent graph.
	 */
	private void shareSubgraphOutputEdges() {
        //update input edges of SubgraphOutput component - can we share edge base from parent graph with local input edge?
		if (getGraph().getRuntimeJobType().isSubJob()) {
			Node subgraphOutput = SubgraphUtils.getSubgraphOutput(this);
			if (subgraphOutput != null) { //subgraph output component can be null for clustered subgraphs, where only one partition has SGI and SGO components
				for (InputPort inputPort : subgraphOutput.getInPorts()) {
					//edge from this graph
					Edge inputEdge = inputPort.getEdge();
					//corresponding edge from parent graph
					Edge parentEdge = getGraph().getAuthorityProxy().getParentGraphTargetEdge(inputPort.getInputPortNumber());
					//is it possible to share edge base between these two edges?
					if (parentEdge != null && SubgraphUtils.isSubgraphOutputEdgeShared(inputEdge, parentEdge)) {
						//let's share the edge base
						inputEdge.setEdge(parentEdge.getEdgeBase());
						inputEdge.setSharedEdgeBaseFromReader(true);
					}
				}
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
		
		// must be the last step of postExecute, since it calls close of graph's classLoaders
		super.postExecute();
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
				seq.free();
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
		if (phases.put(phase.getPhaseNum(), phase) != null){
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
			logger.info("--- Phase [" + phase.getLabel() + "] ---");
			logger.info("\t... nodes ...");
			for (Node node : phase.getNodes().values()) {
				logger.info("\t" + node.getId() + " : " + (node.getName() !=null ? node.getName() : "") + " phase: " + node.getPhase().getLabel());
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
			super.free();
			
	        freeResources();
	    	
	    	//free dictionary /some readers use dictionary in the free method for the incremental reading
	    	dictionary.free();
	    	
	    	if (debugJMX != null) {
	    		debugJMX.free();
	    	}
	    	
	    	if (watchDog != null) {
	    		watchDog.free();
	    		watchDog = null;
	    	}
		} finally {
			//unregister current thread from ContextProvider
			ContextProvider.unregister(c);
		}
    }
    
    void clearCache() {
    	nodesCache = null;
    	subgraphInputComponent = null;
    	subgraphOutputComponent = null;
    }
    
    /**
     * @return list of all nodes in all phases of this graph
     */
    public Map<String, Node> getNodes() {
		if (nodesCache == null) {
			nodesCache = Collections.unmodifiableMap(collectNodes());
		}
		return nodesCache;
    }
    
    private Map<String, Node> collectNodes() {
		Map<String, Node> ret = new LinkedHashMap<String, Node>();
		
		for (Phase phase : phases.values()) {
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
    
    public SubgraphPorts getSubgraphInputPorts() {
    	return subgraphInputPorts;
    }

    public SubgraphPorts getSubgraphOutputPorts() {
    	return subgraphOutputPorts;
    }

    @Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
    	super.checkConfig(status);
    	
    	// CLO-4930: catch and report RecusiveSubgraphException
    	try {
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

    	    	status.joinWith(preCheckConfigStatus);
    			
    	    	graphParameters.checkConfig(status);
    	        
    	        //check dictionary
    	        dictionary.checkConfig(status);
    	        
    	        //check connections configuration
    	        for(IConnection connection : connections.values()) {
    	        	try {
    	        		connection.checkConfig(status);
    	        	} catch (Exception e) {
    	        		status.addError(connection, null, e);
    	        	}
    	        }
    	
    	        //check lookup tables configuration
    	        for(LookupTable lookupTable : lookupTables.values()) {
    	        	try {
    	        		lookupTable.checkConfig(status);
    	        	} catch (Exception e) {
    	        		status.addError(lookupTable, null, e);
    	        	}
    	        }
    	
    	        //check sequences configuration
    	        for(Sequence sequence : sequences.values()) {
    	        	try {
    	        		sequence.checkConfig(status);
    	        	} catch (Exception e) {
    	        		status.addError(sequence, null, e);
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
    	        
    	        //only single instance of SubgraphInput and SubgraphOutput component is allowed in transformation graph
    	        List<Node> subgraphInputComponents = new ArrayList<>();
    	        List<Node> subgraphOutputComponents = new ArrayList<>();
    	        for (Node component : getNodes().values()) {
    	        	if (SubgraphUtils.isSubJobInputComponent(component.getType())) {
    	        		subgraphInputComponents.add(component);
    	        	}
    	        	if (SubgraphUtils.isSubJobOutputComponent(component.getType())) {
    	        		subgraphOutputComponents.add(component);
    	        	}
    	        }
    	        if (subgraphInputComponents.size() > 1) {
    	    		for (Node subgraphInputComponent : subgraphInputComponents) {
    	    			status.addError(subgraphInputComponent, null, "Multiple SubgraphInput component detected in the graph.");
    	    		}
    	        }
    	        if (subgraphOutputComponents.size() > 1) {
    	    		for (Node subgraphOutputComponent : subgraphOutputComponents) {
    	    			status.addError(subgraphOutputComponent, null, "Multiple SubgraphOutput component detected in the graph.");
    	    		}
    	        }
    	        
    	        return status;
    		} finally {
    			//unregister current thread from ContextProvider
    			ContextProvider.unregister(c);
    		}
    	} catch (RecursiveSubgraphException ex) {
    		// CLO-4930:
	    	if (status == null) {
	            status = new ConfigurationStatus();
	        }
    		status.addError(ex.getNode(), SubgraphUtils.XML_JOB_URL_ATTRIBUTE, ex);
    		return status;
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
    
    public DebugJMX getDebugJMX() {
    	return this.debugJMX;
    }
    
    public void setDebugJMX(DebugJMX debugJMX) {
    	this.debugJMX = debugJMX;
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
    
	/**
	 * @return logger for this transformation graph
	 * @deprecated use {@link #getLog()} instead
	 */
    @Deprecated
	public Log getLogger() {
		return TransformationGraph.logger;
	}
    
	/**
	 * @return timestamp when this class instance was created (obtained from {@link System#nanoTime()}) 
	 */
	public long getInstanceCreated() {
		return instanceCreated;
	}
	
	public Dictionary getDictionary() {
		return dictionary;
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

	/**
	 * Checks whether values for all required graph parameters have been passed from executor of this graph. 
	 */
	private void validateRequiredGraphParameters() {
		if (getRuntimeContext().isValidateRequiredParameters()) {
			for (GraphParameter graphParameter : getGraphParameters().getAllGraphParameters()) {
				if (graphParameter.isPublic() && graphParameter.isRequired()
						&& !getRuntimeContext().getAdditionalProperties().containsKey(graphParameter.getName())) {
					throw new JetelRuntimeException("Required graph parameter '" + graphParameter.getName() + "' is not specified.");
				}
			}
		}
	}
	
	public String getAuthor() {
		return author;
	}

	public void setAuthor(String author) {
		this.author = author;
	}

	public String getCreated() {
		return created;
	}

	public void setCreated(String created) {
		this.created = created;
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
	
	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	/**
	 * @return result of automatic metadata propagation
	 */
	public MetadataPropagationResult getMetadataPropagationResult() {
		return metadataPropagationResult;
	}

	/**
	 * Sets result of automatic metadata propagation.
	 * @param metadataPropagationResolver result object of automatic metadata propagation
	 */
	public void setMetadataPropagationResult(MetadataPropagationResult metadataPropagationResult) {
		this.metadataPropagationResult = metadataPropagationResult;
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

	/**
	 * Execution label is human-readable text which describes this graph execution.
	 * Can be parametrised by graph parameters.
	 * @return resolved execution label for this graph instance
	 */
	public String getExecutionLabel() {
		return getPropertyRefResolver().resolveRef(executionLabel, RefResFlag.SPEC_CHARACTERS_OFF);
	}

	/**
	 * Sets human-readable description of execution of this graph.
	 * @param executionLabel
	 */
	public void setExecutionLabel(String executionLabel) {
		this.executionLabel = executionLabel;
	}

	public String getCategory() {
		return getPropertyRefResolver().resolveRef(category, RefResFlag.SPEC_CHARACTERS_OFF);
	}

	public void setCategory(String category) {
		this.category = category;
	}

	public String getSmallIconPath() {
		return getPropertyRefResolver().resolveRef(smallIconPath, RefResFlag.SPEC_CHARACTERS_OFF);
	}

	public void setSmallIconPath(String smallIconPath) {
		this.smallIconPath = smallIconPath;
	}

	public String getMediumIconPath() {
		return getPropertyRefResolver().resolveRef(mediumIconPath, RefResFlag.SPEC_CHARACTERS_OFF);
	}

	public void setMediumIconPath(String mediumIconPath) {
		this.mediumIconPath = mediumIconPath;
	}

	public String getLargeIconPath() {
		return getPropertyRefResolver().resolveRef(largeIconPath, RefResFlag.SPEC_CHARACTERS_OFF);
	}

	public void setLargeIconPath(String largeIconPath) {
		this.largeIconPath = largeIconPath;
	}

	/**
	 * @return configuration status which can be populated by graph factorisation and
	 * later will be part of {@link #checkConfig(ConfigurationStatus)} result
	 */
	public ConfigurationStatus getPreCheckConfigStatus() {
		return preCheckConfigStatus;
	}

	@Override
	public String toString() {
		return getId() + ":" + getRuntimeContext().getRunId();
	}

	/**
	 * @return reference to SubgraphInput component in this graph if any
	 */
	public Node getSubgraphInputComponent() {
		if (subgraphInputComponent == null) {
			for (Node component : getNodes().values()) {
				if (SubgraphUtils.isSubJobInputComponent(component.getType())) {
					subgraphInputComponent = component;
					break;
				}
			}
			if (subgraphInputComponent == null) {
				throw new JetelRuntimeException("SubgraphInput component is not available.");
			}
		}
		return subgraphInputComponent;
	}

	/**
	 * @return reference to SubgraphOutput component in this graph if any
	 */
	public Node getSubgraphOutputComponent() {
		if (subgraphOutputComponent == null) {
			for (Node component : getNodes().values()) {
				if (SubgraphUtils.isSubJobOutputComponent(component.getType())) {
					subgraphOutputComponent = component;
					break;
				}
			}
			if (subgraphOutputComponent == null) {
				throw new JetelRuntimeException("SubgraphOutput component is not available.");
			}
		}
		return subgraphOutputComponent;
	}

	/**
	 * @return the rawComponentEnabledAttribute
	 */
	public Map<Node, String> getRawComponentEnabledAttribute() {
		return rawComponentEnabledAttribute;
	}

	/**
	 * @param rawComponentEnabledAttribute the rawComponentEnabledAttribute to set
	 */
	public void setRawComponentEnabledAttribute(Map<Node, String> rawComponentEnabledAttribute) {
		this.rawComponentEnabledAttribute = rawComponentEnabledAttribute;
	}
	
	/**
	 * 
	 * @return Map with a set of blocked nodes for each blocker component.
	 */
	public Map<Node, Set<Node>> getBlockingComponentsInfo() {
		return blockingComponents;
	}

	public void setBlockingComponentsInfo(Map<Node, Set<Node>> blockingComponents) {
		this.blockingComponents = blockingComponents;
	}
	
	/**
	 * @return Set of IDs of blocked components. The info is gathered during graph analysis in {@link #computeBlockedComponents()}.
	 */
	public Set<String> getBlockedIDs() {
		Set<String> blocked = new HashSet<>();
		
		for (Entry<Node, Set<Node>> blockerInfo : blockingComponents.entrySet()) {
			for (Node blockedComponent : blockerInfo.getValue()) {
				blocked.add(blockedComponent.getId());
			}
		}
		
		return blocked;
	}
	
	public Set<Node> getKeptBlockedComponents() {
		return keptBlocked;
	}

	public EndpointSettings getEndpointSettings() {
		return endpointSettings;
	}

	public void setEndpointSettings(EndpointSettings endpointSettings) {
		this.endpointSettings = endpointSettings;
	}

	public RestJobResponseStatus getRestJobResponseStatus() {
		return responseStatus;
	}

	public void setRestJobResponseStatus(RestJobResponseStatus responseStatus) {
		this.responseStatus = responseStatus;
	}
	
	public String getOutputFormat() {
		return outputFormat;
	}

	public void setOutputFormat(String outputFormat) {
		this.outputFormat = outputFormat;
	}
	
	public Node getRestJobOutputComponent() {
		for (Node component : getNodes().values()) {
			if (RestJobUtils.isRestJobOutputComponent(component.getType())) {
				return component;
			}
		}
		throw new JetelRuntimeException("RestJobOutput component is not available.");
	}

	public String getUniqueNodeId(String nodeType) {
		String defaultNodeId = nodeType + "0";
		return getUniqueId(defaultNodeId, getNodes());
	}

	public String getUniqueEdgeId() {
		return getUniqueId(DEFAULT_EDGE_ID, getEdges());
	}
}
