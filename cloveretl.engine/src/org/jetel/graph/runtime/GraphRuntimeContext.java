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
package org.jetel.graph.runtime;

import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.log4j.Level;
import org.jetel.component.MetadataProvider;
import org.jetel.ctl.debug.Breakpoint;
import org.jetel.data.Defaults;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.graph.IGraphElement;
import org.jetel.graph.JobType;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.TransformationGraphXMLReaderWriter;
import org.jetel.graph.dictionary.DictionaryValuesContainer;
import org.jetel.util.MiscUtils;
import org.jetel.util.file.FileUtils;
import org.jetel.util.string.StringUtils;

/**
 * Common used implementation of IGraphRuntimeContext interface.
 * 
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created 27.11.2007
 */
public class GraphRuntimeContext implements Serializable {

	private static final long serialVersionUID = 2613877772912214702L;
	
	public static final boolean DEFAULT_VERBOSE_MODE = false;
	public static final boolean DEFAULT_WAIT_FOR_JMX_CLIENT = false;
	public static final boolean DEFAULT_USE_JMX = true;
	public static final boolean DEFAULT_EDGE_DEBUGGING = true;
	public static final boolean DEFAULT_SKIP_CHECK_CONFIG = false;
	public static final boolean DEFAULT_SYNCHRONIZED_RUN = false;
	public static final boolean DEFAULT_TRANSACTION_MODE = false;
	public static final boolean DEFAULT_BATCH_MODE = true;
	public static final boolean DEFAULT_TOKEN_TRACKING = true;
	public static final boolean DEFAULT_VALIDATE_REQUIRED_PARAMETERS = true;
	private static final boolean DEFAULT_EMBEDDED_RUN = true;
	
	private long runId;
	private Long parentRunId;
	private String executionGroup;
	private String executionLabel;
	private boolean daemon;
	private String logLocation;
	private Level logLevel;
	private int trackingInterval;
//	private int trackingFlushInterval;
	private boolean useJMX;
	private boolean waitForJMXClient;
	private boolean verboseMode;
	private Properties additionalProperties;
	private boolean skipCheckConfig;
	private String password;
	private boolean edgeDebugging;
	private String debugDirectory;
	private boolean tokenTracking;
	private String timeZone;
	private String locale;
	private boolean ctlDebug;
	private volatile boolean ctlBreakpointsEnabled = true;
	private volatile boolean suspendThreads = false;
	private final Set<Breakpoint> ctlBreakpoints = new CopyOnWriteArraySet<>();
	
	/**
	 * Whether the execution must happen on worker in multi-jvm environment.
	 */
	private Boolean workerExecution;
	
	/**
	 * Whether the execution must happen on the same JVM as parent execution.
	 * Only valid for non-root executions.
	 */
	private boolean forceParentJvm;
	
	/**
	 * Default multi-thread execution is managed by {@link WatchDog}.
	 * Single thread execution is managed by {@link SingleThreadWatchDog}. 
	 */
	private ExecutionType executionType;
	
	/**
	 * This classpath is extension of 'current' classpath used for loading extra classes specified inside the graph.
	 * External transformations for Reformat components, java classes for ExecuteJava components, ...
	 */
	private URL[] runtimeClassPath;
	/**
	 * This classpath is extension of 'current' classpath used in compile time of java code specified inside the graph.
	 */
	private URL[] compileClassPath;
	private boolean synchronizedRun;
	private boolean transactionMode;
	private boolean batchMode;
	private boolean embeddedRun;
	private transient URL contextURL;
	/**
	 * This string representation of contextURL is necessary to keep GraphRuntimeContext
	 * serializable, since contextURL is URL with sandbox protocol, which is not possible to deserialize,
	 * because the respective URL handler (SandboxStreamHandler) cannot be correctly registered.
	 */
	private String contextURLString; 
	private DictionaryValuesContainer dictionaryContent;
	/** Hint for the server environment where to execute the graph */
	private String clusterNodeId;
	private transient ClassLoader classLoader;
	private JobType jobType;
	private String jobUrl;
	/** Only for subgraphs - component id, where this subgraph has been executed. */
	private String parentSubgraphComponentId;
	private transient IAuthorityProxy authorityProxy;
	private MetadataProvider metadataProvider;
	/** Should executor check required graph parameters? */
	private boolean validateRequiredParameters;
	/** Only subgraphs can be executed in fast-propagated mode. This flag indicates,
	 * the Subgraph is in a loop, so all edges between SGI and SGO components has to be
	 * fast-propagated. */
	private boolean fastPropagateExecution;
	/**
	 * This list contains indexes of all input ports of Subgraph component from parent graph
	 * which have an edge connected. It has sense only for subgraphs. This information about
	 * attached edges in parent graph is used to remove optional edges.
	 */
	private List<Integer> parentGraphInputPortsConnected; 
	/**
	 * This list contains indexes of all output ports of Subgraph component from parent graph
	 * which have an edge connected. It has sense only for subgraphs. This information about
	 * attached edges in parent graph is used to remove optional edges.
	 */
	private List<Integer> parentGraphOutputPortsConnected; 
	
	/**
	 * This flag can be used to decide, whether some flaws in graph xml file should be reported or somehow ignored.
	 * See, {@link TransformationGraphXMLReaderWriter#setStrictParsing(boolean)}. 
	 */
	private boolean strictGraphFactorization;
	
	/**
	 * Flag which indicates, whether new classloaders should be created for each transformation
	 * component or should be shared with the others.
	 * @see IAuthorityProxy#createClassLoader(URL[], ClassLoader, boolean)
	 * @see IAuthorityProxy#getClassLoader(URL[], ClassLoader, boolean)
	 */
	private boolean classLoaderCaching;
	
	/**
	 * For all edges is also calculated which metadata would be propagated
	 * to this edge from neighbours, if the edge does not have any metadata directly assigned.
	 * It is useful only for designer purpose (calculation is turned off by default).
	 * Designer shows to user, which metadata would be on the edge, for "no metadata" option on the edge. 
	 */
	private boolean calculateNoMetadata;
	
	public GraphRuntimeContext() {
		trackingInterval = Defaults.WatchDog.DEFAULT_WATCHDOG_TRACKING_INTERVAL;
		useJMX = DEFAULT_USE_JMX;
		waitForJMXClient = DEFAULT_WAIT_FOR_JMX_CLIENT;
		verboseMode = DEFAULT_VERBOSE_MODE;
		additionalProperties = new Properties();
		skipCheckConfig = DEFAULT_SKIP_CHECK_CONFIG;
		edgeDebugging = DEFAULT_EDGE_DEBUGGING;
		synchronizedRun = DEFAULT_SYNCHRONIZED_RUN;
		transactionMode = DEFAULT_TRANSACTION_MODE;
		batchMode = DEFAULT_BATCH_MODE;
		tokenTracking = DEFAULT_TOKEN_TRACKING;
		embeddedRun = DEFAULT_EMBEDDED_RUN;
		runtimeClassPath = new URL[0];
		compileClassPath = new URL[0];
		dictionaryContent = new DictionaryValuesContainer();
		clusterNodeId = null;
		jobType = JobType.DEFAULT;
		locale = null;
		timeZone = null;
		validateRequiredParameters = DEFAULT_VALIDATE_REQUIRED_PARAMETERS;
		fastPropagateExecution = false;
		parentGraphInputPortsConnected = null; 
		parentGraphOutputPortsConnected = null; 
		strictGraphFactorization = true;
		classLoaderCaching = false;
		calculateNoMetadata = false;
		workerExecution = null;
		forceParentJvm = false;
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.IGraphRuntimeContext#createCopy()
	 */
	public GraphRuntimeContext createCopy() {
		GraphRuntimeContext ret = new GraphRuntimeContext();
		
		ret.runId = runId;
		ret.parentRunId = parentRunId;
		ret.additionalProperties = new Properties();
		ret.additionalProperties.putAll(getAdditionalProperties());
		ret.logLevel = getLogLevel();
		ret.trackingInterval = getTrackingInterval();
		ret.skipCheckConfig = isSkipCheckConfig();
		ret.verboseMode = isVerboseMode();
		ret.workerExecution = isWorkerExecution();
		ret.forceParentJvm = isForceParentJvm();
		ret.useJMX = useJMX();
		ret.waitForJMXClient = isWaitForJMXClient();
		ret.password = getPassword();
		ret.edgeDebugging = isEdgeDebugging();
		ret.debugDirectory = getDebugDirectory();
		ret.runtimeClassPath = getRuntimeClassPath();
		ret.compileClassPath = getCompileClassPath();
		ret.synchronizedRun = isSynchronizedRun();
		ret.transactionMode = isTransactionMode();
		ret.batchMode = isBatchMode();
		ret.contextURL = this.contextURL;
		ret.contextURLString = this.contextURLString;
		ret.dictionaryContent = DictionaryValuesContainer.duplicate(getDictionaryContent());
		ret.executionGroup = getExecutionGroup();
		ret.executionLabel = getExecutionLabel();
		ret.daemon = daemon;
		ret.clusterNodeId = clusterNodeId;
		ret.classLoader = getClassLoader();
		ret.jobType = getJobType();
		ret.jobUrl = getJobUrl();
		ret.parentSubgraphComponentId = getParentSubgraphComponentId();
		ret.authorityProxy = getAuthorityProxy();
		ret.executionType = getExecutionType();
		ret.metadataProvider = getMetadataProvider();
		ret.validateRequiredParameters = isValidateRequiredParameters();
		ret.fastPropagateExecution = isFastPropagateExecution();
		ret.parentGraphInputPortsConnected = parentGraphInputPortsConnected != null ? new ArrayList<>(parentGraphInputPortsConnected) : null;
		ret.parentGraphOutputPortsConnected = parentGraphOutputPortsConnected != null ? new ArrayList<>(parentGraphOutputPortsConnected) : null;
		ret.strictGraphFactorization = isStrictGraphFactorization();
		ret.classLoaderCaching = isClassLoaderCaching();
		ret.calculateNoMetadata = isCalculateNoMetadata();
		ret.ctlDebug = isCtlDebug();
		ret.ctlBreakpointsEnabled = isCtlBreakpointsEnabled();
		ret.ctlBreakpoints.addAll(ctlBreakpoints);

		return ret;
	}

	public Properties getAllProperties() {
		Properties prop = new Properties();
		
		prop.setProperty("runId", Long.toString(getRunId()));
		prop.setProperty("parentRunId", String.valueOf(getParentRunId()));
		prop.setProperty("additionProperties", String.valueOf(getAdditionalProperties()));
		prop.setProperty("logLevel", String.valueOf(getLogLevel()));
		prop.setProperty("trackingInterval", Integer.toString(getTrackingInterval()));
		prop.setProperty(PropertyKey.SKIP_CHECK_CONFIG.getKey(), Boolean.toString(isSkipCheckConfig()));
		prop.setProperty("verboseMode", Boolean.toString(isVerboseMode()));
		prop.setProperty("workerExecution", String.valueOf(isWorkerExecution()));
		prop.setProperty("forceParentJvm", Boolean.toString(isForceParentJvm()));
		prop.setProperty("useJMX", Boolean.toString(useJMX()));
		prop.setProperty("waitForJMXClient", Boolean.toString(isWaitForJMXClient()));
		prop.setProperty("password", String.valueOf(getPassword()));
		prop.setProperty("edgeDebugging", Boolean.toString(isEdgeDebugging()));
		prop.setProperty("debugDirectory", String.valueOf(getDebugDirectory()));
		prop.setProperty("runtimeClassPath", Arrays.toString(getRuntimeClassPath()));
		prop.setProperty("compileClassPath", Arrays.toString(getCompileClassPath()));
		prop.setProperty("synchronizedRun", Boolean.toString(isSynchronizedRun()));
		prop.setProperty("transactionMode", Boolean.toString(isTransactionMode()));
		prop.setProperty("batchMode", Boolean.toString(isBatchMode()));
		prop.setProperty("contextURL", String.valueOf(getContextURL()));
		prop.setProperty("dictionaryContent", String.valueOf(getDictionaryContent()));
		prop.setProperty("executionGroup", String.valueOf(getExecutionGroup()));
		prop.setProperty("executionLabel", String.valueOf(getExecutionLabel()));
		prop.setProperty("deamon", Boolean.toString(isDaemon()));
		prop.setProperty("clusterNodeId", String.valueOf(getClusterNodeId()));
		prop.setProperty("jobType", String.valueOf(getJobType()));
		prop.setProperty("jobUrl", String.valueOf(getJobUrl()));
		prop.setProperty("executionType", String.valueOf(getExecutionType()));
		prop.setProperty("validateRequiredParameters", Boolean.toString(isValidateRequiredParameters()));
		prop.setProperty("fastPropagateExecution", Boolean.toString(isFastPropagateExecution()));
		prop.setProperty("parentGraphInputPortsConnected", String.valueOf(getParentGraphInputPortsConnected()));
		prop.setProperty("parentGraphOutputPortsConnected", String.valueOf(getParentGraphOutputPortsConnected()));
		prop.setProperty("strictGraphFactorization", Boolean.toString(isStrictGraphFactorization()));
		prop.setProperty("classLoaderCaching", Boolean.toString(isClassLoaderCaching()));
		prop.setProperty("calculateNoMetadata", Boolean.toString(isCalculateNoMetadata()));

		return prop;
	}

	@Override
	public String toString() {
		return "runtimeContext["+getAuthorityProxy()+" "+getAllProperties()+"]";
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.IGraphRuntimeContext#getTrackingInterval()
	 */
	public int getTrackingInterval() {
		return trackingInterval;
	}

	/**
	 * Sets tracking interval for WatchDog's JMX.
	 * @param trackingInterval
	 */
	public void setTrackingInterval(int trackingInterval) {
		this.trackingInterval = trackingInterval;
	}

//	public String getGraphName() {
//		try {
//			URL url = FileUtils.getFileURL(getProjectURL(), graphFileURL);
//			return new File(url.getPath()).getName();
//		} catch (MalformedURLException ex) {
//			// do nothing, return null
//			return null;
//		}
//	}
	
    /* (non-Javadoc)
     * @see org.jetel.graph.runtime.IGraphRuntimeContext#useJMX()
     */
    public boolean useJMX() {
        return useJMX;
    }

    /**
     * Sets whether JMX should be used during graph processing.
     * @param useJMX
     */
    public void setUseJMX(boolean useJMX) {
        this.useJMX = useJMX;
    }

    /**
     * @return graph run debug mode
     */
    public boolean isEdgeDebugging() {
        return edgeDebugging;
    }

    /**
     * Sets whether graph should run in debug mode.
     * Debug mode for example can turn off debugging on all edges. 
     * @param debugMode
     */
    public void setEdgeDebugging(boolean edgeDebugging) {
        this.edgeDebugging = edgeDebugging;
    }

    /**
     * @return temporary directory for graph debugging (e.g edge debugging)
     */
    public String getDebugDirectory() {
    	if (!StringUtils.isEmpty(debugDirectory)) {
            return debugDirectory;
    	} else {
    		return System.getProperty("java.io.tmpdir");
    	}
    }

    /**
     * Sets temporary directory for graph debugging (e.g edge debugging)
     * @param debugDirectory
     */
    public void setDebugDirectory(String debugDirectory) {
        this.debugDirectory = debugDirectory;
    }

    /**
     * @return whether watchdog should wait for a JMX client; it is necessary for short running graphs
     */
    public boolean isWaitForJMXClient() {
    	return waitForJMXClient;
    }
    
    /**
     * Sets whether watchdog should wait for a JMX client. It is necessary for short running graphs.
     * @param waitForJMXClient
     */
    public void setWaitForJMXClient(boolean waitForJMXClient) {
    	this.waitForJMXClient = waitForJMXClient;
    }
    
    /**
     * Sets whether should be checked graph configuration. (TransformationGraph.checkConfig())
     * @param checkConfig
     */
    public void setSkipCheckConfig(boolean skipCheckConfig) {
    	this.skipCheckConfig = skipCheckConfig;
    }

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.IGraphRuntimeContext#isCheckConfig()
	 */
	public boolean isSkipCheckConfig() {
		return skipCheckConfig;
	}

	/**
	 * Sets whether graph will be processed in so called "verbose mode".
	 * @param verboseMode
	 */
	public void setVerboseMode(boolean verboseMode) {
		this.verboseMode = verboseMode;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.IGraphRuntimeContext#isVerboseMode()
	 */
	public boolean isVerboseMode() {
		return verboseMode;
	}
	
	/**
	 * Sets whether graph should run on a worker.
	 * @param workerExecution
	 */
	public void setWorkerExecution(Boolean workerExecution) {
		this.workerExecution = workerExecution;
	}
	
	public Boolean isWorkerExecution() {
		return workerExecution;
	}

	public boolean isForceParentJvm() {
		return forceParentJvm;
	}

	/**
	 * Sets whether graph must run on the same jvm as parent graph.
	 * @param parentJvm
	 */
	public void setForceParentJvm(boolean parentJvm) {
		this.forceParentJvm = parentJvm;
	}

	/**
	 * Adds new additional property (key-value pair).
	 * @param key
	 * @param value
	 */
	public void addAdditionalProperty(String key, String value) {
		additionalProperties.setProperty(key, value);
	}

	/**
	 * Adds new collection of additional properties.
	 * @param properties
	 */
	public void addAdditionalProperties(Properties properties) {
		additionalProperties.putAll(properties);
	}
	
	/**
	 * Additional properties will be replaced by new set of properties.
	 * @param properties
	 */
	public void setAdditionalProperties(Properties properties) {
		additionalProperties.clear();
		if (properties != null) {
			additionalProperties.putAll(properties);
		}
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.IGraphRuntimeContext#getAdditionalProperties()
	 */
	public Properties getAdditionalProperties() {
		return additionalProperties;
	}

	/**
	 * Sets password for decryption of connection's passwords.
	 * @param password
	 */
	public void setPassword(String password) {
		this.password = password;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.IGraphRuntimeContext#getPassword()
	 */
	public String getPassword() {
		return password;
	}

	/**
	 * @return the runId
	 */
	public long getRunId() {
		return runId;
	}

	/**
	 * @param runId
	 */
	public void setRunId(long runId) {
		this.runId = runId;
	}

	/**
	 * @return runId of parent graph or null if the graph is on top level of execution tree
	 */
	public Long getParentRunId() {
		return parentRunId;
	}

	/**
	 * @param runId runId of parent graph or null if the graph is on top level of execution tree
	 */
	public void setParentRunId(Long parentRunId) {
		this.parentRunId = parentRunId;
	}

	/**
	 * @return logLocation
	 */
	public String getLogLocation() {
		return logLocation;
	}

	/**
	 * Sets location and name of output log file.
	 * @param logLocation to set
	 */
	public void setLogLocation(String logLocation) {
		this.logLocation = logLocation;
	}

	/**
	 * @return logLevel
	 */
	public Level getLogLevel() {
		return logLevel;
	}

	/**
	 * @param logLevel the logLevel to set
	 */
	public void setLogLevel(Level logLevel) {
		this.logLevel = logLevel;
	}

	/** 
	 * This classpath is extension of 'current' classpath used for loading extra classes specified inside the graph.
	 * Each component with transformation class specified (attribute transformClass or generatorClass etc.) will use this class-paths to find it.
	 */
	public URL[] getRuntimeClassPath() {
		return Arrays.copyOf(runtimeClassPath, runtimeClassPath.length);
	}

	/**
	 * @link {@link #getRuntimeClassPath()}
	 */
	public void setRuntimeClassPath(URL[] runtimeClassPath) {
		if (runtimeClassPath != null) {
			this.runtimeClassPath = runtimeClassPath;
		} else {
			this.runtimeClassPath = new URL[0];
		}
	}

	/** 
	 * This classpath is extension of 'current' classpath used in compile time of java code specified inside the graph.
	 */
	public URL[] getCompileClassPath() {
		return Arrays.copyOf(compileClassPath, compileClassPath.length);
	}

	/**
	 * @link {@link #getCompileClassPath()}
	 */
	public void setCompileClassPath(URL[] compileClassPath) {
		if (compileClassPath != null) {
			this.compileClassPath = compileClassPath;
		} else {
			this.compileClassPath = new URL[0];
		}
	}

	/**
	 * @return classpath container for both for runtime classpath and for compile classpath
	 * @see CloverClassPath
	 */
	public CloverClassPath getClassPath() {
		return new CloverClassPath(getRuntimeClassPath(), getCompileClassPath());
	}
	
	/**
	 * 'Synchronized' mode currently means that the watchdog 
	 * between phases waits for an JMX event, which allows next graph processing.
	 * Future interpretation could be different.
	 * @return true if graph is running in 'synchronized mode'
	 */
	public boolean isSynchronizedRun() {
		return synchronizedRun;
	}

	public void setSynchronizedRun(boolean synchronizedRun) {
		this.synchronizedRun = synchronizedRun;
	}

	/**
	 * Transaction mode means that all graph elements should not affect none of their output resources
	 * until postExecute with COMMIT statement is invoked.
	 * 
	 * @see IGraphElement#postExecute(org.jetel.graph.TransactionMethod)
	 * @see TransactionMethod
	 * @return
	 */
	public boolean isTransactionMode() {
		return transactionMode;
	}

	public void setTransactionMode(boolean transactionMode) {
		this.transactionMode = transactionMode;
	}

	public boolean isBatchMode() {
		return batchMode;
	}

	public void setBatchMode(boolean batchMode) {
		this.batchMode = batchMode;
	}
	
	/**
	 * Returns <code>true</code> if jobflow token tracking is enabled.
	 * 
	 * Enabled by default.
	 * 
	 * @return <code>true</code> if token tracking is enabled
	 */
    public boolean isTokenTracking() {
		return tokenTracking;
	}

	/**
	 * Enables or disables jobflow token tracking.
	 * 
	 * Enabled by default.
	 * 
	 * @param tokenTracking
	 */
	public void setTokenTracking(boolean tokenTracking) {
		this.tokenTracking = tokenTracking;
	}

    public URL getContextURL() {
    	if (contextURL != null) {
    		return contextURL;
    	} else if (contextURLString != null) {
    		try {
				contextURL = FileUtils.getFileURL(contextURLString);
			} catch (MalformedURLException e) {
				throw new JetelRuntimeException(e);
			}
    		return contextURL;
    	} else {
    		return null;
    	}
    }

    public void setContextURL(URL contextURL) {
    	this.contextURL = contextURL;
    	this.contextURLString = contextURL != null ? contextURL.toString() : null;
    }

    public void setContextURL(String contextURLString) {
    	this.contextURLString = contextURLString;
    }

	/**
	 * Sets a class loader to be used for loading classes used in a graph
	 * 
	 * @param classLoader
	 */
	public synchronized void setClassLoader(ClassLoader classLoader) {
		this.classLoader = classLoader;
	}
    
    public DictionaryValuesContainer getDictionaryContent() {
    	return dictionaryContent;
    }
    
    public void setDictionaryContent(DictionaryValuesContainer dictionaryContent) {
    	if (dictionaryContent == null) {
    		this.dictionaryContent.clear();
    	} else {
    		this.dictionaryContent = dictionaryContent.duplicate();
    	}
    }

	/**
	 * @return the executionGroup
	 */
	public String getExecutionGroup() {
		return executionGroup;
	}

	/**
	 * @param executionGroup the executionGroup to set
	 */
	public void setExecutionGroup(String executionGroup) {
		this.executionGroup = executionGroup;
	}

	/**
	 * @return human-readable identification of this graph execution
	 */
	public String getExecutionLabel() {
		return executionLabel;
	}

	/**
	 * @param executionLabel human-readable identification of this graph execution
	 */
	public void setExecutionLabel(String executionLabel) {
		this.executionLabel = executionLabel;
	}

	/**
	 * @return the daemon
	 */
	public boolean isDaemon() {
		return daemon;
	}

	/**
	 * @param daemon the daemon to set
	 */
	public void setDaemon(boolean daemon) {
		this.daemon = daemon;
	}

	/**
	 * @return the clusterNodeId
	 */
	public String getClusterNodeId() {
		return clusterNodeId;
	}

	/**
	 * @param clusterNodeId the clusterNodeId to set
	 */
	public void setClusterNodeId(String clusterNodeId) {
		this.clusterNodeId = clusterNodeId;
	}
	
	/**
	 * Returns a class loader to be used for loading classes used in a graph
	 * 
	 * @return
	 */
	public synchronized ClassLoader getClassLoader() {
		return classLoader;
	}

	/**
	 * Expected job type is confronted with type defined in {@link TransformationGraph#getJobType()}.
	 * @return expected job type of executed graph
	 * @see TransformationGraph#checkConfig(org.jetel.exception.ConfigurationStatus)
	 */
	public JobType getJobType() {
		return jobType;
	}

	/**
	 * Sets expected job type. 
	 * @param jobType the jobType to set
	 * @see #getJobType()
	 */
	public void setJobType(JobType jobType) {
		this.jobType = jobType;
	}
	
	/**
	 * Sets the URL of this job (graph)
	 * @return the jobUrl
	 */
	public String getJobUrl() {
		return jobUrl;
	}
	
	/**
	 * 
	 * @param jobUrl the jobUrl to set
	 */
	public void setJobUrl(String jobUrl) {
		this.jobUrl = jobUrl;
	}

	/**
	 * @return component id of Subgraph component, where this subgraph has been executed; null for non-subgraph executions
	 */
	//TODO shouldn't be part of runtime context, it is not necessary to have this information here
	//what about to move it to RuntimeEnvironment
	public String getParentSubgraphComponentId() {
		return parentSubgraphComponentId;
	}

	/**
	 * Shouldn't be set for non-subgraph execution.
	 * @param parentSubgraphComponentId component id of Subgraph component, where this subgraph has been executed 
	 */
	public void setParentSubgraphComponentId(String parentSubgraphComponentId) {
		this.parentSubgraphComponentId = parentSubgraphComponentId;
	}

	/**
	 * @return the locale
	 */
	public String getLocale() {
		return locale;
	}
	
	/**
	 * @param locale the locale to set
	 */
	public void setLocale(String locale) {
		this.locale = locale;
	}

	/**
	 * @deprecated use {@link MiscUtils#getDefaultLocaleId()} instead 
	 */
	@Deprecated
	public static String getDefaultLocale() {
		return MiscUtils.getDefaultLocaleId();
	}

	/**
	 * @return the timeZone
	 */
	public String getTimeZone() {
		return timeZone;
	}

	/**
	 * @param timeZone the timeZone to set
	 */
	public void setTimeZone(String timeZone) {
		this.timeZone = timeZone;
	}

	/**
	 * @deprecated use {@link MiscUtils#getDefaultTimeZone()} instead
	 */
	@Deprecated
	public static String getDefaultTimeZone() {
		return MiscUtils.getDefaultTimeZone();
	}

	/**
	 * @return authority proxy associated with this run
	 */
	public synchronized IAuthorityProxy getAuthorityProxy() {
		if (authorityProxy == null) {
			authorityProxy = AuthorityProxyFactory.createDefaultAuthorityProxy();
		}
		return authorityProxy;
	}
	
	/**
	 * Sets authority proxy with this run.
	 * @param authorityProxy
	 */
	public void setAuthorityProxy(IAuthorityProxy authorityProxy) {
		this.authorityProxy = authorityProxy;
		if (authorityProxy != null) {
			authorityProxy.setGraphRuntimeContext(this);
		}
	}

	/**
	 * @return type of execution - single or multi-thread execution
	 * @see SingleThreadWatchDog
	 */
	public ExecutionType getExecutionType() {
		return executionType;
	}

	/**
	 * @param executionType type of execution - single or multi-thread execution
	 */
	public void setExecutionType(ExecutionType executionType) {
		if (executionType == null) {
			throw new NullPointerException();
		}
		this.executionType = executionType;
	}

	/**
	 * @return class which provides input and output metadata of parent graph
	 */
	public MetadataProvider getMetadataProvider() {
		return metadataProvider;
	}

	/**
	 * Sets provider of input and output metadata of parent graph.
	 * @param metadataProvider parent graph metadata provider
	 */
	public void setMetadataProvider(MetadataProvider metadataProvider) {
		this.metadataProvider = metadataProvider;
	}

	/**
	 * @return true if executor should check required graph parameters
	 */
	public boolean isValidateRequiredParameters() {
		return validateRequiredParameters;
	}

	/**
	 * Sets whether executor should check required graph parameters.
	 */
	public void setValidateRequiredParameters(boolean validateRequiredParameters) {
		this.validateRequiredParameters = validateRequiredParameters;
	}
	
	/**
	 * @return returns true if the runtime server is embedded
	 */
	public boolean isEmbeddedRun() {
		return embeddedRun;
	}

	/**
	 * @param sets whether the runtime server is embedded
	 */
	public void setEmbeddedRun(boolean embeddedRun) {
		this.embeddedRun = embeddedRun;
	}

	/**
	 * @return the flag which indicates the subgraph is executed in fast-propagated mode,
	 * which is used only for subgraphs in loops, where is necessary to have all edges fast-propagating.
	 */
	public boolean isFastPropagateExecution() {
		return fastPropagateExecution;
	}
	
	/** Sets the flag which indicates the subgraph is executed in fast-propagate mode,
	 * which is used only for subgraphs in loops, where is necessary to have all edges fast-propagating.
	 * @param fastPropagateExecution
	 */
	public void setFastPropagateExecution(boolean fastPropagateExecution) {
		this.fastPropagateExecution = fastPropagateExecution;
	}
	
	/**
	 * @return true if optional subgraph input port is connected by parent graph
	 */
	public boolean isParentGraphInputPortConnected(int portIndex) {
		if (parentGraphInputPortsConnected != null) {
			return parentGraphInputPortsConnected.contains(portIndex);
		} else {
			return true;
		}
	}

	/**
	 * @return list of subgraph input port indexes, which are connected by parent graph or null,
	 * which indicates all ports are connected
	 */
	public List<Integer> getParentGraphInputPortsConnected() {
		return parentGraphInputPortsConnected;
	}

	/**
	 * @param connectedParentGraphInputPorts list of all subgraph input port indexes connected by parent graph or null
	 * which indicates all ports are connected
	 */
	public void setParentGraphInputPortsConnected(List<Integer> parentGraphInputPortsConnected) {
		if (parentGraphInputPortsConnected != null) {
			this.parentGraphInputPortsConnected = Collections.unmodifiableList(parentGraphInputPortsConnected);
		} else {
			this.parentGraphInputPortsConnected = null;
		}
	}

	/**
	 * @return true if optional subgraph output port is connected by parent graph
	 */
	public boolean isParentGraphOutputPortConnected(int portIndex) {
		if (parentGraphOutputPortsConnected != null) {
			return parentGraphOutputPortsConnected.contains(portIndex);
		} else {
			return true;
		}
	}

	/**
	 * @return list of subgraph output port indexes, which are connected by parent graph or null,
	 * which indicates all ports are connected
	 */
	public List<Integer> getParentGraphOutputPortsConnected() {
		return parentGraphOutputPortsConnected;
	}
	
	/**
	 * @param connectedParentGraphOutputPorts list of all subgraph output port indexes connected by parent graph or null
	 * which indicates all ports are connected
	 */
	public void setParentGraphOutputPortsConnected(List<Integer> parentGraphOutputPortsConnected) {
		if (parentGraphOutputPortsConnected != null) {
			this.parentGraphOutputPortsConnected = Collections.unmodifiableList(parentGraphOutputPortsConnected);
		} else {
			this.parentGraphOutputPortsConnected = null;
		}
	}
	
	/**
	 * @return the strictGraphFactorization
	 */
	public boolean isStrictGraphFactorization() {
		return strictGraphFactorization;
	}

	/**
	 * @param strictGraphFactorization the strictGraphFactorization to set
	 */
	public void setStrictGraphFactorization(boolean strictGraphFactorization) {
		this.strictGraphFactorization = strictGraphFactorization;
	}

	/**
	 * @return true if classloaders for java transformations should be shared; false otherwise
	 */
	public boolean isClassLoaderCaching() {
		return classLoaderCaching;
	}

	public void setClassLoaderCaching(boolean classLoaderCaching) {
		this.classLoaderCaching = classLoaderCaching;
	}

	/**
	 * For all edges is also calculated which metadata would be propagated
	 * to this edge from neighbours, if the edge does not have any metadata directly assigned.
	 * It is useful only for designer purpose (calculation is turned off by default).
	 * Designer shows to user, which metadata would be on the edge, for "no metadata" option on the edge. 
	 */
	public boolean isCalculateNoMetadata() {
		return calculateNoMetadata;
	}

	/**
	 * For all edges is also calculated which metadata would be propagated
	 * to this edge from neighbours, if the edge does not have any metadata directly assigned.
	 * It is useful only for designer purpose (calculation is turned off by default).
	 * Designer shows to user, which metadata would be on the edge, for "no metadata" option on the edge. 
	 */
	public void setCalculateNoMetadata(boolean calculateNoMetadata) {
		this.calculateNoMetadata = calculateNoMetadata;
	}

	public boolean isCtlDebug() {
		return ctlDebug;
	}

	public void setCtlDebug(boolean ctlDebug) {
		this.ctlDebug = ctlDebug;
	}

	public boolean isSuspendThreads() {
		return suspendThreads;
	}

	public void setSuspendThreads(boolean suspendThreads) {
		this.suspendThreads = suspendThreads;
	}

	public Set<Breakpoint> getCtlBreakpoints() {
		return ctlBreakpoints;
	}

	public void setCtlBreakpoints(Collection<Breakpoint> ctlBreakpoints) {
		this.ctlBreakpoints.clear();
		this.ctlBreakpoints.addAll(ctlBreakpoints);
	}
	
	public boolean isCtlBreakpointsEnabled() {
		return ctlBreakpointsEnabled;
	}

	public void setCtlBreakpointsEnabled(boolean enabled) {
		ctlBreakpointsEnabled = enabled;
	}
	
	/**
	 * This enum is attempt to provide a more generic way to this runtime configuration.
	 * Should not be used by third-party applications, can be changed in the future.
	 */
	public enum PropertyKey {
		
		SKIP_CHECK_CONFIG("skipCheckConfig", Boolean.class) {
			@Override
			public Object parseValue(String s) {
				return parseBoolean(s);
			}
		},
		LOG_LEVEL("logLevel", Level.class) {
			@Override
			public Object parseValue(String s) {
				if (s == null) {
					return null;
				}
				return Level.toLevel(s);
			}
		},
		CLEAR_OBSOLETE_TEMP_FILES("clearObsoleteTempFiles", Boolean.class) {
			@Override
			public Object parseValue(String s) {
				return parseBoolean(s);
			}
		},
		EDGE_DEBUGGING("edgeDebugging", Boolean.class) {
			@Override
			public Object parseValue(String s) {
				return parseBoolean(s);
			}
		},
		LOCALE("locale", String.class) {
			@Override
			public Object parseValue(String s) {
				return s;
			}
		},
		TIME_ZONE("timeZone", String.class) {
			@Override
			public Object parseValue(String s) {
				return s;
			}
		},
		CLASSPATH("classpath", String.class) {
			@Override
			public Object parseValue(String s) {
				return s;
			}
		},
		COMPILE_CLASSPATH("compileClasspath", String.class) {
			@Override
			public Object parseValue(String s) {
				return s;
			}
		},
		VALIDATE_REQUIRED_PARAMETERS("validateRequiredParameters", Boolean.class) {
			@Override
			public Object parseValue(String s) {
				return parseBoolean(s);
			}
		},
		VERBOSE_MODE("verboseMode", Boolean.class) {
			
			@Override
			public Object parseValue(String s) {
				return Boolean.parseBoolean(s);
			}
		},
		WORKER_EXECUTION("workerExecution", Boolean.class) {
			
			@Override
			public Object parseValue(String s) {
				return parseBoolean(s);
			}
		};
		
		String key;
		Class<?> valueType;
		
		PropertyKey(String key, Class<?> valueType){
			this.key = key;
			this.valueType = valueType;
		}
		
		/**
		 * use this method for validation of config values.
		 * @param s
		 * @return
		 */
		public abstract Object parseValue(String s);
		
		private static Boolean parseBoolean(String s) {
			if (s == null || s.trim().length() == 0) {
				return null;
			} else {
				return Boolean.valueOf(s);
			}
		}

		/**
		 * String used as parameter key of this property in various APIs.
		 */
		public String getKey() {
			return key;
		}
		
		public Class<?> getValueType() { 
			return this.valueType;
		}
	}

}
