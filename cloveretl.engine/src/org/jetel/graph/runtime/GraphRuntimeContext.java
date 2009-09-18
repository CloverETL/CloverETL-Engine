/*
 *    jETeL/Clover - Java based ETL application framework.
 *    Copyright (C) 2002-2007  David Pavlis <david.pavlis@centrum.cz> and others.
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
 * Created on 10.7.2007 by dadik
 *
 */

package org.jetel.graph.runtime;

import java.util.Properties;

import org.apache.log4j.Level;
import org.jetel.data.Defaults;
import org.jetel.util.string.StringUtils;

/**
 * Common used implementation of IGraphRuntimeContext interface.
 * 
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created 27.11.2007
 */
public class GraphRuntimeContext {

	public static final boolean DEFAULT_VERBOSE_MODE = false;
	public static final boolean DEFAULT_WAIT_FOR_JMX_CLIENT = false;
	public static final boolean DEFAULT_USE_JMX = true;
	public static final boolean DEFAULT_DEBUG_MODE = true;
	public static final boolean DEFAULT_SKIP_CHECK_CONFIG = false;
	public static final boolean DEFAULT_SYNCHRONIZED_RUN = false;
	
	private long runId;
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
	private boolean debugMode;
	private String debugDirectory;
	private String[] classPaths;
	private boolean synchronizedRun;
	
	public GraphRuntimeContext() {
		trackingInterval = Defaults.WatchDog.DEFAULT_WATCHDOG_TRACKING_INTERVAL;
		useJMX = DEFAULT_USE_JMX;
		waitForJMXClient = DEFAULT_WAIT_FOR_JMX_CLIENT;
		verboseMode = DEFAULT_VERBOSE_MODE;
		additionalProperties = new Properties();
		skipCheckConfig = DEFAULT_SKIP_CHECK_CONFIG;
		debugMode = DEFAULT_DEBUG_MODE;
		synchronizedRun = DEFAULT_SYNCHRONIZED_RUN;
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.IGraphRuntimeContext#createCopy()
	 */
	public GraphRuntimeContext createCopy() {
		GraphRuntimeContext ret = new GraphRuntimeContext();
		
		ret.additionalProperties = new Properties();
		ret.additionalProperties.putAll(getAdditionalProperties());
		ret.trackingInterval = getTrackingInterval();
		ret.skipCheckConfig = isSkipCheckConfig();
		ret.verboseMode = isVerboseMode();
		ret.useJMX = useJMX();
		ret.waitForJMXClient = isWaitForJMXClient();
		ret.password = getPassword();
		ret.debugMode = isDebugMode();
		ret.debugDirectory = getDebugDirectory();
		ret.synchronizedRun = isSynchronizedRun();
		
		return ret;
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
    public boolean isDebugMode() {
        return debugMode;
    }

    /**
     * Sets whether graph should run in debug mode.
     * Debug mode for example can turn off debugging on all edges. 
     * @param debugMode
     */
    public void setDebugMode(boolean debugMode) {
        this.debugMode = debugMode;
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
	 * Class-path of external classes. 
	 * Array of jars and paths. 
	 * Each component with transformation class specified (attribute transformClass or generatorClass etc.) will use this class-paths to find it.
	 * */
	public String[] getClassPaths() {
		return classPaths;
	}

	/**
	 * @link {@link #getClassPaths()}
	 * @param transformPath
	 */
	public void setClassPaths(String[] classPaths) {
		this.classPaths = classPaths;
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
	
//	/**
//	 * @return trackingFlushInterval
//	 */
//	public int getTrackingFlushInterval() {
//		return trackingFlushInterval;
//	}
//
//	/**
//	 * Sets interval which is used for flushing of tracking info to logging output.
//	 * @param trackingFlushInterval to set
//	 */
//	public void setTrackingFlushInterval(int trackingFlushInterval) {
//		this.trackingFlushInterval = trackingFlushInterval;
//	}
	
}
