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

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Logger;
import org.apache.log4j.net.SocketAppender;
import org.jetel.component.fileoperation.FileManager;
import org.jetel.data.Defaults;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.graph.TransformationGraph;
import org.jetel.main.runGraph;
import org.jetel.plugin.PluginLocation;
import org.jetel.plugin.PluginRepositoryLocation;
import org.jetel.plugin.Plugins;
import org.jetel.util.string.StringUtils;

/**
 * Clover.ETL engine initializer.
 * 
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created 27.11.2007
 */
public class EngineInitializer {

	private static Log logger = LogFactory.getLog(EngineInitializer.class);

	private static boolean alreadyInitialized = false;
	
	private static String licensesLocations = null;	
    /**
     * Clover.ETL engine initialization. Should be called only once.
     * @param pluginsRootDirectory directory path, where plugins specification is located 
     *        (can be null, then is used constant from Defaults.DEFAULT_PLUGINS_DIRECTORY)
     * @param defaultPropertiesFile file with external definition of default values usually stored in defaultProperties
     * @param password password for encrypting some hidden part of graphs
     *        <br>i.e. connections password can be encrypted
     *        <br>can be null
     */
    public static synchronized void initEngine(String pluginsRootDirectory, String defaultPropertiesFile, String logHost) {
    	if(alreadyInitialized) {
    		//clover engine is already initialized
    		return;
    	}
    	alreadyInitialized = true;
    	
    	//shared part of initialiation
    	internalInit(defaultPropertiesFile, logHost);
	
        //init clover plugins system
        Plugins.init(pluginsRootDirectory);
    }

    /**
     * Clover.ETL engine initialization. Should be called only once.
     * @param pluginsUrls locations of all individual engine plugins 
     * @param defaultPropertiesFile file with external definition of default values usually stored in defaultProperties
     * @param password password for encrypting some hidden part of graphs
     *        <br>i.e. connections password can be encrypted
     *        <br>can be null
     */
    public static synchronized void initEngine(URL[] pluginsUrls, String defaultPropertiesFile, String logHost) {
    	if(alreadyInitialized) {
    		//clover engine is already initialized
    		return;
    	}
    	alreadyInitialized = true;
    	
		//shared part of initialiation
    	internalInit(defaultPropertiesFile, logHost);
    	
        //init clover plugins system
    	List<PluginLocation> pluginLocations = new ArrayList<PluginLocation>();
    	for (URL pluginUrl : pluginsUrls) {
    		pluginLocations.add(new PluginLocation(pluginUrl));
    	}
        Plugins.init(pluginLocations.toArray(new PluginLocation[pluginLocations.size()]));
    }
    
    public static synchronized void initEngine(URL[] pluginsUrls, URL defaultPropertiesFile, String logHost) {
        if (alreadyInitialized) {
            //clover engine is already initialized
            return;
        }
        alreadyInitialized = true;
        
        //shared part of initialiation
        internalInit(defaultPropertiesFile, logHost);

        //init clover plugins system
        List<PluginLocation> pluginLocations = new ArrayList<PluginLocation>();
        for (URL pluginUrl : pluginsUrls) {
            pluginLocations.add(new PluginLocation(pluginUrl));
        }
        Plugins.init(pluginLocations.toArray(new PluginLocation[pluginLocations.size()]));
    }

    /**
     * Clover.ETL engine initialization. Should be called only once.
     * @param pluginRepositories locations of all engine plugins 
     * @param defaultPropertiesFile file with external definition of default values usually stored in defaultProperties
     * @param password password for encrypting some hidden part of graphs
     *        <br>i.e. connections password can be encrypted
     *        <br>can be null
     */
    public static synchronized void initEngine(PluginRepositoryLocation[] pluginRepositories, String defaultPropertiesFile, String logHost) {
    	if(alreadyInitialized) {
    		//clover engine is already initialized
    		return;
    	}
    	alreadyInitialized = true;

    	//shared part of initialiation
    	internalInit(defaultPropertiesFile, logHost);
    	
        //init clover plugins system
        Plugins.init(pluginRepositories);
    }

    /**
     * Clover.ETL engine initialization. Should be called only once.
     * @param plugin locations of all engine plugins 
     * @param defaultPropertiesFile file with external definition of default values usually stored in defaultProperties
     * @param password password for encrypting some hidden part of graphs
     *        <br>i.e. connections password can be encrypted
     *        <br>can be null
     * @param pluginRepositories
     * @param defaultPropertiesFile
     * @param logHost
     */
    public static synchronized void initEngine(PluginLocation[] pluginLocs, String defaultPropertiesFile, String logHost) {
    	if(alreadyInitialized) {
    		//clover engine is already initialized
    		return;
    	}
    	alreadyInitialized = true;

    	//shared part of initialiation
    	internalInit(defaultPropertiesFile, logHost);
    	
        //init clover plugins system
        Plugins.init(pluginLocs);
    }
    
    private static void internalInit(String defaultPropertiesFile, String logHost) {
    	//init logging
    	initLogging(logHost);

        // print out the basic environment information to log4j interface - has to be after log4j initialization - issue #1911
        runGraph.printRuntimeHeader();

        //init framework constants
        Defaults.init(defaultPropertiesFile);

        //file manager initialisation
        FileManager.init();
    }
    
    private static void internalInit(URL defaultPropertiesFile, String logHost) {
        //init logging
        initLogging(logHost);

        // print out the basic environment information to log4j interface - has to be after log4j initialization - issue #1911
        runGraph.printRuntimeHeader();

        //init framework constants
        Defaults.init(defaultPropertiesFile);

        //file manager initialisation
        FileManager.init();
    }
    
    /**
     * This method forces engine to activate all plugins at once.
     * For engine initialization it is not necessary to call this method,
     * lazy initialization is perfect for most usage of clover engine.
     * This functionality is now used for example by clover GUI.
     */
    public static void forceActivateAllPlugins() {
    	Plugins.activateAllPlugins();
    }

    private static void initLogging(String logHost) {
    	if(StringUtils.isEmpty(logHost)) {
    		return;
    	}
    	
	    String[] hostAndPort = logHost.split(":");
	    if (hostAndPort[0].length() == 0 || hostAndPort.length > 2) {
	        System.err.println("Invalid log destination, i.e. -loghost localhost:4445");
	        System.exit(-1);
	    }
	    int port = 4445;
	    try {
	        if (hostAndPort.length == 2) {
	            port = Integer.parseInt(hostAndPort[1]);
	        }
	    } catch (NumberFormatException e) {
	        System.err.println("Invalid log destination, i.e. -loghost localhost:4445");
	        System.exit(-1);
	    }
	    Logger.getRootLogger().addAppender(new SocketAppender(hostAndPort[0], port));
    }

	/**
	 * Prepares graph for first run. Checks configuration and initializes.
	 * @param graph
	 * @throws ComponentNotReadyException
	 */
	public static void initGraph(TransformationGraph graph) throws ComponentNotReadyException {
		initGraph(graph, graph.getRuntimeContext());
	}
	
	/**
	 * Prepares graph for first run. Checks configuration and initializes.
	 * @param graph
	 * @throws ComponentNotReadyException
	 */
	public static void initGraph(TransformationGraph graph, GraphRuntimeContext runtimeContext) throws ComponentNotReadyException {
		graph.setPassword(runtimeContext.getPassword());
		
		//first perform checkConfig() method on the graph 
		if (!runtimeContext.isSkipCheckConfig()) {
			checkConfig(graph);
		} else {
			logger.info("Graph configuration checking is skipped.");
		}
		
		//initialize the graph
		logger.info("Graph initialization (" + graph.getName() + ")");
        graph.init();
	}

	/**
	 * Checks configuration of the given graph.
	 */
	public static void checkConfig(TransformationGraph graph) throws ComponentNotReadyException {
		logger.info("Checking graph configuration...");
		ConfigurationStatus status = graph.checkConfig(null);
		if (status.isError()) {
			logger.error("Graph configuration is invalid.");
			status.log();
			throw new ComponentNotReadyException(graph, "Graph configuration is invalid.", status.toException());
		} else {
			logger.info("Graph configuration is valid.");
			status.log();
		}
	}
	
	/**
	 * Initializes location of licenses. If you want to use this method, it has to be called BEFORE initEngine call.
	 * @param licenses Location(s) where licenses are stored.
	 */
	public static void initLicenses(String licenses) {
		if(isInitialized()) {
			throw new RuntimeException("Licenses have to be initialized before engine. Call initLicenses before initEngine.");
		}
		licensesLocations = licenses;
	}
	
	public static String getLicensesLocation() {
		if(licensesLocations == null) {
			return Defaults.DEFAULT_LICENSE_LOCATION;
		}
		
		return licensesLocations+";"+Defaults.DEFAULT_LICENSE_LOCATION;
	}
	
	public static boolean isInitialized() {
		return alreadyInitialized;
	}
	
}
