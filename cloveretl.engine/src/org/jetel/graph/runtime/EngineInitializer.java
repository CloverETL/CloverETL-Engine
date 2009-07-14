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
package org.jetel.graph.runtime;

import java.net.URL;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Logger;
import org.apache.log4j.net.SocketAppender;
import org.jetel.data.Defaults;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.graph.TransformationGraph;
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
	
    /**
     * Clover.ETL engine initialization. Should be called only once.
     * @param pluginsRootDirectory directory path, where plugins specification is located 
     *        (can be null, then is used constant from Defaults.DEFAULT_PLUGINS_DIRECTORY)
     * @param defaultPropertiesFile file with external definition of default values usually stored in defaultProperties
     * @param password password for encrypting some hidden part of graphs
     *        <br>i.e. connections password can be encrypted
     *        <br>can be null
     */
    public static void initEngine(String pluginsRootDirectory, String defaultPropertiesFile, String logHost) {
    	if(alreadyInitialized) {
    		//clover engine is already initialized
    		return;
    	}
    	
    	//init logging
    	initLogging(logHost);
    	
        //init framework constants
        Defaults.init(defaultPropertiesFile);

        //init clover plugins system
        Plugins.init(pluginsRootDirectory);
        
        //make a note, engine is already initialized
        alreadyInitialized = true;
    }

    /**
     * Clover.ETL engine initialization. Should be called only once.
     * @param pluginsUrls locations of all engine plugin 
     * @param defaultPropertiesFile file with external definition of default values usually stored in defaultProperties
     * @param password password for encrypting some hidden part of graphs
     *        <br>i.e. connections password can be encrypted
     *        <br>can be null
     */
    public static void initEngine(URL[] pluginsUrls, String defaultPropertiesFile, String logHost) {
    	if(alreadyInitialized) {
    		//clover engine is already initialized
    		return;
    	}
    	
    	//init logging
    	initLogging(logHost);
    	
        //init framework constants
        Defaults.init(defaultPropertiesFile);

        //init clover plugins system
        Plugins.init(pluginsUrls);
        
        //make a note, engine is already initialized
        alreadyInitialized = true;
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
	public static void initGraph(TransformationGraph graph, GraphRuntimeContext runtimeContext) throws ComponentNotReadyException {
		graph.setPassword(runtimeContext.getPassword());
		if (!runtimeContext.isSkipCheckConfig()) {
			logger.info("Checking graph configuration...");
			ConfigurationStatus status = graph.checkConfig(null);
			if(status.isError()) {
				logger.error("Graph configuration is invalid.");
				status.log();
				for (ConfigurationProblem s : status){
					// throw exception with the first error in the list
					if (s.getSeverity() == Severity.ERROR)
						throw new ComponentNotReadyException(graph, "Graph configuration is invalid (" + s + ").");
				} // for
			} else {
				logger.info("Graph configuration is valid.");
				status.log();
			}
		} else {
			logger.info("Graph configuration checking is skipped.");
		}
		logger.info("Graph initialization (" + graph.getName() + ")");
        graph.init();
	}

}
