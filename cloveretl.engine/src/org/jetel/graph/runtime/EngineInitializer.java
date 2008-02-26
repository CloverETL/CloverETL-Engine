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

import org.apache.log4j.Logger;
import org.apache.log4j.net.SocketAppender;
import org.jetel.data.Defaults;
import org.jetel.plugin.Plugins;
import org.jetel.util.protocols.CloverURLStreamHandlerFactory;
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
    	
        //init url protocols
        try {
			URL.setURLStreamHandlerFactory(new CloverURLStreamHandlerFactory());
		} catch (Error e) {
			System.err.println("SFTP protocol cannot be provided: " + e.getMessage());
		}
		
        //init framework constants
        Defaults.init(defaultPropertiesFile);

        //init clover plugins system
        Plugins.init(pluginsRootDirectory);
        
        //make a note, engine is already initialized
        alreadyInitialized = true;
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

}
