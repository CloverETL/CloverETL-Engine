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
package org.jetel.connection.jdbc.driver;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.connection.jdbc.specific.JdbcSpecific;
import org.jetel.connection.jdbc.specific.JdbcSpecificDescription;
import org.jetel.connection.jdbc.specific.JdbcSpecificFactory;
import org.jetel.data.Defaults;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.plugin.Extension;
import org.jetel.plugin.ExtensionParameter;
import org.jetel.util.string.StringUtils;

/**
 * JDBC driver descriptor. This class is a container for all information loaded from 'jdbcDriver' extension point.
 * 
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created 14.9.2007
 */
public class JdbcDriverDescription {
    
    private static Log logger = LogFactory.getLog(JdbcDriverDescription.class);

    private final static String DATABASE_PARAMETER = "database";

    private final static String NAME_PARAMETER = "name";

    private final static String DB_DRIVER_PARAMETER = "dbDriver";

    private final static String DRIVER_LIBRARY_PARAMETER = "driverLibrary";

    private final static String URL_HINT_PARAMETER = "urlHint";

    private final static String JDBC_SPECIFIC_PARAMETER = "jdbcSpecific";

    public final static String[] EXCLUDE_PARAMETERS = 
        new String[] { DATABASE_PARAMETER, NAME_PARAMETER, DB_DRIVER_PARAMETER, DRIVER_LIBRARY_PARAMETER, URL_HINT_PARAMETER, JDBC_SPECIFIC_PARAMETER };
    
    /**
     * Identifier of the JDBC driver.
     */
    private String database;
    
    /**
     * Name of this JDBC driver is used for communication with user - for example in the clover.GUI.
     */
    private String name;
    
    /**
     * Name of the class, which implements java.sql.Driver interface.
     */
    private String dbDriver;
    
    /**
     * Semicolon delimited list of jar/zip libraries of this JDBC driver.
     */
    private String driverLibrary;
    
    /**
     * Hint for a database URL, mainly used in GUI.
     */
    private String urlHint;

    /**
     * JDBC specific behaviour for this driver.
     */
    private String jdbcSpecific;

    /**
     * Custom user properties, all other parameters given via this JDBC extension point 
     * are stored in this properties object. 
     */
    private Properties properties;
    
    /**
     * Extension where was this JDBC driver defined.
     */
    private Extension extension;
    
    /**
     * The only constructor.
     * @param extension
     */
    public JdbcDriverDescription(Extension extension) {
        this.extension = extension;

        //reads 'database' parameter
        if(!extension.hasParameter(DATABASE_PARAMETER)) {
            throw new RuntimeException("JDBC driver can not be created, since 'database' parameter is not set.");
        }
        database = extension.getParameter(DATABASE_PARAMETER).getString();

        //reads 'name' parameter 
        if(extension.hasParameter(NAME_PARAMETER)) {
            name = extension.getParameter(NAME_PARAMETER).getString();
        }
        
        //reads 'dbDriver' parameter
        if(!extension.hasParameter(DB_DRIVER_PARAMETER)) {
            throw new RuntimeException("JDBC driver '" + database + "' can not be created, since 'dbDriver' parameter is not set.");
        }
        dbDriver = extension.getParameter(DB_DRIVER_PARAMETER).getString();

        //reads 'driverLibrary' parameter 
        if(extension.hasParameter(DRIVER_LIBRARY_PARAMETER)) {
            driverLibrary = extension.getParameter(DRIVER_LIBRARY_PARAMETER).getString();
        }

        //reads 'urlHint' parameter
        if(extension.hasParameter(URL_HINT_PARAMETER)) {
            urlHint = extension.getParameter(URL_HINT_PARAMETER).getString();
        }

        //reads 'jdbcSpecific' parameter
        if(extension.hasParameter(JDBC_SPECIFIC_PARAMETER)) {
            jdbcSpecific = extension.getParameter(JDBC_SPECIFIC_PARAMETER).getString();
        }

        //reads other parameters
        properties = new Properties();
        Map<String, ExtensionParameter> parameters = extension.getParameters(EXCLUDE_PARAMETERS);
        for(Entry<String, ExtensionParameter> entry : parameters.entrySet()) {
            properties.setProperty(entry.getKey(), entry.getValue().toString());
        }
    }
    
    public String getDatabase() {
        return database;
    }

    public String getName() {
        return name;
    }
    
    public String getDbDriver() {
        return dbDriver;
    }

    public String getDriverLibrary() {
        return driverLibrary;
    }

    public String getUrlHint() {
        return urlHint;
    }

    public String getJdbcSpecificStr() {
    	return jdbcSpecific;
    }
    
    public Properties getProperties() {
        return properties;
    }
    
    public JdbcDriver createJdbcDriver() throws ComponentNotReadyException {
    	return new JdbcDriver(this);
    }

    public JdbcSpecific getJdbcSpecific() {
    	if(!StringUtils.isEmpty(jdbcSpecific)) {
    		JdbcSpecificDescription jdbcSpecificDescription = JdbcSpecificFactory.getJdbcSpecificDescription(jdbcSpecific);
    		if(jdbcSpecificDescription != null) {
    			return jdbcSpecificDescription.getJdbcSpecific();
    		} else {
    			throw new RuntimeException("JDBC specific extension '" + jdbcSpecific + "' was not found.");
    		}
    	} else {
    		return null;
    	}
    }
    
    public URL[] getDriverLibraryURLs() throws ComponentNotReadyException {
        String[] libraryPaths = driverLibrary.split(Defaults.DEFAULT_PATH_SEPARATOR_REGEX);

    	URL[] urls = new URL[libraryPaths.length];
        for(int i = 0; i < libraryPaths.length; i++) {
            try {
                urls[i] = extension.getPlugin().getURL(libraryPaths[i]);
            } catch (MalformedURLException ex1) {
                throw new ComponentNotReadyException("Can not create JDBC driver '" + database + "'. Malformed URL: " + ex1.getMessage(), ex1);
            }
        }
        
        return urls;
    }

}
