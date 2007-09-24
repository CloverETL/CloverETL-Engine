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
package org.jetel.database.jdbc;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Driver;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import org.jetel.data.Defaults;
import org.jetel.plugin.Extension;
import org.jetel.plugin.ExtensionParameter;
import org.jetel.util.StringUtils;

/**
 * JDBC driver descriptor. This class is a container for all information loaded from 'jdbcDriver' extension point.
 * 
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created 14.9.2007
 */
public class JdbcDriver {
    
    private final static String DATABASE_PARAMETER = "database";

    private final static String NAME_PARAMETER = "name";

    private final static String DB_DRIVER_PARAMETER = "dbDriver";

    private final static String DRIVER_LIBRARY_PARAMETER = "driverLibrary";

    private final static String URL_HINT_PARAMETER = "urlHint";

    public final static String[] EXCLUDE_PARAMETERS = 
        new String[] { DATABASE_PARAMETER, NAME_PARAMETER, DB_DRIVER_PARAMETER, DRIVER_LIBRARY_PARAMETER, URL_HINT_PARAMETER };
    
    /**
     * Identifier of the JDBC driver.
     */
    private String database;
    
    /**
     * Name of this JDBC driver is used for comunication with user - for example in the clover.GUI.
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
     * Custom user properties, all other parameters given via this JDBC extension point 
     * are stored in this properties object. 
     */
    private Properties properties;
    
    /**
     * Extension where was this JDBC driver defined.
     */
    private Extension extension;
    
    /**
     * Class loader used to instancionalization of java.sql.Driver class.
     */
    private ClassLoader classLoader;
    
    /**
     * The only constructor.
     * @param extension
     */
    public JdbcDriver(Extension extension) {
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
        return !StringUtils.isEmpty(name) ? name : database;
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

    public Properties getProperties() {
        return properties;
    }
    
    /**
     * @return class loader, which is used to create java.sql.Driver instance, created in getDriver() method
     */
    public ClassLoader getClassLoader() {
        if(classLoader == null) {
            if(!StringUtils.isEmpty(driverLibrary)) {
                String[] libraryPaths = driverLibrary.split(Defaults.DEFAULT_PATH_SEPARATOR_REGEX);
                
                URL[] myURLs = new URL[libraryPaths.length];
                for(int i = 0; i < libraryPaths.length; i++) {
                    try {
                        myURLs[i] = extension.getPlugin().getURL(libraryPaths[i]);
                    } catch (MalformedURLException ex1) {
                        throw new RuntimeException("Can not create JDBC driver '" + database + "'. Malformed URL: " + ex1.getMessage());
                    }
                }
                    
                classLoader = new URLClassLoader(myURLs, Thread.currentThread().getContextClassLoader());
            } else {
                classLoader = Thread.currentThread().getContextClassLoader();
            }
        }

        return classLoader;
    }
    
    public Driver getDriver() {
        try {
            return (Driver) Class.forName(dbDriver, true, getClassLoader()).newInstance();
        } catch (ClassNotFoundException ex1) {
            throw new RuntimeException("Can not create JDBC driver '" + database + "'. Can not find class: " + ex1);
        } catch (Exception ex1) {
            throw new RuntimeException("Cannot create JDBC driver '" + database + "'. General exception: " + ex1.getMessage());
        }
    }
    
}
