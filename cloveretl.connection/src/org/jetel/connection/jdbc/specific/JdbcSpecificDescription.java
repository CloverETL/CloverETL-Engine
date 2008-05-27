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
package org.jetel.connection.jdbc.specific;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.plugin.Extension;
import org.jetel.plugin.PluginDescriptor;

/**
 * JDBC specific descriptor. This class is a container for all information loaded from 'jdbcSpecific' extension point.
 * 
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created May 23, 2008
 */
public class JdbcSpecificDescription {

    private static Log logger = LogFactory.getLog(JdbcSpecificDescription.class);

    private final static String DATABASE_PARAMETER = "database";

    private final static String NAME_PARAMETER = "name";

    private final static String MAJOR_VERSION_PARAMETER = "majorVersion";

    private final static String CLASS_PARAMETER = "class";

    /**
     * Identifier of the JDBC driver.
     */
    private String database;
    
    /**
     * Name of this JDBC driver is used for communication with user - for example in the clover.GUI.
     */
    private String name;
    
    /**
     * Major version number targeted database.
     */
    private String majorVersion;
    
    /**
     * Fully classified class name of the database specific behaviour.
     */
    private String className;
    
    /**
     * Extension where was this JDBC specific defined.
     */
    private Extension extension;
    
    /**
     * The only constructor.
     * @param extension
     */
    public JdbcSpecificDescription(Extension extension) {
        this.extension = extension;

        //reads 'database' parameter
        if(!extension.hasParameter(DATABASE_PARAMETER)) {
            throw new RuntimeException("JDBC specific can not be created, since 'database' parameter is not set.");
        }
        database = extension.getParameter(DATABASE_PARAMETER).getString();

        //reads 'name' parameter 
        if(extension.hasParameter(NAME_PARAMETER)) {
            name = extension.getParameter(NAME_PARAMETER).getString();
        }

        //reads 'majorVersion' parameter 
        if(extension.hasParameter(MAJOR_VERSION_PARAMETER)) {
            majorVersion = extension.getParameter(MAJOR_VERSION_PARAMETER).getString();
        }

        //reads 'class' parameter
        if(!extension.hasParameter(CLASS_PARAMETER)) {
            throw new RuntimeException("JDBC specific '" + database + "' can not be created, since 'class' parameter is not set.");
        }
        className = extension.getParameter(CLASS_PARAMETER).getString();
    }
    
    public String getDatabase() {
        return database;
    }

    public String getName() {
        return name;
    }
    
    public String getMajorVersion() {
        return majorVersion;
    }

    public String getClassName() {
        return className;
    }

    public Extension getExtension() {
    	return extension;
    }
    
    public JdbcSpecific getJdbcSpecific() {
        try {
            PluginDescriptor pluginDescriptor = getExtension().getPlugin();
            
            //find class of jdbc specific
            Class<JdbcSpecific> c = (Class<JdbcSpecific>) Class.forName(getClassName(), true, pluginDescriptor.getClassLoader());

            //create instance
            return c.newInstance();
        } catch(ClassNotFoundException ex) {
            logger.error("Unknown jdbc specific: " + getDatabase() + " class: " + getClassName());
            throw new RuntimeException("Unknown jdbc specific: " + getDatabase() + " class: " + getClassName());
        } catch(Exception ex) {
            logger.error("Unknown jdbc specific type: " + getDatabase());
            throw new RuntimeException("Unknown jdbc specific type: " + getDatabase());
        }
    }
    
}
