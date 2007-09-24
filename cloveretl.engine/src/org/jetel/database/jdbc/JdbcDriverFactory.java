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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.plugin.Extension;
import org.jetel.plugin.Plugins;

/**
 * This class provides access to all registered JDBC drivers via the 'jdbcDriver' extension point.
 * 
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created 14.9.2007
 */
public class JdbcDriverFactory {

    private static Log logger = LogFactory.getLog(JdbcDriverFactory.class);

    public final static String EXTENSION_POINT_ID = "jdbcDriver";

    private final static Map<String, JdbcDriver> jdbcDrivers = new HashMap<String, JdbcDriver>();
    
    public static void init() {
        //ask plugin framework for all jdbc driver extensions
        List<Extension> jdbcExtensions = Plugins.getExtensions(JdbcDriverFactory.EXTENSION_POINT_ID);
      
        //register all jdbc drivers
        for(Extension extension : jdbcExtensions) {
            try {
                registerJdbcDriver(new JdbcDriver(extension));
            } catch(Exception e) {
                logger.error("Cannot create JDBC driver descriptor, extension in plugin manifest is not valid.\n"
                        + "pluginId = " + extension.getPlugin().getId() + "\n" + extension + "\nReason: " + e.getMessage());
            }
        }

    }

    private static void registerJdbcDriver(JdbcDriver jdbcDriver) {
        String database = jdbcDriver.getDatabase();
        
        if(jdbcDrivers.containsKey(database)) {
            logger.warn("Some of the plugin tried to register already registered JDBC driver under same database name: '" + database + "'.");
            return;
        }
        
        jdbcDrivers.put(database, jdbcDriver);
    }
    
    /**
     * @param driverId
     * @return JDBC driver based on given driver identifier
     */
    public static JdbcDriver getJdbcDriver(String driverId) {
        return jdbcDrivers.get(driverId);
    }
    
    /**
     * @return list of all registered JDBC drivers
     */
    public static JdbcDriver[] getAllJdbcDrivers() {
        return jdbcDrivers.values().toArray(new JdbcDriver[jdbcDrivers.size()]);
    }
    
}
