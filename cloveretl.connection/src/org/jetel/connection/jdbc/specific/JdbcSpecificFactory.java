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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.plugin.Extension;
import org.jetel.plugin.Plugins;

/**
 * This class provides access to all registered JDBC specifics via the 'jdbcSpecific' extension point.
 * 
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created May 23, 2008
 */
public class JdbcSpecificFactory {

    private static Log logger = LogFactory.getLog(JdbcSpecificFactory.class);

    public final static String EXTENSION_POINT_ID = "jdbcSpecific";

    private final static Map<String, JdbcSpecificDescription> jdbcSpecifics = new HashMap<String, JdbcSpecificDescription>();
    
    public static void init() {
        //ask plugin framework for all jdbc specific extensions
        List<Extension> jdbcExtensions = Plugins.getExtensions(JdbcSpecificFactory.EXTENSION_POINT_ID);
      
        //register all jdbc specifics
        for(Extension extension : jdbcExtensions) {
            try {
                registerJdbcSpecific(new JdbcSpecificDescription(extension));
            } catch(Exception e) {
                logger.error("Cannot create JDBC specific descriptor, extension in plugin manifest is not valid.\n"
                        + "pluginId = " + extension.getPlugin().getId() + "\n" + extension + "\nReason: " + e.getMessage());
            }
        }

    }

    private static void registerJdbcSpecific(JdbcSpecificDescription jdbcSpecific) {
        String database = jdbcSpecific.getDatabase();
        
        if(jdbcSpecifics.containsKey(database)) {
            logger.warn("Some of the plugin tried to register already registered JDBC specific under same database name: '" + database + "'.");
            return;
        }
        
        jdbcSpecifics.put(database, jdbcSpecific);
    }
    
    /**
     * @param specificId
     * @return JDBC specific based on given specific identifier
     */
    public static JdbcSpecificDescription getJdbcSpecificDescription(String specificId) {
        return jdbcSpecifics.get(specificId);
    }
    
    /**
     * @return list of all registered JDBC specifics
     */
    public static JdbcSpecificDescription[] getAllJdbcSpecificDescriptions() {
        return jdbcSpecifics.values().toArray(new JdbcSpecificDescription[jdbcSpecifics.size()]);
    }
    
}
