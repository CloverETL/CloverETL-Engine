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
package org.jetel.connection.jdbc.specific;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.connection.jdbc.specific.impl.DefaultJdbcSpecific;
import org.jetel.database.sql.DBConnection;
import org.jetel.database.sql.JdbcSpecific;
import org.jetel.plugin.Extension;
import org.jetel.plugin.Plugins;
import org.jetel.util.string.StringUtils;

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

    private final static Map<String, JdbcSpecificDescription> jdbcSpecifics = new LinkedHashMap<String, JdbcSpecificDescription>();
    
    public static void init() {
        //ask plugin framework for all jdbc specific extensions
        List<Extension> jdbcExtensions = Plugins.getExtensions(JdbcSpecificFactory.EXTENSION_POINT_ID);
      
        //register all jdbc specifics
        for(Extension extension : jdbcExtensions) {
            try {
            	JdbcSpecificDescription description = new JdbcSpecificDescription(extension);
                description.init();
                registerJdbcSpecific(description);
            } catch(Exception e) {
                logger.error("Cannot create JDBC specific descriptor, extension in plugin manifest is not valid.\n"
                        + "pluginId = " + extension.getPlugin().getId() + "\n" + extension, e);
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
     * Returns jdbc specific for given {@link Connection} instance based on product name
     * specified in {@link Connection#getMetaData()#getDatabaseProductName()}.
     * This method is now used by {@link DBConnection} when a {@link Connection} instance is retrieved
     * from JNDI and we need to look up proper {@link JdbcSpecific}.
     */
    public static JdbcSpecificDescription getJdbcSpecificDescription(Connection connection) {
    	for (JdbcSpecificDescription jdbcSpecific : getAllJdbcSpecificDescriptions()) {
    		String productName = jdbcSpecific.getProductName();
    		try {
	    		if (!StringUtils.isEmpty(productName) && productName.equalsIgnoreCase((connection.getMetaData().getDatabaseProductName()))) {
	    			return jdbcSpecific;
	    		}
    		} catch (SQLException e) {
    			//DO NOTHING lets try the other specifics
    		}
    	}
    	return getJdbcSpecificDescription(DefaultJdbcSpecific.DATABASE_ID);
    }

    /**
     * @return list of all registered JDBC specifics
     */
    public static JdbcSpecificDescription[] getAllJdbcSpecificDescriptions() {
        return jdbcSpecifics.values().toArray(new JdbcSpecificDescription[jdbcSpecifics.size()]);
    }
    
}
