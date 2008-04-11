/*
 *    jETeL/Clover - Java based ETL application framework.
 *    Copyright (C) 2004-08 Javlin Consulting <info@javlinconsulting.cz>
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


package org.jetel.connection.jdbc.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author Agata Vackova (agata.vackova@javlinconsulting.cz) ; 
 * (c) JavlinConsulting s.r.o.
 *  www.javlinconsulting.cz
 *
 * @since Apr 8, 2008
 *
 */

public class JdbcConfigFactory {
	
	private static Log logger = LogFactory.getLog(JdbcConfigFactory.class);
	
	//TODO
	
	private final static JdbcBaseConfig baseConfig = JdbcBaseConfig.getInstance();
	private final static JdbcDb2Config db2Config = JdbcDb2Config.getInstance();
	private final static JdbcInformixConfig informixConfig = JdbcInformixConfig.getInstance();
	private final static JdbcOracleConfig oracleConfig = JdbcOracleConfig.getInstance();
	private final static JdbcMySQLConfig mysqlConfig = JdbcMySQLConfig.getInstance();
	private final static JdbcPostgreSQLConfig postgreConfig = JdbcPostgreSQLConfig.getInstance();
	private final static JdbcMssqlConfig mssqlConfig = JdbcMssqlConfig.getInstance();
	
	private static Map<String, JdbcBaseConfig> configMap = new HashMap<String, JdbcBaseConfig>();
	static {
		configMap.put(db2Config.getTargetDBName(), db2Config);
		configMap.put(informixConfig.getTargetDBName(), informixConfig);
		configMap.put(oracleConfig.getTargetDBName(), oracleConfig);
		configMap.put(mysqlConfig.getTargetDBName(), mysqlConfig);
		configMap.put(postgreConfig.getTargetDBName(), postgreConfig);
		configMap.put(mssqlConfig.getTargetDBName(), mssqlConfig);
	}
	
	public static JdbcBaseConfig createConfig(String database){
		JdbcBaseConfig config = null;
		if (configMap.containsKey(database)) config = configMap.get(database);
		else if (database.toUpperCase().contains(db2Config.getTargetDBName())) config = db2Config;
		else if (database.toUpperCase().contains(informixConfig.getTargetDBName())) config = informixConfig;
		else if (database.toUpperCase().contains(oracleConfig.getTargetDBName())) config = oracleConfig;
		else if (database.toUpperCase().contains(mysqlConfig.getTargetDBName())) config = mysqlConfig;
		else if (database.toUpperCase().contains(postgreConfig.getTargetDBName())) config = postgreConfig;
		else if (database.toUpperCase().contains(mssqlConfig.getTargetDBName())) config = mssqlConfig;
		if (config == null) {
			config = baseConfig;
		}
		logger.info("Using connection configuration for " + config.getTargetDBName());
		return config;
	}
	
}
