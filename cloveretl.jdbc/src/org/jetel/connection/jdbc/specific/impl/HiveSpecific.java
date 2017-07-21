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
package org.jetel.connection.jdbc.specific.impl;

import java.sql.Connection;
import java.sql.Types;

import org.jetel.connection.jdbc.specific.conn.HiveConnection;
import org.jetel.database.sql.DBConnection;
import org.jetel.database.sql.SqlConnection;
import org.jetel.exception.JetelException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;

/**
 * JDBC Specific for Apache Hive -- the data warehouse system for Hadoop.
 * 
 * @author tkramolis (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 26.10.2012
 */
public class HiveSpecific extends AbstractJdbcSpecific {

	private static final HiveSpecific INSTANCE = new HiveSpecific();
	
	
	public static HiveSpecific getInstance() {
		return INSTANCE;
	}

	protected HiveSpecific() {
		super();
	}
	
	@Override
	public SqlConnection createSQLConnection(DBConnection dbConnection, Connection connection, OperationType operationType) throws JetelException {
		return new HiveConnection(dbConnection, connection, operationType);	
	}
	
	@Override
	public String sqlType2str(int sqlType) {
		switch (sqlType) {
		case Types.INTEGER:
			return "INT";
		case Types.VARCHAR:
			return "STRING";
		}

		return super.sqlType2str(sqlType);
	}
	
	@Override
	public String jetelType2sqlDDL(DataFieldMetadata field) {
		// Table column size cannot be specified in Hive (as is done in AbstractJdbcSpecific)
		return sqlType2str(jetelType2sql(field));
	}
	
	@Override
	public int jetelType2sql(DataFieldMetadata field) {
		switch (field.getDataType()) {
		case BYTE:
		case CBYTE:
			return Types.BINARY;
		case NUMBER:
			return Types.DOUBLE;
		default:
			return super.jetelType2sql(field);
		}
	}
	
	@Override
	public String getCreateTableSuffix(DataRecordMetadata metadata) {
		String delimiter = metadata.getFieldDelimiter();
		StringBuilder sb = new StringBuilder();
		sb.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY ");
		sb.append("'");
		sb.append(delimiter);
		sb.append("'\n");
		sb.append("STORED AS TEXTFILE\n");
		return sb.toString();
	}
	
	@Override
	public ClassLoader getDriverClassLoaderParent() {
		/*
		 * Hive drivers depend on log4j & commons-logging, that are part of the clover classpath,
		 * so return class that has access to that classpath
		 */
		return Thread.currentThread().getContextClassLoader();
	}
}
