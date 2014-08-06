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
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.Properties;

import org.jetel.connection.jdbc.AbstractCopySQLData.CopyBoolean;
import org.jetel.connection.jdbc.specific.conn.MSAccessConnection;
import org.jetel.data.BooleanDataField;
import org.jetel.data.DataRecord;
import org.jetel.database.sql.CopySQLData;
import org.jetel.database.sql.DBConnection;
import org.jetel.database.sql.SqlConnection;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelException;
import org.jetel.graph.Node;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;

/**
 * @author slamam (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 27.1.2012
 */
public class MSAccessSpecific extends GenericODBCSpecific {

	private static final MSAccessSpecific INSTANCE = new MSAccessSpecific();
	
	private static final String CONVERT_STRING = "Convert the field to another type or use another matching field type.";
	
	protected MSAccessSpecific() {
		super();
	}

	public static MSAccessSpecific getInstance() {
		return INSTANCE;
	}
	
	@Override
	public Connection connect(Driver driver, String url, Properties info) throws SQLException {
		if ("sun.jdbc.odbc.JdbcOdbcDriver".equals(driver.getClass().getName()) &&
			!System.getProperty("os.name", "").toLowerCase().contains("windows")) {
			// prevent JVM crash on Linux due missing libodbc.so library - see CLO-2707
			throw new SQLException("Connection to MS Access using JDBC-ODBC bridge is supported on MS Windows only.");
		}
		return super.connect(driver, url, info);
	}
	
	@Override
	public SqlConnection createSQLConnection(DBConnection dbConnection, Connection connection, OperationType operationType) throws JetelException {
		return new MSAccessConnection(dbConnection, connection, operationType);
	}

	@Override
	public CopySQLData createCopyObject(int sqlType, DataFieldMetadata fieldMetadata, DataRecord record, int fromIndex,
			int toIndex) {
		
		switch(sqlType) {
		case Types.BOOLEAN:
		case Types.BIT:
			if (fieldMetadata.getDataType() == DataFieldType.BOOLEAN) {
				return new ODBCCopyBoolean(record, fromIndex, toIndex);
			} 
		}
		return super.createCopyObject(sqlType, fieldMetadata, record, fromIndex, toIndex);
	}

	@Override
	public String getDbFieldPattern() {
		//allows white spaces
		return "([\\s\\p{Alnum}\\._]+)|([\"\'][\\s\\p{Alnum}\\._ ]+[\"\'])"; 
	}
	
	@Override
	public String quoteString(String string) {
		return quoteIdentifier(string);
	}
	
	@Override
	public String quoteIdentifier(String identifier) {
        return ('[' + identifier + ']');
    }
	
	@Override
	public ConfigurationStatus checkMetadata(ConfigurationStatus status, Collection<DataRecordMetadata> metadata, Node node) {
		for (DataRecordMetadata dataRecordMetadata: metadata) {
			for (DataFieldMetadata dataField: dataRecordMetadata.getFields()) {
				switch (dataField.getDataType()) {
				case LONG:
					status.add(new ConfigurationProblem("Metadata on input port must not use field of type long " +
							"because of restrictions of used driver." + CONVERT_STRING, 
							ConfigurationStatus.Severity.ERROR, node, ConfigurationStatus.Priority.NORMAL));
					break;
				case DECIMAL:
					status.add(new ConfigurationProblem("Metadata on input port must not use field of type decimal " +
							"because of restrictions of used driver. " + CONVERT_STRING, 
							ConfigurationStatus.Severity.ERROR, node, ConfigurationStatus.Priority.NORMAL));
					break;
				}
			}
		}
		return status;
	}
    
	@Override
	public String sqlType2str(int sqlType) {
		switch(sqlType) {
		case Types.TIMESTAMP :
			return "DATETIME";
		case Types.BOOLEAN :
			return "BIT";
		case Types.INTEGER :
			return "INT";
		case Types.NUMERIC :
		case Types.DOUBLE :
			return "FLOAT";
		}
		return super.sqlType2str(sqlType);
	}

	@Override
	public int jetelType2sql(DataFieldMetadata field) {
		switch (field.getDataType()) {
		case BOOLEAN:
			return Types.BIT;
		case NUMBER:
			return Types.DOUBLE;
		default:
			return super.jetelType2sql(field);
		}
	}

	@Override
	public char sqlType2jetel(int sqlType) {
		switch (sqlType) {
		case Types.BIT:
			return DataFieldType.BOOLEAN.getShortName();
		default:
			return super.sqlType2jetel(sqlType);
		}
	}

	@Override
	public String getTablePrefix(String schema, String owner,
			boolean quoteIdentifiers) {
		String tablePrefix;
		String notNullOwner = (owner == null) ? "" : owner;
		if(quoteIdentifiers) {
			tablePrefix = quoteIdentifier(schema);
			//in case when owner is empty or null skip adding
			if(!notNullOwner.isEmpty())
				tablePrefix += quoteIdentifier(notNullOwner);
		} else {
			tablePrefix = notNullOwner.isEmpty() ? schema : (schema+"."+notNullOwner);
		}
		return tablePrefix;
	}
	
	private class ODBCCopyBoolean extends CopyBoolean {

		public ODBCCopyBoolean(DataRecord record, int fieldSQL, int fieldJetel) {
			super(record, fieldSQL, fieldJetel);
		}
		
		@Override
		public void setSQL(PreparedStatement pStatement) throws SQLException {
			if (!field.isNull()) {
				boolean value = ((BooleanDataField) field).getBoolean();
				pStatement.setBoolean(fieldSQL,	value);
			} else {
				//null value cannot be set to the boolean field -> set false
				pStatement.setBoolean(fieldSQL, false);
			}
		}
	}
}
