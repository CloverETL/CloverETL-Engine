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

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

import org.jetel.connection.jdbc.AbstractCopySQLData;
import org.jetel.connection.jdbc.AbstractCopySQLData.CopyDecimal;
import org.jetel.connection.jdbc.SQLUtil;
import org.jetel.connection.jdbc.specific.conn.SQLiteConnection;
import org.jetel.data.DataRecord;
import org.jetel.data.DecimalDataField;
import org.jetel.database.sql.CopySQLData;
import org.jetel.database.sql.DBConnection;
import org.jetel.database.sql.QueryType;
import org.jetel.database.sql.SqlConnection;
import org.jetel.exception.JetelException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;

/**
 * A JdbcSpecific for SQLite serverless database
 *  
 * @author Pavel Najvar (pavel.najvar@javlin.eu)
 *         (c) Javlin, a.s. (www.javlin.eu)
 *
 * @created Sep 2009
 */
public class SQLiteSpecific extends AbstractJdbcSpecific {

	private static final SQLiteSpecific INSTANCE = new SQLiteSpecific();
	
	public static SQLiteSpecific getInstance() {
		return INSTANCE;
	}

	protected SQLiteSpecific() {
		super();
	}

	@Override
	public SqlConnection createSQLConnection(DBConnection dbConnection, Connection connection, OperationType operationType) throws JetelException {
		return new SQLiteConnection(dbConnection, connection, operationType);
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.connection.jdbc.specific.impl.AbstractJdbcSpecific#createCopyObject(int, org.jetel.metadata.DataFieldMetadata, org.jetel.data.DataRecord, int, int)
	 */
	@Override
	public CopySQLData createCopyObject(int sqlType, DataFieldMetadata fieldMetadata, DataRecord record, int fromIndex, int toIndex) {
		CopySQLData copyObj = super.createCopyObject(sqlType, fieldMetadata, record, fromIndex, toIndex);

		//if CopyDecimal was considered to be used we need to create slightly different copy object, since
		//the SQLite JDBC driver does not support BigDecimal data type - PreparedStatement.getBigDecimal() method
		//is not supported - decimal value has to be retrieved by getDouble() method
		if (copyObj instanceof CopyDecimal) {
			copyObj = new CopyDecimalAsDouble(record, fromIndex, toIndex);
			copyObj.setSqlType(sqlType);
		}
		
		return copyObj;
	}
	
	@Override
	public String quoteIdentifier(String identifier) {
        return ('"' + identifier + '"');
    }
	
	@Override
	public String getValidateQuery(String query, QueryType queryType, boolean optimizeSelectQuery) throws SQLException {
		if(queryType==QueryType.SELECT) {
			query = SQLUtil.removeUnnamedFields(query, this);
			String q;
			if (optimizeSelectQuery) {
				q = "SELECT wrapper_table.* FROM (" + query + ") wrapper_table limit 1";
			} else {
				q = query;
			}
			return q;
		}
		return super.getValidateQuery(query, queryType, optimizeSelectQuery);
	}
	
	@Override
	public String getTablePrefix(String schema, String owner,
			boolean quoteIdentifiers) {
		int position = schema.indexOf('[');
		schema = schema.substring(0, position - 1);
		return quoteIdentifiers ? quoteIdentifier(schema) : schema;
	}

	@Override
	public void optimizeResultSet(ResultSet resultSet,
			OperationType operationType) {

		switch (operationType){
		case READ:
			try {
				resultSet.setFetchDirection(ResultSet.FETCH_FORWARD);
				// SQLite driver MUST HAVE fetch size set to 0 - otherwise it limits number of results returned
				resultSet.setFetchSize(0);
			} catch(SQLException ex) {
				//TODO: for now, do nothing
			}
		}

	}
	
	@Override
	public List<Integer> getFieldTypes(ResultSetMetaData resultSetMetadata, DataRecordMetadata cloverMetadata) throws SQLException {
		return SQLUtil.getFieldTypes(cloverMetadata, this);
	}	
	
	/**
	 * This implementation of copy object is very similar to {@link CopyDecimal} class.
	 * However, data from database are retrieved as double data type - not BigDecimal. 
	 */
	private static class CopyDecimalAsDouble extends AbstractCopySQLData {

		public CopyDecimalAsDouble(DataRecord record, int fieldSQL, int fieldJetel) {
			super(record, fieldSQL, fieldJetel);
		}

		@Override
		public void setJetel(ResultSet resultSet) throws SQLException {
			double d = resultSet.getDouble(fieldSQL);
			if (resultSet.wasNull()) {
				((DecimalDataField) field).setNull(true);
			} else {
				((DecimalDataField) field).setValue(d);
			}
		}

		@Override
		public void setJetel(CallableStatement statement) throws SQLException {
			double d = statement.getDouble(fieldSQL);
			if (statement.wasNull()) {
				((DecimalDataField) field).setNull(true);
			} else {
				((DecimalDataField) field).setValue(d);
			}
		}

		@Override
		public void setSQL(PreparedStatement pStatement) throws SQLException {
			if (!field.isNull()) {
				pStatement.setDouble(fieldSQL, ((DecimalDataField) field).getDouble());
			} else {
				pStatement.setNull(fieldSQL, java.sql.Types.DECIMAL);
			}

		}
		
		@Override
		public Object getDbValue(ResultSet resultSet) throws SQLException {
			double d = resultSet.getDouble(fieldSQL);
			return resultSet.wasNull() ? null : d;
		}

		@Override
		public Object getDbValue(CallableStatement statement) throws SQLException {
			double d = statement.getDouble(fieldSQL);
			return statement.wasNull() ? null : d;
		}
		
	}
	
}
