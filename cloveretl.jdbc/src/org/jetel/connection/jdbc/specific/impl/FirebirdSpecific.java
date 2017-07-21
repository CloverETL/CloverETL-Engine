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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.regex.Pattern;

import org.jetel.connection.jdbc.specific.conn.FirebirdConnection;
import org.jetel.database.sql.DBConnection;
import org.jetel.database.sql.SqlConnection;
import org.jetel.exception.JetelException;
import org.jetel.metadata.DataFieldMetadata;

/**
 * @author "Jan Kucera" (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Apr 22, 2011
 */
public class FirebirdSpecific extends AbstractJdbcSpecific {

	private static final FirebirdSpecific INSTANCE = new FirebirdSpecific();
	private static final String ORACLE_TYPES_CLASS_NAME =  "org.firebirdsql.jdbc.field";
	/** the SQL comments pattern conforming to the SQL standard */
	private static final Pattern COMMENTS_PATTERN = Pattern.compile("--[^\r\n]*|/\\*(?!\\+).*?\\*/", Pattern.DOTALL);
	
	public static FirebirdSpecific getInstance() {
		return INSTANCE;
	}

	private FirebirdSpecific() {
		super();
	}
	
	@Override
	public SqlConnection createSQLConnection(DBConnection dbConnection, Connection connection, OperationType operationType) throws JetelException {
		return new FirebirdConnection(dbConnection, connection, operationType); 
	}

	@Override
	public String sqlType2str(int sqlType) {
		switch(sqlType) {
			case Types.VARCHAR :
				return "VARCHAR";	
			case Types.BOOLEAN :
				return "CHAR(1)";
			case Types.TIME :
			case Types.DATE :
			case Types.TIMESTAMP :
				return "TIMESTAMP";
			case Types.INTEGER :
				return "INTEGER";
			case Types.NUMERIC :
				return "DOUBLE PRECISION";
			case Types.BIGINT :
				return "BIGINT";
			case Types.VARBINARY :
			case Types.BINARY :
				return "CHAR";
		}
		return super.sqlType2str(sqlType);
	}
	
	@Override
	public int jetelType2sql(DataFieldMetadata field) {
		// TODO Auto-generated method stub
		return super.jetelType2sql(field);
	}

	@Override
	public String jetelType2sqlDDL(DataFieldMetadata field) {
		// TODO Auto-generated method stub
		return super.jetelType2sqlDDL(field);
	}
	
	@Override
	public char sqlType2jetel(int sqlType, int sqlPrecision) {
		switch(sqlType) {
			case Types.CHAR:
				return (sqlPrecision > 1) ? 
							DataFieldMetadata.STRING_FIELD : DataFieldMetadata.BOOLEAN_FIELD;
			default :
				return sqlType2jetel(sqlType);
		}
	
	}
	
	@Override
	public boolean isJetelTypeConvertible2sql(int sqlType, DataFieldMetadata field) {
		switch (field.getDataType()) {
		case BOOLEAN:
			switch (sqlType) {
			case Types.CHAR:
			case Types.NUMERIC:
				return true;
			}
		case BYTE:
		case CBYTE:
			switch (sqlType) {
			case Types.VARBINARY:
			case Types.LONGVARBINARY:
				return true;
			}
		case DECIMAL:
		case NUMBER:
		case LONG:
		case INTEGER:
			switch (sqlType) {
			case Types.NUMERIC:
				return true;
			}
		case DATE:
		case DATETIME:
			switch (sqlType) {
			case Types.TIMESTAMP:
				return true;
			}
		default:
			return super.isJetelTypeConvertible2sql(sqlType, field);
		}
	}
	
	@Override
	public String getTypesClassName() {
		return ORACLE_TYPES_CLASS_NAME;
	}

	@Override
	public Pattern getCommentsPattern() {
		return COMMENTS_PATTERN;
	}

	@Override
	public String quoteIdentifier(String identifier) {
		return ('"' + identifier + '"');
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
	public String getTablePrefix(String schema, String owner, boolean quoteIdentifiers) {
		return "";
	}
}
