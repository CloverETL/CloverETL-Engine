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
import java.sql.SQLException;
import java.sql.Types;

import org.jetel.connection.jdbc.AbstractCopySQLData.CopyDouble;
import org.jetel.connection.jdbc.specific.conn.ExasolConnection;
import org.jetel.data.DataRecord;
import org.jetel.database.sql.CopySQLData;
import org.jetel.database.sql.DBConnection;
import org.jetel.database.sql.DbMetadata;
import org.jetel.database.sql.SqlConnection;
import org.jetel.exception.JetelException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Aug 19, 2013
 */
public class ExasolSpecific extends AbstractJdbcSpecific {

	private static final ExasolSpecific INSTANCE = new ExasolSpecific();

	public static ExasolSpecific getInstance() {
		return INSTANCE;
	}
	
	protected ExasolSpecific() {
		super();
	}

	@Override
	public SqlConnection createSQLConnection(DBConnection dbConnection, Connection connection,
			OperationType operationType) throws JetelException {
		return new ExasolConnection(dbConnection, connection, operationType);
	}

	@Override
	public boolean isSchemaRequired() {
		return true;
	}

	@Override
	public String quoteIdentifier(String identifier) {
        return ('"' + identifier + '"');
    }
	
	/*
	 * In Exasol, NUMERIC SQL type is an alias for INTEGER.
	 * DOUBLE should be used for DataFieldType.NUMBER instead.
	 */
	@Override
	public CopySQLData createCopyObject(int sqlType, DataFieldMetadata fieldMetadata, DataRecord record, int fromIndex,
			int toIndex) {
		DataFieldType jetelType = fieldMetadata.getDataType();
		switch (sqlType) {
		case Types.FLOAT:
		case Types.DOUBLE:
		case Types.REAL:
			if (jetelType == DataFieldType.NUMBER) {
				return new CopyDouble(record, fromIndex, toIndex);
			}
		}
		return super.createCopyObject(sqlType, fieldMetadata, record, fromIndex, toIndex);
	}

	/*
	 * Used in SQL query editor - query generation
	 */
	@Override
	public boolean isJetelTypeConvertible2sql(int sqlType, DataFieldMetadata field) {
		switch (field.getDataType()) {
			case BYTE:
			case CBYTE:
				// there is no column type that could hold a byte array
				// BLOBs are not supported
				return false;
			case BOOLEAN:
				// BOOLEAN type is returned as Types.BIT
				return (sqlType == Types.BIT) || super.isJetelTypeConvertible2sql(sqlType, field);
			case INTEGER:
			case LONG:
				return (sqlType == Types.DECIMAL) || super.isJetelTypeConvertible2sql(sqlType, field);
			default:
				return super.isJetelTypeConvertible2sql(sqlType, field);
		}
	}

	/*
	 * Used in metadata extraction
	 */
	@Override
	public char sqlType2jetel(int sqlType) {
		switch (sqlType) {
			case Types.BIT:
				// the EXAPlus SQL editor shows the column type as BOOLEAN,
				// but Types.BIT is returned, which is normally converted to a string
				return DataFieldType.BOOLEAN.getShortName();
			default:
				return super.sqlType2jetel(sqlType);
		}
	}

	/*
	 * Metadata extraction, converts DECIMAL(n, 0) to INTEGER or LONG, if possible.
	 */
	@Override
	public DataFieldType sqlType2jetel(DbMetadata dbMetadata, int sqlIndex) throws SQLException {
		int sqlType = dbMetadata.getType(sqlIndex);
		if (sqlType == Types.DECIMAL) {
			int scale = dbMetadata.getScale(sqlIndex);
			if (scale == 0) {
				int precision = dbMetadata.getPrecision(sqlIndex);
				if (precision <= 9) {
					return DataFieldType.INTEGER;
				} else if (precision <= 18) {
					return DataFieldType.LONG;
				}
			}
		}
		return super.sqlType2jetel(dbMetadata, sqlIndex);
	}

	/*
	 * DOUBLE and FLOAT are aliases, prefer DOUBLE.
	 */
	@Override
	public int jetelType2sql(DataFieldMetadata field) {
		switch (field.getDataType()) {
		case NUMBER:
			return Types.DOUBLE;
		}
		return super.jetelType2sql(field);
	}

	/*
	 * Used in Create database table.
	 */
	@Override
	public String jetelType2sqlDDL(DataFieldMetadata field) {
		switch (field.getDataType()) {
		case INTEGER:
			return "DECIMAL(10,0)";
		case LONG:
			return "DECIMAL(19,0)";
		}
		return super.jetelType2sqlDDL(field);
	}

}
