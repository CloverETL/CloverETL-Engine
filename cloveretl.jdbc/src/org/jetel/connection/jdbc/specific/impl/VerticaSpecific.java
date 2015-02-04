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

import org.jetel.connection.jdbc.specific.conn.VerticaConnection;
import org.jetel.database.sql.DBConnection;
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
public class VerticaSpecific extends AbstractJdbcSpecific {

	private static final VerticaSpecific INSTANCE = new VerticaSpecific();

	/**
	 * Vertica SQL type identifiers for intervals (101-113).
	 * Follows ODBC standard.
	 * @see Types
	 */
	public static final int INTERVAL_YEAR = 101;
	public static final int INTERVAL_MONTH = 102;
	public static final int INTERVAL_DAY = 103;
	public static final int INTERVAL_HOUR = 104;
	public static final int INTERVAL_MINUTE = 105;
	public static final int INTERVAL_SECOND = 106;
	public static final int INTERVAL_YEAR_TO_MONTH = 107;
	public static final int INTERVAL_DAY_TO_HOUR = 108;
	public static final int INTERVAL_DAY_TO_MINUTE = 109;
	public static final int INTERVAL_DAY_TO_SECOND = 110;
	public static final int INTERVAL_HOUR_TO_MINUTE = 111;
	public static final int INTERVAL_HOUR_TO_SECOND = 112;
	public static final int INTERVAL_MINUTE_TO_SECOND = 113;

	
	public static VerticaSpecific getInstance() {
		return INSTANCE;
	}
	
	protected VerticaSpecific() {
		super();
	}

	@Override
	public SqlConnection createSQLConnection(DBConnection dbConnection, Connection connection,
			OperationType operationType) throws JetelException {
		return new VerticaConnection(dbConnection, connection, operationType);
	}

	@Override
	public boolean isSchemaRequired() {
		return true;
	}

	@Override
	public String quoteIdentifier(String identifier) {
        return ('"' + identifier + '"');
    }

	@Override
	public boolean canCloseResultSetBeforeCreatingNewOne() {
		return false; // CLO-678
	}
	
	/*
	 * Used in metadata extraction
	 */
	@Override
	public char sqlType2jetel(int sqlType) {
		if (isIntervalType(sqlType)) {
			return DataFieldType.STRING.getShortName();
		}
		switch (sqlType) {
			case Types.OTHER:
				// Vertica interval types and UUID
				return DataFieldType.STRING.getShortName();
			case Types.BIT:
				// the type of boolean columns is returned as Types.BIT,
				// which is normally converted to a string
				return DataFieldType.BOOLEAN.getShortName();
			case Types.INTEGER:
				// Integer is an alias for all fixed-length integer data types, their range exactly matches Java long (64bit)
				return DataFieldType.LONG.getShortName();
			case Types.FLOAT:
				// Float is an alias for all floating point data types, their range exactly matches Java double (64bit)
				return DataFieldType.NUMBER.getShortName();
			case Types.NUMERIC:
				// Numeric is an alias for all decimal data types with fixed precision, they map to Java BigDecimal
				return DataFieldType.DECIMAL.getShortName();
			default:
				return super.sqlType2jetel(sqlType);
		}
	}

	/*
	 * Used in SQL query editor - query generation
	 * and in DBOutputTable checkConfig.
	 */
	@Override
	public boolean isJetelTypeConvertible2sql(int sqlType, DataFieldMetadata field) {
		if ((sqlType == Types.OTHER) || isIntervalType(sqlType)) {
			// Vertica interval types and UUID
			return field.getDataType() == DataFieldType.STRING;
		}
		switch (field.getDataType()) {
			case BOOLEAN:
				// BOOLEAN type is returned as Types.BIT
				return (sqlType == Types.BIT) || super.isJetelTypeConvertible2sql(sqlType, field);
			case INTEGER:
			case LONG:
				return (sqlType == Types.BIGINT) || super.isJetelTypeConvertible2sql(sqlType, field);
			case DECIMAL:
				return (sqlType == Types.NUMERIC) || super.isJetelTypeConvertible2sql(sqlType, field);
			case BYTE: 
			case CBYTE:
				return (sqlType == Types.LONGVARBINARY) || super.isJetelTypeConvertible2sql(sqlType, field);
			case STRING:
				switch (sqlType) {
				case Types.LONGVARCHAR:
				case Types.LONGNVARCHAR:
					return true;
				}
			default:
				return super.isJetelTypeConvertible2sql(sqlType, field);
		}
	}
	
	/**
	 * Returns <code>true</code> if the <code>sqlType</code> 
	 * is one of the INTERVAL_* constants.
	 * 
	 * @param sqlType - SQL type identifier
	 * @return <code>true</code> if <code>sqlType</code> is an interval
	 */
	public static boolean isIntervalType(int sqlType) {
		return (sqlType >= INTERVAL_YEAR) && (sqlType <= INTERVAL_MINUTE_TO_SECOND);
	}
}
