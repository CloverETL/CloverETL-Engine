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


package org.jetel.connection.jdbc.specific.impl;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;

import org.jetel.connection.jdbc.DBConnection;
import org.jetel.connection.jdbc.specific.conn.SybaseConnection;
import org.jetel.exception.JetelException;
import org.jetel.metadata.DataFieldMetadata;

/**
 * @author Agata Vackova (agata.vackova@javlinconsulting.cz) ; 
 * (c) JavlinConsulting s.r.o.
 *  www.javlinconsulting.cz
 *
 * @since Dec 19, 2008
 *
 */

public class SybaseSpecific extends AbstractJdbcSpecific {

	private static final SybaseSpecific INSTANCE = new SybaseSpecific();
	
	public static SybaseSpecific getInstance() {
		return INSTANCE;
	}

	/* (non-Javadoc)
	 * @see org.jetel.connection.jdbc.specific.impl.AbstractJdbcSpecific#createSQLConnection(org.jetel.connection.jdbc.DBConnection, org.jetel.connection.jdbc.specific.JdbcSpecific.OperationType)
	 */
	@Override
	public Connection createSQLConnection(DBConnection dbConnection, OperationType operationType) throws JetelException {
		return new SybaseConnection(dbConnection, operationType);
	}
//
//	@Override
//	public int jetelType2sql(DataFieldMetadata field) {
//		switch (field.getType()) {
//		case DataFieldMetadata.INTEGER_FIELD:
//        case DataFieldMetadata.LONG_FIELD:
//			return Types.INTEGER;
//		case DataFieldMetadata.NUMERIC_FIELD:
//			return Types.DOUBLE;
//		case DataFieldMetadata.STRING_FIELD:
//			return Types.VARCHAR;
//		case DataFieldMetadata.DATE_FIELD:
//			boolean isDate = field.isDateFormat();
//			boolean isTime = field.isTimeFormat();
//			if (isDate && isTime || StringUtils.isEmpty(field.getFormatStr())) 
//				return Types.TIMESTAMP;
//			if (isDate)
//				return Types.DATE;
//			if (isTime)
//				return Types.TIME;
//			return Types.TIMESTAMP;
//        case DataFieldMetadata.DECIMAL_FIELD:
//            return Types.DECIMAL;
//        case DataFieldMetadata.BYTE_FIELD:
//        case DataFieldMetadata.BYTE_FIELD_COMPRESSED:
//        	if (!StringUtils.isEmpty(field.getFormatStr())
//					&& field.getFormatStr().equalsIgnoreCase(DataFieldMetadata.BLOB_FORMAT_STRING)) {
//        		return Types.BLOB;
//        	}
//            return Types.VARBINARY;
//        case DataFieldMetadata.BOOLEAN_FIELD:
//        	//return Types.BIT;
//        	return Types.BOOLEAN;
//		default:
//			throw new IllegalArgumentException("Can't handle Clover's data type :"+field.getTypeAsString());
//		}
//	}

	public String quoteIdentifier(String identifier) {
		return "\"" + identifier + "\"";
	}
	
	public String sqlType2str(int sqlType) {
		switch(sqlType) {
		case Types.BOOLEAN :
			return "TINYINT";
		case Types.INTEGER :
			return "INT";
		case Types.NUMERIC :
		case Types.DOUBLE :
			return "FLOAT";
		case Types.TIMESTAMP :
			return "DATETIME";
		}
		return super.sqlType2str(sqlType);
	}

	@Override
	public String jetelType2sqlDDL(DataFieldMetadata field) {
		return super.jetelType2sqlDDL(field);
	}
    
	@Override
	public ArrayList<String> getSchemas(java.sql.Connection connection)
			throws SQLException {
		return AbstractJdbcSpecific.getMetaCatalogs(connection.getMetaData());
	}
	
}
