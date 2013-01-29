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

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;

import org.jetel.connection.jdbc.specific.conn.PervasiveConnection;
import org.jetel.database.sql.DBConnection;
import org.jetel.database.sql.SqlConnection;
import org.jetel.exception.JetelException;
import org.jetel.metadata.DataFieldMetadata;

/**
 * @author lkrejci (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 29 Apr 2011
 */
public class PervasiveSpecific extends AbstractJdbcSpecific {
	
	private static final PervasiveSpecific INSTANCE = new PervasiveSpecific();
	
	public static PervasiveSpecific getInstance() {
		return INSTANCE;
	}

	@Override
	protected SqlConnection prepareSQLConnection(DBConnection dbConnection, OperationType operationType) throws JetelException {
		return new PervasiveConnection(dbConnection, operationType, getAutoKeyType());
	}

	@Override
	public ArrayList<String> getSchemas(SqlConnection connection) throws SQLException {
		return getMetaCatalogs(connection.getMetaData());
	}
	
	@Override
	public String jetelType2sqlDDL(DataFieldMetadata field) {
		int sqlType = jetelType2sql(field);
		
		switch(sqlType) {
		case Types.BINARY:
		case Types.VARBINARY:
			return sqlType2str(sqlType);
		default :
			return super.jetelType2sqlDDL(field);
		}
	}
	
	@Override
	public String sqlType2str(int sqlType) {
		switch(sqlType) {
		case Types.BOOLEAN:
			return "BIT";
		case Types.VARBINARY:
			return "LONGVARBINARY";
		}
		return super.sqlType2str(sqlType);
	}
	
	@Override
	public int jetelType2sql(DataFieldMetadata field) {
		switch (field.getType()) {
		case DataFieldMetadata.BOOLEAN_FIELD:
			return Types.BIT;
        case DataFieldMetadata.BYTE_FIELD:
        case DataFieldMetadata.BYTE_FIELD_COMPRESSED:
        	if (field.hasFormat() && field.getFormat().equalsIgnoreCase(DataFieldMetadata.BLOB_FORMAT_STRING)) {
        		return Types.BLOB;
        	}
            return field.isFixed() ? Types.BINARY : Types.LONGVARBINARY;
        case DataFieldMetadata.NUMERIC_FIELD:
			return Types.DOUBLE;
		default:
			return super.jetelType2sql(field);
		}
	}
	
	@Override
	public char sqlType2jetel(int sqlType) {
		switch (sqlType) {
			case Types.BIT:
				return DataFieldMetadata.BOOLEAN_FIELD;
			default:
				return super.sqlType2jetel(sqlType);
		}
	}

}
