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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;

import org.jetel.connection.jdbc.specific.conn.SybaseConnection;
import org.jetel.database.sql.DBConnection;
import org.jetel.database.sql.SqlConnection;
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

	@Override
	protected SqlConnection prepareSQLConnection(DBConnection dbConnection, OperationType operationType) throws JetelException {
		return new SybaseConnection(dbConnection, operationType);
	}

	@Override
	public String quoteIdentifier(String identifier) {
		return "\"" + identifier + "\"";
	}
	
	@Override
	public String sqlType2str(int sqlType) {
		switch(sqlType) {
		case Types.BOOLEAN :
			return "BIT";
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
	public int jetelType2sql(DataFieldMetadata field) {
		switch (field.getType()) {
		case DataFieldMetadata.NUMERIC_FIELD:
			return Types.DOUBLE;
		case DataFieldMetadata.BOOLEAN_FIELD:
			return Types.BIT;
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
    
	@Override
	public ArrayList<String> getSchemas(SqlConnection connection) throws SQLException {
		return AbstractJdbcSpecific.getMetaCatalogs(connection.getMetaData());
	}

	@Override
	public ResultSet getTables(SqlConnection connection, String dbName) throws SQLException {
		Statement s = connection.createStatement();
		s.execute("USE " + dbName);		
		return s.executeQuery("EXECUTE sp_tables @table_type = \"'TABLE', 'VIEW'\"");
	}

	@Override
	public String getTablePrefix(String schema, String owner,
			boolean quoteIdentifiers) {
		if (quoteIdentifiers) {
			return quoteIdentifier(schema) + "." + quoteIdentifier(owner);
		} else {
			return schema + "." + owner;
		}
	}
	

	
}
