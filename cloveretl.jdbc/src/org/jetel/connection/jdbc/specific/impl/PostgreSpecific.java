/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2002-04  David Pavlis <david_pavlis@hotmail.com>
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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;

import org.jetel.connection.jdbc.DBConnection;
import org.jetel.connection.jdbc.specific.conn.PostgreConnection;
import org.jetel.exception.JetelException;
import org.jetel.metadata.DataFieldMetadata;

/**
 * Postgre specific behaviour.
 * 
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created Jun 3, 2008
 */
public class PostgreSpecific extends AbstractJdbcSpecific {

	private static final PostgreSpecific INSTANCE = new PostgreSpecific();
	
	public static PostgreSpecific getInstance() {
		return INSTANCE;
	}

	/* (non-Javadoc)
	 * @see org.jetel.connection.jdbc.specific.impl.AbstractJdbcSpecific#createSQLConnection(org.jetel.connection.jdbc.DBConnection, org.jetel.connection.jdbc.specific.JdbcSpecific.OperationType)
	 */
	@Override
	public Connection createSQLConnection(DBConnection connection, OperationType operationType) throws JetelException {
		return new PostgreConnection(connection, operationType);
	}

    public String quoteIdentifier(String identifier) {
        return ('"' + identifier + '"');
    }

    
	public String sqlType2str(int sqlType) {
		switch(sqlType) {
		case Types.BINARY :
		case Types.VARBINARY :
			return "BYTEA";
		case Types.NUMERIC :
			return "REAL";
		case Types.DECIMAL :
			return "NUMERIC";
		}
		return super.sqlType2str(sqlType);
	}

	@Override
	public String jetelType2sqlDDL(DataFieldMetadata field) {
		int type = jetelType2sql(field);
		switch(type) {
		case Types.VARBINARY :
		case Types.BINARY : 
			return sqlType2str(type); 
		}
		return super.jetelType2sqlDDL(field);
	}

	
  public ArrayList<String> getSchemas(java.sql.Connection connection)
      throws SQLException {

    ArrayList<String> tmp;

    ArrayList<String> schemas = new ArrayList<String>();

    DatabaseMetaData dbMeta = connection.getMetaData();

    // add schemas
    tmp = getMetaSchemas(dbMeta);
    if (tmp != null) {
      schemas.addAll(tmp);
    }

    //catalogs not added - postgresql allows only catalog specified in connection url, cannot get matadata from other catalogs 
    // add catalogs

    return schemas;
  }
	
	
  public ResultSet getTables(java.sql.Connection connection, String dbName) throws SQLException {
    return connection.getMetaData().getTables(null, dbName, "%", new String[] {"TABLE", "VIEW"}/*tableTypes*/);
  }
	
}
