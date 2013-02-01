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
package org.jetel.connection.jdbc.specific.conn;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.jetel.database.sql.DBConnection;
import org.jetel.database.sql.JdbcSpecific.OperationType;
import org.jetel.exception.JetelException;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 1.2.2013
 */
public class SQLiteConnection extends BasicSqlConnection {

	public SQLiteConnection(DBConnection dbConnection, Connection connection, OperationType operationType) throws JetelException {
		super(dbConnection, connection, operationType);
	}
	
	@Override
	public List<String> getSchemas() throws SQLException {
		Statement s = connection.createStatement();
		
		ResultSet rs = s.executeQuery("pragma database_list");
		ArrayList<String> dbList = new ArrayList<String>();
		String tmp;
		if (rs != null) while(rs.next()) {
			tmp = rs.getString(2) + " [" + rs.getString(3) + "]";
			dbList.add(tmp);
		}
		
		return dbList;
	}

	@Override
	public ResultSet getTables(String schema) throws SQLException {
		Statement s = connection.createStatement();
		// -pnajvar
		// this is a bit weird, but the result set must have 3rd column the table name
		ResultSet rs = s.executeQuery("select tbl_name, tbl_name, tbl_name from sqlite_master order by tbl_name");
		return rs;
	}

}
