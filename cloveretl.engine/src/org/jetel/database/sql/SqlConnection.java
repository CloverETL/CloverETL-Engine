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
package org.jetel.database.sql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

/**
 * Clover specific extension of regular java.sql.Connection interface
 * 
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created May 29, 2008
 * @see DBConnection#getConnection(String)
 */
public interface SqlConnection extends Connection {

	public JdbcSpecific getJdbcSpecific();
	
	public boolean isTransactionsSupported();
	
	public List<String> getSchemas() throws SQLException;
	
	/**
	 * Returns a ResultSet representing tables in given database
	 * It has to extract it from dbMeta object
	 * 
	 * @param schema
	 * @return
	 */
	public ResultSet getTables(String schema) throws SQLException;

	/**
	 * Returns columns metadata for the given table.
	 * @param schema
	 * @param table
	 * @return
	 * @throws SQLException 
	 */
	public ResultSetMetaData getColumns(String schema, String owner, String table) throws SQLException;

}
