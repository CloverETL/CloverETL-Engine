/*
*    jETeL/Clover.ETL - Java based ETL application framework.
*    Copyright (C) 2002-2004  David Pavlis <david_pavlis@hotmail.com>
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
*/

package org.jetel.metadata;

import org.jetel.database.DBConnection;

/**
 * @author dpavlis
 * @since  20.10.2004
 *
 * Stub class to keep certain parameters for metadata generation
 * based on JDBC ResultSetMetadata. Used during loading graph definition
 * from XML.
 */
public class DataRecordMetadataJDBCStub {
	
	DBConnection dbConnection;
	String sqlQuery;

	public DataRecordMetadataJDBCStub(DBConnection dbConnection,String sqlQuery){
		this.dbConnection=dbConnection;
		this.sqlQuery=sqlQuery;
	}
	
	public DBConnection getDBConnection(){
		return dbConnection;
	}
	
	public String getSQLQuery(){
		return sqlQuery;
	}
}
