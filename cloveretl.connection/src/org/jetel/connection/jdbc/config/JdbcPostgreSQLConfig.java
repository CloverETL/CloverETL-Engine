/*
 *    jETeL/Clover - Java based ETL application framework.
 *    Copyright (C) 2002-2008  David Pavlis <david.pavlis@centrum.cz> and others.
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
 * Created on 5.3.2008 by dadik
 *
 */

/**
 * 
 */
package org.jetel.connection.jdbc.config;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.jetel.connection.jdbc.config.JdbcBaseConfig.OperationType;

/**
 * @author dadik
 *
 */
public class JdbcPostgreSQLConfig extends JdbcBaseConfig {
	
	public static int DEFAULT_FETCH_SIZE=50;

	private static JdbcPostgreSQLConfig instance=new JdbcPostgreSQLConfig();
	
	protected JdbcPostgreSQLConfig(){
		super();
	}

	public static JdbcPostgreSQLConfig getInstance(){
		return instance;
	}

	public String getTargetDBName(){
		return "POSTGRESQL";
	}
	
	public int getTargetDBMajorVersion(){
		return -1;
	}
	
	
	public Statement createStatemetn(Connection con, OperationType operType) throws SQLException{
		Statement stm=super.createStatement(con,operType);
		switch (operType) {
			case READ:
					stm.setFetchSize(DEFAULT_FETCH_SIZE);
				break;
		}
		return stm;
	}
	

	public void optimizeResultSet(ResultSet res,OperationType operType){
		switch(operType){
		case READ:
			try{
				res.setFetchDirection(ResultSet.FETCH_FORWARD);
				res.setFetchSize(DEFAULT_FETCH_SIZE);
			}catch(SQLException ex){
				//TODO: for now, do nothing
			}
		}
	}
	

}
