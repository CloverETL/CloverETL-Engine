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

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.jetel.data.DataField;
import org.jetel.data.DataRecord;

/**
 *  Class for creating mappings between CloverETL's DataRecords and JDBC's
 *  ResultSets.<br>
 *
 * @author      dpavlis
 * @since       October 7, 2002
 * @created     8. ???ervenec 2003
 */
public interface CopySQLData {

	public int getFieldJetel();
	
	public DataField getField();
	
	/**
	 * Assigns different DataField than the original used when creating
	 * copy object
	 * 
	 * @param field	New DataField
	 */
	public void setCloverField(DataField field);
	
	/**
	 * Assings different DataField (DataRecord). The proper field
	 * is taken from DataRecord based on previously saved field number.
	 * 
	 * @param record	New DataRecord
	 */
	public void setCloverRecord(DataRecord record);
    
     /**
     * @return Returns the inBatchUpdate.
     */
    public boolean isInBatchUpdate();
    
    public int getSqlType();
    
	public void setSqlType(int sqlType);

	/**
     * @param inBatchUpdate The inBatchUpdate to set.
     */
    public void setInBatchUpdate(boolean inBatch);
	
	/**
	 *  Sets value of Jetel/Clover data field based on value from SQL ResultSet
	 *
	 * @param  resultSet         Description of Parameter
	 * @exception  SQLException  Description of Exception
	 * @since                    October 7, 2002
	 */
	public void sql2jetel(ResultSet resultSet) throws SQLException;
	
	/**
	 *  Sets value of SQL field in PreparedStatement based on Jetel/Clover data
	 *  field's value
	 *
	 * @param  pStatement        Description of Parameter
	 * @exception  SQLException  Description of Exception
	 * @since                    October 7, 2002
	 */
	public void jetel2sql(PreparedStatement pStatement) throws SQLException;

	/**
	 * @param resultSet
	 * @return current value from result set from corresponding index
	 * @throws SQLException
	 */
	public Object getDbValue(ResultSet resultSet) throws SQLException;

	/**
	 * @param statement
	 * @return current value from callable statement from corresponding index
	 * @throws SQLException
	 */
	public Object getDbValue(CallableStatement statement) throws SQLException;
	
	/**
	 * @return current value from data record from corresponding field
	 */
	public Object getCloverValue();

	/**
	 *  Sets the Jetel attribute of the CopySQLData object
	 *
	 * @param  resultSet         The new Jetel value
	 * @exception  SQLException  Description of Exception
	 * @since                    October 7, 2002
	 */
	public void setJetel(ResultSet resultSet) throws SQLException;

	public void setJetel(CallableStatement statement) throws SQLException;

	/**
	 *  Sets the SQL attribute of the CopySQLData object
	 *
	 * @param  pStatement        The new SQL value
	 * @exception  SQLException  Description of Exception
	 * @since                    October 7, 2002
	 */
	public void setSQL(PreparedStatement pStatement) throws SQLException;

}
