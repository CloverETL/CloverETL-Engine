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
package org.jetel.connection.jdbc;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;
import java.util.Map.Entry;

import org.jetel.data.DataRecord;
import org.jetel.database.sql.CopySQLData;
import org.jetel.database.sql.JdbcSpecific.OperationType;
import org.jetel.database.sql.SqlConnection;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.string.StringUtils;

/**
 * @author Agata Vackova (agata.vackova@javlinconsulting.cz) ; 
 * (c) JavlinConsulting s.r.o.
 *  www.javlinconsulting.cz
 *
 * @since Feb 4, 2008
 *
 */

public class SQLCloverCallableStatement {
	
	public static final String RESULT_SET_OUTPARAMETER_NAME = "result_set";
	
	protected int RESULT_SET_OUTPARAMETER_TYPE = Types.OTHER;
	
	protected String query;
	protected SqlConnection connection;
	protected CallableStatement statement;
	protected CopySQLData[] inTransMap, outTransMap, resultOutMap;
	protected DataRecord inRecord, outRecord;
	protected Map<Integer, String> inParameters, outParameters;
	protected String[] outputFields;
	protected ResultSet resultSet;
	protected int resultSetOutParameterNumber = -1;
	protected int resultSetOutParameterType = RESULT_SET_OUTPARAMETER_TYPE;
	
	private boolean gotOutParams;

	public SQLCloverCallableStatement(SqlConnection connection, String query, DataRecord inRecord, DataRecord outRecord) {
		this.query = query;
		this.connection = connection;
		this.inRecord = inRecord;
		this.outRecord = outRecord;
	}

	public SQLCloverCallableStatement(SqlConnection connection, String query, DataRecord inRecord, DataRecord outRecord,
			int resultSetOutParameterType) {
		this(connection, query, inRecord, outRecord);
		this.resultSetOutParameterType = resultSetOutParameterType;
	}
	
	public Map<Integer, String> getInParameters() {
		return inParameters;
	}

	public void setInParameters(Map<Integer, String> inParameters) {
		this.inParameters = inParameters;
	}

	public Map<Integer, String> getOutParameters() {
		return outParameters;
	}

	public void setOutParameters(Map<Integer, String> outParameters) {
		this.outParameters = outParameters;
	}
	
	public String[] getOutputFields() {
		return outputFields;
	}

	public void setOutputFields(String[] outputFields) {
		this.outputFields = outputFields;
	}

	public boolean prepareCall() throws SQLException, ComponentNotReadyException{
		statement = connection.prepareCall(query);
		int fieldNumber;
		int parameterNumber;
		//prepare transition map for input parameters
		if (inRecord != null && inParameters != null) {
			DataRecordMetadata inMetadata = inRecord.getMetadata();
			inTransMap = new CopySQLData[inParameters.size()];
			int i = 0;
			for (Entry<Integer, String> entry : inParameters.entrySet()) {
				parameterNumber = entry.getKey();
				fieldNumber = inMetadata.getFieldPosition(entry.getValue());
				if (fieldNumber == -1) {
					throw new ComponentNotReadyException("Field " + StringUtils.quote(entry.getValue()) + " doesn't exist in metadata " +
							StringUtils.quote(inMetadata.getName()));
				}
				inTransMap[i++] = connection.getJdbcSpecific().createCopyObject(connection.getJdbcSpecific().jetelType2sql(inMetadata.getField(fieldNumber)), 
						inMetadata.getField(fieldNumber), inRecord, parameterNumber - 1, fieldNumber);
			}
		}
		//prepare transition map for output parameters, register parameter number for output result set
		if (outParameters != null) {
			DataRecordMetadata outMetadata = outRecord.getMetadata();
			outTransMap = new CopySQLData[outParameters.containsValue(RESULT_SET_OUTPARAMETER_NAME) ? outParameters.size() - 1 : 
				outParameters.size()];
			int sqlType;
			int i = 0;
			for (Entry<Integer, String> entry : outParameters.entrySet()) {
				parameterNumber = entry.getKey();
				fieldNumber = outMetadata.getFieldPosition(entry.getValue());
				sqlType = entry.getValue().equalsIgnoreCase(RESULT_SET_OUTPARAMETER_NAME) ? resultSetOutParameterType : 
					connection.getJdbcSpecific().jetelType2sql(outMetadata.getField(fieldNumber));
				if (sqlType != resultSetOutParameterType) {
					outTransMap[i++] = connection.getJdbcSpecific().createCopyObject(sqlType, outMetadata.getField(fieldNumber),
							outRecord, parameterNumber - 1, fieldNumber);
				}else {
					resultSetOutParameterNumber = parameterNumber;
				}
				statement.registerOutParameter(parameterNumber, sqlType);
			}
		}
		return true;
	}

	/**
	 * Executes call and prepares result set for reading
	 * 
	 * @throws SQLException
	 */
	public void executeCall() throws SQLException{
		if (inTransMap != null) {
			for (int i = 0; i < inTransMap.length; i++) {
				inTransMap[i].jetel2sql(statement);
			}
		}
//      don't work on Sybase		
//		resultSet = statement.executeQuery();
//		Petr Uher recommended use statement.executeUpdate() if will return results in query 
//		statement.executeUpdate();
		
		if (outputFields != null) {
			statement.execute();
			resultSet = statement.getResultSet();
		}else {
			statement.executeUpdate();
		}
		
		if (resultSetOutParameterNumber > -1) {
			resultSet = (ResultSet)statement.getObject(resultSetOutParameterNumber);
		}
		
		if (resultSet != null) {
			connection.getJdbcSpecific().optimizeResultSet(resultSet, OperationType.READ);
		}
		
		gotOutParams = false;
	}
	
	/**
	 * Checks if there is next output record
	 * 
	 * @return true if next output record was fulfilled or false if there is no next result
	 * @throws SQLException
	 */
	public boolean isNext() throws SQLException{
		if (outRecord == null || gotOutParams) {//no output or output parameters has been got
			return false;
		}
		
		//get out parameters 
		if (outTransMap != null && !gotOutParams && outTransMap.length > 0 ) {
			for (int i = 0; i < outTransMap.length; i++) {
				try {
					outTransMap[i].setJetel(statement);
				} catch (SQLException ex) {
					throw new SQLException("Error on field " + ((AbstractCopySQLData)outTransMap[i]).field.getMetadata().getName(), ex);
				} catch (ClassCastException ex){
				    throw new SQLException("Incompatible Clover & JDBC field types - field "+((AbstractCopySQLData)outTransMap[i]).field.getMetadata().getName()+
				            " Clover type: "+SQLUtil.jetelType2Str(((AbstractCopySQLData)outTransMap[i]).field.getMetadata().getType()), ex);
				}
			}
			gotOutParams = true;
			return true;
		}

		//check result set
		if(outputFields == null){
			return false;
		}else if (resultSet == null) {
			throw new SQLException("No result set for this query.");
		}else if (!resultSet.next()){
			return false;
		}

		// init transMap if null
		if (resultOutMap == null){
			resultOutMap = AbstractCopySQLData.sql2JetelTransMap(SQLUtil.getFieldTypes(resultSet.getMetaData()),
					outRecord.getMetadata(), outRecord, outputFields, connection.getJdbcSpecific());
		}
			
		for (int i = 0; i < resultOutMap.length; i++) {
			resultOutMap[i].sql2jetel(resultSet);
		}
		
		return true;
	}

	public DataRecord getOutRecord() {
		return outRecord;
	}

	public void setInRecord(DataRecord inRecord) {
		this.inRecord = inRecord;
		AbstractCopySQLData.resetDataRecord(inTransMap, inRecord);
	}

	public void close() throws SQLException {
		statement.close();
	}

}