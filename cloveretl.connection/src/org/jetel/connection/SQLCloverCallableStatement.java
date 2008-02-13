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


package org.jetel.connection;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Map.Entry;

import org.jetel.data.DataRecord;
import org.jetel.metadata.DataRecordMetadata;

import com.sun.java_cup.internal.internal_error;

/**
 * @author Agata Vackova (agata.vackova@javlinconsulting.cz) ; 
 * (c) JavlinConsulting s.r.o.
 *  www.javlinconsulting.cz
 *
 * @since Feb 4, 2008
 *
 */

public class SQLCloverCallableStatement {
	
	private String query;
	private Connection connection;
	private CallableStatement statement;
	private CopySQLData[] inTransMap, outTransMap, resultOutMap;
	private DataRecord inRecord, outRecord;
	private Map<Integer, String> inParameters, outParameters;
	private String[] outputFields;
	private ResultSet resultSet;

	public SQLCloverCallableStatement(Connection connection, String query, DataRecord inRecord, DataRecord outRecord) {
		this.query = query;
		this.connection = connection;
		this.inRecord = inRecord;
		this.outRecord = outRecord;
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

	public boolean prepareCall() throws SQLException{
		statement = connection.prepareCall(query);
		int fieldNumber;
		int parameterNumber;
		if (inRecord != null && inParameters != null) {
			DataRecordMetadata inMetadata = inRecord.getMetadata();
			inTransMap = new CopySQLData[inParameters.size()];
			int i = 0;
			for (Entry<Integer, String> entry : inParameters.entrySet()) {
				parameterNumber = entry.getKey();
				fieldNumber = inMetadata.getFieldPosition(entry.getValue());
				inTransMap[i++] = CopySQLData.createCopyObject(SQLUtil.jetelType2sql(inMetadata.getField(fieldNumber)), 
						inMetadata.getField(fieldNumber), inRecord, parameterNumber - 1, fieldNumber);
			}
		}
		if (outParameters != null) {
			DataRecordMetadata outMetadata = outRecord.getMetadata();
			outTransMap = new CopySQLData[outParameters.size()];
			int sqlType;
			if (outParameters != null) {
				int i = 0;
				for (Entry<Integer, String> entry : outParameters.entrySet()) {
					parameterNumber = entry.getKey();
					fieldNumber = outMetadata.getFieldPosition(entry.getValue());
					sqlType = SQLUtil.jetelType2sql(outMetadata.getField(fieldNumber));
					outTransMap[i++] = CopySQLData.createCopyObject(sqlType, 
							outMetadata.getField(fieldNumber), outRecord, parameterNumber - 1, fieldNumber);
					statement.registerOutParameter(parameterNumber, sqlType);
				}
			}
		}
		return true;
	}

	public void executeCall() throws SQLException{
		if (inTransMap != null) {
			for (int i = 0; i < inTransMap.length; i++) {
				inTransMap[i].jetel2sql(statement);
			}
		}
		resultSet = statement.executeQuery();
		if (outParameters != null) {
			for (int i = 0; i < outTransMap.length; i++) {
				//TODO dodac lapanie bledow, jak w sql2jete
				outTransMap[i].setJetel(statement);
			}
		}
	}
	
	public boolean isNext() throws SQLException{
		if (outRecord == null) {
			return false;
		}
		
		if(outputFields == null || resultSet.next() == false){
			return false;
		}

		// init transMap if null
		if (resultOutMap == null){
			resultOutMap = CopySQLData.sql2JetelTransMap( SQLUtil.getFieldTypes(resultSet.getMetaData()),
					outRecord.getMetadata(), outRecord);
		}
			
		for (int i = 0; i < resultOutMap.length; i++) {
			resultOutMap[i].sql2jetel(resultSet);
		}
		
		return true;
	}

	public DataRecord getOutRecord() {
		return outRecord;
	}

}
