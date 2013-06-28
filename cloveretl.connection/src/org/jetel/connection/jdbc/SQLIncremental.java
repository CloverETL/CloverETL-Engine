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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.database.sql.CopySQLData;
import org.jetel.database.sql.JdbcSpecific;
import org.jetel.database.sql.SqlConnection;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.string.StringUtils;

/**
 * This is class for incremental reading. For initialization it must be called updateQuery(DBConnectionInstance) method.
 * 
 * @author Agata Vackova (agata.vackova@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @since Jul 24, 2008
 */
public class SQLIncremental {
	
	public final static String INCREMENTAL_KEY_INDICATOR = "#";
	
    private final static Pattern KEY_VALUE_PATTERN = Pattern.compile(INCREMENTAL_KEY_INDICATOR + "\\w+");

    
    private Properties keyDefinition;
    private Object[][] keyDef;//each key is in form {name, db_name, type} - type is one of IncrementalKeyType
	private Properties keyValue;
	private String sqlQuery;//original query
	private String preparedQuery;//query with substituded key values
	private DataRecord keyRecord;
	private CopySQLData[] transMap;
	private boolean[] firstUpdate;
	private JdbcSpecific jdbcSpecific;

	final static int NAME = 0;
	final static int DB_NAME = 1;
	final static int TYPE = 2;
	
	/**
	 * Constructor for Incremental object
	 * 
	 * @param key key definition, eg. key1=max(dbField)
	 * @param query sql query in "incremental" form eg., <i>select $f1:=db1, $f2:=db2, ... from myTable where dbX > #myKey1 and dbY <=#myKey2</i>
	 * @param incrementalFile file url with incremental key values
	 * @param jdbcSpecific
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public SQLIncremental(Properties key, String query, String incrementalFile, JdbcSpecific jdbcSpecific) throws FileNotFoundException, IOException, ComponentNotReadyException {
		this.keyDefinition = key;
		this.jdbcSpecific = jdbcSpecific;
		for (Object keyName : keyDefinition.keySet()) {
			if (!StringUtils.isValidObjectName((String)keyName)) {
				throw new ComponentNotReadyException("Invalid key name " + StringUtils.quote((String)keyName) + 
						". Allowed only [_A-Za-z0-9] characters.");
			}
		}
		this.sqlQuery = query;
		keyValue = new Properties();
		
		File file = new File(incrementalFile);
		if (!file.exists()) {
			file.createNewFile();
		}
		keyValue.load(new FileInputStream(incrementalFile));
		setInitialValues(keyValue, key);
		
		firstUpdate = new boolean[keyDefinition.size()];
		Arrays.fill(firstUpdate, true);
	}
	
	/**
	 * Check initial values.
	 * @throws ComponentNotReadyException
	 */
	public void checkConfig() throws ComponentNotReadyException {
		if (!existAllInitialValues()) {
			throw new ComponentNotReadyException("Set up all initial values in the incremental key attribute.");
		}
	}

	
	/**
	 * true if all keys have an initial value.
	 * @param key
	 * @return
	 */
	private boolean existAllInitialValues() {
		Matcher keyValueMatcher = KEY_VALUE_PATTERN.matcher(sqlQuery);
		while (keyValueMatcher.find()) {
			String keyName = keyValueMatcher.group().substring(INCREMENTAL_KEY_INDICATOR.length());
			if (keyName != null && !keyName.equals("")) {
				if (!keyValue.containsKey(keyName)) return false;
			}
		}
		return true;
	}
	
	private void setInitialValues(Properties prop, Properties key) {
		for (Iterator<?> it = key.entrySet().iterator();it.hasNext();) {
			Entry<String, String> e = (Entry<String, String>) it.next();
			String value = e.getValue();
			if (value.startsWith("\"") && value.endsWith("\"")) {
				value = value.substring(1, value.length()-1);
			}
			Matcher keyDefMatcher = getKeyFieldPattern().matcher(value);
			keyDefMatcher.find();
			String sInitialValue;
			Object oKey = prop.get(e.getKey());
			if (oKey != null) continue;
			if (keyDefMatcher.groupCount() >= 5 && (sInitialValue = keyDefMatcher.group(5)) != null) {
				if (sInitialValue.length() <= 1) continue;
				prop.put(e.getKey(), sInitialValue.substring(1));
			}
		}
	}
	
	public SQLIncremental(Properties key, String query, Properties keyValue) throws ComponentNotReadyException{
		this.keyDefinition = key;
		for (Object keyName : keyDefinition.keySet()) {
			if (!StringUtils.isValidObjectName((String)keyName)) {
				throw new ComponentNotReadyException("Invalid key name " + StringUtils.quote((String)keyName) + 
						". Allowed only [_A-Za-z0-9] characters.");
			}
		}
		this.sqlQuery = query;
		this.keyValue = keyValue; 
		firstUpdate = new boolean[keyDefinition.size()];
		Arrays.fill(firstUpdate, true);
	}
	
	/**
	 * Sets new incremental values. For applying method updateQuery(DBConnectionInstance) must be called then
	 * 
	 * @param newValues
	 */
	public void setValues(Properties newValues){
		keyValue = newValues;
	}
	
	/**
	 * @return current values of incremental keys
	 */
	public Object getPosition() {
		if (keyRecord == null) {//this is not initialized -> return inital values
			return keyValue;
		}
		for (int i = 0; i < keyRecord.getNumFields(); i++){
			keyValue.setProperty(keyRecord.getField(i).getMetadata().getName(), keyRecord.getField(i).toString());
		}
		return keyValue;
	}

	/**
	 * Prepares statement for reading data from database. Incremental key is substituted by its current value,
	 * and key definition is created from its description.
	 * 
	 * @param dbConnection
	 * @return
	 * @throws SQLException
	 * @throws ComponentNotReadyException
	 */
	public PreparedStatement updateQuery(SqlConnection dbConnection) throws SQLException, ComponentNotReadyException{
		//create record where current incremental values are stored
    	Statement statement = dbConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    	ResultSet resultSet = statement.executeQuery(createSelectKeyQuery());
        ResultSetMetaData dbMetadata = resultSet.getMetaData();

        //create incremental key definition in internal format
        String dbField;
    	IncrementalKeyType type;
    	Matcher keyDefMatcher;
    	keyDef = new Object[keyDefinition.size()][3];
    	int index = 0;
		for (Iterator iterator = keyDefinition.entrySet().iterator(); iterator.hasNext();) {
			Entry<String, String> key = (Entry<String, String>) iterator.next();
			keyDefMatcher = getKeyFieldPattern().matcher(key.getValue());
			keyDefMatcher.find();
			type = IncrementalKeyType.valueOf(keyDefMatcher.group(1).toUpperCase());
			dbField = keyDefMatcher.group(2);
			keyDef[index] = new Object[]{key.getKey() , dbField, type};
			index++;
		}

		List<Integer> dbTypes = SQLUtil.getFieldTypes(dbMetadata);
		DataRecordMetadata keyMetadata = new DataRecordMetadata("keyMetadata", DataRecordMetadata.DELIMITED_RECORD);
		transMap = new CopySQLData[keyDef.length];
		JdbcSpecific jdbcSpecific = dbConnection.getJdbcSpecific();
		//create clover fields representing db fields
		for (int i = 0; i < keyDef.length; i++) {
			try {
				index = resultSet.findColumn((String)keyDef[i][DB_NAME]);
			} catch (SQLException e) {
				String fullName = (String)keyDef[i][DB_NAME];
				index = resultSet.findColumn(fullName.substring(fullName.lastIndexOf('.') + 1));
			}
			keyMetadata.addField(SQLUtil.dbMetadata2jetel((String)keyDef[i][NAME], dbMetadata, index, jdbcSpecific));
		}
		keyMetadata.normalize();
		keyRecord = DataRecordFactory.newRecord(keyMetadata);
		keyRecord.init();
		//prepare trans map for filling key record with updated values
		for (int i = 0; i < keyDef.length; i++) {
			try {
				index = resultSet.findColumn((String)keyDef[i][DB_NAME]);
			} catch (Exception e) {
				String fullName = (String)keyDef[i][DB_NAME];
				index = resultSet.findColumn(fullName.substring(fullName.lastIndexOf('.') + 1));
			}
			transMap[i] = jdbcSpecific.createCopyObject(dbTypes.get(index - 1), keyRecord.getField((String)keyDef[i][NAME]).getMetadata(), 
					keyRecord, index - 1, i);
		}
		statement.close();
		//initialize key record from starting values
		try {
			initPositionKeyRecord(keyRecord, keyValue);
		} catch (BadDataFormatException e) {
			throw new ComponentNotReadyException(e);
		}
		return setPosition(dbConnection);
	}

	/**
	 * @param keyRecord record to be initialized
	 * @param keyValue values to initialize record with
	 */
	private void initPositionKeyRecord(DataRecord keyRecord, Properties keyValue) {
		for (Iterator iterator = keyValue.entrySet().iterator(); iterator.hasNext();) {
			Entry<String, String> key = (Entry<String, String>) iterator.next();
			try {
				keyRecord.getField(key.getKey()).fromString(key.getValue());
			} catch (BadDataFormatException e) {
				throw new BadDataFormatException("Invalid value for key " + StringUtils.quote(key.getKey()), e);
			} catch (ArrayIndexOutOfBoundsException e) {
				// do nothing: ignore this value - it is not in key definition
			}
		}
	}
	
	/**
	 * updates values of incremental key from current result set
	 * 
	 * @param result curent database result set
	 * @param keyNumber number of incremental key
	 * @throws SQLException
	 */
	public void updatePosition(ResultSet result, int keyNumber) throws SQLException{
		
		switch ((IncrementalKeyType)keyDef[keyNumber][TYPE]) {
		case FIRST:
			if (firstUpdate[keyNumber]) {
				transMap[keyNumber].setJetel(result);
			}
			break;
		case LAST:
			transMap[keyNumber].setJetel(result);
			break;
		case MAX:
			if (firstUpdate[keyNumber]) {
				transMap[keyNumber].setJetel(result);
			}else if (keyRecord.getField(keyNumber).compareTo(transMap[keyNumber].getDbValue(result)) < 0){
				transMap[keyNumber].setJetel(result);
			}
			break;
		case MIN:
			if (firstUpdate[keyNumber]) {
				transMap[keyNumber].setJetel(result);
			}else if (keyRecord.getField(keyNumber).compareTo(transMap[keyNumber].getDbValue(result)) > 0){
				transMap[keyNumber].setJetel(result);
			}
			break;
		}
		
		if (firstUpdate[keyNumber]) {
			firstUpdate[keyNumber] = false;
		}
	}
	
	/**
	 * Creats select query for getting info about incremental key types
	 * 
	 * @return
	 * @throws ComponentNotReadyException
	 */
	private String createSelectKeyQuery() throws ComponentNotReadyException {
		if (StringUtils.isEmpty(sqlQuery)) {
			throw new ComponentNotReadyException("SQL query is empty");
		}
		StringBuilder query = new StringBuilder(sqlQuery);
		int whereIndex = query.toString().toLowerCase().indexOf("where");
		if (whereIndex == -1) {
			throw new ComponentNotReadyException("\"WHERE\" clause not found in SQL query!");
		}
		query.setLength(whereIndex);
		query.append("where 0=1");
		return query.toString();
	}
	
	/**
	 * Creates prepared statement for incremental reading with values from current incremental values
	 * 
	 * @param dbConnection
	 * @return
	 * @throws SQLException
	 */
	private PreparedStatement setPosition(SqlConnection dbConnection) throws SQLException{
		StringBuffer query = new StringBuffer();
		StringBuffer preparedQueryBuilder = new StringBuffer();
		Matcher keyValueMatcher = KEY_VALUE_PATTERN.matcher(sqlQuery);
		Matcher keyValueMatcher1 = KEY_VALUE_PATTERN.matcher(sqlQuery);
		List<CopySQLData> tMap2 = new ArrayList<CopySQLData>();
		int index = 0;
		DataField field;
		JdbcSpecific jdbcSpecific = dbConnection.getJdbcSpecific();
		//find groups for substituting by values (#keyName)
		while (keyValueMatcher.find()) {
			keyValueMatcher1.find();
			String keyName = keyValueMatcher.group().substring(INCREMENTAL_KEY_INDICATOR.length());
			if (keyRecord.hasField(keyName)) field = keyRecord.getField(keyName);
			else throw new RuntimeException("The key name '" + keyName + "' doesn't exist for incremental reading.");
			//replace it with ? in prepared statement
			keyValueMatcher.appendReplacement(query, "?");
			//replace it with proper value for logging
			keyValueMatcher1.appendReplacement(preparedQueryBuilder, field.toString());
			//create trans map for setting proper values in prepared statement
			tMap2.add(jdbcSpecific.createCopyObject(jdbcSpecific.jetelType2sql(field.getMetadata()), 
					field.getMetadata(), keyRecord, index, keyRecord.getMetadata().getFieldPosition(keyValueMatcher.group().substring(INCREMENTAL_KEY_INDICATOR.length()))));
			index++;
		}
		keyValueMatcher.appendTail(query);
		keyValueMatcher1.appendTail(preparedQueryBuilder);
		preparedQuery = preparedQueryBuilder.toString();
		PreparedStatement statement = dbConnection.prepareStatement(query.toString());
		//set starting values from prepared earlier key record
		for (int i = 0; i < index; i++){
			tMap2.get(i).setSQL(statement);
		}
		return statement;
	}

	/**
	 * @return query string for logging, with key names replaced by starting values
	 */
	public String getPreparedQuery() {
		if (preparedQuery == null) return sqlQuery;
		return preparedQuery;
	}
	
	public void setQuery(String sqlQuery) {
		this.sqlQuery = sqlQuery;
	}
	
	public Properties getKey(){
		return keyDefinition;
	}

	/**
	 * @param position values of incremental key to be merged with current values of incremental key of this instance (keyRecord).
	 * Result of the merge is stored in <code>position</code> object.
	 */
	public void mergePosition(Properties position) {
		DataRecord posKeyRecord = DataRecordFactory.newRecord(keyRecord.getMetadata());
		posKeyRecord.init();
		posKeyRecord.setToNull();
		initPositionKeyRecord(posKeyRecord, position);
		
		for (int i = 0; i < keyRecord.getNumFields(); i++){
			DataField f1 = posKeyRecord.getField(i);
			DataField f2 = keyRecord.getField(i);
			if (f2.isNull()) continue;
			if (f1.isNull()) {
				f1.setValue(f2);
			} else {
				switch ((IncrementalKeyType)keyDef[i][TYPE]) {
				case LAST:
					f1.setValue(f2);
					break;
				case MAX:
					if (f1.compareTo(f2) < 0){
						f1.setValue(f2);
					}
					break;
				case MIN:
					if (f1.compareTo(f2) > 0){
						f1.setValue(f2);
					}
					break;
				}
			}
			
			position.setProperty(posKeyRecord.getField(i).getMetadata().getName(), posKeyRecord.getField(i).toString());
		}
		
	}
	
	private Pattern getKeyFieldPattern() {
		return Pattern.compile("(" + IncrementalKeyType.getKeyTypePattern() + ")" + "\\((" + 
				 jdbcSpecific.getDbFieldPattern() + ")\\)" + "(\\!.+)?", Pattern.CASE_INSENSITIVE);//eg. max(dbField)!0
	}
}
