package org.jetel.connection.jdbc;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jetel.connection.jdbc.specific.DBConnectionInstance;
import org.jetel.connection.jdbc.specific.JdbcSpecific;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
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
	
    private final static Pattern KEY_FIELD_PATTERN = Pattern.compile(
    		"(" + IncrementalKeyType.getKeyTypePattern() + ")" + "\\(" + SQLUtil.DB_FIELD_PATTERN + "\\)", Pattern.CASE_INSENSITIVE);//eg. max(dbField)
    private final static Pattern KEY_VALUE_PATTERN = Pattern.compile(INCREMENTAL_KEY_INDICATOR + "\\w+");

    private Properties keyDefinition;
    private Object[][] keyDef;//each key is in form {name, db_name, type} - type is one of IncrementalKeyType
	private Properties keyValue;
	private String sqlQuery;//original query
	private String preparedQuery;//query with substituded key values
	private DataRecord keyRecord;
	private CopySQLData[] transMap;
	private boolean[] firstUpdate;
	
	final static int NAME = 0;
	final static int DB_NAME = 1;
	final static int TYPE = 2;
	
	/**
	 * Constructor for Incremental object
	 * 
	 * @param key key definition, eg. key1=max(dbField)
	 * @param query sql query in "incremental" form eg., <i>select $f1:=db1, $f2:=db2, ... from myTable where dbX > #myKey1 and dbY <=#myKey2</i>
	 * @param incrementalFile file url with incremental key values
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public SQLIncremental(Properties key, String query, String incrementalFile) throws FileNotFoundException, IOException, ComponentNotReadyException {
		this.keyDefinition = key;
		for (Object keyName : keyDefinition.keySet()) {
			if (!StringUtils.isValidObjectName((String)keyName)) {
				throw new ComponentNotReadyException("Invalid key name " + StringUtils.quote((String)keyName) + 
						". Allowed only [_A-Za-z0-9] characters.");
			}
		}
		this.sqlQuery = query;
		keyValue = new Properties();
		keyValue.load(new FileInputStream(incrementalFile));
		firstUpdate = new boolean[keyDefinition.size()];
		Arrays.fill(firstUpdate, true);
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
	 * Prepares statement for reading data from database. Incremental key is substituded by its current value,
	 * and key definition is created from its description.
	 * 
	 * @param dbConnection
	 * @return
	 * @throws SQLException
	 * @throws ComponentNotReadyException
	 */
	public PreparedStatement updateQuery(DBConnectionInstance dbConnection) throws SQLException, ComponentNotReadyException{
		//create record where current incremental values are stored
    	Statement statement = dbConnection.getSqlConnection().createStatement();
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
			keyDefMatcher = KEY_FIELD_PATTERN.matcher(key.getValue());
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
		keyRecord = new DataRecord(keyMetadata);
		keyRecord.init();
		//prepare trans map for filling key record with updated values
		for (int i = 0; i < keyDef.length; i++) {
			try {
				index = resultSet.findColumn((String)keyDef[i][DB_NAME]);
			} catch (Exception e) {
				String fullName = (String)keyDef[i][DB_NAME];
				index = resultSet.findColumn(fullName.substring(fullName.lastIndexOf('.') + 1));
			}
			transMap[i] = CopySQLData.createCopyObject(dbTypes.get(index - 1), keyRecord.getField((String)keyDef[i][NAME]).getMetadata(), 
					keyRecord, index - 1, i);
		}
		statement.close();
		//initialize key record from starting values
		for (Iterator iterator = keyValue.entrySet().iterator(); iterator.hasNext();) {
			Entry<String, String> key = (Entry<String, String>) iterator.next();
			try {
				keyRecord.getField(key.getKey()).fromString(key.getValue());
			} catch (BadDataFormatException e) {
				throw new ComponentNotReadyException("Invalid value for key " + StringUtils.quote(key.getKey()), e);
			}catch(ArrayIndexOutOfBoundsException e){
				//do nothing: ignore this value - it is not in key definition
			}
		}
		return setPosition(dbConnection);
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
	private String createSelectKeyQuery() throws ComponentNotReadyException{
		StringBuilder query = new StringBuilder(sqlQuery);
		int whereIndex = query.indexOf("where");
		if (whereIndex == -1) {
			throw new ComponentNotReadyException("\"where\" clause not found in sql query!!!");
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
	private PreparedStatement setPosition(DBConnectionInstance dbConnection) throws SQLException{
		StringBuffer query = new StringBuffer();
		StringBuffer preparedQueryBuilder = new StringBuffer();
		Matcher keyValueMatcher = KEY_VALUE_PATTERN.matcher(sqlQuery);
		Matcher keyValueMatcher1 = KEY_VALUE_PATTERN.matcher(sqlQuery);
		CopySQLData[] tMap = new CopySQLData[keyDef.length];
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
			tMap[index] = CopySQLData.createCopyObject(jdbcSpecific.jetelType2sql(field.getMetadata()), 
					field.getMetadata(), keyRecord, index, keyRecord.getMetadata().getFieldPosition(keyValueMatcher.group().substring(INCREMENTAL_KEY_INDICATOR.length())));
			index++;
		}
		keyValueMatcher.appendTail(query);
		keyValueMatcher1.appendTail(preparedQueryBuilder);
		preparedQuery = preparedQueryBuilder.toString();
		PreparedStatement statement = dbConnection.getSqlConnection().prepareStatement(query.toString());
		//set starting values from prepared earlier key record
		for (int i = 0; i < index; i++){
			tMap[i].setSQL(statement);
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
	
	public Properties getKey(){
		return keyDefinition;
	}

}
