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
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ParameterMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.connection.jdbc.specific.impl.DefaultJdbcSpecific;
import org.jetel.data.Defaults;
import org.jetel.database.sql.DbMetadata;
import org.jetel.database.sql.JdbcSpecific;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;

/**
 *  Various utilities for working with Databases
 *
 * @author      dpavlis
 * @since       September 25, 2002
 * @created     January 24, 2003
 */
public class SQLUtil {

	private final static String DEFAULT_DELIMITER = ";";
	private final static String END_RECORD_DELIMITER = "\n";
	public final static String BLOB_FORMAT_STRING = "blob";
	public final static String BINARY_FORMAT_STRING = "binary";
	
	static Log logger = LogFactory.getLog(SQLUtil.class);

	/**
	 *  Creates SQL insert statement based on metadata describing data flow and
	 *  supplied table name
	 *
	 * @param  metadata   Metadata describing data flow from which to feed database
	 * @param  tableName  Table name into which insert data
	 * @param  specific   Used JDBC specific.
	 * @return            string containing SQL insert statement
	 * @since             October 2, 2002
	 */
	public static String assembleInsertSQLStatement(DataRecordMetadata metadata, String tableName, JdbcSpecific specific) {
		StringBuffer strBuf = new StringBuffer();
		//StringBuffer strBuf2 = new StringBuffer();

		strBuf.append(" values(");
		for (int i = 0; i < metadata.getNumFields(); i++) {
			//strBuf2.append(metadata.getField(i).getName());
			strBuf.append("?");
			if (i < metadata.getNumFields() - 1) {
				strBuf.append(",");
				//strBuf2.append(",");
			}
		}
		//strBuf.insert(0, strBuf2.toString());
		//strBuf.insert(0, " (");
		strBuf.insert(0, specific.quoteString(tableName));
		strBuf.insert(0, "insert into ");
		strBuf.append(")");
		if (logger.isDebugEnabled()) {
			logger.debug(strBuf.toString());
		}
		return strBuf.toString();
	}


	/**
	 *  Description of the Method
	 *
	 * @param  tableName  Description of the Parameter
	 * @param  dbFields   Description of the Parameter
	 * @param  specific   Used JDBC specific.
	 * @return            Description of the Return Value
	 */
	public static String assembleInsertSQLStatement(String tableName, String[] dbFields, JdbcSpecific specific) {
		
		String quotedTableName = specific.quoteString(tableName);
		String[] quotedDbFields = new String[dbFields.length];
    	for (int i = 0; i < dbFields.length; i++) {
			quotedDbFields[i] = specific.quoteString(dbFields[i]);
		}
		return assembleInsertSQLStatement(quotedTableName, quotedDbFields);
	}
	
	public static String assembleInsertSQLStatement(String quotedTableName, String[] quotedDbFields) {
		StringBuffer strBuf = new StringBuffer("insert into ");

		strBuf.append(quotedTableName).append(" (");

		for (int i = 0; i < quotedDbFields.length; i++) {
			strBuf.append(quotedDbFields[i]);
			if (i < quotedDbFields.length - 1) {
				strBuf.append(", ");
			}
		}
		strBuf.append(") values (");

		for (int i = 0; i < quotedDbFields.length; i++) {
			strBuf.append("?");
			if (i < quotedDbFields.length - 1) {
				strBuf.append(",");
			}
		}
		strBuf.append(")");
		return strBuf.toString();
	}


	/**
	 * Same as dbMetadata2jetel(name, dbMetadata, sqlIndex, jdbcSpecific, true)
	 */
	public static DataFieldMetadata dbMetadata2jetel(String name, ResultSetMetaData dbMetadata, int sqlIndex, JdbcSpecific jdbcSpecific) throws SQLException{
		return dbMetadata2jetel(name, dbMetadata, sqlIndex, jdbcSpecific, true);
	}
	
	/**
	 * Creates clover data field metadata compatible with database column metadata
	 * 
	 * @param name clover field name
	 * @param dbMetadata result set metadata
	 * @param sqlIndex index of result set column (1 based)
	 * @param jdbcSpecific
	 * @param failIfUknownType indicates whether to throw exception if there is unknown column type in DB metadata.
	 * @return clover data field metadata compatible with database column metadata
	 * @throws SQLException
	 */
	public static DataFieldMetadata dbMetadata2jetel(String name, ResultSetMetaData dbMetadata, int sqlIndex, JdbcSpecific jdbcSpecific, boolean failIfUnknownType) throws SQLException{
		return dbMetadata2jetel(name, new ResultSetDbMetadata(dbMetadata), sqlIndex, jdbcSpecific, failIfUnknownType);
	}
	
	public static DataFieldMetadata dbMetadata2jetel(String name, ParameterMetaData dbMetadata, int sqlIndex, JdbcSpecific jdbcSpecific) throws SQLException{
		return dbMetadata2jetel(name, new ParameterDbMetadata(dbMetadata), sqlIndex, jdbcSpecific, true);
	}

	public static DataFieldMetadata dbMetadata2jetel(String name, DbMetadata dbMetadata, int sqlIndex, JdbcSpecific jdbcSpecific, boolean failIfUnknownType) throws SQLException{
		DataFieldMetadata fieldMetadata = new DataFieldMetadata(name, null);
		fieldMetadata.setLabel(name);
		
		DataFieldType cloverType = DataFieldType.UNKNOWN;
		try {
			cloverType = jdbcSpecific.sqlType2jetel(dbMetadata, sqlIndex);
		} catch (IllegalArgumentException e) {
			if (failIfUnknownType) throw e;
		}
		
		//set length and scale for decimal field
		if (cloverType == DataFieldType.DECIMAL) {
			int scale = 0;
			int length = 0;
			try {
				scale = dbMetadata.getScale(sqlIndex);
				if (scale < 0) {
					cloverType = DataFieldType.NUMBER;
				} else {
					fieldMetadata.setProperty(DataFieldMetadata.SCALE_ATTR, Integer.toString(scale));
				}
			} catch (SQLException e) {
				cloverType = DataFieldType.NUMBER;
			}
			try {
				length = dbMetadata.getPrecision(sqlIndex);
				if (length <= scale) {
					cloverType = DataFieldType.NUMBER;
				} else {
					fieldMetadata.setProperty(DataFieldMetadata.LENGTH_ATTR, Integer.toString(length));				
				}
			} catch (SQLException e) {
				cloverType = DataFieldType.NUMBER;
			}
			
		}
		
		fieldMetadata.setDataType(cloverType);
		
		//for Date Data Field set proper format
		int sqlType = dbMetadata.getType(sqlIndex);
		switch (sqlType) {
		case Types.DATE:
			fieldMetadata.setFormatStr(Defaults.DEFAULT_DATE_FORMAT);
			break;
		case Types.TIME:
			fieldMetadata.setFormatStr(Defaults.DEFAULT_TIME_FORMAT);
			break;
		case Types.TIMESTAMP:
			fieldMetadata.setFormatStr(Defaults.DEFAULT_DATETIME_FORMAT);
			break;
		}
		
		if (dbMetadata.isNullable(sqlIndex) == ResultSetMetaData.columnNullable) {
			fieldMetadata.setNullable(true);
		}
		return fieldMetadata;
	}
	
	public static void setSizeAttributeToColumnSizeIfPossible(DataRecordMetadata recordMetadata,
			ResultSetMetaData rsMetaData, JdbcSpecific jdbcSpecific, DatabaseMetaData dbMetaData, String tableName) {
		
		for (int i = 0; i < recordMetadata.getFields().length; i++) {
			boolean limitedField = true;
			int precision = 0;
			DataFieldType cloverType = null;
			try {
				limitedField = true;
				int type = rsMetaData.getColumnType(i + 1);
				precision = rsMetaData.getPrecision(i + 1);
				String typeName = rsMetaData.getColumnTypeName(i + 1);
				try {
					cloverType = jdbcSpecific.sqlType2jetel(type, precision);
				} catch (IllegalArgumentException e) {
					cloverType = DataFieldType.UNKNOWN;
				}
				if (isUnlimitedType(typeName)) {
					limitedField = false;
				}
			} catch (Exception e) {
			} finally {
				if (limitedField && cloverType != DataFieldType.DECIMAL) {
					// Serves as default size in case user decides to switch field to fixed size (see issue #3938)
					if (precision > 0) {
						recordMetadata.getFields()[i].setProperty(DataFieldMetadata.SIZE_ATTR, Integer.toString(precision));
					}
				}
			}
		}
	}
	
	/**
	 * Same as dbMetadata2jetel(dbMetadata, jdbcSpecific, true)
	 */
	public static DataRecordMetadata dbMetadata2jetel(ResultSetMetaData dbMetadata, JdbcSpecific jdbcSpecific) throws SQLException {
		return dbMetadata2jetel(dbMetadata, jdbcSpecific, true);
	}
	
	/**
	 *  Converts SQL metadata into Clover's DataRecordMetadata
	 *
	 * @param  dbMetadata        SQL ResultSet metadata describing which columns are
	 *      returned by query
	 * @param failIfUknownType indicates whether to throw exception if there is unknown column type in DB metadata.
	 * @return                   DataRecordMetadata which correspond to the SQL
	 *      ResultSet
	 * @exception  SQLException  Description of the Exception
	 */
	public static DataRecordMetadata dbMetadata2jetel(ResultSetMetaData dbMetadata, JdbcSpecific jdbcSpecific, boolean failIfUknownType) throws SQLException {
		DataFieldMetadata fieldMetadata;
		DataRecordMetadata jetelMetadata = new DataRecordMetadata(DataRecordMetadata.EMPTY_NAME, DataRecordMetadata.DELIMITED_RECORD);
		jetelMetadata.setLabel(getTableName(dbMetadata));
		jetelMetadata.setFieldDelimiter(DEFAULT_DELIMITER);
		jetelMetadata.setRecordDelimiter(END_RECORD_DELIMITER);
		
		for (int i = 1; i <= dbMetadata.getColumnCount(); i++) {
			fieldMetadata = dbMetadata2jetel(DataFieldMetadata.EMPTY_NAME, dbMetadata, i, jdbcSpecific, failIfUknownType);
			fieldMetadata.setLabel(dbMetadata.getColumnName(i));
			jetelMetadata.addField(fieldMetadata);
		}
		
		jetelMetadata.normalize();
		
		return jetelMetadata;
	}

	private static String getTableName(ResultSetMetaData dbMetadata) {
		try {
			return dbMetadata.getTableName(1);
		} catch (SQLException e) {
			return null;
		}
	}

	@Deprecated 
	public static DataRecordMetadata dbMetadata2jetel(ResultSetMetaData dbMetadata) throws SQLException {
		return dbMetadata2jetel(dbMetadata, DefaultJdbcSpecific.getInstance());
	}

	public static DataRecordMetadata dbMetadata2jetel(ParameterMetaData dbMetadata, String metadataName, JdbcSpecific jdbcSpecific) throws SQLException {
		DataFieldMetadata fieldMetadata;
		DataRecordMetadata jetelMetadata = new DataRecordMetadata(metadataName, DataRecordMetadata.DELIMITED_RECORD);
		jetelMetadata.setLabel(metadataName);
		jetelMetadata.setFieldDelimiter(DEFAULT_DELIMITER);
		jetelMetadata.setRecordDelimiter(END_RECORD_DELIMITER);
		String colName;

		for (int i = 1; i <= dbMetadata.getParameterCount(); i++) {
			colName = "field" + i;
			fieldMetadata = dbMetadata2jetel(colName, dbMetadata, i, jdbcSpecific);
			jetelMetadata.addField(fieldMetadata);
		}
		
		jetelMetadata.normalize();
		
		return jetelMetadata;
	}

	@Deprecated
	public static DataRecordMetadata dbMetadata2jetel(ParameterMetaData dbMetadata, String metadataName) throws SQLException {
		return dbMetadata2jetel(dbMetadata, metadataName, DefaultJdbcSpecific.getInstance());
	}
		/**
	 *  For specified table returns names of individual fileds
	 *
	 * @param  conn       database connection
	 * @param  tableName  name of DB table
	 * @return            array of field names
	 */
	public static String[] getColumnNames(Connection conn, String tableName) {
		List<String> tmp = new ArrayList<String>();
		String[] out = null;
		try {
			ResultSet rs = conn.getMetaData().getColumns(null, null, tableName, "%");

			while (rs.next()) {
				// FIELD NAME - 4 column in resultset
				// get DATA TYPE - 5 column in result set from Database metadata
				//out.add(rs.getString(4).toUpperCase(), new Integer(rs.getInt(5)));
				tmp.add(rs.getString(4));
			}
			out = new String[tmp.size()];
			tmp.toArray(out);
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (out.length == 0) {
			logger.warn("Table " + tableName + " does not exist or has no columns");
		}
		return out;
	}


	/**
	 *  Gets the FieldTypes of fields present in specified DB table
	 *
	 * @param  metadata          Description of Parameter
	 * @param  tableName         name of the table for which to get metadata (field names, types)
	 * @return                   list of  JDBC FieldTypes
	 * @exception  SQLException  Description of Exception
	 * @since                    October 4, 2002
	 * @see                      java.sql.DatabaseMetaData
	 */
	public static List<Integer> getFieldTypes(DatabaseMetaData metadata, String tableName) throws SQLException {
		String[] tableSpec = new String[]{null, tableName.toUpperCase()};
		if (tableName.indexOf(".") != -1) {
			tableSpec = tableName.toUpperCase().split("\\.", 2);
		}
		ResultSet rs = null;
		try {
			rs = metadata.getColumns(null, tableSpec[0], tableSpec[1], "%");//null as last parm
			List<Integer> fieldTypes = new LinkedList<Integer>();
			while (rs.next()) {
				// get DATA TYPE - fifth column in result set from Database metadata
				fieldTypes.add(new Integer(rs.getInt(5)));
			}
			if (fieldTypes.size() == 0) {
				//throw new RuntimeException("No metadata obtained for table: " + tableName);
				//Warn !
				logger.warn("No metadata obtained for table: \"" + tableName + "\", using workaround ...");
				// WE HAVE SOME PATCH, but ...
				ResultSetMetaData fieldsMetadata = getTableFieldsMetadata(metadata.getConnection(), tableName);
				for (int i = 0; i < fieldsMetadata.getColumnCount(); i++) {
					fieldTypes.add(new Integer(fieldsMetadata.getColumnType(i + 1)));
				}
			}
			return fieldTypes;
		} finally {
			if (rs != null)
				rs.close();
		}
	}


	/**
	 *  Gets the FieldTypes of fields (enumerated in dbFields) present in specified DB table
	 *
	 * @param  metadata          Description of the Parameter
	 * @param  tableName         name of the table for which to get metadata (field names, types)
	 * @param  dbFields          array of field names
	 * @return                   list of  JDBC FieldTypes
	 * @exception  SQLException  Description of the Exception
	 */
	public static List<Integer> getFieldTypes(DatabaseMetaData metadata, String tableName, String[] dbFields) throws SQLException {
		String[] tableSpec = new String[]{null, tableName.toUpperCase()};
		// if schema defined in table name, extract schema & table name into separate fields
		if (tableName.indexOf(".") != -1) {
			tableSpec = tableName.toUpperCase().split("\\.", 2);
		}
		ResultSet rs = metadata.getColumns(null, tableSpec[0], tableSpec[1], "%");//null as last parm
		Map<String, Integer> dbFieldsMap = new HashMap<String, Integer>();
		List<Integer> fieldTypes = new LinkedList<Integer>();
		Integer dataType;

		while (rs.next()) {
			// FIELD NAME - fourth columnt in resutl set
			// get DATA TYPE - fifth column in result set from Database metadata
			dbFieldsMap.put(rs.getString(4).toUpperCase(), new Integer(rs.getInt(5)));
		}
		if (dbFieldsMap.size() == 0) {
			//throw new RuntimeException("No metadata obtained for table: " + tableName);
			//Warn !
			logger.warn("No metadata obtained for table: \"" + tableName + "\", using workaround ...");
			// WE HAVE SOME PATCH, but ...
			ResultSetMetaData fieldsMetadata = getTableFieldsMetadata(metadata.getConnection(), tableName);
			for (int i = 0; i < fieldsMetadata.getColumnCount(); i++) {
				dbFieldsMap.put(fieldsMetadata.getColumnName(i + 1).toUpperCase(),
						new Integer(fieldsMetadata.getColumnType(i + 1)));
			}
		}
		for (int i = 0; i < dbFields.length; i++) {
			dataType = (Integer) dbFieldsMap.get(dbFields[i].toUpperCase());
			if (dataType == null) {
				throw new SQLException("Field \"" + dbFields[i] + "\" does not exists in table \"" + tableName + "\"");
			}
			fieldTypes.add(dataType);
		}
		return fieldTypes;
	}


	/**
	 *  Gets the fieldTypes attribute of the SQLUtil class
	 *
	 * @param  metadata          Description of the Parameter
	 * @return                   The fieldTypes value
	 * @exception  SQLException  Description of the Exception
	 */
	public static List<Integer> getFieldTypes(ParameterMetaData metadata) throws SQLException {
		List<Integer> fieldTypes = new LinkedList<Integer>();
		for (int i = 1; i <= metadata.getParameterCount(); i++) {
			fieldTypes.add(new Integer(metadata.getParameterType(i)));
		}
		return fieldTypes;
	}


	/**
	 *  Gets the fieldTypes attribute of the SQLUtil class
	 *
	 * @param  metadata          Description of the Parameter
	 * @return                   The fieldTypes value
	 * @exception  SQLException  Description of the Exception
	 */
	public static List<Integer> getFieldTypes(ResultSetMetaData metadata) throws SQLException {
		List<Integer> fieldTypes = new LinkedList<Integer>();
		for (int i = 1; i <= metadata.getColumnCount(); i++) {
			fieldTypes.add(new Integer(metadata.getColumnType(i)));
		}
		return fieldTypes;
	}


	/**
	 *  Gets the fieldTypes attribute of the SQLUtil class
	 *
	 * @param  metadata          Description of the Parameter
	 * @param  cloverFields      Description of the Parameter
	 * @return                   The fieldTypes value
	 * @exception  SQLException  Description of the Exception
	 */
	@Deprecated
	public static List<Integer> getFieldTypes(DataRecordMetadata metadata, String[] cloverFields) {
		return getFieldTypes(metadata, cloverFields, DefaultJdbcSpecific.getInstance());
	}
	
	public static List<Integer> getFieldTypes(DataRecordMetadata metadata, String[] cloverFields, JdbcSpecific jdbcSpecific) {
		List<Integer> fieldTypes = new LinkedList<Integer>();
		DataFieldMetadata fieldMeta;
		for (int i = 0; i < cloverFields.length; i++) {
			if ((fieldMeta = metadata.getField(cloverFields[i])) != null) {
				fieldTypes.add(new Integer(jdbcSpecific.jetelType2sql(fieldMeta)));
			} else {
				throw new RuntimeException("Field name [" + cloverFields[i] + "] not found in " + metadata.getName());
			}
		}
		return fieldTypes;
	}

	/**
	 *  Gets the fieldTypes attribute of the SQLUtil class
	 *
	 * @param  metadata          Description of the Parameter
	 * @return                   The fieldTypes value
	 * @exception  SQLException  Description of the Exception
	 */
	public static List<Integer> getFieldTypes(DataRecordMetadata metadata, JdbcSpecific jdbcSpecific)  {
		List<Integer> fieldTypes = new LinkedList<Integer>();
		for (int i = 0; i < metadata.getNumFields(); i++) {
				fieldTypes.add(new Integer(jdbcSpecific.jetelType2sql(metadata.getField(i))));
		}
		return fieldTypes;
	}

	@Deprecated
	public static List<Integer> getFieldTypes(DataRecordMetadata metadata)  {
		return getFieldTypes(metadata, DefaultJdbcSpecific.getInstance());
	}

	/**
	 *  Gets the tableFieldsMetadata attribute of the SQLUtil class
	 *
	 * @param  con               Description of the Parameter
	 * @param  tableName         Description of the Parameter
	 * @return                   The tableFieldsMetadata value
	 * @exception  SQLException  Description of the Exception
	 */
	public static ResultSetMetaData getTableFieldsMetadata(Connection con, String tableName) throws SQLException {
		String queryStr = "select * from " + tableName + " where 1=0 ";

		ResultSet rs = con.createStatement().executeQuery(queryStr);
		return rs.getMetaData();
	}


	/**
	 *  Converts Jetel/Clover datatype into String
	 *
	 * @param  fieldType  Jetel datatype
	 * @return            Corresponding string name
	 */
	public static String jetelType2Str(char fieldType) {
		return DataFieldMetadata.type2Str(fieldType);
	}

    /**
     * Converts sql type into string.
     * @param sqlType
     * @return
     */
    public static String sqlType2str(int sqlType) {
        switch(sqlType) {
        case Types.BIT: return "BIT";
        case Types.TINYINT: return "TINYINT";
        case Types.SMALLINT: return "SMALLINT";
        case Types.INTEGER: return "INTEGER";
        case Types.BIGINT: return "BIGINT";
        case Types.FLOAT: return "FLOAT";
        case Types.REAL: return "REAL";
        case Types.DOUBLE: return "DOUBLE";
        case Types.NUMERIC: return "NUMERIC";
        case Types.DECIMAL: return "DECIMAL";
        case Types.CHAR: return "CHAR";
        case Types.VARCHAR: return "VARCHAR";
        case Types.LONGVARCHAR: return "LONGVARCHAR";
        case Types.DATE: return "DATE";
        case Types.TIME: return "TIME";
        case Types.TIMESTAMP: return "TIMESTAMP";
        case Types.BINARY: return "BINARY";
        case Types.VARBINARY: return "VARBINARY";
        case Types.LONGVARBINARY: return "LONGVARBINARY";
        case Types.NULL: return "NULL";
        case Types.OTHER: return "OTHER";
        case Types.JAVA_OBJECT: return "JAVA_OBJECT";
        case Types.DISTINCT: return "DISTINCT";
        case Types.STRUCT: return "STRUCT";
        case Types.ARRAY: return "ARRAY";
        case Types.BLOB: return "BLOB";
        case Types.CLOB: return "CLOB";
        case Types.REF: return "REF";
        case Types.DATALINK: return "DATALINK";
        case Types.BOOLEAN: return "BOOLEAN";
        default: return "<unknown sql type>";
        }
        
    }
	
	/**
	 * Checks whether connection is valid/open. It
	 * sends simple SQL query to DB and waits if
	 * any exception occures
	 * 
	 * @param conn JDBC connection object
	 * @return true if connection valid/open otherwise false
	 */
	public static boolean isActive(Connection conn){
		try{
			conn.createStatement().execute("select 1");
			return true;
		}catch(Exception ex){
			return false;
		}
	}

	/**
	 * method checks if text contains only closed pairs of bars.
	 * for example: "(select 1)), (select " return false;
	 * @param text
	 * @return true if only closed pairs are contained in text
	 */
	private static boolean isAllBarPairsClosed(String text) {
		int currentOpen = 0;
		for (int i = 0; i < text.length(); i++) {
			char current = text.charAt(i);
			switch (current) {
			case '(': {
				currentOpen++;
				break;
			}
			case ')': {
				if (currentOpen > 0) {
					currentOpen --;
					break;
				} else {
					return false;
				}
			}
			}
		}
		
		return currentOpen == 0;
	}

	static String SELECT_KW = "select";
	//we need to ignore case
	static Pattern FROM_KW = Pattern.compile("(?i)\\s+from\\s+");
	static String selectDelim = ",";
	/**
	 * Searches select clause for function calls or other unnamed fields and generates
	 * names for them
	 * @param select
	 * @param specific
	 * @return
	 */
	public static String removeUnnamedFields(String select, JdbcSpecific specific) {
		if (select == null) {
			return null;
		}
		
		String selectlc = select.toLowerCase();
		int selectKwOffset = 0;
		
		int contentOffset;
		String selectPart;
		String parts[];
		int starti = 0;
		
		StringBuilder newQuery = new StringBuilder();
		
		while((selectKwOffset = selectlc.indexOf(SELECT_KW, selectKwOffset)) >= 0) {
			contentOffset = selectKwOffset + SELECT_KW.length();
			newQuery.append(select.substring(starti, contentOffset));
			selectPart = select.substring(contentOffset);
			Matcher m = FROM_KW.matcher(selectPart.toLowerCase());
			boolean founded = false;
			while (m.find()) {
				// we can't just cut it there, it breaks up this case: select a, (select b from t2 ) from t1;
				if (isAllBarPairsClosed(selectPart.substring(0, m.start()))) {	
					selectPart = selectPart.substring(0, m.start());
					founded = true;
					break;
				}
			}
			
			// select without from ? could be something like: select (select 1) from ... - inner select 
			if (!founded) {
				int inPosition;
				inPosition = selectPart.indexOf(')');
				while (inPosition > -1 ) {
					
					if (isAllBarPairsClosed(selectPart.substring(0, inPosition))) {	
						selectPart = selectPart.substring(0, inPosition);	
						break;
					}			
					inPosition = selectPart.indexOf(')',inPosition+1);	
				}
			}
			
			// from original string - select what from ... - what contains another select !
			String innerSelectPart;
			if (selectPart.indexOf(SELECT_KW) != -1) {
				innerSelectPart = removeUnnamedFields(selectPart, specific);
			} else {
				innerSelectPart = selectPart;
			}

			parts = innerSelectPart.split(selectDelim);
			StringBuilder newSelectPart = new StringBuilder();
			for(int i = 0; parts != null && i < parts.length; i++) {
				
				// we can't just add it on ')', it breaks up this case: func1(func2(aaa),bbb,ccc);
				// ignoring delimiter as it is not important for bars
				if (parts[i].trim().endsWith(")") && 
						(isAllBarPairsClosed(newSelectPart.toString()+parts[i]) || specific.isCaseStatement(parts[i]))) {
					parts[i] += " as AUTOCOLUMN" + String.valueOf(Math.round(Math.random() * 100000));
				}
				
				if (i > 0) {
					newSelectPart.append(selectDelim);
				}
				newSelectPart.append(parts[i]);
				
			}

			newQuery.append(newSelectPart);
			
			starti = contentOffset + selectPart.length();
			selectKwOffset = starti;
		}
		newQuery.append(select.substring(starti));
		
		return newQuery.toString();
	}

	/**
	 * Method individually close all given JDBC instances in separate try-catch blocks or 
	 * omit closing when given parameter is null.
	 * @param rs ResultSet
	 * @param stmt Statement
	 * @param conn Connection
	 */
	public static void closeConnection(ResultSet rs, Statement stmt, Connection conn) {
		if(rs != null) {
			try {
				rs.close();
			} catch (SQLException ex) {
				logger.error(ex);
			}
		}
		if(stmt != null) {
			try {
				stmt.close();
			} catch (SQLException ex) {
				logger.error(ex);
			}
		}
		if(conn != null) {
			try {
				conn.close();
			} catch (SQLException ex) {
				logger.error(ex);
			}
		}
	}
	
	private static boolean isUnlimitedType(String typeName) {
		if (typeName != null) {
			typeName = typeName.toLowerCase();
			if (typeName.indexOf("text") >= 0 || typeName.equals("clob") || typeName.equals("blob")) {
				return true;
			}
		}
		return false;
	}
	
	/** Delegates methods to ResultSetMetaData instance. */
	private static class ResultSetDbMetadata implements DbMetadata {
		private ResultSetMetaData dbMetadata;

		public ResultSetDbMetadata(ResultSetMetaData dbMetadata) {
			this.dbMetadata = dbMetadata;
		}

		@Override
		public int getType(int column) throws SQLException {
			return dbMetadata.getColumnType(column);
		}

		@Override
		public int isNullable(int column) throws SQLException {
			return dbMetadata.isNullable(column);
		}

		@Override
		public int getPrecision(int column) throws SQLException {
			return dbMetadata.getPrecision(column);
		}

		@Override
		public int getScale(int column) throws SQLException {
			return dbMetadata.getScale(column);
		}
	}
	
	/** Delegates methods to ParameterMetaData instance. */
	private static class ParameterDbMetadata implements DbMetadata {
		private ParameterMetaData dbMetadata;

		public ParameterDbMetadata(ParameterMetaData dbMetadata) {
			this.dbMetadata = dbMetadata;
		}

		@Override
		public int getType(int param) throws SQLException {
			return dbMetadata.getParameterType(param);
		}

		@Override
		public int isNullable(int param) throws SQLException {
			return dbMetadata.isNullable(param);
		}

		@Override
		public int getPrecision(int param) throws SQLException {
			return dbMetadata.getPrecision(param);
		}

		@Override
		public int getScale(int param) throws SQLException {
			return dbMetadata.getScale(param);
		}
	}

	/**
	 * The purpose of this class is to split
	 * a string into individual queries,
	 * but unlike query.split(";"), it should
	 * ignore semicolons within strings and comments.
	 * 
	 * TODO There's some JdbcSpecific comments mechanism {@link JdbcSpecific#getCommentsPattern()} which should be probably taken into account.
	 * 
	 * @author krivanekm (info@cloveretl.com)
	 *         (c) Javlin, a.s. (www.cloveretl.com)
	 *
	 * @created Sep 18, 2012
	 * @deprecated Use {@link SQLScriptParser} instead
	 */
	@Deprecated
	private static class SQLSplitter {
		
		private enum State {
			DEFAULT,
			STRING,
			ONELINE_COMMENT,
			MULTILINE_COMMENT
		}
		
		private static final char DEFAULT_DELIMITER = ';';
		
		private final String input;
		
		private StringBuilder sb = new StringBuilder();
		
		private List<String> result = new ArrayList<String>();
		
		private State state = State.DEFAULT;
		
		private char previous = 0;
		
		private String customDelimiter;
		
		/**
		 * @param input
		 * @param customDelimiter if <code>null</code>, default ";" will be used.
		 * @deprecated Use {@link SQLScriptParser} instead
		 */
		@Deprecated
		public SQLSplitter(String input, String customDelimiter) {
			this.input = input;
			if (customDelimiter != null && !String.valueOf(DEFAULT_DELIMITER).equals(customDelimiter)) {
				this.customDelimiter = customDelimiter;
			}
		}
		
		private void flush() {
			if (sb.length() > 0) {
				result.add(sb.toString());
			}
			sb.setLength(0);
			previous = 0;
		}
		
		private void setState(State state) {
			this.state = state;
			this.previous = 0; // reset the previous character
		}
		
		private void run() {
			int length = input.length();
			for (int i = 0; i < length; i++) {
				char c = input.charAt(i);
				switch (state) {
				case DEFAULT:
					if (customDelimiter == null) {
						if (c == DEFAULT_DELIMITER) {
							flush();
							continue; // stay in the DEFAULT state, do not append ';' to the StringBuilder
						}
					} else {
						if (input.startsWith(customDelimiter, i)) {
							flush();
							i += customDelimiter.length() - 1;
							continue; // stay in the DEFAULT state, do not append whole customDelimiter to the StringBuilder
						}
					}
					switch (c) {
					case '-':
						if (previous == '-') {
							setState(State.ONELINE_COMMENT);
						}
						break;
					case '*':
						if (previous == '/') {
							setState(state = State.MULTILINE_COMMENT);
						}
						break;
					case '\'':
						setState(State.STRING);
						break;
					}
					break;
				case STRING:
					if (c == '\'') {
						setState(State.DEFAULT);
					}
					break;
				case ONELINE_COMMENT:
					if ((c == '\r') || (c == '\n')) {
						setState(State.DEFAULT);
					}
					break;
				case MULTILINE_COMMENT:
					if ((c == '/') && (previous == '*')) {
						setState(State.DEFAULT);
					}
					break;
				}
				sb.append(c);
				previous = c;
			}
			flush();
		}

		private String[] getResult() {
			return result.toArray(new String[result.size()]);
		}

	}
	
	/**
	 * Splits a string into individual queries,
	 * ignores semicolons within strings and comments.

	 * @param sql
	 * @return individual queries
	 * @deprecated Use {@link SQLScriptParser} instead
	 */
	@Deprecated
	public static String[] split(String sql) {
		return split(sql, null);
	}
	
	/**
	 * Splits a string into individual queries,
	 * ignores SQL statements separator within strings and comments.
	 * 
	 * @param sql
	 * @param delimiter string separating individual SQL statements in the input <code>sql<code>.
	 *                  If <code>null</code>, default ";" separator will be used.
	 * @return individual SQL statements
	 * @deprecated Use {@link SQLScriptParser} instead
	 */
	@Deprecated
	public static String[] split(String sql, String delimiter) {
		SQLSplitter splitter = new SQLSplitter(sql, delimiter);
		splitter.run();
		return splitter.getResult();
	}


	/**
	 * Removes terminating semicolon(s) from the query if the semicolon(s) is last non-whitespace character(s).
	 * Also trims the query.
	 * @param query
	 * @return stripped and trimmed query
	 */
	public static String stripTerminatingSemicolons(String query) {
		query = query.trim();
		while (query.endsWith(DEFAULT_DELIMITER)) {
			query = query.substring(0, query.length() - DEFAULT_DELIMITER.length()).trim();
		}
		return query;
	}
	
	/**
	 * CLO-4510
	 * 
	 * Wraps the query in SELECT query with optimizing "WHERE 0=1"
	 * @param query
	 * @return
	 */
	public static String encloseInQptimizingQuery(String query) {
		return "SELECT wrapper_table.* FROM (" + query + ") wrapper_table where 1=0";
	}


	/**
	 * Appends "WHERE 0=1" to the query. If the query already contains where clause, it is replaced by "WHERE 0=1"
	 * 
	 * @param sqlQuery
	 * @return
	 */
	public static String appendOptimizingWhereClause(String sqlQuery) {
		int index = sqlQuery.toUpperCase().indexOf("WHERE");
		if (index >= 0) {
			sqlQuery = sqlQuery.substring(0, index).concat("WHERE 0=1");
		} else {
			sqlQuery = sqlQuery.concat(" WHERE 0=1");
		}
		return sqlQuery;
	}


	/**
	 * Traverses whole SQL query and limits all WHERE clauses using 0=1 and replaces all parameters with NULL.
	 * 
	 * @param sqlQuery
	 * @return modified query
	 */
	public static String modifyQueryUsingBasicTraversal(String sqlQuery) {
		StringBuilder result = new StringBuilder();
		String lowerQuery = sqlQuery.toLowerCase();
		int queryLength = sqlQuery.length();
		boolean stringMode = false;
		int position = 0;
		int nextQuot = -1;
		int nextWhere = -1;
		int nextParam = -1;
		do {
			nextQuot = lowerQuery.indexOf("'", position);
			
			if (stringMode) {
				if (nextQuot == -1) {
					// String is not terminated
					result.append(sqlQuery.substring(position));
					break;
				}

				int segmentEnd;
				if (lowerQuery.length() == nextQuot + 1 || lowerQuery.charAt(nextQuot + 1) != '\'') {
					// String ends
					stringMode = false;
					segmentEnd = nextQuot;
				} else {
					// Ignore two single quotation marks as character in string
					segmentEnd = nextQuot + 1;
				}
				
				result.append(sqlQuery.substring(position, segmentEnd + 1));
				position = segmentEnd + 1;
			} else {
				nextWhere = lowerQuery.indexOf(" where ", position);
				nextParam = lowerQuery.indexOf("?", position);

				// Determine position of next thing to resolve
				int nextThing = -1;
				if (nextQuot >= 0) {
					nextThing = nextQuot;
				}
				if (nextWhere >= 0 && (nextWhere < nextThing || nextThing == -1)) {
					nextThing = nextWhere;
				}
				if (nextParam >= 0 && (nextParam < nextThing || nextThing == -1)) {
					nextThing = nextParam;
				}
				
				if (nextThing >= 0) {
					if (nextThing == nextQuot) {
						// New string have started
						stringMode = true;
						result.append(sqlQuery.substring(position, nextThing + 1));
						position = nextThing + 1;
					} else if (nextThing == nextWhere) {
						// Add 1=0 condition to where section
						result.append(sqlQuery.substring(position, nextThing + 7));
						result.append("1=0 AND ");
						position = nextThing + 7;
					} else if (nextThing == nextParam) {
						// Replace SQL parameter with NULL
						result.append(sqlQuery.substring(position, nextThing));
						result.append("NULL");
						position = nextThing + 1;
					}
				} else {
					result.append(sqlQuery.substring(position));
					break;
				}
			}
		} while (position < queryLength && nextQuot >= 0 || nextWhere >= 0 || nextParam >=0); 
			
		return result.toString();
	}
	
}

