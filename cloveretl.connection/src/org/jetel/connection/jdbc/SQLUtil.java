/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2002-04  David Pavlis <david_pavlis@hotmail.com>
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
package org.jetel.connection.jdbc;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ParameterMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.connection.jdbc.specific.JdbcSpecific;
import org.jetel.connection.jdbc.specific.impl.DefaultJdbcSpecific;
import org.jetel.data.Defaults;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.string.StringUtils;

/**
 *  Various utilities for working with Databases
 *
 * @author      dpavlis
 * @since       September 25, 2002
 * @revision    $Revision: 1.10
 * @created     January 24, 2003
 */
public class SQLUtil {

	private final static String DEFAULT_DELIMITER = ";";
	private final static String END_RECORD_DELIMITER = "\n";
	public final static String BLOB_FORMAT_STRING = "blob";
	public final static String BINARY_FORMAT_STRING = "binary";
	
	//combination of alphanumeric chars with . and _ - can be quoted
	//sequence between quote is regarded as group by @see java.util.Pattern	
	public final static String DB_FIELD_PATTERN = "([\\p{Alnum}\\._]+)|([\"\'][\\p{Alnum}\\._ ]+[\"\'])"; 

	static Log logger = LogFactory.getLog(SQLUtil.class);

	/**
	 *  Creates SQL insert statement based on metadata describing data flow and
	 *  supplied table name
	 *
	 * @param  metadata   Metadata describing data flow from which to feed database
	 * @param  tableName  Table name into which insert data
	 * @return            string containing SQL insert statement
	 * @since             October 2, 2002
	 */
	public static String assembleInsertSQLStatement(DataRecordMetadata metadata, String tableName) {
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
		strBuf.insert(0, tableName);
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
	 * @return            Description of the Return Value
	 */
	public static String assembleInsertSQLStatement(String tableName, String[] dbFields) {
		StringBuffer strBuf = new StringBuffer("insert into ");

		strBuf.append(tableName).append(" (");

		for (int i = 0; i < dbFields.length; i++) {
			strBuf.append(dbFields[i]);
			if (i < dbFields.length - 1) {
				strBuf.append(", ");
			}
		}
		strBuf.append(") values (");

		for (int i = 0; i < dbFields.length; i++) {
			strBuf.append("?");
			if (i < dbFields.length - 1) {
				strBuf.append(",");
			}
		}
		strBuf.append(")");
		return strBuf.toString();
	}


	/**
	 * Creates clover data field metadata compatible with database column metadata
	 * 
	 * @param name clover field name
	 * @param dbMetadata result set metadata
	 * @param sqlIndex index of result set column (1 based)
	 * @param jdbcSpecific
	 * @return clover data field metadata compatible with database column metadata
	 * @throws SQLException
	 */
	public static DataFieldMetadata dbMetadata2jetel(String name, ResultSetMetaData dbMetadata, int sqlIndex, JdbcSpecific jdbcSpecific) throws SQLException{
		DataFieldMetadata fieldMetadata = new DataFieldMetadata(name, null);
		
		int type = dbMetadata.getColumnType(sqlIndex);
		char cloverType = jdbcSpecific.sqlType2jetel(type);
		//set length and scale for decimal field
		if (cloverType == DataFieldMetadata.DECIMAL_FIELD) {
			int scale = 0;
			int length = 0;
			try {
				scale = dbMetadata.getScale(sqlIndex);
				if (scale < 0) {
					cloverType = DataFieldMetadata.NUMERIC_FIELD;
				}else{
					fieldMetadata.setFieldProperty(DataFieldMetadata.SCALE_ATTR, Integer.toString(scale));
				}
			} catch (SQLException e) {
				cloverType = DataFieldMetadata.NUMERIC_FIELD;
			}
			try {
				length = scale + dbMetadata.getPrecision(sqlIndex);
				if (length <= scale) {
					cloverType = DataFieldMetadata.NUMERIC_FIELD;
				}else{
					fieldMetadata.setFieldProperty(DataFieldMetadata.LENGTH_ATTR, Integer.toString(length));				
				}
			} catch (SQLException e) {
				cloverType = DataFieldMetadata.NUMERIC_FIELD;
			}
		}
		fieldMetadata.setType(cloverType);
		//for Date Data Field set proper format
		switch (type) {
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
	
	/**
	 *  Converts SQL metadata into Clover's DataRecordMetadata
	 *
	 * @param  dbMetadata        SQL ResultSet metadata describing which columns are
	 *      returned by query
	 * @return                   DataRecordMetadata which correspond to the SQL
	 *      ResultSet
	 * @exception  SQLException  Description of the Exception
	 */
	public static DataRecordMetadata dbMetadata2jetel(ResultSetMetaData dbMetadata, JdbcSpecific jdbcSpecific) throws SQLException {
		DataFieldMetadata fieldMetadata;
		String tableName = dbMetadata.getTableName(1);
		if (!StringUtils.isValidObjectName(tableName)) {
			tableName = StringUtils.normalizeName(tableName);
		}
		DataRecordMetadata jetelMetadata = new DataRecordMetadata(tableName, DataRecordMetadata.DELIMITED_RECORD);
		jetelMetadata.setFieldDelimiter(DEFAULT_DELIMITER);
		jetelMetadata.setRecordDelimiters(END_RECORD_DELIMITER);
		String colName;

		for (int i = 1; i <= dbMetadata.getColumnCount(); i++) {
			colName = dbMetadata.getColumnName(i);
			if (!StringUtils.isValidObjectName(colName)) {
				colName = StringUtils.normalizeName(colName);
			}
			fieldMetadata = dbMetadata2jetel(colName, dbMetadata, i, jdbcSpecific);
			jetelMetadata.addField(fieldMetadata);
		}
		return jetelMetadata;
	}

	@Deprecated 
	public static DataRecordMetadata dbMetadata2jetel(ResultSetMetaData dbMetadata) throws SQLException {
		return dbMetadata2jetel(dbMetadata, DefaultJdbcSpecific.getInstance());
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

}

