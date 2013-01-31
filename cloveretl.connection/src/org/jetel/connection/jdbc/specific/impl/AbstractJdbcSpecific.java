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
package org.jetel.connection.jdbc.specific.impl;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.connection.jdbc.AbstractCopySQLData.CopyArray;
import org.jetel.connection.jdbc.AbstractCopySQLData.CopyBlob;
import org.jetel.connection.jdbc.AbstractCopySQLData.CopyBoolean;
import org.jetel.connection.jdbc.AbstractCopySQLData.CopyByte;
import org.jetel.connection.jdbc.AbstractCopySQLData.CopyDate;
import org.jetel.connection.jdbc.AbstractCopySQLData.CopyDecimal;
import org.jetel.connection.jdbc.AbstractCopySQLData.CopyInteger;
import org.jetel.connection.jdbc.AbstractCopySQLData.CopyLong;
import org.jetel.connection.jdbc.AbstractCopySQLData.CopyNumeric;
import org.jetel.connection.jdbc.AbstractCopySQLData.CopyString;
import org.jetel.connection.jdbc.AbstractCopySQLData.CopyTime;
import org.jetel.connection.jdbc.AbstractCopySQLData.CopyTimestamp;
import org.jetel.connection.jdbc.SQLUtil;
import org.jetel.connection.jdbc.specific.conn.BasicSqlConnection;
import org.jetel.data.DataRecord;
import org.jetel.database.sql.CopySQLData;
import org.jetel.database.sql.DBConnection;
import org.jetel.database.sql.JdbcDriver;
import org.jetel.database.sql.JdbcSpecific;
import org.jetel.database.sql.QueryType;
import org.jetel.database.sql.SqlConnection;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelException;
import org.jetel.graph.Node;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.string.StringUtils;


/**
 * Abstract implementation of JdbcSpecific, which is currently ancestor of all 
 * implementation of JdbcSpecific interface.
 * Contains a default result set optimization and a conversion table between sql types 
 * and clover field types.
 * 
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created Jun 3, 2008
 */
abstract public class AbstractJdbcSpecific implements JdbcSpecific {

    private final static Log logger = LogFactory.getLog(AbstractJdbcSpecific.class);

	/** the SQL comments pattern conforming to the SQL standard */
	//&&[^-?=-] part added due to issue 3472
	private static final Pattern COMMENTS_PATTERN = Pattern.compile("--[^\r\n&&[^-?=-]]*|/\\*.*?\\*/", Pattern.DOTALL);

	private static final String TYPES_CLASS_NAME = "java.sql.Types";

	private static final String RESULT_SET_PARAMETER_TYPE_FIELD = "OTHER";

	private final static int DEFAULT_FETCH_SIZE = 50;

	private String id;
	
	@Override
	public String getId() {
		return id;
	}
	
	public void setId(String id) {
		this.id = id;
	}
	
	@Override
	public ConfigurationStatus checkMetadata(ConfigurationStatus status, Collection<DataRecordMetadata> metadata, Node node) {
		return status;
	}
	
	@Override
	public boolean canCloseResultSetBeforeCreatingNewOne() {
		return true;
	}
	
	@Override
	public String getDbFieldPattern() {
		//combination of alphanumeric chars with . and _ - can be quoted
		//sequence between quote is regarded as group by @see java.util.Pattern	
		return "([\\p{Alnum}\\._]+)|([\"\'][\\p{Alnum}\\._ ]+[\"\'])"; 
	}
	
	@Override
	public SqlConnection createSQLConnection(DBConnection dbConnection, OperationType operationType) throws JetelException {
		SqlConnection connection = prepareSQLConnection(dbConnection, operationType);
		connection.init();
		return connection;
	}
	
	@Override
	public SqlConnection wrapSQLConnection(DBConnection dbConnection, OperationType operationType, Connection sqlConnection) throws JetelException {
		SqlConnection connection = prepareSQLConnection(dbConnection, operationType);
		connection.setInnerConnection(sqlConnection);
		connection.init();
		return connection;
	}
	
	/**
	 * Just creates respective implementation of {@link DefaultConnection} for this jdbc specific.
	 * Is intended to be overridden.
	 */
	protected SqlConnection prepareSQLConnection(DBConnection dbConnection, OperationType operationType) throws JetelException {
		return new BasicSqlConnection(dbConnection, operationType, getAutoKeyType());
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.connection.jdbc.specific.JdbcSpecific#getAutoKeyType()
	 */
	@Override
	public AutoGeneratedKeysType getAutoKeyType() {
		return AutoGeneratedKeysType.NONE;
	}

	/* (non-Javadoc)
	 * @see org.jetel.connection.jdbc.specific.JdbcSpecific#optimizeResultSet(java.sql.ResultSet, org.jetel.connection.jdbc.specific.JdbcSpecific.OperationType)
	 */
	@Override
	public void optimizeResultSet(ResultSet resultSet, OperationType operationType) {
		switch (operationType){
		case READ:
			try {
				resultSet.setFetchDirection(ResultSet.FETCH_FORWARD);
				resultSet.setFetchSize(DEFAULT_FETCH_SIZE);
			} catch(SQLException ex) {
				//TODO: for now, do nothing
			}
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.connection.jdbc.specific.JdbcSpecific#jetelType2sql(org.jetel.metadata.DataFieldMetadata)
	 */
	@Override
	public int jetelType2sql(DataFieldMetadata field){
		switch (field.getType()) {
		case DataFieldMetadata.INTEGER_FIELD:
			return Types.INTEGER;
		case DataFieldMetadata.NUMERIC_FIELD:
			return Types.FLOAT;
		case DataFieldMetadata.STRING_FIELD:
			return field.isFixed() ? Types.CHAR : Types.VARCHAR;
		case DataFieldMetadata.DATE_FIELD:
			boolean isDate = field.isDateFormat();
			boolean isTime = field.isTimeFormat();
			if (isDate && isTime || StringUtils.isEmpty(field.getFormatStr())) 
				return Types.TIMESTAMP;
			if (isDate)
				return Types.DATE;
			if (isTime)
				return Types.TIME;
			return Types.TIMESTAMP;
        case DataFieldMetadata.LONG_FIELD:
            return Types.BIGINT;
        case DataFieldMetadata.DECIMAL_FIELD:
            return Types.DECIMAL;
        case DataFieldMetadata.BYTE_FIELD:
        case DataFieldMetadata.BYTE_FIELD_COMPRESSED:
        	if (!StringUtils.isEmpty(field.getFormatStr())
					&& field.getFormatStr().equalsIgnoreCase(DataFieldMetadata.BLOB_FORMAT_STRING)) {
        		return Types.BLOB;
        	}
            return field.isFixed() ? Types.BINARY : Types.VARBINARY;
        case DataFieldMetadata.BOOLEAN_FIELD:
        	return Types.BOOLEAN;
		default:
			throw new IllegalArgumentException("Can't handle Clover's data type :"+field.getTypeAsString());
		}
	}
	
	
	@Override
	public String jetelType2sqlDDL(DataFieldMetadata field) {
		int sqlType = jetelType2sql(field);
		
		switch(sqlType) {
		case Types.BINARY :
		case Types.VARBINARY :
		case Types.VARCHAR :
		case Types.CHAR :
			return sqlType2str(sqlType) + "(" + (field.isFixed() ? String.valueOf(field.getSize()) : "80") + ")";
		case Types.DECIMAL :
			String base = sqlType2str(sqlType);
			String prec = "";
			if (field.getProperty("length") != null) {
				if (field.getProperty("scale") != null) {
					prec = "(" + field.getProperty("length") + "," + field.getProperty("scale") + ")";
				} else {
					prec = "(" + field.getProperty("length") + ",0)";
				}
			}
			return base + prec;
		default :
			return sqlType2str(sqlType);
		}
		
	}
	
	@Override
	public char sqlType2jetel(int sqlType, int sqlPrecision) {
		return sqlType2jetel(sqlType);
	}

	/* (non-Javadoc)
	 * @see org.jetel.connection.jdbc.specific.JdbcSpecific#sqlType2jetel(int)
	 */
	@Override
	public char sqlType2jetel(int sqlType) {
		switch (sqlType) {
			case Types.INTEGER:
			case Types.SMALLINT:
			case Types.TINYINT:
			    return DataFieldMetadata.INTEGER_FIELD;
			//-------------------
			case Types.BIGINT:
			    return DataFieldMetadata.LONG_FIELD;
			//-------------------
			case Types.DECIMAL:
			case Types.NUMERIC:
				return DataFieldMetadata.DECIMAL_FIELD;
			case Types.DOUBLE:
			case Types.FLOAT:
			case Types.REAL:
				return DataFieldMetadata.NUMERIC_FIELD;
			//------------------
			case Types.CHAR:
			case Types.LONGVARCHAR:
			case Types.VARCHAR:
			case Types.CLOB:
			case Types.NCHAR:
			case Types.NVARCHAR:
			case Types.NCLOB:
				return DataFieldMetadata.STRING_FIELD;
			//------------------
			case Types.DATE:
			case Types.TIME:
			case Types.TIMESTAMP:
				return DataFieldMetadata.DATE_FIELD;
            //-----------------
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB:
			case Types.OTHER:
                return DataFieldMetadata.BYTE_FIELD;
			//-----------------
			case Types.BOOLEAN:
				return DataFieldMetadata.BOOLEAN_FIELD;
			// proximity assignment
			case Types.BIT:
			case Types.NULL:
				return DataFieldMetadata.STRING_FIELD;
			case Types.STRUCT:
				throw new IllegalArgumentException("Can't handle JDBC type STRUCT");
			default:
				throw new IllegalArgumentException("Can't handle JDBC.Type :"+sqlType);
		}
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.connection.jdbc.specific.JdbcSpecific#createCopyObject(int, org.jetel.metadata.DataFieldMetadata, org.jetel.data.DataRecord, int, int)
	 */
	@Override
	public CopySQLData createCopyObject(int sqlType, DataFieldMetadata fieldMetadata, DataRecord record, int fromIndex, int toIndex) {
		String format = fieldMetadata.getFormat();
		char jetelType = fieldMetadata.getType();
		CopySQLData obj = null;
		switch (sqlType) {
			case Types.ARRAY:
				obj = new CopyArray(record, fromIndex, toIndex);
				break;
			case Types.CHAR:
			case Types.LONGVARCHAR:
			case Types.VARCHAR:
				obj = new CopyString(record, fromIndex, toIndex);
				break;
			case Types.INTEGER:
			case Types.SMALLINT:
				if (jetelType == DataFieldMetadata.BOOLEAN_FIELD) {
					obj = new CopyBoolean(record, fromIndex, toIndex);
				} else {
					obj = new CopyInteger(record, fromIndex, toIndex);
				}
				break;
			case Types.BIGINT:
			    obj = new CopyLong(record,fromIndex,toIndex);
			    break;
			case Types.DECIMAL:
			case Types.DOUBLE:
			case Types.FLOAT:
			case Types.REAL:
				// fix for copying when target is numeric and
				// clover source is integer - no precision can be
				// lost so we can use CopyInteger
				if (jetelType == DataFieldMetadata.INTEGER_FIELD) {
					obj = new CopyInteger(record, fromIndex, toIndex);
				} else if (jetelType == DataFieldMetadata.LONG_FIELD) {
					obj = new CopyLong(record, fromIndex, toIndex);
				} else if(jetelType == DataFieldMetadata.NUMERIC_FIELD) {
				    obj = new CopyNumeric(record, fromIndex, toIndex);
				} else {
					obj = new CopyDecimal(record, fromIndex, toIndex);
				}
				break;
			case Types.NUMERIC:
				// Oracle doesn't have boolean type, data type SMALLINT is the same as NUMBER(38);
				// see issue #3815
				if (jetelType == DataFieldMetadata.BOOLEAN_FIELD) {
					obj = new CopyBoolean(record, fromIndex, toIndex);
				}else if (jetelType == DataFieldMetadata.INTEGER_FIELD) {
					obj = new CopyInteger(record, fromIndex, toIndex);
				} else if (jetelType == DataFieldMetadata.LONG_FIELD) {
					obj = new CopyLong(record, fromIndex, toIndex);
				} else if(jetelType == DataFieldMetadata.NUMERIC_FIELD) {
				    obj = new CopyNumeric(record, fromIndex, toIndex);
				} else {
					obj = new CopyDecimal(record, fromIndex, toIndex);
				}
				break;
			case Types.DATE:
				if (StringUtils.isEmpty(format)) {
					obj = new CopyDate(record, fromIndex, toIndex);
					break;
				}				
			case Types.TIME:
				if (StringUtils.isEmpty(format)) {
					obj = new CopyTime(record, fromIndex, toIndex);
					break;
				}				
			case Types.TIMESTAMP:
				if (StringUtils.isEmpty(format)) {
					obj = new CopyTimestamp(record, fromIndex, toIndex);
					break;
				}
				boolean isDate = fieldMetadata.isDateFormat();
				boolean isTime = fieldMetadata.isTimeFormat();
				if (isDate && isTime) {
					obj = new CopyTimestamp(record, fromIndex, toIndex);
				}else if (isDate) {
					obj = new CopyDate(record, fromIndex, toIndex);
				}else if (isTime){
					obj = new CopyTime(record, fromIndex, toIndex);
				}else {
					obj = new CopyTimestamp(record, fromIndex, toIndex);
				}
				break;
			case Types.BOOLEAN:
			case Types.BIT:
				if (jetelType == DataFieldMetadata.BOOLEAN_FIELD) {
					obj = new CopyBoolean(record, fromIndex, toIndex);
					break;
				} 
        		logger.warn("Metadata mismatch; type:" + jetelType + " SQLType:" + sqlType + " - using CopyString object.");
        		obj = new CopyString(record, fromIndex, toIndex);
        		break;
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB:
            	if (!StringUtils.isEmpty(format) && format.equalsIgnoreCase(SQLUtil.BLOB_FORMAT_STRING)) {
                	obj = new CopyBlob(record, fromIndex, toIndex);
                	break;
            	}
            	if (!StringUtils.isEmpty(format) && !format.equalsIgnoreCase(SQLUtil.BINARY_FORMAT_STRING)){
            		logger.warn("Unknown format " + StringUtils.quote(format) + " - using CopyByte object.");
            	}
                obj = new CopyByte(record, fromIndex, toIndex);
                break;
			// when Types.OTHER or unknown, try to copy it as STRING
			// this works for most of the NCHAR/NVARCHAR types on Oracle, MSSQL, etc.
			default:
			//case Types.OTHER:// When other, try to copy it as STRING - should work for NCHAR/NVARCHAR
				obj = new CopyString(record, fromIndex, toIndex);
				break;
			//default:
			//	throw new RuntimeException("SQL data type not supported: " + SQLType);
		}
		
		obj.setSqlType(sqlType);
		return obj;
		
	}

	/* (non-Javadoc)
	 * @see org.jetel.connection.jdbc.specific.JdbcSpecific#getResultSetParameterTypeField()
	 */
	@Override
	public String getResultSetParameterTypeField() {
		return RESULT_SET_PARAMETER_TYPE_FIELD;
	}

	/* (non-Javadoc)
	 * @see org.jetel.connection.jdbc.specific.JdbcSpecific#getTypesClassName()
	 */
	@Override
	public String getTypesClassName() {
		return TYPES_CLASS_NAME;
	}

	@Override
	public Pattern getCommentsPattern() {
		return COMMENTS_PATTERN;
	}

	@Override
	public String sqlType2str(int sqlType) {
		return SQLUtil.sqlType2str(sqlType);
	}

    @Override
	public String quoteIdentifier(String identifier) {
        return identifier;
    }
    
    @Override
    public String quoteString(String string) {
    	return string;
    }

    @Override
	public String getValidateQuery(String query, QueryType queryType, boolean optimizeSelectQuery) throws SQLException {
		
		String q = null;
        String where = "WHERE";
        int indx;
        
        switch(queryType) {
		case INSERT:
			throw new SQLException("INSERT query cannot be validated");
		case UPDATE:
		case DELETE:
			
			q = query.toUpperCase();
			
			indx = q.indexOf(where);
            if (indx >= 0){
            	q = q.substring(0, indx + where.length()) + " 0=1 and " + q.substring(indx + where.length());
            }else{
            	q += " where 0=1";
            }
            break;
            
		case SELECT:
			
			query = SQLUtil.removeUnnamedFields(query, this);
			if (optimizeSelectQuery) {
				q = "SELECT wrapper_table.* FROM (" + query + ") wrapper_table where 1=0";
			} else {
				q = query;
			}
			break;
		}
	
        return q;
        
	}

	/**
	 * Default behavior for literal detection
	 */
	@Override
	public boolean isLiteral(String s) {
		
		if (s == null) {
			return true;
		}
		
		s = s.trim();

		// numbers are literals
		try {
			Integer.parseInt(s);
			return true;
		} catch (NumberFormatException e) {
		}
		try {
			Double.parseDouble(s);
			return true;
		} catch (NumberFormatException e) {
		}
		
		return s.startsWith("'");
		
	}
	
	@Override
	public boolean isCaseStatement(String statement) {
		if (statement != null) {
			String s = statement.trim().toLowerCase();
			return s.startsWith("case") && s.endsWith("end");
		}
		return false;
	}

	/**
	 * A static method that retrieves schemas from dbMeta objects.
	 * Returns it as arraylist of strings in the format either <schema> or <catalog>.<schema>
	 * e.g.:
	 * mytable
	 * dbo.anothertable
	 * 
	 * @param dbMeta
	 * @return
	 * @throws SQLException
	 */
	protected static ArrayList<String> getMetaSchemas(DatabaseMetaData dbMeta) throws SQLException {
		ArrayList<String> ret = new ArrayList<String>();
		ResultSet result = dbMeta.getSchemas();
		String tmp;
		while (result.next()) {
			tmp = "";
			try {
				if (result.getString(2) != null) {
					tmp = result.getString(2) + dbMeta.getCatalogSeparator();
				}
			} catch (Exception e) {
				// -pnajvar
				// this is here deliberately
				// some dbms don't provide second column and that is not wrong, just have to ignore
			}
			tmp += result.getString(1);
			ret.add(tmp);
		}
		result.close();
		return ret;
	}
	
	protected static ArrayList<String> getMetaCatalogs(DatabaseMetaData dbMeta) throws SQLException {
		ArrayList<String> ret = new ArrayList<String>();
		ResultSet result = dbMeta.getCatalogs();
		String tmp;
		while (result.next()) {
			tmp = result.getString(1);
			ret.add(tmp);
		}
		result.close();
		return ret;
	}
	
	@Override
	public ArrayList<String> getSchemas(SqlConnection connection) throws SQLException {

		ArrayList<String> tmp;
		
		ArrayList<String> schemas = new ArrayList<String>();

		DatabaseMetaData dbMeta = connection.getMetaData();
		
		// add schemas
		tmp = getMetaSchemas(dbMeta);
		if (tmp != null) {
			schemas.addAll(tmp);
		}

		// add catalogs
		tmp = getMetaCatalogs(dbMeta);
		if (tmp != null) {
			schemas.addAll(tmp);
		}
		
		return schemas;
	}

	@Override
	public ResultSet getTables(SqlConnection connection, String dbName) throws SQLException {
		// by default, database `dbName` is considered a schema, sometimes it needs to be considered
		// as a catalog
		return connection.getMetaData().getTables(dbName, null, "%", new String[] {"TABLE", "VIEW" }/*tableTypes*/); //fix by kokon - show only tables and views
	}

    /* (non-Javadoc)
     * @see org.jetel.connection.jdbc.specific.JdbcSpecific#getColumns(java.sql.Connection, java.lang.String, java.lang.String)
     */
    @Override
	public ResultSetMetaData getColumns(SqlConnection connection, String schema, String owner, String table) throws SQLException {
		String sqlQuery = compileSelectQuery4Table(schema, owner, table) + " where 0=1";
		ResultSet resultSet = connection.createStatement().executeQuery(sqlQuery);

		return resultSet.getMetaData();
    }
    
    /* (non-Javadoc)
     * @see org.jetel.connection.jdbc.specific.JdbcSpecific#compileSelectQuery4Table(java.lang.String, java.lang.String)
     */
    @Override
	public String compileSelectQuery4Table(String schema, String owner, String table) {
    	if (isSchemaRequired() && !StringUtils.isEmpty(schema)) {
    		return "select * from " + quoteIdentifier(schema) + "." + quoteIdentifier(table);
    	} else {
    		return "select * from " + quoteIdentifier(table);
    	}
    }
    
	
	@Override
	public Set<ResultSet> getColumns(SqlConnection connection) throws SQLException {
		Set<ResultSet> resultSets = new HashSet<ResultSet>();
		try {
			resultSets.add(connection.getMetaData().getColumns(null, null, null, "%"));
		} catch (SQLException e) {
		}
		return resultSets;
	}
	
	
	@Override
	public boolean isSchemaRequired() {
		return false;
	}

	@Override
	public String getTablePrefix(String schema, String owner,
			boolean quoteIdentifiers) {
		return quoteIdentifiers ? quoteIdentifier(schema) : schema;
	}
	
	@Override
	public boolean isJetelTypeConvertible2sql(int sqlType, DataFieldMetadata field) {
		if (field.getDataType() == DataFieldType.STRING) {
    		//handle string type
    		try {
				//check if given type represents string
				if (sqlType == Types.CHAR || sqlType == Types.NCHAR || sqlType == Types.VARCHAR ||  
						sqlType == Types.NVARCHAR || sqlType == Types.CLOB || sqlType == Types.NCLOB) {
					return true;
				}
			} catch (NumberFormatException e) {
				return false;
			}
    	}
		return sqlType == jetelType2sql(field);
	}

	@Override
	public boolean isSqlTypeConvertible2jetel(int sqlType, DataFieldMetadata field) {
		return sqlType2jetel(sqlType) == field.getType();
	}
	
	@Override
	public boolean supportsGetGeneratedKeys(DatabaseMetaData metadata) throws SQLException {
		try {
			boolean result = metadata.supportsGetGeneratedKeys();
			return result;
		}
		catch (Exception e) {
			if (e instanceof SQLException) throw (SQLException)e;  
			return false;
		}
	}

	@Override
	public List<Integer> getFieldTypes(ResultSetMetaData resultSetMetadata, DataRecordMetadata cloverMetadata) throws SQLException {
		return SQLUtil.getFieldTypes(resultSetMetadata);
	}
	
	@Override
	public int getSqlTypeByTypeName(String sqlTypeName) {
		return Types.CLOB;
	}

	@Override
	public boolean useSavepoints() {
		return false;
	}

	@Override
	public void unloadDriver(JdbcDriver driver) {
		// do nothing by default
	}

	@Override
	public ResultSet wrapResultSet(ResultSet resultSet) {
		return resultSet;
	}
}
