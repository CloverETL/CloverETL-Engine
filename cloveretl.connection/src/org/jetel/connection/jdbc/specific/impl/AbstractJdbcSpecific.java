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
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
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
import org.jetel.connection.jdbc.AbstractCopySQLData.CopyStringToString;
import org.jetel.connection.jdbc.AbstractCopySQLData.CopyTime;
import org.jetel.connection.jdbc.AbstractCopySQLData.CopyTimestamp;
import org.jetel.connection.jdbc.SQLUtil;
import org.jetel.connection.jdbc.specific.conn.BasicSqlConnection;
import org.jetel.data.DataRecord;
import org.jetel.database.sql.CopySQLData;
import org.jetel.database.sql.DBConnection;
import org.jetel.database.sql.DbMetadata;
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

    /**
	 * 
	 */
	private static final String DEFAULT_CHAR_FIELD_SIZE = "80";

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
	public Connection connect(Driver driver, String url, Properties info) throws SQLException {
		return driver.connect(url, info);
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
	public SqlConnection createSQLConnection(DBConnection dbConnection, Connection connection, OperationType operationType) throws JetelException {
		return new BasicSqlConnection(dbConnection, connection, operationType);
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
		switch (field.getDataType()) {
		case INTEGER:
			return Types.INTEGER;
		case NUMBER:
			return Types.FLOAT;
		case STRING:
			return field.isFixed() ? Types.CHAR : Types.VARCHAR;
		case DATE:
			boolean isDate = field.isDateFormat();
			boolean isTime = field.isTimeFormat();
			if (isDate && isTime || StringUtils.isEmpty(field.getFormatStr())) 
				return Types.TIMESTAMP;
			if (isDate)
				return Types.DATE;
			if (isTime)
				return Types.TIME;
			return Types.TIMESTAMP;
        case LONG:
            return Types.BIGINT;
        case DECIMAL:
            return Types.DECIMAL;
        case BYTE:
        case CBYTE:
        	if (!StringUtils.isEmpty(field.getFormatStr())
					&& field.getFormatStr().equalsIgnoreCase(DataFieldMetadata.BLOB_FORMAT_STRING)) {
        		return Types.BLOB;
        	}
            return field.isFixed() ? Types.BINARY : Types.VARBINARY;
        case BOOLEAN:
        	return Types.BOOLEAN;
		default:
			throw new IllegalArgumentException("Can't handle Clover's data type :"+field.getDataType().getName());
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
			return sqlType2str(sqlType) + "(" + (field.getSize()>0 ? String.valueOf(field.getSize()) : DEFAULT_CHAR_FIELD_SIZE) + ")";
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
	public DataFieldType sqlType2jetel(DbMetadata dbMetadata, int sqlIndex) throws SQLException {
		int sqlType = dbMetadata.getType(sqlIndex);
		int precision = dbMetadata.getPrecision(sqlIndex);
		return this.sqlType2jetel(sqlType, precision);
	}

	@Override
	public DataFieldType sqlType2jetel(int sqlType, int sqlPrecision) {
		return DataFieldType.fromChar(sqlType2jetel(sqlType));
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
				return DataFieldType.INTEGER.getShortName();
			//-------------------
			case Types.BIGINT:
				return DataFieldType.LONG.getShortName();
			//-------------------
			case Types.DECIMAL:
			case Types.NUMERIC:
				return DataFieldType.DECIMAL.getShortName();
			case Types.DOUBLE:
			case Types.FLOAT:
			case Types.REAL:
				return DataFieldType.NUMBER.getShortName();
			//------------------
			case Types.CHAR:
			case Types.LONGVARCHAR:
			case Types.VARCHAR:
			case Types.CLOB:
			case Types.NCHAR:
			case Types.NVARCHAR:
			case Types.NCLOB:
				return DataFieldType.STRING.getShortName();
			//------------------
			case Types.DATE:
			case Types.TIME:
			case Types.TIMESTAMP:
				return DataFieldType.DATE.getShortName();
            //-----------------
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB:
			case Types.OTHER:
				return DataFieldType.BYTE.getShortName();
			//-----------------
			case Types.BOOLEAN:
				return DataFieldType.BOOLEAN.getShortName();
			// proximity assignment
			case Types.BIT:
			case Types.NULL:
				return DataFieldType.STRING.getShortName();
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
		DataFieldType jetelType = fieldMetadata.getDataType();
		CopySQLData obj = null;
		switch (sqlType) {
			case Types.ARRAY:
				obj = new CopyArray(record, fromIndex, toIndex);
				break;
			case Types.CHAR:
			case Types.LONGVARCHAR:
			case Types.VARCHAR:
				if (jetelType == DataFieldType.STRING) {
					// copy from string DB fields to string Clover fields
					obj = new CopyStringToString(record, fromIndex, toIndex);
				} else {
					// copy from string DB fields to any Clover type
					obj = new CopyString(record, fromIndex, toIndex);
				}
				break;
			case Types.INTEGER:
			case Types.SMALLINT:
				if (jetelType == DataFieldType.BOOLEAN) {
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
				if (jetelType == DataFieldType.INTEGER) {
					obj = new CopyInteger(record, fromIndex, toIndex);
				} else if (jetelType == DataFieldType.LONG) {
					obj = new CopyLong(record, fromIndex, toIndex);
				} else if(jetelType == DataFieldType.NUMBER) {
				    obj = new CopyNumeric(record, fromIndex, toIndex);
				} else {
					obj = new CopyDecimal(record, fromIndex, toIndex);
				}
				break;
			case Types.NUMERIC:
				// Oracle doesn't have boolean type, data type SMALLINT is the same as NUMBER(38);
				// see issue #3815
				if (jetelType == DataFieldType.BOOLEAN) {
					obj = new CopyBoolean(record, fromIndex, toIndex);
				}else if (jetelType == DataFieldType.INTEGER) {
					obj = new CopyInteger(record, fromIndex, toIndex);
				} else if (jetelType == DataFieldType.LONG) {
					obj = new CopyLong(record, fromIndex, toIndex);
				} else if(jetelType == DataFieldType.NUMBER) {
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
				if (jetelType == DataFieldType.BOOLEAN) {
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
	public boolean isBackslashEscaping() {
		return false;
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
		
		switch (field.getDataType()) {
		case NUMBER:
			switch (sqlType) {
			case Types.DOUBLE:
				return true;
			}
		case INTEGER:
			switch (sqlType) {
			case Types.BIGINT:
				return true;
			}
		case STRING:
			switch (sqlType) {
			case Types.CHAR:
			case Types.NCHAR:
			case Types.VARCHAR:
			case Types.NVARCHAR:
			case Types.CLOB:
			case Types.NCLOB:
				return true;
			}
		default:
			return sqlType == jetelType2sql(field);
		}
	}

	@Override
	public boolean isSqlTypeConvertible2jetel(int sqlType, DataFieldMetadata field) {
		return sqlType2jetel(sqlType) == field.getDataType().getShortName();
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
	
	@Override
	public String getCreateTableSuffix(DataRecordMetadata metadata) {
		return "";
	}
}
