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

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jetel.connection.jdbc.AbstractCopySQLData;
import org.jetel.connection.jdbc.AbstractCopySQLData.CopyDecimal;
import org.jetel.connection.jdbc.SQLUtil;
import org.jetel.connection.jdbc.specific.conn.SQLiteConnection;
import org.jetel.data.DataRecord;
import org.jetel.data.DecimalDataField;
import org.jetel.database.sql.CopySQLData;
import org.jetel.database.sql.DBConnection;
import org.jetel.database.sql.QueryType;
import org.jetel.database.sql.SqlConnection;
import org.jetel.exception.JetelException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.string.StringUtils;

/**
 * A JdbcSpecific for SQLite serverless database
 *  
 * @author Pavel Najvar (pavel.najvar@javlin.eu)
 *         (c) Javlin, a.s. (www.javlin.eu)
 *
 * @created Sep 2009
 */
public class SQLiteSpecific extends AbstractJdbcSpecific {

	private static final SQLiteSpecific INSTANCE = new SQLiteSpecific();
	
	public static SQLiteSpecific getInstance() {
		return INSTANCE;
	}

	protected SQLiteSpecific() {
		super();
	}

	@Override
	public SqlConnection createSQLConnection(DBConnection dbConnection, Connection connection, OperationType operationType) throws JetelException {
		return new SQLiteConnection(dbConnection, connection, operationType);
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.connection.jdbc.specific.impl.AbstractJdbcSpecific#createCopyObject(int, org.jetel.metadata.DataFieldMetadata, org.jetel.data.DataRecord, int, int)
	 */
	@Override
	public CopySQLData createCopyObject(int sqlType, DataFieldMetadata fieldMetadata, DataRecord record, int fromIndex, int toIndex) {
		CopySQLData copyObj = super.createCopyObject(sqlType, fieldMetadata, record, fromIndex, toIndex);

		//if CopyDecimal was considered to be used we need to create slightly different copy object, since
		//the SQLite JDBC driver does not support BigDecimal data type - PreparedStatement.getBigDecimal() method
		//is not supported - decimal value has to be retrieved by getDouble() method
		if (copyObj instanceof CopyDecimal) {
			copyObj = new CopyDecimalAsDouble(record, fromIndex, toIndex);
			copyObj.setSqlType(sqlType);
		}
		
		return copyObj;
	}
	
	@Override
	public String quoteIdentifier(String identifier) {
        return ('"' + identifier + '"');
    }
	
	@Override
	public String getValidateQuery(String query, QueryType queryType, boolean optimizeSelectQuery) throws SQLException {
		if(queryType==QueryType.SELECT) {
			query = SQLUtil.removeUnnamedFields(query, this);
			String q;
			if (optimizeSelectQuery) {
				q = "SELECT wrapper_table.* FROM (" + query + ") wrapper_table limit 1";
			} else {
				q = query;
			}
			return q;
		}
		return super.getValidateQuery(query, queryType, optimizeSelectQuery);
	}
	
	@Override
	public String getTablePrefix(String schema, String owner,
			boolean quoteIdentifiers) {
		int position = schema.indexOf('[');
		schema = schema.substring(0, position - 1);
		return quoteIdentifiers ? quoteIdentifier(schema) : schema;
	}

	@Override
	public void optimizeResultSet(ResultSet resultSet,
			OperationType operationType) {

		switch (operationType){
		case READ:
			try {
				resultSet.setFetchDirection(ResultSet.FETCH_FORWARD);
				// SQLite driver MUST HAVE fetch size set to 0 - otherwise it limits number of results returned
				resultSet.setFetchSize(0);
			} catch(SQLException ex) {
				//TODO: for now, do nothing
			}
		}

	}
	
	@Override
	public Set<ResultSet> getColumns(SqlConnection connection) throws SQLException {
		Set<ResultSet> resultSets = new HashSet<ResultSet>();
	    DatabaseMetaData md = connection.getMetaData();
	    
	    ResultSet rs = md.getTables(null, null, null, null);
	    ArrayList<String> tables = new ArrayList<String>();
	    while(rs.next()) {
	    	tables.add(rs.getString(3));
	    }
	    rs.close();
	    
	    for (String table : tables) {
			resultSets.add(md.getColumns(null, null, table, null));
		}
	    return resultSets;
	}

	@Override
	public ResultSetMetaData getColumns(SqlConnection connection, String schema, String owner, String table) throws SQLException {
		if (connection == null || StringUtils.isEmpty(table)) {
			return null;
		}
		
		Statement stat = connection.createStatement();
	    ResultSet rs = stat.executeQuery("PRAGMA table_info(" + table + ");");
	    
	    SQLiteRSMetaData metadata = new SQLiteRSMetaData(table);
	    while (rs.next()) {
	    	String columnName = rs.getString(2);
	    	String columnType = rs.getString(3);
	    	metadata.addColumn(columnName, columnType);
	    }
	    rs.close();
	    
		return metadata;
	}
	
    class SQLiteRSMetaData implements  ResultSetMetaData {
    	private String tableName = null;
    	private ArrayList<String> columnNames = new ArrayList<String>();
    	private ArrayList<String> columnTypes = new ArrayList<String>();
    	
    	private Pattern TYPE_WITH_LENGTH = Pattern.compile("(.+)\\((.+)\\)");
    	private Pattern TYPE_WITH_PRECISION = Pattern.compile("(.+)\\((.+),(.+)\\)");
    	
    	protected SQLiteRSMetaData(String tableName) {
    		this.tableName = tableName;
    	}
    	
    	protected void addColumn(String name, String type) {
    		this.columnNames.add(name);
    		if(type==null) {
    			type="";
    		}
    		if(StringUtils.isEmpty(type.trim())) {
    			type = "unknown";
    		}
    		this.columnTypes.add(type);
    	}
    	
		@Override
		public String getCatalogName(int column) throws SQLException {
			return this.tableName;
		}

		@Override
		public String getColumnClassName(int column) throws SQLException {
			return null;
		}

		@Override
		public int getColumnCount() throws SQLException {
			return this.columnNames.size();
		}

		@Override
		public int getColumnDisplaySize(int column) throws SQLException {
			if(this.columnTypes!=null) {
				Matcher m = TYPE_WITH_LENGTH.matcher(this.columnTypes.get(column-1));
				if(m.matches()) {
					try {
						return Integer.parseInt(m.group(2));
					} catch (NumberFormatException e) {
						return 0;
					}
					
				}
			}
			return Integer.MAX_VALUE;
		}

		@Override
		public String getColumnLabel(int column) throws SQLException {
			return this.getColumnName(column);
		}

		@Override
		public String getColumnName(int column) throws SQLException {
			
			// TODO Auto-generated method stub
			return this.columnNames.get(column-1);
		}

		@Override
		public int getColumnType(int column) throws SQLException {
			String type = this.getColumnTypeName(column);
			if(type==null) {
				return 0;
			}
			
			if(type.equalsIgnoreCase("int")) return Types.INTEGER;
			if(type.equalsIgnoreCase("integer")) return Types.INTEGER;
			if(type.equalsIgnoreCase("TINYINT")) return Types.TINYINT;
			if(type.equalsIgnoreCase("SMALLINT")) return Types.SMALLINT;
			if(type.equalsIgnoreCase("MEDIUMINT")) return Types.INTEGER;
			if(type.equalsIgnoreCase("BIGINT")) return Types.BIGINT;
			if(type.equalsIgnoreCase("UNSIGNED BIG INT")) return Types.BIGINT;
			if(type.equalsIgnoreCase("int2")) return Types.INTEGER;
			if(type.equalsIgnoreCase("int8")) return Types.INTEGER;

			if(type.equalsIgnoreCase("text")) return Types.CLOB;
			if(type.equalsIgnoreCase("char")) return Types.CHAR;
			if(type.equalsIgnoreCase("CHARACTER")) return Types.CHAR;
			if(type.equalsIgnoreCase("varchar")) return Types.VARCHAR;
			if(type.equalsIgnoreCase("VARYING CHARACTER")) return Types.VARCHAR;
			if(type.equalsIgnoreCase("NCHAR")) return Types.CHAR;
			if(type.equalsIgnoreCase("NATIVE CHARACTER")) return Types.CHAR;
			if(type.equalsIgnoreCase("NVARCHAR")) return Types.VARCHAR;

			if(type.equalsIgnoreCase("varbinary")) return Types.VARBINARY;
			if(type.equalsIgnoreCase("binary")) return Types.BINARY;
			
			if(type.equalsIgnoreCase("blob")) return Types.BLOB;
			
			if(type.equalsIgnoreCase("DOUBLE")) return Types.DOUBLE;
			if(type.equalsIgnoreCase("DOUBLE PRECISION")) return Types.DOUBLE;
			if(type.equalsIgnoreCase("FLOAT")) return Types.FLOAT;

			if(type.equalsIgnoreCase("NUMERIC")) return Types.NUMERIC;
			if(type.equalsIgnoreCase("DECIMAL")) return Types.DECIMAL;
			if(type.equalsIgnoreCase("BOOLEAN")) return Types.BOOLEAN;
			if(type.equalsIgnoreCase("DATE")) return Types.DATE;
			if(type.equalsIgnoreCase("DATETIME")) return Types.TIMESTAMP;
			if(type.equalsIgnoreCase("TIMESTAMP")) return Types.TIMESTAMP;
			if(type.equalsIgnoreCase("TIME")) return Types.TIME;
			
			return 0;
		}

		@Override
		public String getColumnTypeName(int column) throws SQLException {
			if(this.columnTypes!=null) {
				Matcher m = TYPE_WITH_LENGTH.matcher(this.columnTypes.get(column-1));
				if(m.matches()) {
					return m.group(1);
				}
			}
			return this.columnTypes.get(column-1);
		}

		@Override
		public int getPrecision(int column) throws SQLException {
			if(this.columnTypes!=null) {
				Matcher m = TYPE_WITH_PRECISION.matcher(this.columnTypes.get(column-1));
				if(m.matches()) {
					try {
						int prec = Integer.parseInt(m.group(2));;
						int scale = Integer.parseInt(m.group(3));
						if(scale>=0) {
							prec -= scale;
						}
						return prec; 
					} catch (NumberFormatException e) {
						return 0;
					}
					
				}
			}
			return 0;
		}

		@Override
		public int getScale(int column) throws SQLException {
			if(this.columnTypes!=null) {
				Matcher m = TYPE_WITH_PRECISION.matcher(this.columnTypes.get(column-1));
				if(m.matches()) {
					try {
						return Integer.parseInt(m.group(3));
					} catch (NumberFormatException e) {
						return 0;
					}
					
				}
			}
			return 0;
		}

		@Override
		public String getSchemaName(int column) throws SQLException {
			return this.tableName;
		}

		@Override
		public String getTableName(int column) throws SQLException {
			// TODO Auto-generated method stub
			return this.tableName;
		}

		@Override
		public boolean isAutoIncrement(int column) throws SQLException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean isCaseSensitive(int column) throws SQLException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean isCurrency(int column) throws SQLException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean isDefinitelyWritable(int column) throws SQLException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public int isNullable(int column) throws SQLException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public boolean isReadOnly(int column) throws SQLException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean isSearchable(int column) throws SQLException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean isSigned(int column) throws SQLException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean isWritable(int column) throws SQLException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean isWrapperFor(Class<?> iface) throws SQLException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public <T> T unwrap(Class<T> iface) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}
    	
    }
	
	@Override
	public List<Integer> getFieldTypes(ResultSetMetaData resultSetMetadata, DataRecordMetadata cloverMetadata) throws SQLException {
		return SQLUtil.getFieldTypes(cloverMetadata, this);
	}	
	
	/**
	 * This implementation of copy object is very similar to {@link CopyDecimal} class.
	 * However, data from database are retrieved as double data type - not BigDecimal. 
	 */
	private static class CopyDecimalAsDouble extends AbstractCopySQLData {

		public CopyDecimalAsDouble(DataRecord record, int fieldSQL, int fieldJetel) {
			super(record, fieldSQL, fieldJetel);
		}

		@Override
		public void setJetel(ResultSet resultSet) throws SQLException {
			double d = resultSet.getDouble(fieldSQL);
			if (resultSet.wasNull()) {
				((DecimalDataField) field).setNull(true);
			} else {
				((DecimalDataField) field).setValue(d);
			}
		}

		@Override
		public void setJetel(CallableStatement statement) throws SQLException {
			double d = statement.getDouble(fieldSQL);
			if (statement.wasNull()) {
				((DecimalDataField) field).setNull(true);
			} else {
				((DecimalDataField) field).setValue(d);
			}
		}

		@Override
		public void setSQL(PreparedStatement pStatement) throws SQLException {
			if (!field.isNull()) {
				pStatement.setDouble(fieldSQL, ((DecimalDataField) field).getDouble());
			} else {
				pStatement.setNull(fieldSQL, java.sql.Types.DECIMAL);
			}

		}
		
		@Override
		public Object getDbValue(ResultSet resultSet) throws SQLException {
			double d = resultSet.getDouble(fieldSQL);
			return resultSet.wasNull() ? null : d;
		}

		@Override
		public Object getDbValue(CallableStatement statement) throws SQLException {
			double d = statement.getDouble(fieldSQL);
			return statement.wasNull() ? null : d;
		}
		
	}
	
}
