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

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.ListIterator;

import javax.sql.rowset.serial.SerialBlob;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.connection.jdbc.specific.impl.DefaultJdbcSpecific;
import org.jetel.data.BooleanDataField;
import org.jetel.data.ByteDataField;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.DateDataField;
import org.jetel.data.DecimalDataField;
import org.jetel.data.IntegerDataField;
import org.jetel.data.LongDataField;
import org.jetel.data.NumericDataField;
import org.jetel.data.StringDataField;
import org.jetel.data.primitive.Decimal;
import org.jetel.data.primitive.HugeDecimal;
import org.jetel.database.sql.CopySQLData;
import org.jetel.database.sql.JdbcSpecific;
import org.jetel.exception.JetelException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.string.StringUtils;

/**
 *  Class for creating mappings between CloverETL's DataRecords and JDBC's
 *  ResultSets.<br>
 *  It also contains inner classes for translating various CloverETL's DataField
 *  types onto JDBC types.
 *
 * @author      dpavlis
 * @since       October 7, 2002
 * @created     8. ???ervenec 2003
 */
public abstract class AbstractCopySQLData implements CopySQLData {

	/**
	 *  Description of the Field
	 *
	 * @since    October 7, 2002
	 */
	protected int fieldSQL,fieldJetel;
	/**
	 *  Description of the Field
	 *
	 * @since    October 7, 2002
	 */
	protected DataField field;

	/*
	 * SQL type (java.sql.Types) of the corresponding field
	 */
	protected int sqlType;
	
    protected boolean inBatchUpdate = false; // indicates whether batchMode is used when populating target DB

	static Log logger = LogFactory.getLog(CopySQLData.class);

	/**
	 *  Constructor for the CopySQLData object
	 *
	 * @param  record      Clover record which will be source or target
	 * @param  fieldSQL    index of the field in SQL statement
	 * @param  fieldJetel  index of the field in Clover record
	 * @since              October 7, 2002
	 */
	protected AbstractCopySQLData(DataRecord record, int fieldSQL, int fieldJetel) {
		this.fieldSQL = fieldSQL + 1;
		// fields in ResultSet start with index 1
		this.fieldJetel=fieldJetel;
		field = record.getField(fieldJetel);
	}

	@Override
	public int getFieldJetel() {
		return fieldJetel;
	}
	
	@Override
	public DataField getField() {
		return field;
	}
	
	/**
	 * Assigns different DataField than the original used when creating
	 * copy object
	 * 
	 * @param field	New DataField
	 */
	@Override
	public void setCloverField(DataField field){
	    this.field=field;
	}
	
	
	/**
	 * Assings different DataField (DataRecord). The proper field
	 * is taken from DataRecord based on previously saved field number.
	 * 
	 * @param record	New DataRecord
	 */
	@Override
	public void setCloverRecord(DataRecord record){
	    this.field=record.getField(fieldJetel);
	}

    
     /**
     * @return Returns the inBatchUpdate.
     */
    @Override
	public boolean isInBatchUpdate() {
        return inBatchUpdate;
    }

    
    @Override
	public int getSqlType() {
		return sqlType;
	}

	@Override
	public void setSqlType(int sqlType) {
		this.sqlType = sqlType;
	}

	/**
     * @param inBatchUpdate The inBatchUpdate to set.
     */
    @Override
	public void setInBatchUpdate(boolean inBatch) {
        this.inBatchUpdate = inBatch;
    }
	
	/**
	 *  Sets value of Jetel/Clover data field based on value from SQL ResultSet
	 *
	 * @param  resultSet         Description of Parameter
	 * @exception  SQLException  Description of Exception
	 * @since                    October 7, 2002
	 */
	@Override
	public void sql2jetel(ResultSet resultSet) throws SQLException {
		try {
			setJetel(resultSet);
		} catch (SQLException ex) {
			throw new SQLException("Error on field '" + field.getMetadata().getName() + "'", ex);
		} catch (ClassCastException ex){
		    throw new SQLException("Incompatible Clover & JDBC field types - field '"+field.getMetadata().getName()+
		            "'; Clover type: "+SQLUtil.jetelType2Str(field.getMetadata().getType()) + "; SQL type: " + SQLUtil.sqlType2str(getSqlType()));
		}
	}


	/**
	 *  Sets value of SQL field in PreparedStatement based on Jetel/Clover data
	 *  field's value
	 *
	 * @param  pStatement        Description of Parameter
	 * @exception  SQLException  Description of Exception
	 * @since                    October 7, 2002
	 */
	@Override
	public void jetel2sql(PreparedStatement pStatement) throws SQLException {
		try {
			setSQL(pStatement);
		} catch (SQLException ex) {
			throw new SQLException("Error on field '" + field.getMetadata().getName() + "'", ex);
		}catch (ClassCastException ex){
		    throw new SQLException("Incompatible Clover & JDBC field types - field '"+field.getMetadata().getName()+
		            "'; Clover type: "+SQLUtil.jetelType2Str(field.getMetadata().getType()) + "; SQL type: " + SQLUtil.sqlType2str(getSqlType()), ex);
		}
	}

	/**
	 * @param resultSet
	 * @return current value from result set from corresponding index
	 * @throws SQLException
	 */
	@Override
	public abstract Object getDbValue(ResultSet resultSet) throws SQLException;

	/**
	 * @param statement
	 * @return current value from callable statement from corresponding index
	 * @throws SQLException
	 */
	@Override
	public abstract Object getDbValue(CallableStatement statement) throws SQLException;
	
	/**
	 * @return current value from data record from corresponding field
	 */
	@Override
	public Object getCloverValue() {
		return field.getValue();
	}


	/**
	 *  Sets the Jetel attribute of the CopySQLData object
	 *
	 * @param  resultSet         The new Jetel value
	 * @exception  SQLException  Description of Exception
	 * @since                    October 7, 2002
	 */
	@Override
	public abstract void setJetel(ResultSet resultSet) throws SQLException;

	@Override
	public abstract void setJetel(CallableStatement statement) throws SQLException;

	/**
	 *  Sets the SQL attribute of the CopySQLData object
	 *
	 * @param  pStatement        The new SQL value
	 * @exception  SQLException  Description of Exception
	 * @since                    October 7, 2002
	 */
	@Override
	public abstract void setSQL(PreparedStatement pStatement) throws SQLException;


	
	
	/**
	 * Assigns new instance of DataRecord to existing CopySQLData structure (array).
	 * Useful when CopySQLData (aka transmap) was created using some other
	 * DataRecord and new one needs to be used
	 * 
	 * @param transMap	Array of CopySQLData object - the transmap
	 * @param record	New DataRecord object to be used within transmap
	 */
	public static void resetDataRecord(CopySQLData[] transMap,DataRecord record){
	    for(int i=0;i<transMap.length;i++){
	    	if (transMap[i] != null) {
	    		transMap[i].setCloverRecord(record);
	    	}
	    }
	}
	
    /**
     * Set on/off working in batchUpdate mode. In this mode (especially on Oracle 10)
     * we have to create new object for each setDate(), setTimestamp() statement call.
     * 
     * @param transMap
     * @param inBatch
     */
    public static void setBatchUpdate(CopySQLData[] transMap, boolean inBatch){
        for(int i=0;i<transMap.length;transMap[i++].setInBatchUpdate(inBatch));
    }
    
	/**
	 *  Creates translation array for copying data from Database record into Jetel
	 *  record
	 *
	 * @param  metadata    Metadata describing Jetel data record
	 * @param  record      Jetel data record
	 * @param  fieldTypes  Description of the Parameter
	 * @return             Array of CopySQLData objects which can be used when getting
	 *      data from DB into Jetel record
	 * @since              September 26, 2002
	 */
	public static CopySQLData[] sql2JetelTransMap(List fieldTypes, DataRecordMetadata metadata, DataRecord record, JdbcSpecific jdbcSpecific) {
		/* test that both sides have at least the same number of fields, less
		 * fields on DB side is O.K. (some of Clover fields won't get assigned value).
		 */
		if (fieldTypes.size()>metadata.getNumFields()){
			throw new RuntimeException("CloverETL data record "+metadata.getName()+
					" contains less fields than source JDBC record !");
		}
		
		CopySQLData[] transMap = new CopySQLData[fieldTypes.size()];
		int i = 0;
		Integer type;
		for (ListIterator iterator = fieldTypes.listIterator(); iterator.hasNext(); ) {
			type = (Integer) iterator.next();
			/*
			 * pnajvar
			 * If target Clover type is string, we'll always use CopyString
			 * else we use whatever is appropriate
			 */
			transMap[i] = jdbcSpecific.createCopyObject(
					(record.getField(i).getMetadata().getType() == DataFieldMetadata.STRING_FIELD
					        && type != CopyOracleXml.XML_TYPE && type != Types.ARRAY) ? Types.VARCHAR : type.shortValue(),
					record.getField(i).getMetadata(),
					record, i, i);
			i++;
		}

		return transMap;
	}

	/**
	 *  Creates translation array for copying data from Database record into Jetel
	 *  record
	 *
	 * @param fieldTypes
	 * @param metadata Metadata describing Jetel data record
	 * @param record Jetel data record
	 * @param keyFields fields used for creating translation array
	 * @return Array of CopySQLData objects which can be used when getting
	 *      data from DB into Jetel record
	 */
	public static CopySQLData[] sql2JetelTransMap(List fieldTypes, DataRecordMetadata metadata, DataRecord record,
			String[] keyFields, JdbcSpecific jdbcSpecific) {

		if (fieldTypes.size() != keyFields.length){
			throw new RuntimeException("Number of db fields (" + fieldTypes.size() + ") is different then " +
					"number of key fields " + keyFields.length + ")." );
		}
		CopySQLData[] transMap = new CopySQLData[fieldTypes.size()];
		int fieldIndex;
		Integer type;
		for (int i=0; i < keyFields.length; i++) {
			fieldIndex = record.getMetadata().getFieldPosition(keyFields[i]);
			if (fieldIndex == -1) {
				throw new RuntimeException("Field " + StringUtils.quote(keyFields[i]) + " doesn't exist in metadata " +
						StringUtils.quote(record.getMetadata().getName()));
			}
			type = (Integer) fieldTypes.get(i);
			transMap[i] = jdbcSpecific.createCopyObject(
					(type != CopyOracleXml.XML_TYPE && record.getField(fieldIndex).getMetadata().getType()
					        == DataFieldMetadata.STRING_FIELD) ? Types.VARCHAR : type.shortValue(),
					record.getField(fieldIndex).getMetadata(),
					record, i, metadata.getFieldPosition(keyFields[i]));
		}
		
		return transMap;
		
	}

	/**
	 *  Creates translation array for copying data from Jetel record into Database
	 *  record
	 *
	 * @param  fieldTypes        JDBC field types - of the target JDBC data fields
	 * @param  record            DataRecord which will be used to populate DB
	 * @return                   the transMap (array of CopySQLData) object
	 * @exception  SQLException  Description of Exception
	 * @since                    October 4, 2002
	 */
	public static CopySQLData[] jetel2sqlTransMap(List fieldTypes, DataRecord record, JdbcSpecific jdbcSpecific) {
		int i = 0;
		/* test that both sides have at least the same number of fields, less
		 * fields on DB side is O.K. (some of Clover fields won't be assigned to JDBC).
		 */
		if (fieldTypes.size()>record.getMetadata().getNumFields()){
			throw new RuntimeException("CloverETL data record "+record.getMetadata().getName()+
					" contains less fields than target JDBC record !");
		}
		CopySQLData[] transMap = new CopySQLData[fieldTypes.size()];
		ListIterator iterator = fieldTypes.listIterator();
		
		while (iterator.hasNext()) {
			transMap[i] = jdbcSpecific.createCopyObject(((Integer) iterator.next()).shortValue(),
					record.getField(i).getMetadata(),
					record, i, i);
			i++;
		}
		return transMap;
	}
	
	


	/**
	 *  Creates translation array for copying data from Jetel record into Database
	 *  record. It allows only certain (specified) Clover fields to be considered
	 *
	 * @param  fieldTypes          DJDBC field types - of the target JDBC data fields
	 * @param  record              Description of the Parameter
	 * @param  cloverFields        array of DataRecord record's field names which should be considered for mapping
	 * @return                     Description of the Return Value
	 * @exception  SQLException    Description of the Exception
	 * @exception  JetelException  Description of the Exception
	 */
	public static CopySQLData[] jetel2sqlTransMap(List fieldTypes, DataRecord record, String[] cloverFields, JdbcSpecific jdbcSpecific)
			 throws SQLException, JetelException {
		int i = 0;
		int fromIndex = 0;
		int toIndex = 0;
		short jdbcType;

		CopySQLData[] transMap = new CopySQLData[fieldTypes.size()];
		ListIterator iterator = fieldTypes.listIterator();

		while (iterator.hasNext()) {
			jdbcType = ((Integer) iterator.next()).shortValue();
			// from index is index of specified cloverField in the Clover record
			if (i >= cloverFields.length) {
				throw new JetelException(" Number of db fields (" + fieldTypes.size() + ") is diffrent then number of clover fields (" + cloverFields.length + ") !" );
			}
			fromIndex = record.getMetadata().getFieldPosition(cloverFields[i]);
			if (fromIndex == -1) {
				throw new JetelException(" Field \"" + cloverFields[i] + "\" does not exist in DataRecord !");
			}
			// we copy from Clover's field to JDBC - toIndex/fromIndex is switched here
			transMap[i++] = jdbcSpecific.createCopyObject(jdbcType, record.getField(fromIndex).getMetadata(), 
					record, toIndex, fromIndex);
			toIndex++;// we go one by one - order defined by insert/update statement

		}
		return transMap;
	}

	/**
	 *  Creates translation array for copying data from Jetel record into Database
	 *  record. It allows only certain (specified) Clover fields to be considered
	 *
	 * @param  fieldTypes          JDBC field types - of the target JDBC data fields
	 * @param  record              Description of the Parameter
	 * @param  cloverFields        array of DataRecord record's field numbers which should be considered for mapping
	 * @return                     Description of the Return Value
	 * @exception  SQLException    Description of the Exception
	 * @exception  JetelException  Description of the Exception
	 */
	public static CopySQLData[] jetel2sqlTransMap(List fieldTypes,
            DataRecord record, int[] cloverFields, JdbcSpecific jdbcSpecific) throws SQLException,
            JetelException {
        int i = 0;
        int fromIndex = 0;
        int toIndex = 0;
        short jdbcType;
     
        CopySQLData[] transMap = new CopySQLData[fieldTypes.size()];
        ListIterator iterator = fieldTypes.listIterator();

        while (iterator.hasNext()) {
            jdbcType = ((Integer) iterator.next()).shortValue();
            // from index is index of specified cloverField in the Clover record
            fromIndex = cloverFields[i];
            if (fromIndex == -1) {
                throw new JetelException(" Field \"" + cloverFields[i]
                        + "\" does not exist in DataRecord !");
            }
            // we copy from Clover's field to JDBC - toIndex/fromIndex is
            // switched here
            transMap[i++] = jdbcSpecific.createCopyObject(jdbcType, record.getField(fromIndex).getMetadata(), 
            		record, toIndex, fromIndex);
            toIndex++;// we go one by one - order defined by insert/update statement

        }
        return transMap;
    }
	
	/**
	 * Creates translation array for copying data from Jetel record into Database
	 *  record. It allows only certain (specified) Clover fields to be considered.<br>
	 * <i>Note:</i> the target (JDBC) data types are guessed from Jetel field types - so use this
	 * method only as the last resort.
	 * 
	 * @param record
	 * @param cloverFields  array of DataRecord record's field numbers which should be considered for mapping
	 * @return
	 * @throws JetelException
	 */
	@Deprecated
	public static CopySQLData[] jetel2sqlTransMap(DataRecord record, int[] cloverFields) throws JetelException {
        return jetel2sqlTransMap(record, cloverFields, DefaultJdbcSpecific.getInstance());
    }
	
	public static CopySQLData[] jetel2sqlTransMap(DataRecord record, int[] cloverFields, JdbcSpecific jdbcSpecific) throws JetelException {
        int fromIndex;
        int toIndex;
        int jdbcType;
        DataFieldMetadata jetelField;

        CopySQLData[] transMap = new CopySQLData[cloverFields.length];

       for(int i=0;i<cloverFields.length;i++) {
            jetelField=record.getField(cloverFields[i]).getMetadata();
            jdbcType = jdbcSpecific.jetelType2sql(jetelField);
            
            // from index is index of specified cloverField in the Clover record
            fromIndex = cloverFields[i];
            toIndex=i;
            // we copy from Clover's field to JDBC - toIndex/fromIndex is
            // switched here
            transMap[i] = jdbcSpecific.createCopyObject(jdbcType, jetelField, record,
                    toIndex, fromIndex);
        }
        return transMap;
    }
	
	/**
	 * This method validates translation map between clover record and prepared statement
	 * 
	 * @param transMap translation map to validate
	 * @param statement prepared statement
	 * @param inMetadata input metadata
	 * @param jdbcSpecific jdbc specific for checking types
	 * @return error message if map is invalid, null in other case
	 * @throws SQLException
	 */
	public static List<String> validateJetel2sqlMap(CopySQLData[] transMap, PreparedStatement statement, 
		DataRecordMetadata inMetadata, JdbcSpecific jdbcSpecific) throws SQLException{
		List<String> messages = new ArrayList<String>();
		
		try {
			ParameterMetaData pMeta = statement.getParameterMetaData();
			if (transMap.length == pMeta.getParameterCount()) {
				for (int i = 0; i < transMap.length; i++) {
					if (!jdbcSpecific.isJetelTypeConvertible2sql(pMeta.getParameterType(i + 1), inMetadata.getField(transMap[i].getFieldJetel()))) {
						if (pMeta.getParameterType(i + 1) != Types.NULL) {
							messages.add("Invalid SQL query. Incompatible Clover & JDBC field types - field " + inMetadata.getField(transMap[i].getFieldJetel()).getName() + ". Clover type: " + SQLUtil.jetelType2Str(inMetadata.getFieldType(transMap[i].getFieldJetel())) + ", sql type: " + SQLUtil.sqlType2str(pMeta.getParameterType(i + 1)));
							messages.add("Invalid SQL query. Incompatible types - field " + inMetadata.getField(transMap[i].getFieldJetel()).getName() + ", clover type: " + inMetadata.getDataFieldType(transMap[i].getFieldJetel()).getName() + ", sql type: " + SQLUtil.sqlType2str(pMeta.getParameterType(i + 1)));
						} else {
							// MSSQL returns NULL parameter types
							messages.add("Compatibility of field types could not have been validated (not supported by the driver).");
							break; // do not check the others
						}
					}
				}
			} else {
				messages.add("Invalid SQL query. Wrong number of parameteres - actually: " + transMap.length + ", required: " + pMeta.getParameterCount());
			}
		} catch (SQLException ex) {
			// S1C00 MySQL, 99999 Oracle
			if ("S1C00".equals(ex.getSQLState()) || "99999".equals(ex.getSQLState())) {
				messages.add("Compatibility of field types could not have been validated (not supported by the driver).");
				// 42704 , 42P01 postgre
			} else if ("42704".equals(ex.getSQLState()) || "42P01".equals(ex.getSQLState())) {
				messages.add("Table does not exist.");
			} else {
				messages.add(ExceptionUtils.getMessage(ex));
			}
		}
		return messages;
	}

	/**
	 * This method validates translation map between result set and clover record
	 * 
	 * @param transMap translation map to validate
	 * @param sqlMeta statement result set metadata
	 * @param outMetadata output metadata
	 * @param jdbcSpecific jdbc specific for checking types
	 * @return error message if map is invalid, null in other case
	 * @throws SQLException
	 */
	public static String validateSql2JetelMap(CopySQLData[] transMap, ResultSetMetaData sqlMeta,
			DataRecordMetadata outMetadata, JdbcSpecific jdbcSpecific) throws SQLException{
		if (transMap.length != sqlMeta.getColumnCount()) {
			return "Wrong number of output fields - actually: " + transMap.length + ", required: " + sqlMeta.getColumnCount();
		}
		for (int i = 0; i < transMap.length; i++) {
			if (outMetadata.getFieldType(transMap[i].getFieldJetel()) != jdbcSpecific.sqlType2jetel(sqlMeta.getColumnType(i + 1))){
				return "Incompatible Clover & JDBC field types - field "+outMetadata.getField(transMap[i].getFieldJetel()).getName()+
	            ". Clover type: "+ SQLUtil.jetelType2Str(outMetadata.getFieldType(transMap[i].getFieldJetel())) + 
	            ", SQL type: " + SQLUtil.sqlType2str(sqlMeta.getColumnType(i + 1));
			}
		}
		return null;
	}
	
	public static class CopyArray extends AbstractCopySQLData {

		/**
		 * @param record
		 * @param fieldSQL
		 * @param fieldJetel
		 */
		public CopyArray(DataRecord record, int fieldSQL, int fieldJetel) {
			super(record, fieldSQL, fieldJetel);
		}

		/**
		 *  Sets the Jetel attribute of the CopyString object
		 *
		 * @param  resultSet         The new Jetel value
		 * @exception  SQLException  Description of Exception
		 * @since                    October 7, 2002
		 */
		@Override
		public void setJetel(ResultSet resultSet) throws SQLException {
			Array fieldVal = resultSet.getArray(fieldSQL);
			Object obj = fieldVal.getArray();

        	Object [] objectArray = (Object []) obj;   // cast it to an array of objects
        	
        	StringBuffer buffer = new StringBuffer("{");
        	buffer.append(String.valueOf(objectArray[0]));
        	for (int j=1; j < objectArray.length; j++)
        	   {
        			buffer.append(", ").append(String.valueOf(objectArray[j]));
        	   }
        	buffer.append("}");
			if (resultSet.wasNull()) {
				((StringDataField) field).setValue((Object)null);
			} else {
				((StringDataField) field).setValue(buffer.toString());
			}
		}

		@Override
		public void setJetel(CallableStatement statement) throws SQLException {
			Array fieldVal = statement.getArray(fieldSQL);
			Object obj = fieldVal.getArray();

        	Object [] objectArray = (Object []) obj;   // cast it to an array of objects
        	
        	StringBuffer buffer = new StringBuffer("{");
        	buffer.append(String.valueOf(objectArray[0]));
        	for (int j=1; j < objectArray.length; j++)
        	   {
        			buffer.append(", ").append(String.valueOf(objectArray[j]));
        	   }
        	buffer.append("}");
			if (statement.wasNull()) {
				((StringDataField) field).setValue((Object)null);
			} else {
				((StringDataField) field).setValue(buffer.toString());
			}
		}

		/**
		 *  Need a vector field for DataTypes
		 *
		 * @param  pStatement        The new SQL value
		 * @exception  SQLException  Description of Exception
		 * @since                    October 7, 2002
		 */
		@Override
		public void setSQL(PreparedStatement pStatement) throws SQLException {
			if (!field.isNull()) {
		    	pStatement.setString(fieldSQL, field.toString());
		   	}else{
			   	pStatement.setNull(fieldSQL, java.sql.Types.ARRAY);
		   	}
		}
		
		@Override
		public Object getDbValue(ResultSet resultSet) throws SQLException {
			Array fieldVal = resultSet.getArray(fieldSQL);
			return resultSet.wasNull() ? null : fieldVal;
		}


		@Override
		public Object getDbValue(CallableStatement statement)
				throws SQLException {
			Array fieldVal = statement.getArray(fieldSQL);
			return statement.wasNull() ? null : fieldVal;
		}

		
	}
	/**
	 *  Description of the Class
	 *
	 * @author      dpavlis
	 * @since       October 7, 2002
	 * @created     8. ???ervenec 2003
	 */
	public static class CopyNumeric extends AbstractCopySQLData {
		/**
		 *  Constructor for the CopyNumeric object
		 *
		 * @param  record      Description of Parameter
		 * @param  fieldSQL    Description of Parameter
		 * @param  fieldJetel  Description of Parameter
		 * @since              October 7, 2002
		 */
		public CopyNumeric(DataRecord record, int fieldSQL, int fieldJetel) {
			super(record, fieldSQL, fieldJetel);
		}


		/**
		 *  Sets the Jetel attribute of the CopyNumeric object
		 *
		 * @param  resultSet         The new Jetel value
		 * @exception  SQLException  Description of Exception
		 * @since                    October 7, 2002
		 */
		@Override
		public void setJetel(ResultSet resultSet) throws SQLException {
			double i = resultSet.getDouble(fieldSQL);
			if (resultSet.wasNull()) {
				((NumericDataField) field).setValue((Object)null);
			} else {
				((NumericDataField) field).setValue(i);
			}
		}

		@Override
		public void setJetel(CallableStatement statement) throws SQLException{
			double i = statement.getDouble(fieldSQL);
			if (statement.wasNull()) {
				((NumericDataField) field).setValue((Object)null);
			} else {
				((NumericDataField) field).setValue(i);
			}
		}

		/**
		 *  Sets the SQL attribute of the CopyNumeric object
		 *
		 * @param  pStatement        The new SQL value
		 * @exception  SQLException  Description of Exception
		 * @since                    October 7, 2002
		 */
		@Override
		public void setSQL(PreparedStatement pStatement) throws SQLException {
			if (!field.isNull()) {
				pStatement.setDouble(fieldSQL, ((NumericDataField) field).getDouble());
			} else {
				pStatement.setNull(fieldSQL, java.sql.Types.NUMERIC);
			}

		}


		@Override
		public Object getDbValue(ResultSet resultSet) throws SQLException {
			double i = resultSet.getDouble(fieldSQL);
			return resultSet.wasNull() ? null : i;
		}


		@Override
		public Object getDbValue(CallableStatement statement)
				throws SQLException {
			double i = statement.getDouble(fieldSQL);
			return statement.wasNull() ? null : i;
		}
	}


	/**
	 *  Description of the Class
	 *
	 * @author      dpavlis
	 * @since       October 7, 2002
	 * @created     8. ???ervenec 2003
	 */
	public static class CopyDecimal extends AbstractCopySQLData {
		/**
		 *  Constructor for the CopyDecimal object
		 *
		 * @param  record      Description of Parameter
		 * @param  fieldSQL    Description of Parameter
		 * @param  fieldJetel  Description of Parameter
		 * @since              October 7, 2002
		 */
		public CopyDecimal(DataRecord record, int fieldSQL, int fieldJetel) {
			super(record, fieldSQL, fieldJetel);
		}


		/**
		 *  Sets the Jetel attribute of the CopyDecimal object
		 *
		 * @param  resultSet         The new Jetel value
		 * @exception  SQLException  Description of Exception
		 * @since                    October 7, 2002
		 */
		@Override
		public void setJetel(ResultSet resultSet) throws SQLException {
			BigDecimal i = resultSet.getBigDecimal(fieldSQL);
			if (resultSet.wasNull()) {
				((DecimalDataField) field).setValue((Object)null);
			} else {
				((DecimalDataField) field).setValue(new HugeDecimal(i, Integer.parseInt(field.getMetadata().getProperty(DataFieldMetadata.LENGTH_ATTR)), Integer.parseInt(field.getMetadata().getProperty(DataFieldMetadata.SCALE_ATTR)), false));
			}
		}

		@Override
		public void setJetel(CallableStatement statement) throws SQLException {
			BigDecimal i = statement.getBigDecimal(fieldSQL);
			if (statement.wasNull()) {
				((DecimalDataField) field).setValue((Object)null);
			} else {
				((DecimalDataField) field).setValue(new HugeDecimal(i, Integer.parseInt(field.getMetadata().getProperty(DataFieldMetadata.LENGTH_ATTR)), Integer.parseInt(field.getMetadata().getProperty(DataFieldMetadata.SCALE_ATTR)), false));
			}
		}

		/**
		 *  Sets the SQL attribute of the CopyNumeric object
		 *
		 * @param  pStatement        The new SQL value
		 * @exception  SQLException  Description of Exception
		 * @since                    October 7, 2002
		 */
		@Override
		public void setSQL(PreparedStatement pStatement) throws SQLException {
			if (!field.isNull()) {
				pStatement.setBigDecimal(fieldSQL, ((Decimal) ((DecimalDataField) field).getValue()).getBigDecimalOutput());
			} else {
				pStatement.setNull(fieldSQL, java.sql.Types.DECIMAL);
			}

		}
		
		@Override
		public Object getDbValue(ResultSet resultSet) throws SQLException {
			BigDecimal i = resultSet.getBigDecimal(fieldSQL);
			return resultSet.wasNull() ? null : i;
		}


		@Override
		public Object getDbValue(CallableStatement statement)
				throws SQLException {
			BigDecimal i = statement.getBigDecimal(fieldSQL);
			return statement.wasNull() ? null : i;
		}

	}

	/**
	 *  Description of the Class
	 *
	 * @author      dpavlis
	 * @since       2. b???ezen 2004
	 * @created     8. ???ervenec 2003
	 */
	public static class CopyInteger extends AbstractCopySQLData {
		/**
		 *  Constructor for the CopyNumeric object
		 *
		 * @param  record      Description of Parameter
		 * @param  fieldSQL    Description of Parameter
		 * @param  fieldJetel  Description of Parameter
		 * @since              October 7, 2002
		 */
		public CopyInteger(DataRecord record, int fieldSQL, int fieldJetel) {
			super(record, fieldSQL, fieldJetel);
		}


		/**
		 *  Sets the Jetel attribute of the CopyNumeric object
		 *
		 * @param  resultSet         The new Jetel value
		 * @exception  SQLException  Description of Exception
		 * @since                    October 7, 2002
		 */
		@Override
		public void setJetel(ResultSet resultSet) throws SQLException {
			int i = resultSet.getInt(fieldSQL);
			if (resultSet.wasNull()) {
				((IntegerDataField) field).setValue((Object)null);
			} else {
				((IntegerDataField) field).setValue(i);
			}
		}

		@Override
		public void setJetel(CallableStatement statement) throws SQLException {
			int i = statement.getInt(fieldSQL);
			if (statement.wasNull()) {
				((IntegerDataField) field).setValue((Object)null);
			} else {
				((IntegerDataField) field).setValue(i);
			}
		}

		/**
		 *  Sets the SQL attribute of the CopyNumeric object
		 *
		 * @param  pStatement        The new SQL value
		 * @exception  SQLException  Description of Exception
		 * @since                    October 7, 2002
		 */
		@Override
		public void setSQL(PreparedStatement pStatement) throws SQLException {
			if (!field.isNull()) {
				pStatement.setInt(fieldSQL, ((IntegerDataField) field).getInt());
			} else {
				pStatement.setNull(fieldSQL, java.sql.Types.INTEGER);
			}

		}
		
		@Override
		public Object getDbValue(ResultSet resultSet) throws SQLException {
			int i = resultSet.getInt(fieldSQL);
			return resultSet.wasNull() ? null : i;
		}


		@Override
		public Object getDbValue(CallableStatement statement)
				throws SQLException {
			int i = statement.getInt(fieldSQL);
			return statement.wasNull() ? null : i;
		}

	}

	public static class CopyLong extends AbstractCopySQLData {
		/**
		 *  Constructor for the CopyNumeric object
		 *
		 * @param  record      Description of Parameter
		 * @param  fieldSQL    Description of Parameter
		 * @param  fieldJetel  Description of Parameter
		 * @since              October 7, 2002
		 */
		public CopyLong(DataRecord record, int fieldSQL, int fieldJetel) {
			super(record, fieldSQL, fieldJetel);
		}


		/**
		 *  Sets the Jetel attribute of the CopyNumeric object
		 *
		 * @param  resultSet         The new Jetel value
		 * @exception  SQLException  Description of Exception
		 * @since                    October 7, 2002
		 */
		@Override
		public void setJetel(ResultSet resultSet) throws SQLException {
			long i = resultSet.getLong(fieldSQL);
			if (resultSet.wasNull()) {
				((LongDataField) field).setValue((Object)null);
			} else {
				((LongDataField) field).setValue(i);
			}
		}

		@Override
		public void setJetel(CallableStatement statement) throws SQLException {
			long i = statement.getLong(fieldSQL);
			if (statement.wasNull()) {
				((LongDataField) field).setValue((Object)null);
			} else {
				((LongDataField) field).setValue(i);
			}
		}

		/**
		 *  Sets the SQL attribute of the CopyNumeric object
		 *
		 * @param  pStatement        The new SQL value
		 * @exception  SQLException  Description of Exception
		 * @since                    October 7, 2002
		 */
		@Override
		public void setSQL(PreparedStatement pStatement) throws SQLException {
			if (!field.isNull()) {
				pStatement.setLong(fieldSQL, ((LongDataField) field).getLong());
			} else {
				pStatement.setNull(fieldSQL, java.sql.Types.BIGINT);
			}

		}
		
		@Override
		public Object getDbValue(ResultSet resultSet) throws SQLException {
			long i = resultSet.getLong(fieldSQL);
			return resultSet.wasNull() ? null : i;
		}


		@Override
		public Object getDbValue(CallableStatement statement)
				throws SQLException {
			long i = statement.getLong(fieldSQL);
			return statement.wasNull() ? null : i;
		}

	}

	/**
	 *  Description of the Class
	 *
	 * @author      dpavlis
	 * @since       October 7, 2002
	 * @created     8. ???ervenec 2003
	 */
	public static class CopyString extends AbstractCopySQLData {
		/**
		 *  Constructor for the CopyString object
		 *
		 * @param  record      Description of Parameter
		 * @param  fieldSQL    Description of Parameter
		 * @param  fieldJetel  Description of Parameter
		 * @since              October 7, 2002
		 */
		public CopyString(DataRecord record, int fieldSQL, int fieldJetel) {
			super(record, fieldSQL, fieldJetel);
		}


		/**
		 *  Sets the Jetel attribute of the CopyString object
		 *
		 * @param  resultSet         The new Jetel value
		 * @exception  SQLException  Description of Exception
		 * @since                    October 7, 2002
		 */
		@Override
		public void setJetel(ResultSet resultSet) throws SQLException {
			String fieldVal = resultSet.getString(fieldSQL);
			// uses fromString - field should _not_ be a StringDataField
			if (resultSet.wasNull()) {
				field.fromString(null);
			} else {
				field.fromString(fieldVal);
			}
		}

		@Override
		public void setJetel(CallableStatement statement) throws SQLException {
			String fieldVal = statement.getString(fieldSQL);
			// uses fromString - field should _not_ be a StringDataField
			if (statement.wasNull()) {
				field.fromString(null);
			} else {
				field.fromString(fieldVal);
			}
		}

		/**
		 *  Sets the SQL attribute of the CopyString object
		 *
		 * @param  pStatement        The new SQL value
		 * @exception  SQLException  Description of Exception
		 * @since                    October 7, 2002
		 */
		@Override
		public void setSQL(PreparedStatement pStatement) throws SQLException {
			if (!field.isNull()) {
		    	pStatement.setString(fieldSQL, field.toString());
		   	}else{
			   	pStatement.setNull(fieldSQL, java.sql.Types.VARCHAR);
		   	}
		}
		
		@Override
		public Object getDbValue(ResultSet resultSet) throws SQLException {
			String fieldVal = resultSet.getString(fieldSQL);
			return resultSet.wasNull() ? null : fieldVal;
		}


		@Override
		public Object getDbValue(CallableStatement statement)
				throws SQLException {
			String fieldVal = statement.getString(fieldSQL);
			return statement.wasNull() ? null : fieldVal;
		}


	}
	/**
	 *  Description of the Class
	 *
	 * @author      dpavlis
	 * @since       October 7, 2002
	 * @created     8. ???ervenec 2003
	 */
	public static class CopyDate extends AbstractCopySQLData {

		java.sql.Date dateValue;


		/**
		 *  Constructor for the CopyDate object
		 *
		 * @param  record      Description of Parameter
		 * @param  fieldSQL    Description of Parameter
		 * @param  fieldJetel  Description of Parameter
		 * @since              October 7, 2002
		 */
		public CopyDate(DataRecord record, int fieldSQL, int fieldJetel) {
			super(record, fieldSQL, fieldJetel);
			dateValue = new java.sql.Date(0);
		}


		/**
		 *  Sets the Jetel attribute of the CopyDate object
		 *
		 * @param  resultSet         The new Jetel value
		 * @exception  SQLException  Description of Exception
		 * @since                    October 7, 2002
		 */
		@Override
		public void setJetel(ResultSet resultSet) throws SQLException {
			Date date = resultSet.getDate(fieldSQL);
			if (resultSet.wasNull()) {
				((DateDataField) field).setValue((Object)null);
			}else{
				((DateDataField) field).setValue(date);
			}
			
		}

		@Override
		public void setJetel(CallableStatement statement) throws SQLException {
			Date date = statement.getDate(fieldSQL);
			if (statement.wasNull()) {
				((DateDataField) field).setValue((Object)null);
			}else{
				((DateDataField) field).setValue(date);
			}
			
		}

		/**
		 *  Sets the SQL attribute of the CopyDate object
		 *
		 * @param  pStatement        The new SQL value
		 * @exception  SQLException  Description of Exception
		 * @since                    October 7, 2002
		 */
		@Override
		public void setSQL(PreparedStatement pStatement) throws SQLException {
			if (!field.isNull()) {
			    if (inBatchUpdate){
                    pStatement.setDate(fieldSQL, new java.sql.Date(((DateDataField) field).getDate().getTime()));
			    }else{
			        dateValue.setTime(((DateDataField) field).getDate().getTime());
			        pStatement.setDate(fieldSQL, dateValue);
			    }
			} else {
				pStatement.setNull(fieldSQL, java.sql.Types.DATE);
			}
		}
		
		@Override
		public Object getDbValue(ResultSet resultSet) throws SQLException {
			Date date = resultSet.getDate(fieldSQL);
			return resultSet.wasNull() ? null : date;
		}


		@Override
		public Object getDbValue(CallableStatement statement)
				throws SQLException {
			Date date = statement.getDate(fieldSQL);
			return statement.wasNull() ? null : date;
		}

	}
	
	/**
	 * CL-2748
	 * 
	 * This class should be used instead of {@link CopyString}
	 * when the target field is a {@link StringDataField}.
	 * Uses {@link StringDataField#setValue(Object)}
	 * instead of {@link DataField#fromString(CharSequence)}
	 * so that empty strings are not replaced with <code>null</code>.
	 *  
	 * @author krivanekm (info@cloveretl.com)
	 *         (c) Javlin, a.s. (www.cloveretl.com)
	 *
	 * @created Mar 26, 2013
	 */
	public static class CopyStringToString extends CopyString {

		/**
		 * @param record
		 * @param fieldSQL
		 * @param fieldJetel
		 */
		public CopyStringToString(DataRecord record, int fieldSQL, int fieldJetel) {
			super(record, fieldSQL, fieldJetel);
		}

		@Override
		public void setJetel(ResultSet resultSet) throws SQLException {
			String fieldVal = resultSet.getString(fieldSQL);
			// CL-949, CL-2748: use setValue() instead of fromString()
			if (resultSet.wasNull()) {
				field.setValue(null);
			} else {
				field.setValue(fieldVal);
			}
		}

		@Override
		public void setJetel(CallableStatement statement) throws SQLException {
			String fieldVal = statement.getString(fieldSQL);
			// CL-949, CL-2748: use setValue() instead of fromString()
			if (statement.wasNull()) {
				field.setValue(null);
			} else {
				field.setValue(fieldVal);
			}
		}
		
	}


	/**
	 *  Description of the Class
	 *
	 * @author      dpavlis
	 * @since       October 7, 2002
	 * @created     8. ???ervenec 2003
	 */
	public static class CopyTime extends AbstractCopySQLData {

		java.sql.Time timeValue;


		/**
		 *  Constructor for the CopyDate object
		 *
		 * @param  record      Description of Parameter
		 * @param  fieldSQL    Description of Parameter
		 * @param  fieldJetel  Description of Parameter
		 * @since              October 7, 2002
		 */
		public CopyTime(DataRecord record, int fieldSQL, int fieldJetel) {
			super(record, fieldSQL, fieldJetel);
			timeValue = new java.sql.Time(0);
		}


		/**
		 *  Sets the Jetel attribute of the CopyDate object
		 *
		 * @param  resultSet         The new Jetel value
		 * @exception  SQLException  Description of Exception
		 * @since                    October 7, 2002
		 */
		@Override
		public void setJetel(ResultSet resultSet) throws SQLException {
			Date time = resultSet.getTime(fieldSQL);
			if (resultSet.wasNull()) {
				((DateDataField) field).setValue((Object)null);
			}else{
				((DateDataField) field).setValue(time);
			}
			
		}

		@Override
		public void setJetel(CallableStatement statement) throws SQLException {
			Date time = statement.getTime(fieldSQL);
			if (statement.wasNull()) {
				((DateDataField) field).setValue((Object)null);
			}else{
				((DateDataField) field).setValue(time);
			}
			
		}

		/**
		 *  Sets the SQL attribute of the CopyDate object
		 *
		 * @param  pStatement        The new SQL value
		 * @exception  SQLException  Description of Exception
		 * @since                    October 7, 2002
		 */
		@Override
		public void setSQL(PreparedStatement pStatement) throws SQLException {
			if (!field.isNull()) {
			    if (inBatchUpdate){
                    pStatement.setTime(fieldSQL, new java.sql.Time(((DateDataField) field).getDate().getTime()));
			    }else{
			        timeValue.setTime(((DateDataField) field).getDate().getTime());
			        pStatement.setTime(fieldSQL, timeValue);
			    }
			} else {
				pStatement.setNull(fieldSQL, java.sql.Types.DATE);
			}
		}
		
		@Override
		public Object getDbValue(ResultSet resultSet) throws SQLException {
			Date time = resultSet.getTime(fieldSQL);
			return resultSet.wasNull() ? null : time;
		}


		@Override
		public Object getDbValue(CallableStatement statement)
				throws SQLException {
			Date time = statement.getTime(fieldSQL);
			return statement.wasNull() ? null : time;
		}

	}




	/**
	 *  Description of the Class
	 *
	 * @author      dpavlis
	 * @since       October 7, 2002
	 * @created     8. ???ervenec 2003
	 */
	public static class CopyTimestamp extends AbstractCopySQLData {

		Timestamp timeValue;


		/**
		 *  Constructor for the CopyTimestamp object
		 *
		 * @param  record      Description of Parameter
		 * @param  fieldSQL    Description of Parameter
		 * @param  fieldJetel  Description of Parameter
		 * @since              October 7, 2002
		 */
		public CopyTimestamp(DataRecord record, int fieldSQL, int fieldJetel) {
			super(record, fieldSQL, fieldJetel);
			timeValue = new Timestamp(0);
			timeValue.setNanos(0);// we don't count with nanos!
		}


		/**
		 *  Sets the Jetel attribute of the CopyTimestamp object
		 *
		 * @param  resultSet         The new Jetel value
		 * @exception  SQLException  Description of Exception
		 * @since                    October 7, 2002
		 */
		@Override
		public void setJetel(ResultSet resultSet) throws SQLException {
			Timestamp timestamp = resultSet.getTimestamp(fieldSQL);
			if (resultSet.wasNull()) {
				((DateDataField) field).setValue((Object)null);
			}else{
				((DateDataField) field).setValue(timestamp);
			}
			
		}

		@Override
		public void setJetel(CallableStatement statement) throws SQLException {
			Timestamp timestamp = statement.getTimestamp(fieldSQL);
			if (statement.wasNull()) {
				((DateDataField) field).setValue((Object)null);
			}else{
				((DateDataField) field).setValue(timestamp);
			}
			
		}

		/**
		 *  Sets the SQL attribute of the CopyTimestamp object
		 *
		 * @param  pStatement        The new SQL value
		 * @exception  SQLException  Description of Exception
		 * @since                    October 7, 2002
		 */
		@Override
		public void setSQL(PreparedStatement pStatement) throws SQLException {
			if (!field.isNull()) {
			    if (inBatchUpdate){
                    pStatement.setTimestamp(fieldSQL, new Timestamp(((DateDataField) field).getDate().getTime()));
			    }else{
			        if (((DateDataField) field).getDate() != null) {
			        	timeValue.setTime(((DateDataField) field).getDate().getTime());
				        pStatement.setTimestamp(fieldSQL, timeValue);
			        } else {
			        	pStatement.setNull(fieldSQL, java.sql.Types.TIMESTAMP);
			        }
			    }
			} else {
				pStatement.setNull(fieldSQL, java.sql.Types.TIMESTAMP);
			}
		}
		
		@Override
		public Object getDbValue(ResultSet resultSet) throws SQLException {
			Timestamp timestamp = resultSet.getTimestamp(fieldSQL);
			return resultSet.wasNull() ? null : timestamp;
		}


		@Override
		public Object getDbValue(CallableStatement statement)
				throws SQLException {
			Timestamp timestamp = statement.getTimestamp(fieldSQL);
			return statement.wasNull() ? null : timestamp;
		}

	}


	/**
	 *  Copy object for boolean/bit type fields. Expects String data
	 *  representation on Clover's side
	 *  for logical
	 *
	 * @author      dpavlis
	 * @since       November 27, 2003
	 */
	public static class CopyBoolean extends AbstractCopySQLData {

		/**
		 *  Constructor for the CopyBoolean object
		 *
		 * @param  record      Description of Parameter
		 * @param  fieldSQL    Description of Parameter
		 * @param  fieldJetel  Description of Parameter
		 * @since              October 7, 2002
		 */
		public CopyBoolean(DataRecord record, int fieldSQL, int fieldJetel) {
			super(record, fieldSQL, fieldJetel);
		}


		/**
		 *  Sets the Jetel attribute of the CopyBoolean object
		 *
		 * @param  resultSet         The new Jetel value
		 * @exception  SQLException  Description of Exception
		 * @since                    October 7, 2002
		 */
		@Override
		public void setJetel(ResultSet resultSet) throws SQLException {
			boolean b = resultSet.getBoolean(fieldSQL);
			if (resultSet.wasNull()) {
				field.setValue((Object)null);
			}else{
				field.setValue(b);	
			}
			
		}

		@Override
		public void setJetel(CallableStatement statement) throws SQLException {
			boolean b = statement.getBoolean(fieldSQL);
			if (statement.wasNull()) {
				field.setValue((Object)null);
			}else{
				field.setValue(b);	
			}
			
		}

		/**
		 *  Sets the SQL attribute of the CopyString object
		 *
		 * @param  pStatement        The new SQL value
		 * @exception  SQLException  Description of Exception
		 * @since                    October 7, 2002
		 */
		@Override
		public void setSQL(PreparedStatement pStatement) throws SQLException {
			if (!field.isNull()) {
				boolean value = ((BooleanDataField) field).getBoolean();
				pStatement.setBoolean(fieldSQL,	value);
			}else{
				pStatement.setNull(fieldSQL, java.sql.Types.BOOLEAN);
			}
		}

		@Override
		public Object getDbValue(ResultSet resultSet) throws SQLException {
			boolean b = resultSet.getBoolean(fieldSQL);
			return resultSet.wasNull() ? null : b;
		}


		@Override
		public Object getDbValue(CallableStatement statement)
				throws SQLException {
			boolean b = statement.getBoolean(fieldSQL);
			return statement.wasNull() ? null : b;
		}

	}
    
	public static class CopyByte extends AbstractCopySQLData {
        /**
         *  Constructor for the CopyByte object
         *
         * @param  record      Description of Parameter
         * @param  fieldSQL    Description of Parameter
         * @param  fieldJetel  Description of Parameter
         * @since              October 7, 2002
         */
		public CopyByte(DataRecord record, int fieldSQL, int fieldJetel) {
            super(record, fieldSQL, fieldJetel);
        }


        /**
         *  Sets the Jetel attribute of the CopyByte object
         *
         * @param  resultSet         The new Jetel value
         * @exception  SQLException  Description of Exception
         * @since                    October 7, 2002
         */
		@Override
		public void setJetel(ResultSet resultSet) throws SQLException {
            byte[] i = resultSet.getBytes(fieldSQL);
            if (resultSet.wasNull()) {
                ((ByteDataField) field).setValue((Object)null);
            } else {
                ((ByteDataField) field).setValue(i);
            }
        }

		@Override
		public void setJetel(CallableStatement statement) throws SQLException {
            byte[] i = statement.getBytes(fieldSQL);
            if (statement.wasNull()) {
                ((ByteDataField) field).setValue((Object)null);
            } else {
                ((ByteDataField) field).setValue(i);
            }
        }

        /**
         *  Sets the SQL attribute of the CopyByte object
         *
         * @param  pStatement        The new SQL value
         * @exception  SQLException  Description of Exception
         * @since                    October 7, 2002
         */
		@Override
		public void setSQL(PreparedStatement pStatement) throws SQLException {
            if (!field.isNull()) {
                pStatement.setBytes(fieldSQL, (byte[]) ((ByteDataField) field).getValueDuplicate());
            } else {
                pStatement.setNull(fieldSQL, java.sql.Types.BINARY);
            }

        }
        
		@Override
		public Object getDbValue(ResultSet resultSet) throws SQLException {
            byte[] i = resultSet.getBytes(fieldSQL);
			return resultSet.wasNull() ? null : i;
		}


		@Override
		public Object getDbValue(CallableStatement statement)
				throws SQLException {
            byte[] i = statement.getBytes(fieldSQL);
			return statement.wasNull() ? null : i;
		}

    }

	public static class CopyBlob extends AbstractCopySQLData {
    	    	
    	Blob blob;
    	
        /**
         *  Constructor for the CopyByte object
         *
         * @param  record      Description of Parameter
         * @param  fieldSQL    Description of Parameter
         * @param  fieldJetel  Description of Parameter
         * @since              October 7, 2002
         */
    	public CopyBlob(DataRecord record, int fieldSQL, int fieldJetel) {
            super(record, fieldSQL, fieldJetel);
        }


        /**
         *  Sets the Jetel attribute of the CopyByte object
         *
         * @param  resultSet         The new Jetel value
         * @exception  SQLException  Description of Exception
         * @since                    October 7, 2002
         */
    	@Override
		public void setJetel(ResultSet resultSet) throws SQLException {
        	blob = resultSet.getBlob(fieldSQL);
			if (blob != null) {
				blob = new SerialBlob(blob);
				if (blob.length() > Integer.MAX_VALUE) {
					throw new SQLException("Blob value is too long: " + blob.length());
				}
			}            
            if (resultSet.wasNull()) {
                ((ByteDataField) field).setValue((Object)null);
            } else {
                ((ByteDataField) field).setValue(blob.getBytes(1, (int)blob.length()));
            }
        }

    	@Override
		public void setJetel(CallableStatement statement) throws SQLException {
        	blob = statement.getBlob(fieldSQL);
			if (blob != null) {
				blob = new SerialBlob(blob);
				if (blob.length() > Integer.MAX_VALUE) {
					throw new SQLException("Blob value is too long: " + blob.length());
				}
			}            
            if (statement.wasNull()) {
                ((ByteDataField) field).setValue((Object)null);
            } else {
                ((ByteDataField) field).setValue(blob.getBytes(1, (int)blob.length()));
            }
        }

        /**
         *  Sets the SQL attribute of the CopyByte object
         *
         * @param  pStatement        The new SQL value
         * @exception  SQLException  Description of Exception
         * @since                    October 7, 2002
         */
    	@Override
		public void setSQL(PreparedStatement pStatement) throws SQLException {
            if (!field.isNull()) {
                pStatement.setBlob(fieldSQL, new SerialBlob(((ByteDataField) field).getByteArray()));
            } else {
                pStatement.setNull(fieldSQL, java.sql.Types.BLOB);
            }

        }
        
		@Override
		public Object getDbValue(ResultSet resultSet) throws SQLException {
        	blob = resultSet.getBlob(fieldSQL);
			if (blob != null) {
				blob = new SerialBlob(blob);
				if (blob.length() > Integer.MAX_VALUE) {
					throw new SQLException("Blob value is too long: " + blob.length());
				}
			}            
			return resultSet.wasNull() ? null : blob;
		}


		@Override
		public Object getDbValue(CallableStatement statement)
				throws SQLException {
        	blob = statement.getBlob(fieldSQL);
			if (blob != null) {
				blob = new SerialBlob(blob);
				if (blob.length() > Integer.MAX_VALUE) {
					throw new SQLException("Blob value is too long: " + blob.length());
				}
			}            
			return statement.wasNull() ? null : blob;
		}

    }

    //
    // TODO: The following class is kind of messy. There might be no need for this class since JDBC 4, check on that.
    //

    /**
     * This class is used SOLELY for conversion of Oracle's XML columns into string data fields. Implementation of this
     * conversion might be slow and buggy as it uses reflection. However, this class should serve its purpose.
     * <p>
     * For this class to work properly, xdb.jar (Oracle XML DB) and xmlparsev2.jar (XML parser used by XML DB) have to
     * be on class path and loaded by the same class loader as the Oracle's JDBC driver.
     *
     * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
     *
     * @version 19th August 2009
     * @since 19th August 2009
     */
	public static class CopyOracleXml extends AbstractCopySQLData {

        // TODO: We cannot access the oracle.jdbc.OracleTypes class from this place, don't know why. The XML_TYPE
        // constant is therefore set statically to the correct value. However, this might be a problem. If there
        // is a cleaner way to do this, we should fix that.

        /** the ID of XMLType used by Oracle (oracle.jdbc.OracleTypes.OPAQUE) */
        public static final int XML_TYPE = 2007;

        /** the name of the method declared in the oracle.xdb.XMLType class used to get XML documents as strings */
        private static final String GET_STRING_VAL_METHOD_NAME = "getStringVal";

        public CopyOracleXml(DataRecord record, int fieldSQL, int fieldJetel) {
            super(record, fieldSQL, fieldJetel);
        }

        @Override
        public void setJetel(ResultSet resultSet) throws SQLException {
            try {
                setJetel(resultSet.getObject(fieldSQL), resultSet.wasNull());
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }

        @Override
        public void setJetel(CallableStatement statement) throws SQLException {
            try {
                setJetel(statement.getObject(fieldSQL), statement.wasNull());
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }

        /**
         * Sets the encapsulated data field to a string value represented by the given field value object. The given
         * field value object is expected to be an instance of the oracle.xdb.XMLType class, otherwise this method fails.
         *
         * @param fieldValue an object to be converted to string, an instance of the oracle.xdb.XMLType class is expected
         * @param wasNull a flag specifying whether a null was encountered or not
         *
         * @throws SQLException if the conversion to a string value failed
         */
        private void setJetel(Object fieldValue, boolean wasNull) throws SQLException {
			// TODO: Issue 3650; consider using setValue() for string fields here, fromString() takes the nullValue
			// attribute into account and that might lead to incorrect results when the value equals nullValue
            if (!wasNull) {
                try {
                    Method getStringValMethod = fieldValue.getClass().getDeclaredMethod(GET_STRING_VAL_METHOD_NAME);
                    field.fromString(getStringValMethod.invoke(fieldValue).toString());
                } catch (Exception exception) {
                    throw new SQLException("Cannot convert the field to string!");
                }
            } else {
                field.fromString(null);
            }
        }

        @Override
        public void setSQL(PreparedStatement statement) throws SQLException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object getDbValue(ResultSet resultSet) throws SQLException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object getDbValue(CallableStatement statement) throws SQLException {
            throw new UnsupportedOperationException();
        }

    }

    //
    // End of the messy class.
    //

}
