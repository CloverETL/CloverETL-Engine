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
package org.jetel.database;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;

import java.util.List;
import java.util.ListIterator;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.DateDataField;
import org.jetel.data.IntegerDataField;
import org.jetel.data.NumericDataField;
import org.jetel.data.LongDataField;
import org.jetel.data.StringDataField;
import org.jetel.exception.JetelException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;

/**
 *  Class for creating mappings between CloverETL's DataRecords and JDBC's
 *  ResultSets.<br>
 *  It also contains inner classes for translating various CloverETL's DataField
 *  types onto JDBC types.
 *
 * @author      dpavlis
 * @since       October 7, 2002
 * @revision    $Revision$
 * @created     8. èervenec 2003
 */
public abstract class CopySQLData {

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


	/**
	 *  Constructor for the CopySQLData object
	 *
	 * @param  record      Clover record which will be source or target
	 * @param  fieldSQL    index of the field in SQL statement
	 * @param  fieldJetel  index of the field in Clover record
	 * @since              October 7, 2002
	 */
	CopySQLData(DataRecord record, int fieldSQL, int fieldJetel) {
		this.fieldSQL = fieldSQL + 1;
		// fields in ResultSet start with index 1
		this.fieldJetel=fieldJetel;
		field = record.getField(fieldJetel);
	}

	/**
	 * Assigns different DataField than the original used when creating
	 * copy object
	 * 
	 * @param field	New DataField
	 */
	public void setCloverField(DataField field){
	    this.field=field;
	}
	
	
	/**
	 * Assings different DataField (DataRecord). The proper field
	 * is taken from DataRecord based on previously saved field number.
	 * 
	 * @param record	New DataRecord
	 */
	public void setCloverRecord(DataRecord record){
	    this.field=record.getField(fieldJetel);
	}

	
	/**
	 *  Sets value of Jetel/Clover data field based on value from SQL ResultSet
	 *
	 * @param  resultSet         Description of Parameter
	 * @exception  SQLException  Description of Exception
	 * @since                    October 7, 2002
	 */
	public void sql2jetel(ResultSet resultSet) throws SQLException {
		try {
			setJetel(resultSet);
		} catch (SQLException ex) {
			throw new SQLException(ex.getMessage() + " with field " + field.getMetadata().getName());
		} catch (ClassCastException ex){
		    throw new SQLException("Incompatible Clover & JDBC field types - field "+field.getMetadata().getName()+
		            " Clover type: "+SQLUtil.jetelType2Str(field.getMetadata().getType()));
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
	public void jetel2sql(PreparedStatement pStatement) throws SQLException {
		try {
			setSQL(pStatement);
		} catch (SQLException ex) {
			throw new SQLException(ex.getMessage() + " with field " + field.getMetadata().getName());
		}catch (ClassCastException ex){
		    throw new SQLException("Incompatible Clover & JDBC field types - field "+field.getMetadata().getName()+
		            " Clover type: "+SQLUtil.jetelType2Str(field.getMetadata().getType()));
		}
	}


	/**
	 *  Sets the Jetel attribute of the CopySQLData object
	 *
	 * @param  resultSet         The new Jetel value
	 * @exception  SQLException  Description of Exception
	 * @since                    October 7, 2002
	 */
	abstract void setJetel(ResultSet resultSet) throws SQLException;


	/**
	 *  Sets the SQL attribute of the CopySQLData object
	 *
	 * @param  pStatement        The new SQL value
	 * @exception  SQLException  Description of Exception
	 * @since                    October 7, 2002
	 */
	abstract void setSQL(PreparedStatement pStatement) throws SQLException;


	
	
	/**
	 * Assigns new instance of DataRecord to existing CopySQLData structure (array).
	 * Useful when CopySQLData (aka transmap) was created using some other
	 * DataRecord and new one needs to be used
	 * 
	 * @param transMap	Array of CopySQLData object - the transmap
	 * @param record	New DataRecord object to be used within transmap
	 */
	public static void resetDataRecord(CopySQLData[] transMap,DataRecord record){
	    for(int i=0;i<transMap.length;transMap[i++].setCloverRecord(record));
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
	public static CopySQLData[] sql2JetelTransMap(List fieldTypes, DataRecordMetadata metadata, DataRecord record) {
		/* test that both sides have at least the same number of fields, less
		 * fields on DB side is O.K. (some of Clover fields won't get assigned value).
		 */
		if (fieldTypes.size()>metadata.getNumFields()){
			throw new RuntimeException("CloverETL data record "+metadata.getName()+
					" contains less fields than source JDBC record !");
		}
		
		CopySQLData[] transMap = new CopySQLData[metadata.getNumFields()];
		int i = 0;
		for (ListIterator iterator = fieldTypes.listIterator(); iterator.hasNext(); ) {
			transMap[i] = createCopyObject(((Integer) iterator.next()).shortValue(),
					record.getMetadata().getFieldType(i),
					record, i, i);
			i++;
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
	public static CopySQLData[] jetel2sqlTransMap(List fieldTypes, DataRecord record) throws SQLException {
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
			transMap[i] = createCopyObject(((Integer) iterator.next()).shortValue(),
					record.getMetadata().getFieldType(i),
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
	public static CopySQLData[] jetel2sqlTransMap(List fieldTypes, DataRecord record, String[] cloverFields)
			 throws SQLException, JetelException {
		int i = 0;
		int fromIndex = 0;
		int toIndex = 0;
		short jdbcType;
		char jetelFieldType;

		CopySQLData[] transMap = new CopySQLData[fieldTypes.size()];
		ListIterator iterator = fieldTypes.listIterator();

		while (iterator.hasNext()) {
			jdbcType = ((Integer) iterator.next()).shortValue();
			// from index is index of specified cloverField in the Clover record
			fromIndex = record.getMetadata().getFieldPosition(cloverFields[i]);
			jetelFieldType = record.getMetadata().getFieldType(fromIndex);
			if (fromIndex == -1) {
				throw new JetelException(" Field \"" + cloverFields[i] + "\" does not exist in DataRecord !");
			}
			// we copy from Clover's field to JDBC - toIndex/fromIndex is switched here
			transMap[i++] = createCopyObject(jdbcType, jetelFieldType, record, toIndex, fromIndex);
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
            DataRecord record, int[] cloverFields) throws SQLException,
            JetelException {
        int i = 0;
        int fromIndex = 0;
        int toIndex = 0;
        short jdbcType;
        char jetelFieldType;

        CopySQLData[] transMap = new CopySQLData[fieldTypes.size()];
        ListIterator iterator = fieldTypes.listIterator();

        while (iterator.hasNext()) {
            jdbcType = ((Integer) iterator.next()).shortValue();
            // from index is index of specified cloverField in the Clover record
            fromIndex = cloverFields[i];
            jetelFieldType = record.getMetadata().getFieldType(fromIndex);
            if (fromIndex == -1) {
                throw new JetelException(" Field \"" + cloverFields[i]
                        + "\" does not exist in DataRecord !");
            }
            // we copy from Clover's field to JDBC - toIndex/fromIndex is
            // switched here
            transMap[i++] = createCopyObject(jdbcType, jetelFieldType, record,
                    toIndex, fromIndex);
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
	public static CopySQLData[] jetel2sqlTransMap(DataRecord record, int[] cloverFields) throws JetelException {
        int fromIndex;
        int toIndex;
        int jdbcType;
        char jetelFieldType;

        CopySQLData[] transMap = new CopySQLData[cloverFields.length];

       for(int i=0;i<cloverFields.length;i++) {
            jetelFieldType=record.getField(cloverFields[i]).getType();
            jdbcType = SQLUtil.jetelType2sql(jetelFieldType);
            
            // from index is index of specified cloverField in the Clover record
            fromIndex = cloverFields[i];
            toIndex=i;
            // we copy from Clover's field to JDBC - toIndex/fromIndex is
            // switched here
            transMap[i] = createCopyObject(jdbcType, jetelFieldType, record,
                    toIndex, fromIndex);
        }
        return transMap;
    }
	
	/**
	 *  Creates copy object - bridge between JDBC data types and Clover data types
	 *
	 * @param  SQLType         Description of the Parameter
	 * @param  jetelFieldType  Description of the Parameter
	 * @param  record          Description of the Parameter
	 * @param  fromIndex       Description of the Parameter
	 * @param  toIndex         Description of the Parameter
	 * @return                 Description of the Return Value
	 */
	private static CopySQLData createCopyObject(int SQLType, char jetelFieldType, DataRecord record, int fromIndex, int toIndex) {
		switch (SQLType) {
			case Types.CHAR:
			case Types.LONGVARCHAR:
			case Types.VARCHAR:
				return new CopyString(record, fromIndex, toIndex);
			case Types.INTEGER:
			case Types.SMALLINT:
				return new CopyInteger(record, fromIndex, toIndex);
			case Types.BIGINT:
			    return new CopyLong(record,fromIndex,toIndex);
			case Types.DECIMAL:
			case Types.DOUBLE:
			case Types.FLOAT:
			case Types.NUMERIC:
			case Types.REAL:
				// fix for copying when target is numeric and
				// clover source is integer - no precision can be
				// lost so we can use CopyInteger
				if (jetelFieldType == DataFieldMetadata.INTEGER_FIELD) {
					return new CopyInteger(record, fromIndex, toIndex);
				} else if (jetelFieldType == DataFieldMetadata.LONG_FIELD) {
					return new CopyLong(record, fromIndex, toIndex);
				} else {
					return new CopyNumeric(record, fromIndex, toIndex);
				}
			case Types.DATE:
			case Types.TIME:
				return new CopyDate(record, fromIndex, toIndex);
			case Types.TIMESTAMP:
				return new CopyTimestamp(record, fromIndex, toIndex);
			case Types.BOOLEAN:
			case Types.BIT:
				if (jetelFieldType == DataFieldMetadata.STRING_FIELD) {
					return new CopyBoolean(record, fromIndex, toIndex);
				}
			// when Types.OTHER or unknown, try to copy it as STRING
			// this works for most of the NCHAR/NVARCHAR types on Oracle, MSSQL, etc.
			default:
			//case Types.OTHER:// When other, try to copy it as STRING - should work for NCHAR/NVARCHAR
				return new CopyString(record, fromIndex, toIndex);
			//default:
			//	throw new RuntimeException("SQL data type not supported: " + SQLType);
		}
	}


	/**
	 *  Description of the Class
	 *
	 * @author      dpavlis
	 * @since       October 7, 2002
	 * @revision    $Revision$
	 * @created     8. èervenec 2003
	 */
	static class CopyDate extends CopySQLData {

		java.sql.Date dateValue;


		/**
		 *  Constructor for the CopyDate object
		 *
		 * @param  record      Description of Parameter
		 * @param  fieldSQL    Description of Parameter
		 * @param  fieldJetel  Description of Parameter
		 * @since              October 7, 2002
		 */
		CopyDate(DataRecord record, int fieldSQL, int fieldJetel) {
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
		void setJetel(ResultSet resultSet) throws SQLException {
			((DateDataField) field).setValue(resultSet.getDate(fieldSQL));
		}


		/**
		 *  Sets the SQL attribute of the CopyDate object
		 *
		 * @param  pStatement        The new SQL value
		 * @exception  SQLException  Description of Exception
		 * @since                    October 7, 2002
		 */
		void setSQL(PreparedStatement pStatement) throws SQLException {
			if (!field.isNull()) {
				dateValue.setTime(((DateDataField) field).getDate().getTime());
				pStatement.setDate(fieldSQL, dateValue);
			} else {
				pStatement.setNull(fieldSQL, java.sql.Types.DATE);
			}
		}
	}


	/**
	 *  Description of the Class
	 *
	 * @author      dpavlis
	 * @since       October 7, 2002
	 * @revision    $Revision$
	 * @created     8. èervenec 2003
	 */
	static class CopyNumeric extends CopySQLData {
		/**
		 *  Constructor for the CopyNumeric object
		 *
		 * @param  record      Description of Parameter
		 * @param  fieldSQL    Description of Parameter
		 * @param  fieldJetel  Description of Parameter
		 * @since              October 7, 2002
		 */
		CopyNumeric(DataRecord record, int fieldSQL, int fieldJetel) {
			super(record, fieldSQL, fieldJetel);
		}


		/**
		 *  Sets the Jetel attribute of the CopyNumeric object
		 *
		 * @param  resultSet         The new Jetel value
		 * @exception  SQLException  Description of Exception
		 * @since                    October 7, 2002
		 */
		void setJetel(ResultSet resultSet) throws SQLException {
			double i = resultSet.getDouble(fieldSQL);
			if (resultSet.wasNull()) {
				((NumericDataField) field).setValue(null);
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
		void setSQL(PreparedStatement pStatement) throws SQLException {
			if (!field.isNull()) {
				pStatement.setDouble(fieldSQL, ((NumericDataField) field).getDouble());
			} else {
				pStatement.setNull(fieldSQL, java.sql.Types.NUMERIC);
			}

		}
	}


	/**
	 *  Description of the Class
	 *
	 * @author      dpavlis
	 * @since       2. bøezen 2004
	 * @revision    $Revision$
	 * @created     8. èervenec 2003
	 */
	static class CopyInteger extends CopySQLData {
		/**
		 *  Constructor for the CopyNumeric object
		 *
		 * @param  record      Description of Parameter
		 * @param  fieldSQL    Description of Parameter
		 * @param  fieldJetel  Description of Parameter
		 * @since              October 7, 2002
		 */
		CopyInteger(DataRecord record, int fieldSQL, int fieldJetel) {
			super(record, fieldSQL, fieldJetel);
		}


		/**
		 *  Sets the Jetel attribute of the CopyNumeric object
		 *
		 * @param  resultSet         The new Jetel value
		 * @exception  SQLException  Description of Exception
		 * @since                    October 7, 2002
		 */
		void setJetel(ResultSet resultSet) throws SQLException {
			int i = resultSet.getInt(fieldSQL);
			if (resultSet.wasNull()) {
				((IntegerDataField) field).setValue(null);
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
		void setSQL(PreparedStatement pStatement) throws SQLException {
			if (!field.isNull()) {
				pStatement.setInt(fieldSQL, ((IntegerDataField) field).getInt());
			} else {
				pStatement.setNull(fieldSQL, java.sql.Types.INTEGER);
			}

		}
	}

	static class CopyLong extends CopySQLData {
		/**
		 *  Constructor for the CopyNumeric object
		 *
		 * @param  record      Description of Parameter
		 * @param  fieldSQL    Description of Parameter
		 * @param  fieldJetel  Description of Parameter
		 * @since              October 7, 2002
		 */
		CopyLong(DataRecord record, int fieldSQL, int fieldJetel) {
			super(record, fieldSQL, fieldJetel);
		}


		/**
		 *  Sets the Jetel attribute of the CopyNumeric object
		 *
		 * @param  resultSet         The new Jetel value
		 * @exception  SQLException  Description of Exception
		 * @since                    October 7, 2002
		 */
		void setJetel(ResultSet resultSet) throws SQLException {
			long i = resultSet.getLong(fieldSQL);
			if (resultSet.wasNull()) {
				((LongDataField) field).setValue(null);
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
		void setSQL(PreparedStatement pStatement) throws SQLException {
			if (!field.isNull()) {
				pStatement.setLong(fieldSQL, ((LongDataField) field).getLong());
			} else {
				pStatement.setNull(fieldSQL, java.sql.Types.BIGINT);
			}

		}
	}

	/**
	 *  Description of the Class
	 *
	 * @author      dpavlis
	 * @since       October 7, 2002
	 * @revision    $Revision$
	 * @created     8. èervenec 2003
	 */
	static class CopyString extends CopySQLData {
		/**
		 *  Constructor for the CopyString object
		 *
		 * @param  record      Description of Parameter
		 * @param  fieldSQL    Description of Parameter
		 * @param  fieldJetel  Description of Parameter
		 * @since              October 7, 2002
		 */
		CopyString(DataRecord record, int fieldSQL, int fieldJetel) {
			super(record, fieldSQL, fieldJetel);
		}


		/**
		 *  Sets the Jetel attribute of the CopyString object
		 *
		 * @param  resultSet         The new Jetel value
		 * @exception  SQLException  Description of Exception
		 * @since                    October 7, 2002
		 */
		void setJetel(ResultSet resultSet) throws SQLException {
			String fieldVal = resultSet.getString(fieldSQL);
			if (resultSet.wasNull()) {
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
		void setSQL(PreparedStatement pStatement) throws SQLException {
			pStatement.setString(fieldSQL, field.toString());
		}

	}


	/**
	 *  Description of the Class
	 *
	 * @author      dpavlis
	 * @since       October 7, 2002
	 * @revision    $Revision$
	 * @created     8. èervenec 2003
	 */
	static class CopyTimestamp extends CopySQLData {

		Timestamp timeValue;


		/**
		 *  Constructor for the CopyTimestamp object
		 *
		 * @param  record      Description of Parameter
		 * @param  fieldSQL    Description of Parameter
		 * @param  fieldJetel  Description of Parameter
		 * @since              October 7, 2002
		 */
		CopyTimestamp(DataRecord record, int fieldSQL, int fieldJetel) {
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
		void setJetel(ResultSet resultSet) throws SQLException {
			((DateDataField) field).setValue(resultSet.getTimestamp(fieldSQL));
		}


		/**
		 *  Sets the SQL attribute of the CopyTimestamp object
		 *
		 * @param  pStatement        The new SQL value
		 * @exception  SQLException  Description of Exception
		 * @since                    October 7, 2002
		 */
		void setSQL(PreparedStatement pStatement) throws SQLException {
			if (!field.isNull()) {
				timeValue.setTime(((DateDataField) field).getDate().getTime());
				pStatement.setTimestamp(fieldSQL, timeValue);
			} else {
				pStatement.setNull(fieldSQL, java.sql.Types.TIMESTAMP);
			}
		}
	}


	/**
	 *  Copy object for boolean/bit type fields. Expects String data
	 *  representation on Clover's side
	 *  for logical
	 *
	 * @author      dpavlis
	 * @since       November 27, 2003
	 * @revision    $Revision$
	 */
	static class CopyBoolean extends CopySQLData {
		private static String _TRUE_ = "T";
		private static String _FALSE_ = "F";
		private static char _TRUE_CHAR_ = 'T';
		private static char _TRUE_SMCHAR_ = 't';


		/**
		 *  Constructor for the CopyString object
		 *
		 * @param  record      Description of Parameter
		 * @param  fieldSQL    Description of Parameter
		 * @param  fieldJetel  Description of Parameter
		 * @since              October 7, 2002
		 */
		CopyBoolean(DataRecord record, int fieldSQL, int fieldJetel) {
			super(record, fieldSQL, fieldJetel);
		}


		/**
		 *  Sets the Jetel attribute of the CopyString object
		 *
		 * @param  resultSet         The new Jetel value
		 * @exception  SQLException  Description of Exception
		 * @since                    October 7, 2002
		 */
		void setJetel(ResultSet resultSet) throws SQLException {
			field.fromString(resultSet.getBoolean(fieldSQL) ? _TRUE_ : _FALSE_);
		}


		/**
		 *  Sets the SQL attribute of the CopyString object
		 *
		 * @param  pStatement        The new SQL value
		 * @exception  SQLException  Description of Exception
		 * @since                    October 7, 2002
		 */
		void setSQL(PreparedStatement pStatement) throws SQLException {
			char value = ((StringDataField) field).getCharSequence().charAt(0);
			pStatement.setBoolean(fieldSQL, ((value == _TRUE_CHAR_) || (value == _TRUE_SMCHAR_)));
		}

	}

}


