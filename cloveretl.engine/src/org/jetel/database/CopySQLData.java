/*
 *  jETeL/Clover - Java based ETL application framework.
 *  Copyright (C) 2002  David Pavlis
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package org.jetel.database;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.HashSet;

import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.DateDataField;
import org.jetel.data.IntegerDataField;
import org.jetel.data.NumericDataField;
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
	protected int fieldSQL;
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
		field = record.getField(fieldJetel);
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
	 * @param  fieldTypes        Description of Parameter
	 * @param  record            Description of Parameter
	 * @return                   Description of the Returned Value
	 * @exception  SQLException  Description of Exception
	 * @since                    October 4, 2002
	 */
	public static CopySQLData[] jetel2sqlTransMap(List fieldTypes, DataRecord record) throws SQLException {
		int i = 0;
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
	 * @param  fieldTypes          Description of the Parameter
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
			jetelFieldType=record.getMetadata().getFieldType(fromIndex);
			if (fromIndex == -1) {
				throw new JetelException(" Field \"" + cloverFields[i] + "\" does not exist in DataRecord !");
			}
			// we copy from Clover's field to JDBC - toIndex/fromIndex is switched here
			transMap[i++] = createCopyObject(jdbcType,jetelFieldType, record, toIndex, fromIndex);
			toIndex++;// we go one by one - order defined by insert/update statement

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
			case Types.OTHER:// When other, try to copy it as STRING - should work for NCHAR/NVARCHAR
				return new CopyString(record, fromIndex, toIndex);
			default:
				throw new RuntimeException("SQL data type not supported: " + SQLType);
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
			dateValue.setTime(((Date) field.getValue()).getTime());
			pStatement.setDate(fieldSQL, dateValue);
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
			pStatement.setDouble(fieldSQL, ((NumericDataField) field).getDouble());
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
			pStatement.setInt(fieldSQL, ((IntegerDataField) field).getInt());
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
			field.fromString(resultSet.getString(fieldSQL));
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
			timeValue.setTime(((java.util.Date) field.getValue()).getTime());
			pStatement.setTimestamp(fieldSQL, timeValue);
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


