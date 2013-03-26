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
package org.jetel.data;

import java.util.Date;

import org.jetel.data.primitive.Numeric;
/**
 *  Supporting "macros" for getting values of differend data fields.<br>
 * <i>Note: methods which takes field's position within record as input
 * parameter are much faster than those accepting field's name.</i> 
 *
 * @author     dpavlis
 * @since    August 20, 2002
 */
public class GetVal {

	/**
	 *  Return field's value as integer (int primitive)
	 *
	 * @param  record   DataRecord
	 * @param  fieldNo  index of data field
	 * @return          integer value
	 * @since           August 20, 2002
	 */
	public final static int getInt(DataRecord record, int fieldNo) {
		DataField field = record.getField(fieldNo);
		if (field instanceof Numeric) {
			return ((Numeric) field).getInt();
		} 
		throw new RuntimeException("Not a numeric DataField!");
	}


	/**
	 *  Return field's value as double (double primitive)
	 *
	 * @param  record   DataRecord
	 * @param  fieldNo  index of data field
	 * @return          double value
	 * @since           August 20, 2002
	 */
	public final static double getDouble(DataRecord record, int fieldNo) {
		DataField field = record.getField(fieldNo);
		if (field instanceof Numeric) {
			return ((Numeric) field).getDouble();
		}
		throw new RuntimeException("Not a numeric DataField!");
	}

	/**
	 * Return field's value as long (long primitive)
	 * 
	 * @param record	DataRecord
	 * @param fieldNo	index of data field
	 * @return			long value
	 */
	public final static long getLong(DataRecord record, int fieldNo) {
		DataField field = record.getField(fieldNo);
		if (field instanceof Numeric) {
			return ((Numeric) field).getLong();
		} 
		throw new RuntimeException("Not a numeric DataField!");
	}


	/**
	 *  Return field's value as String. If field is not
	 * of type StringDataField, the field's value is converted
	 * to String based on format specified in field's metadata 
	 *
	 * @param  record   DataRecord
	 * @param  fieldNo  index of data field
	 * @return          String value of the field
	 * @since           August 20, 2002
	 */
	public final static String getString(DataRecord record, int fieldNo) {
		DataField field = record.getField(fieldNo);
		return field.toString();
	}


	/**
	 *  Return field's value as Date (java.util.Date).<br>
	 *
	 * @param  record   DataRecord
	 * @param  fieldNo  index of data field
	 * @return          The Date value
	 * @since           August 23, 2002
	 */
	public final static Date getDate(DataRecord record, int fieldNo) {
		DataField field = record.getField(fieldNo);
		if (field instanceof DateDataField) {
			return (Date) (field.getValue());
		} 
		throw new RuntimeException("Not a DateDataField!");
	}


	/**
	 *  Return field's value as Object (internal representation of the value).<br>
	 *
	 * @param  record   DataRecord
	 * @param  fieldNo  index of data field
	 * @return          The Value value
	 * @since           August 23, 2002
	 */
	public final static Object getValue(DataRecord record, int fieldNo) {
		return record.getField(fieldNo).getValue();
	}
	
	
	/**
	 *  Return field's value as Object (internal representation of the value).
	 *  If record or field happens to be NULL, use provided defaultValue instead.<br>
	 * 
	 * @param record	DataRecord
	 * @param fieldNo	index of data field
	 * @param defaultValue	value (Object) to be used if field's value is null or record is null
	 * @return
	 */
	public final static Object getValueNVL(DataRecord record, int fieldNo,Object defaultValue){
	    
	    if (record!=null && ! record.getField(fieldNo).isNull()){
	        return record.getField(fieldNo).getValue();
	    }

	    return defaultValue;
	}
	

	/**
	 * Returns field's NULL status (true/false)
	 * 
	 * @param record	DataRecord
	 * @param fieldNo	index of data field
	 * @return		true if field has NULL indicator (value) set
	 */
	public final static boolean isNull(DataRecord record, int fieldNo) {
		return record.getField(fieldNo).isNull();
	}
	

	/**
	 *  Gets the Int attribute of the GetVal class
	 *
	 * @param  record     Description of Parameter
	 * @param  fieldName  Description of Parameter
	 * @return            The Int value
	 * @since             August 20, 2002
	 */
	public final static int getInt(DataRecord record, String fieldName) {
		DataField field = record.getField(record.getMetadata().getFieldPosition(fieldName));
		if (field instanceof Numeric) {
			return ((Numeric) field).getInt();
		} 	
		throw new RuntimeException("Not a numeric DataField!");
	}


	/**
	 *  Gets the Double attribute of the GetVal class
	 *
	 * @param  record     Description of Parameter
	 * @param  fieldName  Description of Parameter
	 * @return            The Double value
	 * @since             August 20, 2002
	 */
	public final static double getDouble(DataRecord record, String fieldName) {
		DataField field = record.getField(record.getMetadata().getFieldPosition(fieldName));
		if (field instanceof Numeric) {
			return ((Numeric) field).getDouble();
		}
		throw new RuntimeException("Not a number DataField!");
	}

	/**
	 * Return field's value as long (long primitive)
	 * 
	 * @param record
	 * @param fieldName
	 * @return
	 */
	public final static long getLong(DataRecord record, String fieldName) {
		DataField field = record.getField(record.getMetadata().getFieldPosition(fieldName));
		if (field instanceof Numeric) {
			return ((Numeric) field).getLong();
		}
		throw new RuntimeException("Not a number DataField!");
	}

	/**
	 *  Gets the String attribute of the GetVal class
	 *
	 * @param  record     Description of Parameter
	 * @param  fieldName  Description of Parameter
	 * @return            The String value
	 * @since             August 20, 2002
	 */
	public final static String getString(DataRecord record, String fieldName) {
		DataField field = record.getField(record.getMetadata().getFieldPosition(fieldName));
		return field.toString();
	}


	/**
	 *  Gets the Date attribute of the GetVal class
	 *
	 * @param  record     Description of Parameter
	 * @param  fieldName  Description of Parameter
	 * @return            The Date value
	 * @since             August 23, 2002
	 */
	public final static Date getDate(DataRecord record, String fieldName) {
		DataField field = record.getField(record.getMetadata().getFieldPosition(fieldName));
		if (field instanceof DateDataField) {
			return (Date) (field.getValue());
		}
		throw new RuntimeException("Not a DateDataField!");
	}


	/**
	 *  Gets the Value attribute of the GetVal class
	 *
	 * @param  record     Description of Parameter
	 * @param  fieldName  Description of Parameter
	 * @return            The Value value
	 * @since             August 23, 2002
	 */
	public final static Object getValue(DataRecord record, String fieldName) {
		return record.getField(record.getMetadata().getFieldPosition(fieldName)).getValue();
	}
	
	
	/**
	 *  Return field's value as Object (internal representation of the value).
	 *  If record or field happens to be NULL, use provided defaultValue instead.<br>
	 * 
	 * @param record	DataRecord
	 * @param fieldName	Description of Parameter
	 * @param defaultValue	value (Object) to be used if field's value is null or record is null
	 * @return
	 */
	public final static Object getValueNVL(DataRecord record, String fieldName,Object defaultValue){
	    
	    if (record!=null && ! record.getField(record.getMetadata().getFieldPosition(fieldName)).isNull()){
	        return record.getField(record.getMetadata().getFieldPosition(fieldName)).getValue();
	    }

	    return defaultValue;
	}
	
	
	/**
	 * Returns field's NULL status (true/false)
	 * 
	 * @param record		DataRecord
	 * @param fieldName		field's name
	 * @return				true if field has NULL indicator (value) set
	 */
	public final static boolean isNull(DataRecord record, String fieldName) {
		return record.getField(record.getMetadata().getFieldPosition(fieldName)).isNull();
	}
	
}

