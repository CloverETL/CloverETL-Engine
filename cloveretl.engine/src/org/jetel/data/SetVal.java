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

import org.jetel.data.primitive.Numeric;

/**
 *  Supporting "macros" for setting values of differend data fields
 *
 * @author     dpavlis
 * @since    August 20, 2002
 */
public class SetVal {

	/**
	 *  Assign integer value into numeric field
	 *
	 * @param  record   data record containing field to set
	 * @param  fieldNo  field ordinal number within record
	 * @param  value    The new int value
	 * @since           August 20, 2002
	 */
	public final static void setInt(DataRecord record, int fieldNo, int value) {
		DataField field = record.getField(fieldNo);
		if (field instanceof Numeric) {
			((Numeric) field).setValue(value);
		}else {
			throw new RuntimeException("Not a numeric DataField!");
		}
	}


	/**
	 *  Assign double value into numeric field
	 *
	 * @param  record   data record containing field to set
	 * @param  fieldNo  field ordinal number within record
	 * @param  value    The new double value
	 * @since           August 20, 2002
	 */
	public final static void setDouble(DataRecord record, int fieldNo, double value) {
		DataField field = record.getField(fieldNo);
		if (field instanceof Numeric) {
			((Numeric) field).setValue(value);
		} else {
			throw new RuntimeException("Not a numeric DataField!");
		}
	}
	
	/**
	 *  Assign long value into numeric field
	 *
	 * @param  record   data record containing field to set
	 * @param  fieldNo  field ordinal number within record
	 * @param  value    The new long value
	 * @since           August 20, 2002
	 */
	public final static void setLong(DataRecord record, int fieldNo, long value) {
		DataField field = record.getField(fieldNo);
		if (field instanceof Numeric) {
			((Numeric) field).setValue(value);
		} else {
			throw new RuntimeException("Not a numeric DataField!");
		}
	}


	/**
	 *  Assign String value into field.<br>
	 *  The exact behaviour depens on type of the data filed. For fields other than String, the String is parsed
	 *  to provide appropriate value.
	 *
	 * @param  record   data record containing field to set
	 * @param  fieldNo  field ordinal number within record
	 * @param  value    The new String value
	 * @since           August 20, 2002
	 */
	public final static void setString(DataRecord record, int fieldNo, String value) {
		DataField field = record.getField(fieldNo);
		field.fromString(value);
	}

	/**
	 *  Assign Date value into field.<br>
	 *
	 * @param  record   data record containing field to set
	 * @param  fieldNo  field ordinal number within record
	 * @param  value    The new Date value
	 * @since           September 16, 2004
	 */
	public final static void setDate(DataRecord record, int fieldNo, java.util.Date value) {
		DataField field = record.getField(fieldNo);
		if (field instanceof DateDataField) {
			 field.setValue(value);
		} else if (field instanceof StringDataField){
			field.fromString(value.toString());
		}else{
			throw new RuntimeException("Not a DateDataField!");
		}
	}

	/**
	 *  Assign Object value/reference into data field.<br>
	 * What is going to be assigned depends on DataField type and
	 * Object type. This method/function can be used to set NULL value 
	 * to any field type.
	 *
	 * @param  record   data record containing field to set
	 * @param  fieldNo  field ordinal number within record
	 * @param  value    The new Value value
	 * @since           August 23, 2002
	 */
	public final static void setValue(DataRecord record, int fieldNo, Object value) {
		record.getField(fieldNo).setValue(value);
	}

	/**
	 * Set field's value to NULL.<br>Warning: setting to NULL field
	 * which has specified not nullable option will raise
	 * BadDataFormatException
	 * 
	 * @param record
	 * @param fieldNo
	 * @see org.jetel.exception.BadDataFormatException
	 */
	public final static void setNull(DataRecord record, int fieldNo) {
		record.getField(fieldNo).setNull(true);
	}

	
	
	/**
	 *  Assign integer value into Numeric field
	 *
	 * @param  record     data record containing field to set
	 * @param  fieldName  name of the field to set
	 * @param  value      The new Int value
	 * @since             August 20, 2002
	 */
	public final static void setInt(DataRecord record, String fieldName, int value) {
		DataField field = record.getField(record.getMetadata().getFieldPosition(fieldName));
		if (field instanceof Numeric) {
			((Numeric) field).setValue(value);
		} else {
			throw new RuntimeException("Not a numeric DataField!");
		}
	}


	/**
	 *  Assign double value into Numeric field
	 *
	 * @param  record     data record containing field to set
	 * @param  fieldName  name of the field to set
	 * @param  value      The new Double value
	 * @since             August 20, 2002
	 */
	public final static void setDouble(DataRecord record, String fieldName, double value) {
		DataField field = record.getField(record.getMetadata().getFieldPosition(fieldName));
		if (field instanceof Numeric) {
			((Numeric) field).setValue(value);
		} else {
			throw new RuntimeException("Not a numeric DataField!");
		}
	}

	/**
	 *  Assign long value into Numeric field
	 *
	 * @param  record     data record containing field to set
	 * @param  fieldName  name of the field to set
	 * @param  value      The new Double value
	 * @since             August 20, 2002
	 */
	public final static void setLong(DataRecord record, String fieldName, long value) {
		DataField field = record.getField(record.getMetadata().getFieldPosition(fieldName));
		if (field instanceof Numeric) {
			((Numeric) field).setValue(value);
		} else {
			throw new RuntimeException("Not a numeric DataField!");
		}
	}

	/**
	 *  Assign String value into field.<br>
	 *  The exact behaviour depens on type of the data filed. For fields other than String, the String is parsed
	 *  to provide appropriate value.
	 *
	 * @param  record     data record containing field to set
	 * @param  fieldName  name of the field to set
	 * @param  value      The new String value
	 * @since             August 20, 2002
	 */
	public final static void setString(DataRecord record, String fieldName, String value) {
		DataField field = record.getField(record.getMetadata().getFieldPosition(fieldName));
		field.fromString(value);
	}
	
	/**
	 *  Assign Date value into field.<br>
	 *
	 * @param  record   data record containing field to set
	 * @param  fieldNo  field ordinal number within record
	 * @param  value    The new Date value
	 * @since           September 16, 2004
	 */
	public final static void setDate(DataRecord record, String fieldName, java.util.Date value) {
		DataField field = record.getField(record.getMetadata().getFieldPosition(fieldName));
		if (field instanceof DateDataField) {
			 field.setValue(value);
		} else if (field instanceof StringDataField){
			field.fromString(value.toString());
		}else{
			throw new RuntimeException("Not a DateDataField!");
		}
	}


	/**
	 *  Assign Object value/reference into data field.<br>
	 * What is going to be assigned depends on DataField type and
	 * Object type. This method/function can be used to set NULL value 
	 * to any field type.
	 *
	 * @param  record     data record containing field to set
	 * @param  fieldName  name of the field to set
	 * @param  value      The new Value value
	 * @since             August 23, 2002
	 */
	public final static void setValue(DataRecord record, String fieldName, Object value) {
		record.getField(record.getMetadata().getFieldPosition(fieldName)).setValue(value);
	}
	
	/**
	 * Set field's value to NULL.<br>Warning: setting to NULL field
	 * which has specified not nullable option will raise
	 * BadDataFormatException
	 * 
	 * @param record
	 * @param fieldNo
	 * @see org.jetel.exception.BadDataFormatException
	 */
	public final static void setNull(DataRecord record, String fieldName) {
		record.getField(record.getMetadata().getFieldPosition(fieldName)).setNull(true);
	}
}

