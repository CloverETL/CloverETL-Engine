/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2002  David Pavlis
*
*    This program is free software; you can redistribute it and/or modify
*    it under the terms of the GNU General Public License as published by
*    the Free Software Foundation; either version 2 of the License, or
*    (at your option) any later version.
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU General Public License for more details.
*
*    You should have received a copy of the GNU General Public License
*    along with this program; if not, write to the Free Software
*    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/

package org.jetel.data;

/**
 *  Supporting "macros" for setting values of differend data fields
 *
 * @author     dpavlis
 * @since    August 20, 2002
 */
public class SetVal {

	/**
	 *  Assign integer value into Numeric field
	 *
	 * @param  record   data record containing field to set
	 * @param  fieldNo  field ordinal number within record
	 * @param  value    The new Int value
	 * @since           August 20, 2002
	 */
	public final static void setInt(DataRecord record, int fieldNo, int value) {
		DataField field = record.getField(fieldNo);
		if (field instanceof NumericDataField) {
			((NumericDataField) field).setValue(value);
		}else if (field instanceof IntegerDataField) {
			((IntegerDataField) field).setValue(value);
		} else {
			throw new RuntimeException("Not a NumericDataField!");
		}
	}


	/**
	 *  Assign double value into Numeric field
	 *
	 * @param  record   data record containing field to set
	 * @param  fieldNo  field ordinal number within record
	 * @param  value    The new Double value
	 * @since           August 20, 2002
	 */
	public final static void setDouble(DataRecord record, int fieldNo, double value) {
		DataField field = record.getField(fieldNo);
		if (field instanceof NumericDataField) {
			((NumericDataField) field).setValue(value);
		} else {
			throw new RuntimeException("Not a NumericDataField!");
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
	 *  Assign Object value/reference into data field
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
	 *  Assign integer value into Numeric field
	 *
	 * @param  record     data record containing field to set
	 * @param  fieldName  name of the field to set
	 * @param  value      The new Int value
	 * @since             August 20, 2002
	 */
	public final static void setInt(DataRecord record, String fieldName, int value) {
		DataField field = record.getField(record.getMetadata().getFieldPosition(fieldName));
		if (field instanceof NumericDataField) {
			((NumericDataField) field).setValue(value);
		} else if (field instanceof IntegerDataField) {
			((IntegerDataField) field).setValue(value);
		} else {
			throw new RuntimeException("Not a NumericDataField!");
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
		if (field instanceof NumericDataField) {
			((NumericDataField) field).setValue(value);
		} else {
			throw new RuntimeException("Not a NumericDataField!");
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
	 *  Assign Object value/reference into data field
	 *
	 * @param  record     data record containing field to set
	 * @param  fieldName  name of the field to set
	 * @param  value      The new Value value
	 * @since             August 23, 2002
	 */
	public final static void setValue(DataRecord record, String fieldName, Object value) {
		record.getField(record.getMetadata().getFieldPosition(fieldName)).setValue(value);
	}
}

