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

import java.util.Date;
/**
 *  Supporting "macros" for getting values of differend data fields 
 *
 * @author     dpavlis
 * @since    August 20, 2002
 */
public class GetVal {

	/**
	 *  Gets the Int attribute of the GetVal class
	 *
	 * @param  record   Description of Parameter
	 * @param  fieldNo  Description of Parameter
	 * @return          The Int value
	 * @since           August 20, 2002
	 */
	public final static int getInt(DataRecord record, int fieldNo) {
		DataField field = record.getField(fieldNo);
		if (field instanceof NumericDataField) {
			return ((NumericDataField) field).getInt();
		}else if (field instanceof IntegerDataField) {
			return ((IntegerDataField) field).getInt();
		} else {
			throw new RuntimeException("Not a NumericDataField!");
		}
	}


	/**
	 *  Gets the Double attribute of the GetVal class
	 *
	 * @param  record   Description of Parameter
	 * @param  fieldNo  Description of Parameter
	 * @return          The Double value
	 * @since           August 20, 2002
	 */
	public final static double getDouble(DataRecord record, int fieldNo) {
		DataField field = record.getField(fieldNo);
		if (field instanceof NumericDataField) {
			return ((NumericDataField) field).getDouble();
		} else {
			throw new RuntimeException("Not a NumericDataField!");
		}
	}


	/**
	 *  Gets the String attribute of the GetVal class
	 *
	 * @param  record   Description of Parameter
	 * @param  fieldNo  Description of Parameter
	 * @return          The String value
	 * @since           August 20, 2002
	 */
	public final static String getString(DataRecord record, int fieldNo) {
		DataField field = record.getField(fieldNo);
		return field.toString();
	}


	/**
	 *  Gets the Date attribute of the GetVal class
	 *
	 * @param  record   Description of Parameter
	 * @param  fieldNo  Description of Parameter
	 * @return          The Date value
	 * @since           August 23, 2002
	 */
	public final static Date getDate(DataRecord record, int fieldNo) {
		DataField field = record.getField(fieldNo);
		if (field instanceof DateDataField) {
			return (Date) (field.getValue());
		} else {
			throw new RuntimeException("Not a DateDataField!");
		}
	}


	/**
	 *  Gets the Value attribute of the GetVal class
	 *
	 * @param  record   Description of Parameter
	 * @param  fieldNo  Description of Parameter
	 * @return          The Value value
	 * @since           August 23, 2002
	 */
	public final static Object getValue(DataRecord record, int fieldNo) {
		return record.getField(fieldNo).getValue();
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
		if (field instanceof NumericDataField) {
			return ((NumericDataField) field).getInt();
		} else if (field instanceof IntegerDataField) {
			return ((IntegerDataField) field).getInt();
		} else {			
			throw new RuntimeException("Not a NumericDataField!");
		}
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
		if (field instanceof NumericDataField) {
			return ((NumericDataField) field).getDouble();
		} else {
			throw new RuntimeException("Not a NumericDataField!");
		}
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
		} else {
			throw new RuntimeException("Not a DateDataField!");
		}
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
}

