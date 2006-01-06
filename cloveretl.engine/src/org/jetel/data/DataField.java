/*
 *  jETeL/Clover - Java based ETL application framework.
 *  Copyright (C) 2002-04  David Pavlis <david_pavlis@hotmail.com>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 */
// FILE: c:/projects/jetel/org/jetel/data/DataField.java

package org.jetel.data;
import java.io.Serializable;
import java.nio.*;
import java.nio.charset.*;

import org.jetel.exception.BadDataFormatException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.util.StringUtils;

/**
 *  A class that represents data field (its value).
 *
 * @author      David Pavlis
 * @since       March 26, 2002
 * @revision    $Revision$
 * @see         OtherClasses
 */
public abstract class DataField implements Serializable, Comparable {

	// Associations
	/**
	 * @since
	 */
	protected transient DataFieldMetadata metadata;

	/**
	 *  Description of the Field
	 *
	 * @since    September 16, 2002
	 */
	protected boolean isNull;


	// Attributes

	/**
	 *  Constructor
	 *
	 * @param  _metadata  Description of Parameter
	 * @since
	 */
	public DataField(DataFieldMetadata _metadata) {
		this.metadata = _metadata;
	}


	/**
	 * Creates deep copy of existing field. 
	 * 
	 * @return new Field (exact copy of current field)
	 */
	public abstract DataField duplicate();
	
	/**
	 * Sets current field's value from DataField passed as argument
	 * @param fieldFrom DataField from which to get the value
	 */
	public abstract void copyFrom(DataField fieldFrom);
	
	/**
	 *  An operation that sets value of the data field.
	 *
	 * @param  _value  The new Value value
	 * @since
	 */
	public abstract void setValue(Object _value);


	/**
	 *  An operation that sets value of the data field to default value.
	 *
	 * @exception  BadDataFormatException  Description of the Exception
	 */
	public void setToDefaultValue() throws BadDataFormatException {
		try {
			fromString(StringUtils.stringToSpecChar(metadata.getDefaultValue()));
		} catch (Exception ex) {
			String tmp = metadata.getDefaultValue();
			if (tmp == null || tmp.equals("")) {
				if (metadata.isNullable()) {
					throw new BadDataFormatException(ex.getMessage());
				} else {
					throw new BadDataFormatException(metadata.getName() + " is not nullable and is being set to null!");
				}
			} else {
				// here, the only reason to fail is bad DefaultValue
				throw new BadDataFormatException(metadata.getName() + " has incorrect default value(" + metadata.getDefaultValue() + ")!");
			}
		}
	}


	/**
	 *  Sets the Null value indicator/status. If passed-in value
	 * is True, then the field's internal value is set to NULL. The nullability
	 * of the field is checked and if the NULL value is not permitted, the
	 * BadDataFormatException is thrown.
	 *
	 * @param  isNull  The new Null value
	 * @since          September 16, 2002
	 */
	public void setNull(boolean isNull) {
		if (isNull && !metadata.isNullable()) {
			throw new BadDataFormatException(metadata.getName() + " is not nullable and is being set to null!");
		}
		this.isNull = isNull;
	}


	/**
	 *  Gets the internal value of the data field. If field's value is
	 * deemed to be NULL (isNull() return true) then the returned
	 * value is NULL. Otherwise the internal values is converted
	 * to appropriate object representation and returned.<br>
	 *
	 * @return    The Value value
	 * @since
	 */
	public abstract Object getValue();


	/**
	 *  Returns type of the field (String, Numeric, Date, etc.)
	 *
	 * @return    The Type value
	 * @since
	 */
	public abstract char getType();


	/**
	 *  Returns metadata associated with the data field
	 *
	 * @return    The Metadata value
	 * @since
	 */
	public DataFieldMetadata getMetadata() {
		return metadata;
	}


	/**
	 *  Checks the field value for being NULL.
	 *
	 * @return    True if the data field's internal value is
	 * deemed to be NULL otherwise false;
	 * @since     September 16, 2002
	 */
	public boolean isNull() {
		return isNull;
	}


	// Operations
	/**
	 *  Converts field's value into String representation
	 *
	 * @return    Description of the Returned Value
	 * @since
	 */
	public abstract String toString();


	/**
	 *  Parses field's value from String
	 *
	 * @param  _str  Description of Parameter
	 * @since
	 */
	public abstract void fromString(String _str);



	/**
	 *  Get the field's value from ByteBuffer
	 *
	 * @param  decoder                       Charset decoder which could be used to decode characters
	 * @param  dataBuffer                    Description of the Parameter
	 * @exception  CharacterCodingException  Description of the Exception
	 * @since                                October 31, 2002
	 */
	public abstract void fromByteBuffer(ByteBuffer dataBuffer, CharsetDecoder decoder) throws CharacterCodingException;


	/**
	 *  Encode the field's value into ByteBuffer
	 *
	 * @param  encoder                       Charset encoder which could be used to encode characters
	 * @param  dataBuffer                    Description of the Parameter
	 * @exception  CharacterCodingException  Description of the Exception
	 * @since                                October 31, 2002
	 */
	public abstract void toByteBuffer(ByteBuffer dataBuffer, CharsetEncoder encoder) throws CharacterCodingException;


	/**
	 *  Serializes data field into provided byte buffer
	 *
	 * @param  buffer  Description of Parameter
	 * @since          September 16, 2002
	 */
	public abstract void serialize(ByteBuffer buffer);


	/**
	 *  Deserializes data field from provided byte buffer
	 *
	 * @param  buffer  Description of Parameter
	 * @since          September 16, 2002
	 */
	public abstract void deserialize(ByteBuffer buffer);


	/**
	 *  Checks whether two DataField objects are equal. Both
	 * fiels should be of the same type. If they are not, ClassCastException
	 * is thrown.<br> 
	 * Two datafields where each is flagged as to be NULL (isNull() returns true) are
	 * deemed <b>NOT equal</b>.
	 *
	 * @param  obj  Description of Parameter
	 * @return      Description of the Returned Value
	 * @since       September 16, 2002
	 */
	public abstract boolean equals(Object obj);


	/**
	 *  Compares two fields and returs -1,0,1 depending on result of comparison.
	 * Both fiels should be of the same type. If they are not, ClassCastException
	 * is thrown.<br>
	 * When comparing fields where one or both are NULL (isNull() returns true), the
	 * result of comparison is that if <code>obj</code> is NULL the result is 1,
	 * if <code>this.value</code> (value of this field) is NULL, the result is -1; 
	 *
	 * @param  obj  Description of the Parameter
	 * @return      Description of the Return Value
	 */
	public abstract int compareTo(Object obj);


	/**
	 *  Gets the actual size of field (in bytes).<br>
	 *  <i>How many bytes are required for field serialization</i>
	 * @return    The size in bytes
	 */
	public abstract int getSizeSerialized();
}
/*
 *  end class DataField
 */

