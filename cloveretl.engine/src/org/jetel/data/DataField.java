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
			fromString(metadata.getDefaultValue());
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
	 *  Sets the Null value indicator
	 *
	 * @param  isNull  The new Null value
	 * @since          September 16, 2002
	 */
	public void setNull(boolean isNull) {
		if (metadata.isNullable()) {
			this.isNull = isNull;
		} else {
			throw new BadDataFormatException(metadata.getName() + " is not nullable and is being set to null!");
		}
	}


	/**
	 *  Gets the value of the data field
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
	 *  Gets the Null value indicator
	 *
	 * @return    The Null value
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
	 *  Checks whether two DataField objects are equal
	 *
	 * @param  obj  Description of Parameter
	 * @return      Description of the Returned Value
	 * @since       September 16, 2002
	 */
	public abstract boolean equals(Object obj);


	/**
	 *  Compares two fields and returs -1,0,1 depending on result of comparison.
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

