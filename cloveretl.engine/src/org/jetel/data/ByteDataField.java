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
package org.jetel.data;
import java.nio.ByteBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.util.Arrays;
import org.jetel.metadata.DataFieldMetadata;

/**
 *  A class that represents array of bytes field.<br>
 *  <br>
 *  <i>Note: it has no sence to test this field for null value as even all zeros
 *  can indicate meaningful value. Yet, the isNull & setNull is implemented.
 *
 *@author     D.Pavlis
 *@created    January 26, 2003
 *@since      October 29, 2002
 *@see        org.jetel.metadata.DataFieldMetadata
 */
public class ByteDataField extends DataField {

	// Attributes

	/**
	 *  Description of the Field
	 *
	 *@since    October 29, 2002
	 */
	protected byte[] value;
	private static final int ARRAY_LENGTH_INDICATOR_SIZE=4;
	
	private final static int INITIAL_BYTE_ARRAY_CAPACITY = 8;
	


	/**
	 *  Constructor for the NumericDataField object
	 *
	 *@param  _metadata  Metadata describing field
	 *@since             October 29, 2002
	 */
	public ByteDataField(DataFieldMetadata _metadata) {
		super(_metadata);
		if (_metadata.getSize() < 1) {
			value = new byte[INITIAL_BYTE_ARRAY_CAPACITY];
		} else {
			value = new byte[_metadata.getSize()];
		}
		setNull(true);
	}


	/**
	 *  Constructor for the NumericDataField object
	 *
	 *@param  _metadata  Metadata describing field
	 *@param  value      Value to assign to field
	 *@since             October 29, 2002
	 */
	public ByteDataField(DataFieldMetadata _metadata, byte[] value) {
		super(_metadata);
		this.value = value;
		setNull(false);
	}


	/**
	 *  Sets the value of the field
	 *
	 *@param  _value  The new Value value
	 *@since          October 29, 2002
	 */
	public void setValue(Object _value) {
		if (_value == null) {
			setNull(true);
			return;
		}
		if (_value instanceof byte[]) {
			value = (byte[]) _value;
			setNull(false);
		} else {
			throw new RuntimeException("not byte array");
		}
	}


	/**
	 *  Sets the value of the field
	 *
	 *@param  value  value is copied into internal byte array using
	 *      System.arraycopy method
	 *@since         October 29, 2002
	 */
	public void setValue(byte[] value) {
		System.arraycopy(value, 0, this.value, 0, this.value.length);
		setNull(false);
	}


	/**
	 *  Sets the value of the field
	 *
	 *@param  value  The new byte value - the whole byte array is filled with this
	 *      value
	 *@since         October 29, 2002
	 */
	public void setValue(byte value) {
		Arrays.fill(this.value, value);
		setNull(false);
	}


	// Associations

	// Operations

	/**
	 *  Gets the Metadata attribute of the ByteDataField object
	 *
	 *@return    The Metadata value
	 *@since     October 29, 2002
	 */
	public DataFieldMetadata getMetadata() {
		return super.getMetadata();
	}


	/**
	 *  Gets the Field Type
	 *
	 *@return    The Type value
	 *@since     October 29, 2002
	 */
	public char getType() {
		return DataFieldMetadata.BYTE_FIELD;
	}


	/**
	 *  Gets the decimal value represented by this object (as Decimal object)
	 *
	 *@return    The Value value
	 *@since     October 29, 2002
	 */
	public Object getValue() {
		return value;
	}


	/**
	 *  Gets the byte value represented by this object as byte primitive
	 *
	 *@param  position  offset in byte array
	 *@return           The Byte value
	 *@since            October 29, 2002
	 */
	public byte getByte(int position) {
		return value[position];
	}


	/**
	 *  Gets the Byte array value of the ByteDataField object
	 *
	 *@return    The Byte[] value
	 *@since     October 29, 2002
	 */
	public byte[] getByte() {
		return value;
	}


	/**
	 *  Formats internal byte array value into string representation
	 *
	 *@return    String representation of byte array
	 *@since     October 29, 2002
	 */
	public String toString() {
		return value.toString();
	}


	/**
	 *  Parses byte array value from string (convers characters in string into byte
	 *  array using system's default charset encoder)
	 *
	 *@param  valueStr  Description of Parameter
	 *@since            October 29, 2002
	 */
	public void fromString(String valueStr) {
		System.arraycopy(valueStr.getBytes(), 0, this.value, 0, this.value.length);
	}


	/**
	 *  Description of the Method
	 *
	 *@param  dataBuffer  Description of Parameter
	 *@param  decoder     Description of Parameter
	 *@since              October 31, 2002
	 */
	public void fromByteBuffer(ByteBuffer dataBuffer, CharsetDecoder decoder) {
		dataBuffer.get(value);
	}


	/**
	 *  Description of the Method
	 *
	 *@param  dataBuffer  Description of Parameter
	 *@param  encoder     Description of Parameter
	 *@since              October 31, 2002
	 */
	public void toByteBuffer(ByteBuffer dataBuffer, CharsetEncoder encoder) {
		dataBuffer.put(value);
	}


	/**
	 *  Performs serialization of the internal value into ByteBuffer (used when
	 *  moving data records between components).
	 *
	 *@param  buffer  Description of Parameter
	 *@since          October 29, 2002
	 */
	public void serialize(ByteBuffer buffer) {
		buffer.putInt(value.length);
		buffer.put(value);
	}


	/**
	 *  Performs deserialization of data
	 *
	 *@param  buffer  Description of Parameter
	 *@since          October 29, 2002
	 */
	public void deserialize(ByteBuffer buffer) {
		int length = buffer.getInt();
		buffer.get(value, 0, length);
		setNull(false);
	}


	/**
	 *  Description of the Method
	 *
	 *@param  obj  Description of Parameter
	 *@return      Description of the Returned Value
	 *@since       October 29, 2002
	 */
	public boolean equals(Object obj) {
		return Arrays.equals(this.value, ((ByteDataField) obj).getByte());
	}


	/**
	 *  Compares this object with the specified object for order.
	 *
	 *@param  obj  Description of the Parameter
	 *@return      Description of the Return Value
	 */
	public int compareTo(Object obj) {
		byte[] byteObj = (byte[]) ((ByteDataField) obj).getValue();
		int compLength = value.length >= byteObj.length ? value.length : byteObj.length;
		for (int i = 0; i < compLength; i++) {
			if (value[i] > byteObj[i]) {
				return 1;
			} else if (value[i] < byteObj[i]) {
				return -1;
			}
		}
		// arrays seem to be the same (so far), decide according to the length
		if (value.length == byteObj.length) {
			return 0;
		} else if (value.length > byteObj.length) {
			return 1;
		} else {
			return -1;
		}
	}
	
	/**
	 *  Gets the size attribute of the IntegerDataField object
	 *
	 * @return    The size value
	 * @see	      org.jetel.data.DataField
	 */
	public int getSizeSerialized() {
		return value.length+ARRAY_LENGTH_INDICATOR_SIZE;
	}

}
/*
 *  end class NumericDataField
 */

