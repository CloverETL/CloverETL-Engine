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
import org.jetel.metadata.DataFieldMetadata;

// TODO: implement Sequence server which would make sequences persistent
/**
 *  A class that represents sequence of integers.<br>
 *  It can generate sequence of integers based on parameters.
 *  When generated from XML metadata, default values are used.
 *  <br>
 *
 * @author      D.Pavlis
 * @since       October 19, 2003
 * @revision    $Revision$
 * @created     October 19, 2003
 * @see         org.jetel.metadata.DataFieldMetadata
 */

public class SequenceField extends DataField {

	// Attributes

	/**
	 *  Description of the Field
	 *
	 * @since    October 29, 2002
	 */
	protected int value;
	protected int increment;
	protected boolean cycle;

	private final static int FIELD_SIZE_BYTES = 4;// standard size of field
	private final static int DEFAULT_INIT_VALUE = 0;
	private final static int DEFAULT_INCREMENT = 1;
	private final static int DEFAULT_MAX_VALUE = Integer.MAX_VALUE;
	private final boolean DEFAULT_CYCLE = true;



	/**
	 *  Constructor for the NumericDataField object
	 *
	 * @param  _metadata  Metadata describing field
	 * @since             October 29, 2002
	 */
	public SequenceField(DataFieldMetadata _metadata) {
		super(_metadata);
		value = DEFAULT_INIT_VALUE;
		increment = DEFAULT_INCREMENT;
		cycle = DEFAULT_CYCLE;
	}


	/**
	 *  Constructor for the NumericDataField object
	 *
	 * @param  _metadata  Metadata describing field
	 * @param  value      Initial value of sequence
	 * @since             October 29, 2002
	 */
	public SequenceField(DataFieldMetadata _metadata, int value) {
		super(_metadata);
		this.value = value;
		increment = DEFAULT_INCREMENT;
		cycle = DEFAULT_CYCLE;
	}


	/**
	 *Constructor for the ByteDataField object
	 *
	 * @param  _metadata  Description of the Parameter
	 * @param  value      Initial value of sequence
	 * @param  increment  Increment of sequence
	 */
	public SequenceField(DataFieldMetadata _metadata, int value, int increment) {
		super(_metadata);
		this.value = value;
		this.increment = increment;
		cycle = DEFAULT_CYCLE;
	}


	
	/* (non-Javadoc)
	 * @see org.jetel.data.DataField#copy()
	 */
	public DataField duplicate(){
	    return new SequenceField(metadata,value,increment);
	}
	
	
	/* (non-Javadoc)
	 * @see org.jetel.data.DataField#copyField(org.jetel.data.DataField)
	 */
	public void copyFrom(DataField fromField){
	    this.setValue(fromField.getValue());
	}
	/**
	 *  Sets the value of the field
	 *
	 * @param  _value  The new Value value
	 * @since          October 29, 2002
	 */
	public void setValue(Object _value) {
		if (_value == null) {
			return;
		}
		if (_value instanceof Integer) {
			value = ((Integer) _value).intValue();
		} else {
			throw new RuntimeException("not an Integer");
		}
	}


	/**
	 *  Sets the value of the field
	 *
	 * @param  value  value is copied into internal byte array using
	 *      System.arraycopy method
	 * @since         October 29, 2002
	 */
	public void setValue(int value) {
		this.value = value;
	}

	// Operations

	/**
	 *  Gets the Metadata attribute of the ByteDataField object
	 *
	 * @return    The Metadata value
	 * @since     October 29, 2002
	 */
	public DataFieldMetadata getMetadata() {
		return super.getMetadata();
	}


	/**
	 *  Gets the Field Type
	 *
	 * @return    The Type value
	 * @since     October 29, 2002
	 */
	public char getType() {
		return DataFieldMetadata.SEQUENCE_FIELD;
	}


	/**
	 *  Gets the decimal value represented by this object (as Decimal object)
	 *
	 * @return    The Value value
	 * @since     October 29, 2002
	 */
	public Object getValue() {
		return new Integer(value);
	}


	/**
	 *  Gets the nextValue attribute of the SequenceField object
	 *
	 * @return    The nextValue value
	 */
	public synchronized Object getNextValue() {

		value += increment;
		return new Integer(value);
	}


	/**
	 *  Gets the nextIntValue attribute of the SequenceField object
	 *
	 * @return    The nextIntValue value
	 */
	public synchronized int getNextIntValue() {
		value += increment;
		return value;
	}


	/**
	 *  Gets the byte value represented by this object as byte primitive
	 *
	 * @return           The Byte value
	 * @since            October 29, 2002
	 */
	public int getInt() {
		return value;
	}


	/**
	 *  Formats internal byte array value into string representation
	 *
	 * @return    String representation of byte array
	 * @since     October 29, 2002
	 */
	public String toString() {
		return "" + value;
	}


	/**
	 *  Parses byte array value from string (convers characters in string into byte
	 *  array using system's default charset encoder)
	 *
	 * @param  valueStr  Description of Parameter
	 * @since            October 29, 2002
	 */
	public void fromString(String valueStr) {
		value = Integer.parseInt(valueStr);
	}


	/**
	 *  Description of the Method
	 *
	 * @param  dataBuffer  Description of Parameter
	 * @param  decoder     Description of Parameter
	 * @since              October 31, 2002
	 */
	public void fromByteBuffer(ByteBuffer dataBuffer, CharsetDecoder decoder) {
		value = dataBuffer.getInt();
	}


	/**
	 *  Description of the Method
	 *
	 * @param  dataBuffer  Description of Parameter
	 * @param  encoder     Description of Parameter
	 * @since              October 31, 2002
	 */
	public void toByteBuffer(ByteBuffer dataBuffer, CharsetEncoder encoder) {
		dataBuffer.putInt(value);
	}


	/**
	 *  Performs serialization of the internal value into ByteBuffer (used when
	 *  moving data records between components).
	 *
	 * @param  buffer  Description of Parameter
	 * @since          October 29, 2002
	 */
	public void serialize(ByteBuffer buffer) {
		buffer.putInt(value);
	}


	/**
	 *  Performs deserialization of data
	 *
	 * @param  buffer  Description of Parameter
	 * @since          October 29, 2002
	 */
	public void deserialize(ByteBuffer buffer) {
		value = buffer.getInt();
	}


	/**
	 *  Description of the Method
	 *
	 * @param  obj  Description of Parameter
	 * @return      Description of the Returned Value
	 * @since       October 29, 2002
	 */
	public boolean equals(Object obj) {
		return (value == ((SequenceField) obj).getInt());
	}


	/**
	 *  Compares this object with the specified object for order.
	 *
	 * @param  obj  Description of the Parameter
	 * @return      Description of the Return Value
	 */
	public int compareTo(Object obj) {
		int compareToValue = ((SequenceField) obj).getInt();
		if (value > compareToValue) {
			return 1;
		} else if (value < compareToValue) {
			return -1;
		} else {
			return 0;
		}
	}

	/**
	 *  Returns how many bytes will be occupied when this field with current
	 *  value is serialized into ByteBuffer
	 *
	 * @return    The size value
	 * @see	      org.jetel.data.DataField
	 */
	public int getSizeSerialized() {
		return FIELD_SIZE_BYTES;
	}

	
}
/*
 *  end class IntSequence
 */

