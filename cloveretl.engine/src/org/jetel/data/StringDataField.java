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
// FILE: c:/projects/jetel/org/jetel/data/StringDataField.java

package org.jetel.data;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharacterCodingException;

import org.jetel.exception.BadDataFormatException;
import org.jetel.metadata.DataFieldMetadata;

/**
 *  A class that represents String type data field
 *
 * @author      D.Pavlis
 * @since       March 27, 2002
 * @revision    $Revision$
 * @created     January 26, 2003
 * @see         org.jetel.metadata.DataFieldMetadata
 */
public class StringDataField extends DataField {

	private StringBuffer value;

	// Attributes
	/**
	 *  An attribute that represents ...
	 *
	 * @since
	 */
	private final static int INITIAL_STRING_BUFFER_CAPACITY = 32;


	// Associations

	// Operations

	/**
	 *  Constructor for the StringDataField object
	 *
	 * @param  _metadata  Description of Parameter
	 * @since             April 23, 2002
	 */
	public StringDataField(DataFieldMetadata _metadata) {
		super(_metadata);
		if (_metadata.getSize() < 1) {
			value = new StringBuffer(INITIAL_STRING_BUFFER_CAPACITY);
		} else {
			value = new StringBuffer(_metadata.getSize());
		}
	}


	/**
	 *  Constructor for the StringDataField object
	 *
	 * @param  _metadata  Description of Parameter
	 * @param  _value     Description of Parameter
	 * @since             April 23, 2002
	 */
	public StringDataField(DataFieldMetadata _metadata, String _value) {
		this(_metadata);
		setValue(_value);
	}


	/**
	 *  Sets the Value attribute of the StringDataField object
	 *
	 * @param  value                       The new value value
	 * @exception  BadDataFormatException  Description of the Exception
	 * @since                              April 23, 2002
	 */
	public void setValue(Object value) throws BadDataFormatException {
		this.value.setLength(0);
		if (value == null || ((value instanceof StringBuffer) && (((StringBuffer) value).length() == 0))) {
			if (this.metadata.isNullable()) {
				super.setNull(true);
			} else {
				throw new BadDataFormatException(getMetadata().getName() + " field can not be set to null!(nullable=false)", null);
			}
			return;
		}

		super.setNull(false);
		this.value.append(value);
	}


	/**
	 *  Sets the value attribute of the StringDataField object
	 *
	 * @param  seq  The character sequence from which to set the value (either
	 *      String, StringBuffer or Char Buffer)
	 * @since       October 29, 2002
	 */
	public void setValue(CharSequence seq) {
		value.setLength(0);
		if (seq != null && seq.length() > 0) {
			for (int i = 0; i < seq.length(); i++) {
				value.append(seq.charAt(i));
			}
			setNull(false);
		} else {
			if (this.metadata.isNullable()) {
				super.setNull(true);
			} else {
				throw new BadDataFormatException(getMetadata().getName() + " field can not be set to null!(nullable=false)", null);
			}
			return;
		}
	}


	/**
	 *  Sets the Null value indicator
	 *
	 * @param  isNull  The new Null value
	 * @since          October 29, 2002
	 */
	public void setNull(boolean isNull) {
		super.setNull(isNull);
		if (isNull) {
			value.setLength(0);
		}
	}


	/**
	 *  Gets the Null value indicator
	 *
	 * @return    The Null value
	 * @since     October 29, 2002
	 */
	public boolean isNull() {
		return super.isNull();
	}


	/**
	 *  Gets the Value attribute of the StringDataField object
	 *
	 * @return    The Value value
	 * @since     April 23, 2002
	 */
	public Object getValue() {
		return (value.length() == 0 ? null : value);
	}


	/**
	 *  Gets the value of the StringDataField object as charSequence
	 *
	 * @return    The charSequence value
	 */
	public CharSequence getCharSequence() {
		return value;
	}


	/**
	 *  Gets the Type attribute of the StringDataField object
	 *
	 * @return    The Type value
	 * @since     April 23, 2002
	 */
	public char getType() {
		return DataFieldMetadata.STRING_FIELD;
	}


	/**
	 *  Description of the Method
	 *
	 * @param  dataBuffer                    Description of Parameter
	 * @param  decoder                       Description of Parameter
	 * @exception  CharacterCodingException  Description of Exception
	 * @since                                October 31, 2002
	 */
	public void fromByteBuffer(ByteBuffer dataBuffer, CharsetDecoder decoder) throws CharacterCodingException {
		fromString(decoder.decode(dataBuffer).toString());
	}


	/**
	 *  Description of the Method
	 *
	 * @param  dataBuffer                    Description of Parameter
	 * @param  encoder                       Description of Parameter
	 * @exception  CharacterCodingException  Description of Exception
	 * @since                                October 31, 2002
	 */
	public void toByteBuffer(ByteBuffer dataBuffer, CharsetEncoder encoder) throws CharacterCodingException {
		dataBuffer.put(encoder.encode(CharBuffer.wrap(toString())));
	}


	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Returned Value
	 * @since     April 23, 2002
	 */
	public String toString() {
		return value.toString();
	}


	/**
	 *  Description of the Method
	 *
	 * @param  value  Description of the Parameter
	 * @since         April 23, 2002
	 */
	public void fromString(String value) {
		setValue(value);
	}


	/**
	 *  Description of the Method
	 *
	 * @param  buffer  Description of Parameter
	 * @since          April 23, 2002
	 */
	public void serialize(ByteBuffer buffer) {
		int length = value.length();
		int counter = 0;
		buffer.putInt(length);
		while (counter < length) {
			buffer.putChar(value.charAt(counter++));
		}
		/*
		 *  byte byteArray[]=value.getBytes();
		 *  buffer.putInt(byteArray.length);
		 *  buffer.put(byteArray);
		 */
	}


	/**
	 *  Description of the Method
	 *
	 * @param  buffer  Description of Parameter
	 * @since          April 23, 2002
	 */
	public void deserialize(ByteBuffer buffer) {
		int length = buffer.getInt();
		//StringBuffer strBuf = new StringBuffer(length);
		int counter = 0;
		value.setLength(0);

		if (length == 0) {
			setNull(true);
		} else {
			while (counter < length) {
				value.append(buffer.getChar());
				counter++;
			}
			setNull(false);
		}
	}


	/**
	 *  Description of the Method
	 *
	 * @param  obj  Description of Parameter
	 * @return      Description of the Returned Value
	 * @since       April 23, 2002
	 */
	public boolean equals(Object obj) {
		// THIS DOES NOT WORK !!!! --->return value.toString().equals(((StringDataField) obj).getValue());
		StringBuffer strObj = (StringBuffer) ((StringDataField) obj).getValue();

		if (strObj == null || value.length() != strObj.length()) {
			return false;
		}
		for (int i = 0; i < value.length(); i++) {
			if (value.charAt(i) != strObj.charAt(i)) {
				return false;
			}
		}
		return true;
	}


	/**
	 *  Compares this object with the specified object for order.
	 *
	 * @param  obj  Description of the Parameter
	 * @return      Description of the Return Value
	 */
	public int compareTo(Object obj) {
		//THIS DOES NOT WORK !!!! -> return value.toString().compareTo(((StringDataField) obj).getValue());

		StringBuffer strObj = (StringBuffer) ((StringDataField) obj).getValue();
		if (strObj == null) {
			return 1;// ?? shall we raise an exception ??
		}

		int valueLenght = value.length();
		int strObjLenght = strObj.length();
		int compLength = (valueLenght < strObjLenght ? valueLenght : strObjLenght);
		for (int i = 0; i < compLength; i++) {
			if (value.charAt(i) > strObj.charAt(i)) {
				return 1;
			} else if (value.charAt(i) < strObj.charAt(i)) {
				return -1;
			}
		}
		// strings seem to be the same (so far), decide according to the length
		if (valueLenght == strObjLenght) {
			return 0;
		} else if (valueLenght > strObjLenght) {
			return 1;
		} else {
			return -1;
		}
	}

}
/*
 *  end class StringDataField
 */

