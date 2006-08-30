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
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;

import org.jetel.exception.BadDataFormatException;
import org.jetel.metadata.DataFieldMetadata;

/**
 *  A class that represents String type data field.<br>
 *  It can hold String of arbitrary length, however due
 *  to use of short value as length specifier when serializing/
 * deserializing value to/from ByteBuffer, the maximum length
 * is limited to 32762 characters (Short.MAX_VALUE);
 *
 * @author      D.Pavlis
 * @since       March 27, 2002
 * @revision    $Revision$
 * @created     January 26, 2003
 * @see         org.jetel.metadata.DataFieldMetadata
 */
public class StringDataField extends DataField implements CharSequence{

	private StringBuffer value;
	
	// Attributes
	/**
	 *  An attribute that represents ...
	 *
	 * @since
	 */
	private final static int INITIAL_STRING_BUFFER_CAPACITY = 32;
	private final static int STRING_LENGTH_INDICATOR_SIZE = 2; // sizeof(short)

	private static final int SIZE_OF_CHAR = 2;

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
	 * Private constructor used internally when clonning
	 * 
	 * @param _metadata
	 * @param _value
	 */
	private StringDataField(DataFieldMetadata _metadata, StringBuffer _value){
	    super(_metadata);
	    this.value=new StringBuffer(_value.length());
	    this.value.append(_value);
	}
	
	

	/* (non-Javadoc)
	 * @see org.jetel.data.DataField#copy()
	 */
	public DataField duplicate(){
	    StringDataField newField=new StringDataField(metadata,value);
	    newField.setNull(this.isNull());
	    return newField;
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.data.DataField#copyField(org.jetel.data.DataField)
	 */
	public void copyFrom(DataField fieldFrom){
	    if (fieldFrom instanceof StringDataField ){
	        if (!fieldFrom.isNull){
	            this.value.setLength(0);
	            this.value.append(((StringDataField)fieldFrom).value);
	        }
	        setNull(fieldFrom.isNull);
	    }else{
	        throw new ClassCastException("Incompatible DataField type "+DataFieldMetadata.type2Str(fieldFrom.getType()));
	    }
	}
	
	/**
	 *  Sets the Value attribute of the StringDataField object
	 *
	 * @param  value                       The new value to set. Valid types are char[] or CharSequence descendant, or <code>null</code>
	 * @exception  BadDataFormatException  When <code>null</code> value was set, but metadata definition requires field not-nullability
     * @exception  IllegalArgumentException When value of types other then char[], CharSequence descendant is passed to the function <code>value</code>
	 * @since                              April 23, 2002
	 */
	public void setValue(Object value) throws BadDataFormatException {
        if(value instanceof CharSequence || value == null) {
            setValue((CharSequence) value);
        } else if (value instanceof char[]) {
            setValue(new String((char[]) value));
        } else {
            throw new IllegalArgumentException(getMetadata().getName() + " field can not be set from class " + value.getClass().getName());
        }
	}


	/**
	 *  Sets the value attribute of the StringDataField object
	 *
	 * @param  seq  The character sequence from which to set the value (either
	 *      String, StringBuffer or Char Buffer)
	 * @since       October 29, 2002
	 */
	void setValue(CharSequence seq) {
		value.setLength(0);
		if (seq != null && seq.length() > 0) {
		    value.append(seq);
			setNull(false);
		} else {
		    setNull(true);
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
		if (this.isNull) {
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
	    return (isNull  ? null : value);
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
		dataBuffer.put(encoder.encode(CharBuffer.wrap(value)));
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
	    int chars = length;
	    
	    do {
	    	buffer.put((byte)(0x80 | (byte) length));
            length = length >> 7;
	    } while((length >> 7) > 0);
    	buffer.put((byte) length);
	    
		for(int counter = 0; counter < chars; counter++) {
			buffer.putChar(value.charAt(counter));
		}
	}


	/**
	 *  Description of the Method
	 *
	 * @param  buffer  Description of Parameter
	 * @since          April 23, 2002
	 */
	public void deserialize(ByteBuffer buffer) {
	    int length; 
	    int size;
	    length = size = 0;
	    int count = 0;
	    
	    do {
	       size = buffer.get();
	       length = length | ((size & 0x7F) << (7 * count++));
	    } while(size < 0);
	   
		// empty value - so we can store new string
		value.setLength(0);

		if (length == 0) {
			setNull(true);
		} else {
		    for(int counter = 0; counter < length; counter++){
		        value.append(buffer.getChar());
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
	    if (isNull || obj==null ) return false;
	    if (this==obj) return true;
		CharSequence data;
		
		if (obj instanceof StringDataField){
		    if (((StringDataField)obj).isNull()) return false;
			data = (CharSequence)((StringDataField) obj).getValue();
		}else if (obj instanceof CharSequence){
			data = (CharSequence)obj;
		}else{
	        return false;
		}
		
		if (value.length() != data.length()) {
			return false;
		}
		for (int i = 0; i < value.length(); i++) {
			if (value.charAt(i) != data.charAt(i)) {
				return false;
			}
		}
		return true;
	}


	/**
	 *  Compares this object with the specified object for order.
	 *
	 * @param  obj  Any object implementing CharSequence interface
	 * @return      Description -1;0;1 based on comparison result
	 */
	public int compareTo(Object obj) {
	    CharSequence strObj;
	    
		if (isNull) return -1;

        if (obj instanceof StringDataField) {
            if (((StringDataField) obj).isNull())
                return 1;
            strObj=((StringDataField) obj).value;
        }else if (obj instanceof CharSequence) {
            strObj = (CharSequence) obj;
        }else {
            throw new ClassCastException("Can't compare StringDataField to "
                    + obj.getClass().getName());
        }

        int valueLenght = value.length();
        int strObjLenght = strObj.length();
        int compLength = (valueLenght < strObjLenght ? valueLenght
                : strObjLenght);
        for (int i = 0; i < compLength; i++) {
            if (value.charAt(i) > strObj.charAt(i)) {
                return 1;
            } else if (value.charAt(i) < strObj.charAt(i)) {
                return -1;
            }
        }
        // strings seem to be the same (so far), decide according to the
        // length
        if (valueLenght == strObjLenght) {
            return 0;
        } else if (valueLenght > strObjLenght) {
            return 1;
        } else {
            return -1;
        }
        
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	public int hashCode(){
		int hash=5381;
		for (int i=0;i<value.length();i++){
			hash = ((hash << 5) + hash) + value.charAt(i); 
		}
		return (hash & 0x7FFFFFFF);
	}
	
	/**
	 *  Returns how many bytes will be occupied when this field with current
	 *  value is serialized into ByteBuffer
	 *
	 * @return    The size value
	 * @see	      org.jetel.data.DataField
	 */
	public int getSizeSerialized() {
		// lentgh in characters multiplied of 2 (each char occupies 2 bytes in UNICODE) plus
		// size of length indicator (basically int variable)
	    int length=value.length();
	    int count=0;
	    
	    do {
	    	count++;
            length = length >> 7;
	    } while((length >> 7) > 0);
    	count++;

		return value.length()*SIZE_OF_CHAR+count;
	}
	
	/**
	 * Method which implements charAt method of CharSequence interface
	 * 
	 * @param position
	 * @return
	 */
	public char charAt(int position){
		return value.charAt(position);
	}
	
	/**Method which implements length method of CharSequence interfaceMethod which ...
	 * 
	 * @return
	 */
	public int length(){
		return value.length();
	}
	
	/**
	 * Method which implements subSequence method of CharSequence interface
	 * 
	 * @param start
	 * @param end
	 * @return
	 */
	public CharSequence subSequence(int start, int end){
		return value.subSequence(start,end);
	}
}
/*
 *  end class StringDataField
 */

