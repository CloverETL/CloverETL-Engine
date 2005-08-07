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
public class StringDataField extends DataField implements CharSequence, Comparable{

	private CharBuffer value;
	
	// Attributes
	/**
	 *  An attribute that represents ...
	 *
	 * @since
	 */
	private final static int INITIAL_STRING_BUFFER_CAPACITY = 32;
	private final static int STRING_LENGTH_INDICATOR_SIZE = 2; // sizeof(short)

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
			value = CharBuffer.allocate(INITIAL_STRING_BUFFER_CAPACITY);
		} else {
		    allocateSpace(_metadata.getSize());
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

	public StringDataField(DataFieldMetadata _metadata, CharSequence _value) {
		this(_metadata);
		setValue(_value);
	}
	

	/**
	 * Private constructor used internally when clonning
	 * 
	 * @param _metadata
	 * @param _value
	 */
	private StringDataField(DataFieldMetadata _metadata, CharBuffer _value){
	    super(_metadata);
	    allocateSpace(_value.length());
	    this.value.put(_value);
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
	            setValue(((StringDataField)fieldFrom).value);
	        }
	        setNull(fieldFrom.isNull);
	    }else{
	        throw new ClassCastException("Incompatible DataField type "+DataFieldMetadata.type2Str(fieldFrom.getType()));
	    }
	}
	
	/**
	 *  Sets the Value attribute of the StringDataField object
	 *
	 * @param  value                       The new value value
	 * @exception  BadDataFormatException  Description of the Exception
	 * @since                              April 23, 2002
	 */
	public void setValue(Object value) throws BadDataFormatException {
		if (value instanceof StringDataField){
		    setValue((CharBuffer)((StringDataField)value).getValue());
		}else if (value instanceof String){
		    fromString((String)value);
		}else if (value instanceof StringBuffer){
		    fromStringBuffer((StringBuffer)value);
		}else if (value instanceof char[]){
		    fromCharArray((char[])value);
		}else if (value==null){
		    if (this.metadata.isNullable()) {
				super.setNull(true);
			} else {
				throw new BadDataFormatException(getMetadata().getName() + " field can not be set to null!(nullable=false)", null);
			}
			return;
		}
		super.setNull(false);
	}

	
	public void setValue(CharBuffer value){
	    if (value.length()>0){
	    allocateSpace(value.length());
	    this.value.put(value);
	    this.value.flip();
	    this.setNull(false);
	    }else{
	        if (this.metadata.isNullable()) {
				super.setNull(true);
			} else {
				throw new BadDataFormatException(getMetadata().getName() + " field can not be set to null!(nullable=false)", null);
			}
	    }
	}

	/**
	 *  Sets the value attribute of the StringDataField object
	 *
	 * @param  seq  The character sequence from which to set the value (either
	 *      String, StringBuffer or Char Buffer)
	 * @since       October 29, 2002
	 */
	public void setValue(CharSequence seq) {
		if (seq != null && seq.length() > 0) {
		    allocateSpace(seq.length());
		    for (int i=0;i<seq.length();this.value.put(seq.charAt(i++)));
		    this.value.flip();
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
	 * Ensures that the internal character buffer has enough capacity
	 * 
	 * @param size  size of the new char buffer (in num of characters)
	 */
	private void allocateSpace(int size){
	    if ((this.value==null)||(this.value.capacity()<size)){
	        this.value=CharBuffer.allocate(((size/4)+1)*4);
	    }
	    this.value.clear();
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
			value.clear();
		}
	}


	
	/**
	 *  Gets the Value attribute of the StringDataField object
	 *
	 * @return    The Value value
	 * @since     April 23, 2002
	 */
	public Object getValue() {
		return (isNull ? null : this.value.rewind());
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
		this.value.clear();
		this.value.put(decoder.decode(dataBuffer));
		this.value.flip();
		if (this.value.length()>0){
		    setNull(false);
		}else{
		    setNull(true);
		}
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
		this.value.rewind();
	    dataBuffer.put(encoder.encode(this.value));
	}


	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Returned Value
	 * @since     April 23, 2002
	 */
	public String toString() {
	    if (isNull()){
	        return null;
	    }else{
	        value.rewind();
	        return value.toString();
	    }
	}


	/**
	 *  Description of the Method
	 *
	 * @param  value  Description of the Parameter
	 * @since         April 23, 2002
	 */
	public void fromString(String value) {
		if (value==null || value.length()==0){
		    if (this.metadata.isNullable()) {
				super.setNull(true);
			} else {
				throw new BadDataFormatException(getMetadata().getName() + " field can not be set to null!(nullable=false)", null);
			}
		}else{
		    allocateSpace(value.length());
		    this.value.put(value);
		    this.value.flip();
		    setNull(false);
		}
	}
	
	public void fromCharArray(char[] value) {
		if (value==null || value.length==0){
		    if (this.metadata.isNullable()) {
				super.setNull(true);
			} else {
				throw new BadDataFormatException(getMetadata().getName() + " field can not be set to null!(nullable=false)", null);
			}
		}else{
		    allocateSpace(value.length);
		    this.value.put(value);
		    this.value.flip();
		    setNull(false);
		}
	}

	private void fromStringBuffer(StringBuffer value){
	    if (value==null || value.length()==0){
		    if (this.metadata.isNullable()) {
				super.setNull(true);
			} else {
				throw new BadDataFormatException(getMetadata().getName() + " field can not be set to null!(nullable=false)", null);
			}
		}else{
		    allocateSpace(value.length());
		    for (int i=0;i<value.length();this.value.put(value.charAt(i++)));
		    this.value.flip();
		    setNull(false);
		}
	}
	

	/**
	 *  Description of the Method
	 *
	 * @param  buffer  Description of Parameter
	 * @since          April 23, 2002
	 */
	public void serialize(ByteBuffer buffer) {
		value.rewind();
	    int length=value.remaining();
		buffer.putShort((short)length);
		length=length*2+buffer.position(); // the new limit
		buffer.asCharBuffer().put(this.value);
		buffer.position(length);
	}


	/**
	 *  Description of the Method
	 *
	 * @param  buffer  Description of Parameter
	 * @since          April 23, 2002
	 */
	public void deserialize(ByteBuffer buffer) {
		int length = buffer.getShort();
		if (length == 0) {
		    setNull(true);
		} else {
		    final int savedLimit=buffer.limit();
		    buffer.limit(buffer.position()+length*2);
		    allocateSpace(length);
			value.put(buffer.asCharBuffer());
			buffer.position(buffer.position()+length*2);
			buffer.limit(savedLimit);
			value.flip();
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
	    if (obj==null || isNull) return false;
	    
	    this.value.rewind();
	    
	    if (obj instanceof StringDataField){
	        if (((StringDataField)obj).isNull()) return false;
	        return this.value.equals(((StringDataField)obj).value);
	    }else if (obj instanceof CharSequence){
	        CharSequence data;
	        data = (CharSequence)obj;
	        if (value.length() != data.length()) return false;
	        for (int i = 0; i < value.length(); i++) {
	            if (value.charAt(i) != data.charAt(i)) {
	                return false;
	            }
	        }
	        return true;
	    }
	    return false;
	}


	/**
	 *  Compares this object with the specified object for order.
	 *
	 * @param  obj  Any object implementing CharSequence interface
	 * @return      Description -1;0;1 based on comparison result
	 */
	public int compareTo(Object obj) {
		if (obj==null) return 1;
		if (isNull) return -1;

        // we are going to calculate something, so rewind
        this.value.rewind();

        if (obj instanceof StringDataField) {
            if (((StringDataField) obj).isNull())
                return 1;
            return this.value.compareTo(((StringDataField) obj).value);
        } else if (obj instanceof CharSequence) {
            CharSequence strObj = (CharSequence) obj;

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
        } else {
            throw new ClassCastException("Can't compare StringDataField to "
                    + obj.getClass().getName());
        }

    }

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	public int hashCode(){
	    value.rewind();
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
	    value.rewind();
		return value.length()*2+STRING_LENGTH_INDICATOR_SIZE;
	}
	
	/**
	 * Method which implements charAt method of CharSequence interface
	 * 
	 * @param position
	 * @return
	 */
	public char charAt(int position){
	    value.rewind();
		return value.charAt(position);
	}
	
	/**Method which implements length method of CharSequence interfaceMethod which ...
	 * 
	 * @return
	 */
	public int length(){
	    value.rewind();
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

