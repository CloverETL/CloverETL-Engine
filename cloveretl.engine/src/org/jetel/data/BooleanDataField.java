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
// FILE: c:/projects/jetel/org/jetel/data/DateDataField.java

package org.jetel.data;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;

import org.jetel.exception.BadDataFormatException;
import org.jetel.metadata.DataFieldMetadata;

/**
 * Instance of this class represents boolean value. 
 * 
 * @author Martin Varecha <martin.varecha@javlinconsulting.cz>
 * (c) JavlinConsulting s.r.o.
 * www.javlinconsulting.cz
 * @created Nov 20, 2007
 * @since Nov 20, 2007
 */
public class BooleanDataField extends DataField implements Comparable{

	private boolean value;
	// standard size of field in serialized form
	private final static int FIELD_SIZE_BYTES = 1;

    /**
     *  Constructor for the BooleanDataField object
     *
     * @param  _metadata  Metadata describing field
     * @since Nov 20, 2007
     */
    public BooleanDataField(DataFieldMetadata _metadata){
        this(_metadata,false);
    }

	/**
	 * Private constructor to be used internally when clonning object.
	 * @param _metadata
	 * @param _value
	 */
	private BooleanDataField(DataFieldMetadata _metadata, boolean _value){
	    super(_metadata);
	    setValue(_value);
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.data.DataField#copy()
	 */
	public DataField duplicate(){
	    BooleanDataField newField=new BooleanDataField(metadata,value);
	    newField.setNull(isNull());
	    return newField;
	}
	
	/**
	 * @see org.jetel.data.DataField#copyField(org.jetel.data.DataField)
     * @deprecated use setValue(DataField) instead
	 */
	public void copyFrom(DataField fromField){
	    if (fromField instanceof BooleanDataField){
       		this.value = ((BooleanDataField)fromField).value;
	        setNull(fromField.isNull);
	    } else {
	        super.copyFrom(fromField);
        }
	}
	
	/**
	 *  Sets the boolean value.
	 *
	 * @param  _value                      The new Value value
	 * @exception  BadDataFormatException  Description of the Exception
	 */
	public void setValue(Object _value) throws BadDataFormatException {
        if(_value == null) {
            setNull(true);
        } else if(_value instanceof Boolean) {
			value = (Boolean)_value;
			setNull(false);
        } else if(_value instanceof String) {
			value = Boolean.valueOf( (String)_value );
			setNull(false);
		}else {
		    throw new BadDataFormatException(getMetadata().getName() + " field can not be set with this object - " + _value.toString(), _value.toString());
		}
	}
	
    /* (non-Javadoc)
     * @see org.jetel.data.DataField#setValue(org.jetel.data.DataField)
     */
    @Override
    public void setValue(DataField fromField) {
        if (fromField instanceof BooleanDataField){
        	this.value = ((BooleanDataField)fromField).value;
            setNull(fromField.isNull);
        } else {
            super.setValue(fromField);
        }
    }

	/**
	 *  Gets the date represented by DateDataField object
	 *
	 * @return    The Value value
	 * @since     April 23, 2002
	 */
	public Object getValue() {
		return isNull ? null : Boolean.valueOf(value);
	}

    /**
     * @see org.jetel.data.DataField#getValueDuplicate()
     */
    public Object getValueDuplicate() {
        return getValue();
    }

	/**
	 *  Gets the boolean attribute of the BooleanDataField object
	 *
	 * @return    The boolean value
	 */
	public boolean getBoolean() {
		return value;
	}

	/**
	 *  Sets the Null value indicator
	 *
	 * @param  isNull  The new Null value
	 */
	public void setNull(boolean isNull) {
		super.setNull(isNull);
		if (this.isNull) {
			value = Boolean.FALSE;
		}
	}
    
     /* (non-Javadoc)
     * @see org.jetel.data.DataField#reset()
     */
    public void reset(){
            if (metadata.isNullable()){
                setNull(true);
            }else if (metadata.isDefaultValue()){
                setToDefaultValue();
            }else{
            	throw new IllegalStateException(getMetadata().getName() + " Field cannot be null and default value is not defined");
            }
    }

	/**
	 *  Gets the DataField type
	 *
	 * @return    The Type value
	 */
	public char getType() {
		return DataFieldMetadata.BOOLEAN_FIELD;
	}

	/**
	 * Returns value in String format.
	 * @return    
	 */
	public String toString() {
		if (isNull()) {
			return "";
		}
		return Boolean.toString(value);
	}

	/**
	 * Deserializes value from byteBuffer into this instance.
	 *
	 * @param  dataBuffer                    
	 * @param  decoder                       
	 * @exception  CharacterCodingException  
	 */
	public void fromByteBuffer(ByteBuffer dataBuffer, CharsetDecoder decoder) throws CharacterCodingException {
		fromString(decoder.decode(dataBuffer));
	}

	/**
	 * Serializes value from byteBuffer into this instance.
	 *
	 * @param  dataBuffer                    
	 * @param  encoder                       
	 * @exception  CharacterCodingException  
	 */
	public void toByteBuffer(ByteBuffer dataBuffer, CharsetEncoder encoder) throws CharacterCodingException {
		try {
			dataBuffer.put(encoder.encode(CharBuffer.wrap(toString())));
		} catch (BufferOverflowException e) {
			throw new RuntimeException("The size of data buffer is only " + dataBuffer.limit() + ". Set appropriate parameter in defautProperties file.", e);
		}
	}

    @Override
    public void toByteBuffer(ByteBuffer dataBuffer) {
        if(!isNull) {
        	try {
        		dataBuffer.put( value ? (byte)1 : (byte)0 ); // dataBuffer accepts only bytes
        	} catch (BufferOverflowException e) {
    			throw new RuntimeException("The size of data buffer is only " + dataBuffer.limit() + ". Set appropriate parameter in defautProperties file.", e);
        	}
        }
    }


	/* (non-Javadoc)
	 * @see org.jetel.data.DataField#fromString(java.lang.CharSequence)
	 */
	public void fromString(CharSequence seq) {
		//parsePosition.setIndex(0);
		if (seq == null || seq.length() == 0) {
		    setNull(true);
			return;
		}
		value = Boolean.valueOf( seq.toString() );
		setNull(false);
	}

	/**
	 *  Performs serialization of the internal value into ByteBuffer (used when
	 *  moving data records between components).
	 *
	 * @param  buffer  
	 */
	public void serialize(ByteBuffer buffer) {
		try {
       		buffer.put( value ? (byte)1 : (byte)0 ); // dataBuffer accepts only bytes
		} catch (BufferOverflowException e) {
			throw new RuntimeException("The size of data buffer is only " + buffer.limit() + ". Set appropriate parameter in defautProperties file.", e);
		}
	}


	/**
	 *  Performs deserialization of data
	 *
	 * @param  buffer
	 */
	public void deserialize(ByteBuffer buffer) {
		byte tmpl = buffer.get();
		if (tmpl == 0) {
			setNull(true);
			return;
		}
		value = (tmpl == (byte)1);
		setNull(false);
	}


	/**
	 *  Description of the Method
	 *
	 * @param  obj  
	 * @return      
	 */
	public boolean equals(Object obj) {
	    if (isNull || obj==null) return false;
	    
	    if (obj instanceof BooleanDataField){
	        return ( this.value == ((BooleanDataField) obj).value );
	    } else if (obj instanceof Boolean){
	        return this.value == ((Boolean)obj).booleanValue() ;
	    }else{
	        return false;
	    }
	}

	/**
	 *  Compares this object with the specified object for order
	 *
	 * @param  obj  
	 * @return      
	 */
	public int compareTo(Object obj) {
		if (isNull) return -1;
	    
		if (obj instanceof Boolean){
			Boolean v = Boolean.valueOf(value);
			return v.compareTo((Boolean) obj);
		}else if (obj instanceof BooleanDataField){
			if (!((BooleanDataField) obj).isNull()) {
				Boolean v = Boolean.valueOf(value);
				return v.compareTo(((BooleanDataField) obj).value);
			}else{
				return 1;
			}
		}else throw new ClassCastException("Can't compare DateDataField and "+obj.getClass().getName());
	}

	public int hashCode(){
		if (isNull) return 123;
		Boolean v = Boolean.valueOf(value);
		return v.hashCode();
	}
	
	/**
	 *  Gets the size attribute of the IntegerDataField object
	 *
	 * @return    The size value
	 * @see	      org.jetel.data.DataField
	 */
	public int getSizeSerialized() {
		return FIELD_SIZE_BYTES;
	}

}
/*
 *  end class BooleanDataField
 */

