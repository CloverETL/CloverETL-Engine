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
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.util.Arrays;

import org.jetel.exception.BadDataFormatException;
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
public class ByteDataField extends DataField implements Comparable{

	// Attributes

	/**
	 *  Description of the Field
	 *
	 *@since    October 29, 2002
	 */
	protected byte[] value;
	private static final int ARRAY_LENGTH_INDICATOR_SIZE = Integer.SIZE / 8;
	
	private final static int INITIAL_BYTE_ARRAY_CAPACITY = 8;
	


	/**
	 *  Constructor for the NumericDataField object
	 *
	 *@param  _metadata  Metadata describing field
	 *@since             October 29, 2002
	 */
    public ByteDataField(DataFieldMetadata _metadata){
        this(_metadata, false);
    }
    
	/**
     * Constructor for the NumericDataField object
     * 
	 * @param _metadata Metadata describing field
	 * @param plain <i>not used (only for compatibility reason)</i>
	 */
	public ByteDataField(DataFieldMetadata _metadata, boolean plain) {
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
		setValue(value);
	}


	/* (non-Javadoc)
	 * @see org.jetel.data.DataField#copy()
	 */
	public DataField duplicate(){
	    return new ByteDataField(metadata, value);
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.DataField#copyField(org.jetel.data.DataField)
	 */
	public void copyFrom(DataField fromField){
	    if (fromField instanceof ByteDataField){
	        if (!fromField.isNull){
	            int length = ((ByteDataField) fromField).value.length;
	            if (this.value.length != length){
	                this.value = new byte[length];
	            }
	            System.arraycopy(this.value, 0, ((ByteDataField) fromField).value, 0, length);
	        }
	        setNull(fromField.isNull);
	    }
	}
	
    @Override
    public void setNull(boolean isNull) {
        super.setNull(isNull);
        if (this.isNull) {
            value = null;
        }
    }
    
	/**
	 *  Sets the value of the field - accepts byte[], Byte, Byte[]
	 *
	 *@param  _value  The new value
	 *@since          October 29, 2002
	 */
	public void setValue(Object value) {
        if(value == null) {
		    setNull(true);
        }else if (value instanceof byte[]){
            setValue((byte[])value);
        }else if(value instanceof Byte) {
            setValue(((Byte) value).byteValue());
        } else if(value instanceof Byte[]) {
            //convert Byte[] into byte[]
            Byte[] valueByte = (Byte[]) value;
            byte[] result = new byte[valueByte.length];
            int i = 0;
            for(Byte b : valueByte) {
                result[i++] = b.byteValue();
            }
            setValue(result);
		} else {
		    throw new BadDataFormatException("Not a byte/byte_array " + value.getClass().getName());
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
	    this.value = value;
		setNull(value == null);
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

     /* (non-Javadoc)
     * @see org.jetel.data.DataField#reset()
     */
    public void reset(){
         if (metadata.isNullable()){
             setNull(true);
         }else if (metadata.isDefaultValue()){
             setToDefaultValue();
         }else{
             setValue(0);
         }
     }    

	// Associations

	// Operations

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
	 *  Gets the value represented by this object (as byte[] object)
	 *
	 *@return    The Value value
	 *@since     October 29, 2002
	 */
	public Object getValue() {
		return isNull ? null : value;
	}

    /**
     * @see org.jetel.data.DataField#getValueDuplicate()
     */
    public Object getValueDuplicate() {
        if(isNull) {
            return null;
        }
        byte[] ret = new byte[value.length];
        System.arraycopy(value, 0, ret, 0, value.length);
        return ret;
    }

	/**
	 *  Gets the byte value represented by this object as byte primitive
	 *
	 *@param  position  offset in byte array
	 *@return           The Byte value
	 *@since            October 29, 2002
	 */
	public byte getByte(int position) {
        if(isNull) {
            return 0;
        }
		return value[position];
	}


	/**
	 *  Gets the Byte array value of the ByteDataField object
	 *
	 *@return    The Byte[] value
	 *@since     October 29, 2002
	 */
	public byte[] getByteArray() {
		return value;
	}


	/**
	 *  Formats internal byte array value into string representation
	 *
	 *@return    String representation of byte array
	 *@since     October 29, 2002
	 */
	public String toString() {
		return new String(value);
	}

    
    
   
    /**
     * Formats internal byte array value into string representation
     * 
     * @param charset charset to be used for converting bytes into String
     * @return String representation of byte array
     * @since 19.11.2006
     */
    public String toString(String charset) {
        try{
            return new String(value,charset);
        }catch(UnsupportedEncodingException ex){
            throw new RuntimeException(ex.toString()+" when calling toString() on field \""+
                    this.metadata.getName()+"\"",ex);
        }
    }

	/**
	 *  Parses byte array value from string (convers characters in string into byte
	 *  array using system's default charset encoder)
	 *
	 *@param  valueStr  value
	 *@since            October 29, 2002
	 */
	public void fromString(String valueStr) {
        if(valueStr == null || valueStr.length() == 0) {
            setNull(true);
            return;
        }
        this.value = valueStr.getBytes();
        setNull(false);
	}

    /**
     * Parses byte array value from string (convers characters in string into byte
     *  array using specified charset encoder)
     * 
     * @param valueStr  value
     * @param charset charset to be used for encoding String into bytes
     * @since 19.11.2006
     */
    public void fromString(String valueStr,String charset){
        if(valueStr == null || valueStr.length() == 0) {
            setNull(true);
            return;
        }
        try{
            this.value = valueStr.getBytes(charset);
        }catch(UnsupportedEncodingException ex){
            throw new RuntimeException(ex.toString()+" when calling fromString() on field \""+
                    this.metadata.getName()+"\"",ex);
        }
        setNull(false);
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
        if(!isNull) {
            dataBuffer.put(value);
        }
	}

    @Override
    public void toByteBuffer(ByteBuffer dataBuffer) {
        if(!isNull) {
            dataBuffer.put(value);
        }
    }

	/**
	 *  Performs serialization of the internal value into ByteBuffer (used when
	 *  moving data records between components).
	 *
	 *@param  buffer  Description of Parameter
	 *@since          October 29, 2002
	 */
	public void serialize(ByteBuffer buffer) {
        if(isNull) {
            buffer.putInt(0);
        } else {
            buffer.putInt(value.length);
            buffer.put(value);
        }
	}


	/**
	 *  Performs deserialization of data
	 *
	 *@param  buffer  Description of Parameter
	 *@since          October 29, 2002
	 */
	public void deserialize(ByteBuffer buffer) {
		int length = buffer.getInt();
        
        if(length == 0) {
            setNull(true);
        } else {
            if(value == null || length != value.length) {
                value = new byte[length];
            }
            buffer.get(value);
            setNull(false);
        }
	}


	/**
	 *  Description of the Method
	 *
	 *@param  obj  Description of Parameter
	 *@return      Description of the Returned Value
	 *@since       October 29, 2002
	 */
	public boolean equals(Object obj) {
	    if (isNull || obj==null) return false;
	    
		if (obj instanceof ByteDataField){
			return Arrays.equals(this.value, ((ByteDataField) obj).value);
		}else if (obj instanceof byte[]){
			return Arrays.equals(this.value, (byte[])obj);
		}else {
		    return false;
		}
	}


	/**
	 *  Compares this object with the specified object for order.
	 *
	 *@param  obj  Description of the Parameter
	 *@return      Description of the Return Value
	 */
	public int compareTo(Object obj) {
		byte[] byteObj;
		if (isNull) return -1;
		
		if (obj instanceof ByteDataField){
			byteObj = ((ByteDataField) obj).value;
		}else if (obj instanceof byte[]){
			byteObj= (byte[])obj;
		}else {
		    throw new ClassCastException("Can't compare ByteDataField and "+obj.getClass().getName());
		}
		 
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
	
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	public int hashCode(){
		// return Arrays.hashCode(this.value); works since j2se 1.5
	    /* changed by D.Pavlis 07-Aug-2005 */
	    int hash=5381;
		for (int i=0;i<value.length;i++){
			hash = ((hash << 5) + hash) + value[i]; 
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
        if(isNull) {
            return ARRAY_LENGTH_INDICATOR_SIZE;
        } else {
            return value.length + ARRAY_LENGTH_INDICATOR_SIZE;
        }
	}

}
/*
 *  end class NumericDataField
 */

