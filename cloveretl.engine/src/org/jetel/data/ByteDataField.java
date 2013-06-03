/*
 * jETeL/CloverETL - Java based ETL application framework.
 * Copyright (c) Javlin, a.s. (info@cloveretl.com)
 *  
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.jetel.data;

import java.io.UnsupportedEncodingException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.util.Arrays;

import org.jetel.exception.BadDataFormatException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.util.HashCodeUtil;
import org.jetel.util.bytes.ByteBufferUtils;
import org.jetel.util.bytes.CloverBuffer;
import org.jetel.util.string.Compare;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;

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
@SuppressWarnings("EI")
public class ByteDataField extends DataField implements Comparable<Object> {

	private static final long serialVersionUID = 3823545028385612760L;

	/**
	 *  Description of the Field
	 *
	 *@since    October 29, 2002
	 */
	protected byte[] value;

	protected final static int INITIAL_BYTE_ARRAY_CAPACITY = 8;

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
        reset();
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

	private void prepareBuf() {
		if (this.value == null) {
			int len = metadata.getSize();
			this.value = new byte[len > 0 ? len : INITIAL_BYTE_ARRAY_CAPACITY];
		}
	}

	private void prepareBuf(CloverBuffer newValue) {		
		this.value = new byte[newValue.remaining()];
	}

	@Override
	public DataField duplicate(){
	    return new ByteDataField(metadata, value);
	}

	/**
	 * @see org.jetel.data.DataField#copyField(org.jetel.data.DataField)
     * @deprecated use setValue(DataField) instead
	 */
	@Override
	public void copyFrom(DataField fromField){
	    if (fromField instanceof ByteDataField && !(fromField instanceof CompressedByteDataField) && !(this instanceof CompressedByteDataField)){
	        if (!fromField.isNull){
	            int length = ((ByteDataField) fromField).value.length;
	            if (this.value == null || this.value.length != length){
	                this.value = new byte[length];
	            }
                System.arraycopy(((ByteDataField) fromField).value,
                        0, this.value, 0, length);
	        }
	        setNull(fromField.isNull);
	    } else {
	        super.copyFrom(fromField);
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
	@Override
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
		}else {
			BadDataFormatException ex = new BadDataFormatException("Not a byte/byte_array " + value.getClass().getName());
        	ex.setFieldNumber(getMetadata().getNumber());
        	throw ex;
		}
	}

	@Override
	public void setValue(DataField fromField) {
        if (fromField instanceof ByteDataField && !(fromField instanceof CompressedByteDataField) && !(this instanceof CompressedByteDataField)){
            if (!fromField.isNull){
                int length = ((ByteDataField) fromField).value.length;
                if (this.value == null || this.value.length != length){
                    this.value = new byte[length];
                }
                System.arraycopy(((ByteDataField) fromField).value,
                        0, this.value, 0, length);
            }
            setNull(fromField.isNull);
        } else {
            super.setValue(fromField);
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
        if(value != null) {
            this.value = new byte[value.length];
            System.arraycopy(value, 0, this.value, 0, value.length);
            setNull(false);
        } else {
            setNull(true);
        }
	}

	/**
	 * Sets subarray of a byte array as value of this field. 
	 * @param value byte array whose subarray will be set as value of this field
	 * @param fromOffset position of first byte of the subarray
	 * @param length length of the subarray
	 * @since November 2012
	 */
	public void setValue(byte[] value, int fromOffset, int length) {
		if (value != null) {
			this.value = new byte[length];
			System.arraycopy(value, fromOffset, this.value, 0, length);
			setNull(false);
		} else {
			setNull(true);
		}
	}

	/**
	 *  Sets the value of the field
	 *
	 *@param  value  The new byte value - the whole byte array is filled with this
	 *      value
	 *@since         October 29, 2002
	 */
	public void setValue(byte value) {
	    prepareBuf();
	    Arrays.fill(this.value, value);
		setNull(false);
	}

	/**
	 *  Sets the value of the field
	 *
	 *@param  value  value is copied into internal byte array using
	 */
	public void setValue(ByteBuffer value) {
        if(value != null) {
            this.value = new byte[value.remaining()];
            value.get(this.value);
            setNull(false);
        } else {
            setNull(true);
        }
	}

	
	
    @Override
	public void reset(){
         if (metadata.isNullable()){
             setNull(true);
         }else if (metadata.isDefaultValueSet()){
             setToDefaultValue();
         }else{
             value = null;
         }
     }    

	/**
	 *  Gets the Field Type
	 *
	 *@return    The Type value
	 *@since     October 29, 2002
	 */
	@Override
	@Deprecated
	public char getType() {
		return DataFieldMetadata.BYTE_FIELD;
	}


	/**
	 *  Gets the value represented by this object (as byte[] object)
	 *
	 *@return    The Value value
	 *@since     October 29, 2002
	 */
	@Override
	public byte[] getValue() {
		return isNull ? null : getByteArray();
	}

    /**
     * @see org.jetel.data.DataField#getValueDuplicate()
     */
    @Override
	public byte[] getValueDuplicate() {
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
		return getByteArray()[position];
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
    @Override
	public String toString() {
        return toString(Defaults.DataFormatter.DEFAULT_CHARSET_ENCODER);
    }

    
    
   
    /**
     * Formats internal byte array value into string representation
     * 
     * @param charset charset to be used for converting bytes into String
     * @return String representation of byte array
     * @since 19.11.2006
     */
    public String toString(String charset) {
        if (isNull()) {
			return metadata.getNullValue();
        }
        if (value == null) {
        	return "";
        }
        try{
            return new String(getByteArray(),charset);
        }catch(UnsupportedEncodingException ex){
            throw new RuntimeException(ex.toString()+" when calling toString() on field \""+
                    this.metadata.getName()+"\"",ex);
        }
    }

	@Override
	public void fromString(CharSequence seq) {
		fromString(seq, Defaults.DataFormatter.DEFAULT_CHARSET_ENCODER);
	}

    /**
     * Parses byte array value from string (converts characters in string into byte
     * array using specified charset encoder)
     *
	 * @param seq
     * @param charset charset to be used for encoding String into bytes
	 * @since            11.12.2006
     */
	public void fromString(CharSequence seq, String charset) {
		if (seq == null || Compare.equals(seq, metadata.getNullValue())) {
			setNull(true);
			return;
		}

		try {
			this.value = seq.toString().getBytes(charset);
		} catch (UnsupportedEncodingException ex) {
			throw new RuntimeException(ex.toString() + " when calling fromString() on field \""
					+ this.metadata.getName() + "\" (" + getMetadata().getDataType().getName() + ") ", ex);
		}

		setNull(false);
    }

	@Override
	public void fromByteBuffer(CloverBuffer dataBuffer, CharsetDecoder decoder) throws CharacterCodingException {
		prepareBuf(dataBuffer);
		dataBuffer.get(value);
		setNull(Arrays.equals(value, metadata.getNullValue().getBytes(decoder.charset())));
	}

	@Override
	public void toByteBuffer(CloverBuffer dataBuffer, CharsetEncoder encoder) throws CharacterCodingException {
		try {
			if (!isNull()) {
				dataBuffer.put(getByteArray());
			} else {
				dataBuffer.put(encoder.encode(CharBuffer.wrap(metadata.getNullValue())));
			}
		} catch (BufferOverflowException e) {
			throw new RuntimeException("The size of data buffer is only " + dataBuffer.limit() + ". Set appropriate parameter in defaultProperties file.", e);
		}
	}

	/**
	 *  Performs serialization of the internal value into ByteBuffer (used when
	 *  moving data records between components).
	 *
	 *@param  buffer  Description of Parameter
	 *@since          October 29, 2002
	 */
	@Override
	public void serialize(CloverBuffer buffer) {
        try {
            if(isNull) {
    			// encode nulls as zero
                ByteBufferUtils.encodeLength(buffer, 0);
            } else {
            	// increment length of non-null values by one
                ByteBufferUtils.encodeLength(buffer, value.length + 1);
               	buffer.put(value);
            }
    	} catch (BufferOverflowException e) {
    		throw new RuntimeException("The size of data buffer is only " + buffer.maximumCapacity() + ". Set appropriate parameter in defaultProperties file.", e);
    	}
	}


	/**
	 *  Performs deserialization of data
	 *
	 *@param  buffer  Description of Parameter
	 *@since          October 29, 2002
	 */
	@Override
	public void deserialize(CloverBuffer buffer) {
		// encoded length is incremented by one, decrement it back to normal
		final int length = ByteBufferUtils.decodeLength(buffer) - 1;

		if (length < 0) {
			setNull(true);
		} else {
			if (value == null || length != value.length) {
				value = new byte[length];
			}

			buffer.get(value);
			setNull(false);
		}
	}

	@Override
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
	@Override
	public int compareTo(Object obj) {
		byte[] byteObj;
		if (isNull) return -1;
		if (obj == null) return 1;
		
		if (obj instanceof ByteDataField){
			if (!((ByteDataField) obj).isNull()) {
				byteObj = ((ByteDataField) obj).value;
			}else {
				return 1; 			
			}
		}else if (obj instanceof byte[]){
			byteObj= (byte[])obj;
		}else {
		    throw new IllegalArgumentException("Can't compare ByteDataField and "+obj.getClass().getName());
		}
		 
		int compLength = value.length <= byteObj.length ? value.length : byteObj.length;
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

	@Override
	public int hashCode(){
		 return HashCodeUtil.getHash(value);
	}
	
	/**
	 *  Returns how many bytes will be occupied when this field with current
	 *  value is serialized into ByteBuffer
	 *
	 * @return    The size value
	 * @see	      org.jetel.data.DataField
	 */
	@Override
	public int getSizeSerialized() {
        if(isNull) {
            return ByteBufferUtils.lengthEncoded(0);
        } else {
            final int length = value.length;
            return length + ByteBufferUtils.lengthEncoded(length);
        }
	}

}
