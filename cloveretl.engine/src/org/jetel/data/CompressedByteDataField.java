/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2006 Javlin Consulting <info@javlinconsulting>
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

import org.jetel.metadata.DataFieldMetadata;
import org.jetel.util.ByteBufferUtils;
import org.jetel.util.ZipUtils;

/**
 * Class implementing field which represents gzip-compressed array of bytes. 
 * @author Jan Hadrava (jan.hadrava@javlinconsulting.cz), Javlin Consulting (www.javlinconsulting.cz)
 * @since 12/01/06  
 */
public class CompressedByteDataField extends ByteDataField {
	private static final long serialVersionUID = 1L;
	
	/** lenght of data represented by the field. */ 
	private int dataLen;

	
	/**
	 * @param _metadata
	 */
	public CompressedByteDataField(DataFieldMetadata _metadata) {
		super(_metadata);
		dataLen = 0;
	}

	/**
	 * @param _metadata
	 * @param plain
	 */
	public CompressedByteDataField(DataFieldMetadata _metadata, boolean plain) {
		super(_metadata, plain);
		dataLen = 0;
	}

	/**
	 * @param _metadata
	 * @param value
	 */
	public CompressedByteDataField(DataFieldMetadata _metadata, byte[] value) {
		super(_metadata, ZipUtils.compress(value));
		dataLen = value == null ? 0 : value.length;
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.data.DataField#copy()
	 */
	public DataField duplicate(){
	    return new CompressedByteDataField(metadata, value);
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.DataField#copyField(org.jetel.data.DataField)
	 */
	public void copyFrom(DataField fromField){
	    if (fromField instanceof CompressedByteDataField){
	        if (!fromField.isNull){
	            int length = ((CompressedByteDataField)fromField).value.length;
	            if (this.value == null || this.value.length != length){
	                this.value = new byte[length];
	            }
	            System.arraycopy(((CompressedByteDataField) fromField).value, 0, this.value, 0, length);
	        }
	        setNull(fromField.isNull);
	        dataLen = ((CompressedByteDataField)fromField).dataLen;
	    }
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.data.ByteDataField#setValue(byte[])
	 */
	public void setValue(byte[] value) {
		dataLen = value == null ? 0 : value.length;
		super.setValue(ZipUtils.compress(value));
	}


	/* (non-Javadoc)
	 * @see org.jetel.data.ByteDataField#setValue(byte)
	 */
	public void setValue(byte value) {
		dataLen = metadata.getSize();
		if (dataLen <= 0) {
			dataLen = INITIAL_BYTE_ARRAY_CAPACITY;
		}
		byte[] buf = new byte[dataLen];

		Arrays.fill(buf, value);
		setValue(buf);
		setNull(false);
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.ByteDataField#getType()
	 */
	public char getType() {
		return DataFieldMetadata.BYTE_FIELD_COMPRESSED;
	}


    /* (non-Javadoc)
     * @see org.jetel.data.ByteDataField#getValueDuplicate()
     */
    public Object getValueDuplicate() {
    	return getValue();
    }

	/* (non-Javadoc)
	 * @see org.jetel.data.ByteDataField#getByte(int)
	 */
	public byte getByte(int position) {
        if(isNull) {
            return 0;
        }
		return getByteArray()[position];
	}


	/* (non-Javadoc)
	 * @see org.jetel.data.ByteDataField#getByteArray()
	 */
	public byte[] getByteArray() {
		return ZipUtils.decompress(super.value, dataLen);
	}

	/**
	 * @deprecated
	 */
	public void fromString(String valueStr) {
        if(valueStr == null || valueStr.length() == 0) {
            setNull(true);
            return;
        }
        byte[] bytes = valueStr.getBytes();
        setValue(bytes);
        dataLen = bytes.length;
        setNull(false);
	}

	/**
	 * @deprecated
	 */
    public void fromString(String valueStr,String charset){
        if(valueStr == null || valueStr.length() == 0) {
            setNull(true);
            return;
        }
        try{
            byte[] bytes = valueStr.getBytes(charset);
            setValue(bytes);
            dataLen = bytes.length;
        }catch(UnsupportedEncodingException ex){
            throw new RuntimeException(ex.toString()+" when calling fromString() on field \""+
                    this.metadata.getName()+"\"",ex);
        }
        setNull(false);
    }

	/* (non-Javadoc)
	 * @see org.jetel.data.ByteDataField#fromString(java.lang.String)
	 */
	public void fromString(CharSequence seq) {
        if(seq == null || seq.length() == 0) {
            setNull(true);
            return;
        }
        byte[] bytes = seq.toString().getBytes();
        setValue(bytes);
        dataLen = bytes.length;
        setNull(false);
	}

    /* (non-Javadoc)
     * @see org.jetel.data.ByteDataField#fromString(java.lang.String, java.lang.String)
     */
    public void fromString(CharSequence seq, String charset){
        if(seq == null || seq.length() == 0) {
            setNull(true);
            return;
        }
        try{
            byte[] bytes = seq.toString().getBytes(charset);
            setValue(bytes);
            dataLen = bytes.length;
        }catch(UnsupportedEncodingException ex){
            throw new RuntimeException(ex.toString()+" when calling fromString() on field \""+
                    this.metadata.getName()+"\"",ex);
        }
        setNull(false);
    }

	/* (non-Javadoc)
	 * @see org.jetel.data.ByteDataField#fromByteBuffer(java.nio.ByteBuffer, java.nio.charset.CharsetDecoder)
	 */
	public void fromByteBuffer(ByteBuffer dataBuffer, CharsetDecoder decoder) {
		dataLen = metadata.getSize();
		if (dataLen <= 0) {
			dataLen = INITIAL_BYTE_ARRAY_CAPACITY;
		}
		byte[] buf = new byte[dataLen];
		dataBuffer.get(buf);
		setValue(buf);
		setNull(false);
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.ByteDataField#toByteBuffer(java.nio.ByteBuffer, java.nio.charset.CharsetEncoder)
	 */
	public void toByteBuffer(ByteBuffer dataBuffer, CharsetEncoder encoder) {
        if(!isNull) {
            dataBuffer.put(getByteArray());
        }
	}

    /* (non-Javadoc)
     * @see org.jetel.data.ByteDataField#toByteBuffer(java.nio.ByteBuffer)
     */
    public void toByteBuffer(ByteBuffer dataBuffer) {
        if(!isNull) {
            dataBuffer.put(getByteArray());
        }
    }

	/* (non-Javadoc)
	 * @see org.jetel.data.ByteDataField#serialize(java.nio.ByteBuffer)
	 */
	public void serialize(ByteBuffer buffer) {
        if(isNull) {
            ByteBufferUtils.encodeLength(buffer, 0);
        } else {
            ByteBufferUtils.encodeLength(buffer, dataLen);
            ByteBufferUtils.encodeLength(buffer, value.length);
            buffer.put(value);
        }
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.ByteDataField#deserialize(java.nio.ByteBuffer)
	 */
	public void deserialize(ByteBuffer buffer) {
		
		dataLen = ByteBufferUtils.decodeLength(buffer);
        
        if(dataLen == 0) {
            setNull(true);
            return;
        }

        int bufLen = ByteBufferUtils.decodeLength(buffer);
        
        if(value == null || bufLen != value.length) {
        	value = new byte[bufLen];
        }
        buffer.get(value);
        setNull(false);
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.ByteDataField#equals(java.lang.Object)
	 */
	public boolean equals(Object obj) {
	    if (isNull || obj==null) return false;
	    
		if (obj instanceof CompressedByteDataField){
			return this.dataLen == ((CompressedByteDataField) obj).dataLen
									&& Arrays.equals(this.value, ((CompressedByteDataField) obj).value);
		}else if (obj instanceof byte[]){
			return Arrays.equals(getByteArray(), (byte[])obj);
		}else {
		    return false;
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.ByteDataField#compareTo(java.lang.Object)
	 */
	public int compareTo(Object obj) {
		if (isNull) return -1;
		
		byte[] left;
		byte[] right;

		if (obj instanceof CompressedByteDataField){
			left = value;
			right = ((CompressedByteDataField)obj).value;
		}else if (obj instanceof byte[]){
			left = getByteArray();
			right = (byte[])obj;
		}else {
		    throw new ClassCastException("Can't compare CompressedByteDataField and "+obj.getClass().getName());
		}
		 
		int compLength = left.length >= right.length ? left.length : right.length;
		for (int i = 0; i < compLength; i++) {
			if (left[i] > right[i]) {
				return 1;
			} else if (left[i] < right[i]) {
				return -1;
			}
		}
		// arrays seem to be the same (so far), decide according to the length
		if (left.length == right.length) {
			return 0;
		} else if (left.length > right.length) {
			return 1;
		} else {
			return -1;
		}
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.data.ByteDataField#getSizeSerialized()
	 */
	public int getSizeSerialized() {
        final int length=value.length;
        if(isNull) {
            return ByteBufferUtils.lengthEncoded(0);
        } else {
            return 2*ByteBufferUtils.lengthEncoded(length) + length;
        }
	}
    
}
