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

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;

import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.exception.NullDataFormatException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.util.bytes.CloverBuffer;
import org.jetel.util.string.StringUtils;

/**
 *  A class that represents data field and its value.
 *
 * @author      David Pavlis
 * @since       March 26, 2002
 * @see         OtherClasses
 */
@SuppressWarnings("serial")
public abstract class DataFieldImpl extends DataField {

	/**
     * Reference to metadata object describing this field
     *  
	 * @since
	 */
	protected transient DataFieldMetadata metadata;

	/**
	 *  Does this field currently contain NULL value ? 
	 *
	 * @since    September 16, 2002
	 */
	protected boolean isNull;


	/**
	 *  Constructor
	 *
	 * @param  _metadata  Metadata describing field
	 * @since
	 */
	public DataFieldImpl(DataFieldMetadata _metadata) {
		this.metadata = _metadata;
	}

	/**
     * Constructor
     * 
	 * @param _metadata Metadata describing field
	 * @param plain if true,create plain data field - no formatters,etc. 
     * will be created & assigned to field
	 */
	public DataFieldImpl(DataFieldMetadata _metadata, boolean plain) {
        this.metadata = _metadata;
    }
    
	@SuppressWarnings("deprecation")
	@Override
	@Deprecated
	public void copyFrom(DataField fieldFrom) {
	    setValue(fieldFrom.getValue());   
    }
	
	@Override
	public void setValue(DataField fromField){
		if (fromField != null) {
            setValue(fromField.getValue());   
		}else{
			setNull(true);
		}
	}

	@Override
	public void setToDefaultValue() {
		try {
            Object val;
            if((val = metadata.getDefaultValue()) != null) {
                setValue(val);
                return;
            }
			if (metadata.getDefaultValueStr() != null) {
				//do we really need to convert the string form of default value to 'SpecChar'?
				//this conversion was already done in DataRecordMetadataXMLReaderWriter.parseRecordMetadata()
				fromString(StringUtils.stringToSpecChar(metadata.getDefaultValueStr()));
				if (isNull()) {
					throw new NullDataFormatException("Null cannot be a default value (field '" + metadata.getName() + "').");
				}
				metadata.setDefaultValue(getValueDuplicate());
				return;
			}
			if (metadata.isNullable()) {
				setNull(true);
			}else{
				throw new NullDataFormatException(metadata.getName() + 
						" has not dafault value defined and is not nullable!");
			}
		} catch (Exception ex) {
			// here, the only reason to fail is bad DefaultValue
			throw new BadDataFormatException(metadata.getName() + " has incorrect default value", metadata.getDefaultValueStr());
		}
	}

	@Override
	public void setNull(boolean isNull) {
		if (isNull && !metadata.isNullable()) {
            try {
                setToDefaultValue();
                return;
            } catch(NullDataFormatException e) {
                throw new NullDataFormatException(getMetadata().getName() + " field can not be set to null! (nullable == false)");
            }
        }
		this.isNull = isNull;
	}

	@Override
	public DataFieldMetadata getMetadata() {
		return metadata;
	}

	@Override
	public boolean isNull() {
		return isNull;
	}

	@Override
	public void fromByteBuffer(CloverBuffer dataBuffer, CharsetDecoder decoder) throws CharacterCodingException {
		fromString(decoder.decode(dataBuffer.buf()));
	}

	@SuppressWarnings("deprecation")
	@Override
	@Deprecated
	public void fromByteBuffer(ByteBuffer dataBuffer, CharsetDecoder decoder) throws CharacterCodingException {
		CloverBuffer wrappedBuffer = CloverBuffer.wrap(dataBuffer);
		fromByteBuffer(wrappedBuffer, decoder);
		if (wrappedBuffer.buf() != dataBuffer) {
			throw new JetelRuntimeException("Deprecated method invocation failed. Please use CloverBuffer instead of ByteBuffer.");
		}
	}

	@Override
	public int toByteBuffer(CloverBuffer dataBuffer, CharsetEncoder encoder) throws CharacterCodingException {
		return toByteBuffer(dataBuffer, encoder, Integer.MAX_VALUE);
	}
	
	@Override
	public int toByteBuffer(CloverBuffer dataBuffer, CharsetEncoder encoder, int maxLength) throws CharacterCodingException {
		try {
			String s = toString();
			if (s.length() > maxLength) {
				s = s.substring(0, maxLength);
			}
			dataBuffer.put(encoder.encode(CharBuffer.wrap(s)));
			return s.length();
		} catch (BufferOverflowException e) {
			throw new RuntimeException("The size of data buffer is only " + dataBuffer.limit() + ". Set appropriate parameter in defaultProperties file.", e);
		}
	}

	@SuppressWarnings("deprecation")
	@Override
	@Deprecated
	public void toByteBuffer(ByteBuffer dataBuffer, CharsetEncoder encoder) throws CharacterCodingException {
		CloverBuffer wrappedBuffer = CloverBuffer.wrap(dataBuffer);
		toByteBuffer(wrappedBuffer, encoder);
		if (wrappedBuffer.buf() != dataBuffer) {
			throw new JetelRuntimeException("Deprecated method invocation failed. Please use CloverBuffer instead of ByteBuffer.");
		}
	}

}
