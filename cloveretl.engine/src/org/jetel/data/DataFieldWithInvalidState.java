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

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;

import org.jetel.metadata.DataFieldMetadata;
import org.jetel.util.bytes.CloverBuffer;


/**
 * This data field implementation is simple wrapper for a {@link DataField} instance.
 * Only additional functionality is possibility to mark the field as invalid.
 * Any access to invalid value throw {@link DataFieldInvalidStateException}.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 12. 5. 2014
 */
public class DataFieldWithInvalidState implements DataField {

	private static final long serialVersionUID = 5871757668871924104L;

	/** Wrapped data field. */
	private DataField dataField;

	/** Validity flag. */
	private boolean valid = true;

	public DataFieldWithInvalidState(DataField dataField) {
		this.dataField = dataField;
	}

	/**
	 * Sets validity flag for this data field.
	 * Invalid value cannot be accessed.
	 */
	public void setValid(boolean valid) {
		this.valid = valid;
	}
	
	/**
	 * @return validity flag for this data field, invalid value cannot be accessed
	 */
	public boolean isValid() {
		return valid;
	}
	
	/**
	 * @see org.jetel.data.DataField#duplicate()
	 */
	@Override
	public DataField duplicate() {
		DataFieldWithInvalidState result = new DataFieldWithInvalidState(dataField.duplicate());
		result.setValid(valid);
		return result;
	}

	/**
	 * @see org.jetel.data.DataField#copyFrom(org.jetel.data.DataField)
	 */
	@SuppressWarnings("deprecation")
	@Deprecated
	@Override
	public void copyFrom(DataField fieldFrom) {
		dataField.copyFrom(fieldFrom);
		if (fieldFrom instanceof DataFieldWithInvalidState) {
			setValid(((DataFieldWithInvalidState) fieldFrom).valid);
		} else {
			setValid(true);
		}
	}

	/**
	 * @see org.jetel.data.DataField#setValue(java.lang.Object)
	 */
	@Override
	public void setValue(Object _value) {
		dataField.setValue(_value);
		setValid(true);
	}

	/**
	 * @see org.jetel.data.DataField#setValue(org.jetel.data.DataField)
	 */
	@Override
	public void setValue(DataField fromField) {
		dataField.setValue(fromField);
		if (fromField instanceof DataFieldWithInvalidState) {
			setValid(((DataFieldWithInvalidState) fromField).valid);
		} else {
			setValid(true);
		}
	}

	/**
	 * @see org.jetel.data.DataField#setToDefaultValue()
	 */
	@Override
	public void setToDefaultValue() {
		dataField.setToDefaultValue();
		setValid(true);
	}

	/**
	 * @see org.jetel.data.DataField#setNull(boolean)
	 */
	@Override
	public void setNull(boolean isNull) {
		dataField.setNull(isNull);
		setValid(true);
	}

	/**
	 * @see org.jetel.data.DataField#reset()
	 */
	@Override
	public void reset() {
		dataField.reset();
		setValid(true);
	}

	/**
	 * @see org.jetel.data.DataField#getValue()
	 */
	@Override
	public Object getValue() {
		if (isValid()) {
			return dataField.getValue();
		} else {
			throw new DataFieldInvalidStateException();
		}
	}

	/**
	 * @see org.jetel.data.DataField#getValueDuplicate()
	 */
	@Override
	public Object getValueDuplicate() {
		if (isValid()) {
			return dataField.getValueDuplicate();
		} else {
			throw new DataFieldInvalidStateException();
		}
	}

	/**
	 * @return
	 * @see org.jetel.data.DataField#getType()
	 */
	@SuppressWarnings("deprecation")
	@Deprecated
	@Override
	public char getType() {
		return dataField.getType();
	}

	/**
	 * @see org.jetel.data.DataField#getMetadata()
	 */
	@Override
	public DataFieldMetadata getMetadata() {
		return dataField.getMetadata();
	}

	/**
	 * @see org.jetel.data.DataField#isNull()
	 */
	@Override
	public boolean isNull() {
		if (isValid()) {
			return dataField.isNull();
		} else {
			throw new DataFieldInvalidStateException();
		}
	}

	/**
	 * @see org.jetel.data.DataField#toString()
	 */
	@Override
	public String toString() {
		if (isValid()) {
			return dataField.toString();
		} else {
			throw new DataFieldInvalidStateException();
		}
	}

	/**
	 * @see org.jetel.data.DataField#fromString(java.lang.CharSequence)
	 */
	@Override
	public void fromString(CharSequence seq) {
		dataField.fromString(seq);
		setValid(true);
	}

	/**
	 * @see org.jetel.data.DataField#fromByteBuffer(org.jetel.util.bytes.CloverBuffer, java.nio.charset.CharsetDecoder)
	 */
	@Override
	public void fromByteBuffer(CloverBuffer dataBuffer, CharsetDecoder decoder) throws CharacterCodingException {
		dataField.fromByteBuffer(dataBuffer, decoder);
		setValid(true);
	}

	/**
	 * @see org.jetel.data.DataField#fromByteBuffer(java.nio.ByteBuffer, java.nio.charset.CharsetDecoder)
	 */
	@SuppressWarnings("deprecation")
	@Deprecated
	@Override
	public void fromByteBuffer(ByteBuffer dataBuffer, CharsetDecoder decoder) throws CharacterCodingException {
		dataField.fromByteBuffer(dataBuffer, decoder);
		setValid(true);
	}

	/**
	 * @see org.jetel.data.DataField#toByteBuffer(org.jetel.util.bytes.CloverBuffer, java.nio.charset.CharsetEncoder)
	 */
	@Override
	public int toByteBuffer(CloverBuffer dataBuffer, CharsetEncoder encoder) throws CharacterCodingException {
		if (isValid()) {
			return dataField.toByteBuffer(dataBuffer, encoder);
		} else {
			throw new DataFieldInvalidStateException();
		}
	}

	/**
	 * @see org.jetel.data.DataField#toByteBuffer(org.jetel.util.bytes.CloverBuffer, java.nio.charset.CharsetEncoder, int)
	 */
	@Override
	public int toByteBuffer(CloverBuffer dataBuffer, CharsetEncoder encoder, int maxLength)
			throws CharacterCodingException {
		if (isValid()) {
			return dataField.toByteBuffer(dataBuffer, encoder, maxLength);
		} else {
			throw new DataFieldInvalidStateException();
		}
	}

	/**
	 * @see org.jetel.data.DataField#toByteBuffer(java.nio.ByteBuffer, java.nio.charset.CharsetEncoder)
	 */
	@SuppressWarnings("deprecation")
	@Deprecated
	@Override
	public void toByteBuffer(ByteBuffer dataBuffer, CharsetEncoder encoder) throws CharacterCodingException {
		if (isValid()) {
			dataField.toByteBuffer(dataBuffer, encoder);
		} else {
			throw new DataFieldInvalidStateException();
		}
	}

	/**
	 * @see org.jetel.data.DataField#serialize(org.jetel.util.bytes.CloverBuffer)
	 */
	@Override
	public void serialize(CloverBuffer buffer) {
		if (isValid()) {
			dataField.serialize(buffer);
		} else {
			throw new DataFieldInvalidStateException();
		}
	}

	/**
	 * @see org.jetel.data.DataField#deserialize(org.jetel.util.bytes.CloverBuffer)
	 */
	@Override
	public void deserialize(CloverBuffer buffer) {
		dataField.deserialize(buffer);
		setValid(true);
	}

	/**
	 * @see org.jetel.data.DataField#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		return dataField.equals(obj);
	}

	/**
	 * @see org.jetel.data.DataField#hashCode()
	 */
	@Override
	public int hashCode() {
		return dataField.hashCode();
	}

	/**
	 * @see org.jetel.data.DataField#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(Object obj) {
		return dataField.compareTo(obj);
	}

	/**
	 * @see org.jetel.data.DataField#getSizeSerialized()
	 */
	@Override
	public int getSizeSerialized() {
		if (isValid()) {
			return dataField.getSizeSerialized();
		} else {
			throw new DataFieldInvalidStateException();
		}
	}

}
