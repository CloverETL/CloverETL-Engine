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
 * {@link DataField} that loads data lazily from specified source.
 * 
 * Use {@link #setSourceData(Object)} to define a data source and {@link #setLazyLoader(LazyDataFieldLoader)} to assign
 * a loader for the data.
 * 
 * @author salamonp (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 11. 5. 2015
 */
public class DataFieldWithLazyLoading extends DataField {

	/** Wrapped data field. */
	private DataField dataField;

	/** Data used for lazy loading */
	private Object sourceData;

	private boolean needsToBeLoaded = false;
	private LazyDataFieldLoader lazyLoader;

	/**
	 * The default loader just sets the source data into the field.
	 */
	public static final LazyDataFieldLoader DEFAULT_LAZY_LOADER = new LazyDataFieldLoader() {

		@Override
		public void load(DataFieldWithLazyLoading field) {
			field.setValue(field.getSourceData());
		}
	};

	public DataFieldWithLazyLoading(DataField dataField) {
		this.dataField = dataField;
		lazyLoader = DEFAULT_LAZY_LOADER;
	}

	public void setLazyLoader(LazyDataFieldLoader loader) {
		this.lazyLoader = loader;
	}

	/** This method must be used if lazy loading feature is needed. */
	public void setSourceData(Object data) {
		sourceData = data;
		needsToBeLoaded = true;
	}

	public Object getSourceData() {
		return sourceData;
	}

	public boolean isLoaded() {
		return !needsToBeLoaded;
	}

	private void loadIfNeeded() {
		if (needsToBeLoaded) {
			lazyLoader.load(this);
			needsToBeLoaded = false;
		}
	}

	/**
	 * Source data for lazy loading are NOT cloned! Only reference is copied!
	 */
	@Override
	public DataField duplicate() {
		DataFieldWithLazyLoading result = new DataFieldWithLazyLoading(dataField.duplicate());
		result.setSourceData(sourceData);
		result.needsToBeLoaded = needsToBeLoaded;
		result.lazyLoader = lazyLoader;
		return result;
	}

	/**
	 * Source data for lazy loading are NOT cloned! Only reference is copied!
	 */
	@Deprecated
	@Override
	public void copyFrom(DataField fieldFrom) {
		dataField.copyFrom(fieldFrom);
		if (fieldFrom instanceof DataFieldWithLazyLoading) {
			setSourceData(((DataFieldWithLazyLoading) fieldFrom).sourceData);
			((DataFieldWithLazyLoading) fieldFrom).needsToBeLoaded = needsToBeLoaded;
			((DataFieldWithLazyLoading) fieldFrom).lazyLoader = lazyLoader;
		}
	}

	/**
	 * @see org.jetel.data.DataField#setValue(java.lang.Object)
	 */
	@Override
	public void setValue(Object _value) {
		dataField.setValue(_value);
		needsToBeLoaded = false;
	}

	/**
	 * @see org.jetel.data.DataField#setValue(org.jetel.data.DataField)
	 */
	@Override
	public void setValue(DataField fromField) {
		dataField.setValue(fromField);
		needsToBeLoaded = false;
	}

	/**
	 * @see org.jetel.data.DataField#setToDefaultValue()
	 */
	@Override
	public void setToDefaultValue() {
		dataField.setToDefaultValue();
		needsToBeLoaded = false;
	}

	/**
	 * @see org.jetel.data.DataField#setNull(boolean)
	 */
	@Override
	public void setNull(boolean isNull) {
		dataField.setNull(isNull);
		needsToBeLoaded = false;
	}

	/**
	 * @see org.jetel.data.DataField#reset()
	 */
	@Override
	public void reset() {
		dataField.reset();
		needsToBeLoaded = false;
	}

	/**
	 * @see org.jetel.data.DataField#getValue()
	 */
	@Override
	public Object getValue() {
		loadIfNeeded();
		return dataField.getValue();
	}

	/**
	 * @see org.jetel.data.DataField#getValueDuplicate()
	 */
	@Override
	public Object getValueDuplicate() {
		loadIfNeeded();
		return dataField.getValueDuplicate();
	}

	/**
	 * @return
	 * @see org.jetel.data.DataField#getType()
	 */
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
		loadIfNeeded();
		return dataField.isNull();
	}

	/**
	 * @see org.jetel.data.DataField#toString()
	 */
	@Override
	public String toString() {
		loadIfNeeded();
		return dataField.toString();
	}

	/**
	 * @see org.jetel.data.DataField#fromString(java.lang.CharSequence)
	 */
	@Override
	public void fromString(CharSequence seq) {
		dataField.fromString(seq);
		needsToBeLoaded = false;
	}

	/**
	 * @see org.jetel.data.DataField#fromByteBuffer(org.jetel.util.bytes.CloverBuffer, java.nio.charset.CharsetDecoder)
	 */
	@Override
	public void fromByteBuffer(CloverBuffer dataBuffer, CharsetDecoder decoder) throws CharacterCodingException {
		dataField.fromByteBuffer(dataBuffer, decoder);
		needsToBeLoaded = false;
	}

	/**
	 * @see org.jetel.data.DataField#fromByteBuffer(java.nio.ByteBuffer, java.nio.charset.CharsetDecoder)
	 */
	@Deprecated
	@Override
	public void fromByteBuffer(ByteBuffer dataBuffer, CharsetDecoder decoder) throws CharacterCodingException {
		dataField.fromByteBuffer(dataBuffer, decoder);
		needsToBeLoaded = false;
	}

	/**
	 * @see org.jetel.data.DataField#toByteBuffer(org.jetel.util.bytes.CloverBuffer, java.nio.charset.CharsetEncoder)
	 */
	@Override
	public int toByteBuffer(CloverBuffer dataBuffer, CharsetEncoder encoder) throws CharacterCodingException {
		loadIfNeeded();
		return dataField.toByteBuffer(dataBuffer, encoder);
	}

	/**
	 * @see org.jetel.data.DataField#toByteBuffer(org.jetel.util.bytes.CloverBuffer, java.nio.charset.CharsetEncoder,
	 *      int)
	 */
	@Override
	public int toByteBuffer(CloverBuffer dataBuffer, CharsetEncoder encoder, int maxLength)
			throws CharacterCodingException {
		loadIfNeeded();
		return dataField.toByteBuffer(dataBuffer, encoder, maxLength);
	}

	/**
	 * @see org.jetel.data.DataField#toByteBuffer(java.nio.ByteBuffer, java.nio.charset.CharsetEncoder)
	 */
	@Deprecated
	@Override
	public void toByteBuffer(ByteBuffer dataBuffer, CharsetEncoder encoder) throws CharacterCodingException {
		loadIfNeeded();
		dataField.toByteBuffer(dataBuffer, encoder);
	}

	/**
	 * @see org.jetel.data.DataField#serialize(org.jetel.util.bytes.CloverBuffer)
	 */
	@Override
	public void serialize(CloverBuffer buffer) {
		loadIfNeeded();
		dataField.serialize(buffer);
	}

	@Override
	public void serialize(CloverBuffer buffer, DataRecordSerializer serializer) {
		loadIfNeeded();
		dataField.serialize(buffer, serializer);
	}

	/**
	 * @see org.jetel.data.DataField#deserialize(org.jetel.util.bytes.CloverBuffer)
	 */
	@Override
	public void deserialize(CloverBuffer buffer) {
		dataField.deserialize(buffer);
		needsToBeLoaded = false;
	}

	@Override
	public void deserialize(CloverBuffer buffer, DataRecordSerializer serializer) {
		dataField.deserialize(buffer, serializer);
		needsToBeLoaded = false;
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
		return dataField.getSizeSerialized();
	}
}
