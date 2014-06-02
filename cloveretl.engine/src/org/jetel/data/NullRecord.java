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

import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;

import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.bytes.CloverBuffer;

/**
 * This class represents record will all fields with null value.
 * 
 * @author avackova (agata.vackova@javlinconsulting.cz) ; 
 * (c) JavlinConsulting s.r.o.
 *  www.javlinconsulting.cz
 *  
 *@since April 21, 2008
 */
public class NullRecord extends DataRecordImpl {
	
	private static final long serialVersionUID = 1L;

	/**
	 * Constant used for representation of record will all fields with null value.
	 */
	public final static DataRecord NULL_RECORD = new NullRecord();
	
	/**
	 * Creates NullRecord object based on NullMetadata
	 */
	@SuppressWarnings("deprecation")
	private NullRecord(){
		//constructor should not be deprecated, just protected
		super(NullMetadata.NULL_METADATA);
	}
	
	@Override
	public NullRecord duplicate() {
		return (NullRecord) NULL_RECORD;
	}
	
	@Override
	public void delField(int _fieldNum) {
	}
	
	@Override
	public void deserialize(CloverBuffer buffer) {
		throw new UnsupportedOperationException("Can't deserialize NullRecord");
	}
	
	@Override
	public void deserialize(CloverBuffer buffer, int[] whichFields) {
		throw new UnsupportedOperationException("Can't deserialize NullRecord");
	}
	
	@Override
	public void deserialize(CloverBuffer buffer,DataRecordSerializer serializer) {
		throw new UnsupportedOperationException("Can't serialize NullRecord");
	}
	
	@Override
	public DataField getField(int _fieldNum) {
		return NullField.NULL_FIELD;
	}
	
	@Override
	public DataField getField(String _name) {
		return NullField.NULL_FIELD;
	}
	
	@Override
	public void serialize(CloverBuffer buffer) {
		throw new UnsupportedOperationException("Can't serialize NullRecord");
	}
	
	@Override
	public void serialize(CloverBuffer buffer, int[] whichFields) {
		throw new UnsupportedOperationException("Can't serialize NullRecord");
	}
	
	@Override
	public void serialize(CloverBuffer buffer,DataRecordSerializer serializer) {
		throw new UnsupportedOperationException("Can't serialize NullRecord");
	}
	
	
	@Override
	public void setMetadata(DataRecordMetadata metadata) {
	}
	
	@Override
	public void setToDefaultValue(int _fieldNum) {
	}
	
	@Override
	public void setToNull(int _fieldNum) {
	}
	
	@Override
	public void reset(int _fieldNum) {
	}
	
	@Override
	public String toString() {
		return "NULL_RECORD";
	}
	
	@Override
	public boolean hasField(String name) {
		return true;
	}
}

/**
 * Metadata for NullRecord
 *
 */
final class NullMetadata extends DataRecordMetadata{

	private static final long serialVersionUID = 1L;
	
	final static DataRecordMetadata NULL_METADATA = new NullMetadata();

	private NullMetadata() {
		super("null_metadata");
	}
	
	@Override
	public DataRecordMetadata duplicate() {
		return NULL_METADATA;
	}
	
	@Override
	public DataFieldMetadata getField(int _fieldNum) {
		return NullField.NULL_FIELD_METADATA;
	}
	
	@Override
	public DataFieldMetadata getField(String _fieldName) {
		return NullField.NULL_FIELD_METADATA;
	}
	
	@Override
	@Deprecated
	public char getFieldType(int fieldNo) {
		return DataFieldMetadata.NULL_FIELD;
	}
	
	@Override
	@Deprecated
	public char getFieldType(String fieldName) {
		return DataFieldMetadata.NULL_FIELD;
	}
	
	@Override
	@Deprecated
	public String getFieldTypeAsString(int fieldNo) {
		return DataFieldMetadata.type2Str(DataFieldMetadata.NULL_FIELD);
	}
	
	@Override
	public void setRecordSize(int recSize) {
		if (recSize > 0) throw new IllegalArgumentException();
		super.setRecordSize(recSize);
	}
	
	@Override
	public void addField(DataFieldMetadata _field) {
	}
	
	@Override
	public void delField(int _fieldNum) {
	}
	
	@Override
	public void delField(String _fieldName) {
	}
}

/**
 * Fields of NullRecord
 *
 */
final class NullField extends DataFieldImpl {
	
	private static final long serialVersionUID = 1L;

	final static DataFieldMetadata NULL_FIELD_METADATA = new DataFieldMetadata("null_field",(short)0);
	static{
		NULL_FIELD_METADATA.setDefaultValueStr("");
		NULL_FIELD_METADATA.setNullable(true);
	}
	
	final static DataField NULL_FIELD = new NullField();
	
	NullField() {
		super(NULL_FIELD_METADATA);
		setNull(true);
	}

	@Override
	public int compareTo(Object obj) {
		return -1;
	}

	@Override
	public void deserialize(CloverBuffer buffer) {
		throw new UnsupportedOperationException("Can't deserialize NullField");
	}
	
	@Override
	public void deserialize(CloverBuffer buffer,DataRecordSerializer serializer) {
		throw new UnsupportedOperationException("Can't serialize NullRecord");
	}

	@Override
	public DataField duplicate() {
		return NULL_FIELD;
	}

	@Override
	public int hashCode() {
		return 0;
	}
	
	@Override
	public boolean equals(Object obj) {
		return this == obj;
	}

	@Override
	public void fromByteBuffer(CloverBuffer dataBuffer, CharsetDecoder decoder) throws CharacterCodingException {
	}

	@Override
	public int toByteBuffer(CloverBuffer dataBuffer, CharsetEncoder encoder, int maxLength) throws CharacterCodingException {
		return 0;
	}

	@Override
	public void fromString(CharSequence seq) {
	}

	@Override
	public int getSizeSerialized() {
		return 0;
	}

	@Override
	@Deprecated
	public char getType() {
		return DataFieldMetadata.NULL_FIELD;
	}

	@Override
	public Object getValue() {
		return null;
	}

	@Override
	public Object getValueDuplicate() {
		return null;
	}

	@Override
	public void reset() {
	}

	@Override
	public void serialize(CloverBuffer buffer) {
		throw new UnsupportedOperationException("Can't serialize NullField");
	}
	
	@Override
	public void serialize(CloverBuffer buffer,DataRecordSerializer serializer) {
		throw new UnsupportedOperationException("Can't serialize NullRecord");
	}

	@Override
	public void setValue(Object _value) {
	}

	@Override
	public String toString() {
		return "";
	}
	
	@Override
	public void setNull(boolean isNull) {
		if (!isNull) throw new IllegalArgumentException(
				"Can't set nullable=false for " + DataFieldType.NULL.getName() + " data field!!!");
		super.setNull(isNull);
	}
}