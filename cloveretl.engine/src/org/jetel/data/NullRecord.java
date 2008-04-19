/**
 * 
 */
package org.jetel.data;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;

import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;

/**
 * @author Jakubv
 *
 */
public class NullRecord extends DataRecord {
	
	private static final long serialVersionUID = 1L;

	public final static DataRecord NULL_RECORD = new NullRecord();
	
	private NullRecord(){
		super(NullMetadata.NULL_METADATA);
	}
	
	@Override
	public DataRecord duplicate() {
		return NULL_RECORD;
	}
	
	@Override
	public void delField(int _fieldNum) {
	}
	
	@Override
	public void deserialize(ByteBuffer buffer, int[] whichFields) {
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
	public void serialize(ByteBuffer buffer, int[] whichFields) {
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
		return false;
	}
}

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
	public char getFieldType(int fieldNo) {
		return DataFieldMetadata.NULL_FIELD;
	}
	
	@Override
	public char getFieldType(String fieldName) {
		return DataFieldMetadata.NULL_FIELD;
	}
	
	@Override
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

final class NullField extends DataField{
	
	private static final long serialVersionUID = 1L;

	final static DataFieldMetadata NULL_FIELD_METADATA = new DataFieldMetadata("null_field",(short)0);
	static{
		NULL_FIELD_METADATA.setDefaultValue(null);
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
	public void deserialize(ByteBuffer buffer) {
	}

	@Override
	public DataField duplicate() {
		return NULL_FIELD;
	}

	@Override
	public boolean equals(Object obj) {
		return this == obj;
	}

	@Override
	public void fromByteBuffer(ByteBuffer dataBuffer, CharsetDecoder decoder) throws CharacterCodingException {
	}

	@Override
	public void fromString(CharSequence seq) {
	}

	@Override
	public int getSizeSerialized() {
		return 0;
	}

	@Override
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
	public void serialize(ByteBuffer buffer) {
	}

	@Override
	public void setValue(Object _value) {
	}

	@Override
	public void toByteBuffer(ByteBuffer dataBuffer, CharsetEncoder encoder) throws CharacterCodingException {
	}

	@Override
	public void toByteBuffer(ByteBuffer dataBuffer) {
	}

	@Override
	public String toString() {
		return "";
	}
	
	@Override
	public void setNull(boolean isNull) {
		if (!isNull) throw new IllegalArgumentException(
				"Can't set nullable=false for " + DataFieldMetadata.type2Str(DataFieldMetadata.NULL_FIELD) + " data field!!!");
		super.setNull(isNull);
	}
}