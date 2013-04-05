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
package org.jetel.interpreter.data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.interpreter.TransformLangExecutorRuntimeException;
import org.jetel.metadata.DataRecordMetadata;

public class TLRecordValue extends TLContainerValue {

	private DataRecord record;
	
	public TLRecordValue(DataRecordMetadata metadata){
		super(TLValueType.RECORD);
		record=DataRecordFactory.newRecord(metadata);
		record.init();
		record.getMetadata().getFieldTypes();
	}
	
	
	public TLRecordValue(DataRecord record){
		super(TLValueType.RECORD);
		this.record=record;
	}
	
	@Override
	public Collection<TLValue> getCollection() {
		List<TLValue> col=new ArrayList<TLValue>(record.getNumFields());
		for(DataField field: record ){
			col.add(TLValue.convertValue(field));
		}
		return col;
	}

	@Override
	public int getLength() {
		// TODO Auto-generated method stub
		return record.getNumFields();
	}

	@Override
	public TLValue getStoredValue(int index) {
		return TLValue.convertValue(record.getField(index));
	}

	@Override
	public TLValue getStoredValue(TLValue key) {
		if (key.type.isNumeric()){
			return TLValue.convertValue(record.getField(key.getNumeric().getInt()));
		}else{
			return TLValue.convertValue(record.getField(key.toString()));
		}
	}

	@Override
	public void setStoredValue(int index, TLValue _value) {
		_value.copyToDataField(record.getField(index));
	}

	@Override
	public void setStoredValue(TLValue key, TLValue _value) {
		if (key.type.isNumeric()) {
			int fieldIndex = key.getNumeric().getInt();
			if (fieldIndex >= 0 && fieldIndex < record.getNumFields()) {
				_value.copyToDataField(record.getField(fieldIndex));
			} else {
				throw new TransformLangExecutorRuntimeException("referenced field \"" + fieldIndex + "\" does not exist");
			}
		}else{
			int fieldIndex = record.getMetadata().getFieldPosition(key.toString());
			if (fieldIndex != -1) {
				_value.copyToDataField(record.getField(fieldIndex));
			} else {
				throw new TransformLangExecutorRuntimeException("referenced field \"" + key.toString() + "\" does not exist");
			}
		}
	}

	@Override
	public int compareTo(TLValue arg0) {
		if (arg0 instanceof TLRecordValue){
			return record.compareTo(((TLRecordValue)arg0).record);
		}
		return 0;
	}

	@Override public boolean equals(Object val){
		if (val instanceof TLRecordValue){
			record.equals(((TLRecordValue)val).record);
		}
		return false;
	}
	
	@Override public int hashCode(){
		return record.hashCode();
	}
	
	@Override
	public TLValue duplicate() {
		TLRecordValue newval=new TLRecordValue(record);
		newval.record=record.duplicate();
		return newval;
	}

	@Override
	public Object getValue() {
		return record;
	}
	
	@Override
	 public void setValue(Object _value){
		if (_value instanceof DataRecord){
			this.record=(DataRecord)_value;
		}else{
			throw new IllegalArgumentException("Can't assign value type: "+_value.getClass().getName());
		}
	}
	
	@Override
	public void setValue(TLValue _value){
		if (_value instanceof TLRecordValue){
			this.record=((TLRecordValue)_value).record;
		}else{
			throw new IllegalArgumentException("Can't assign value type: "+_value.getType());
		}
	}
	

	@Override
	public String toString() {
		return record.toString();
	}

	 @Override public void clear(){
	    	record.reset();
	 }
	
}