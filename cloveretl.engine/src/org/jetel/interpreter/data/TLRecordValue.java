/*
 *    jETeL/Clover - Java based ETL application framework.
 *    Copyright (C) 2002-2007  David Pavlis <david.pavlis@centrum.cz> and others.
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
 * Created on 8.11.2007 by dadik
 *
 */

package org.jetel.interpreter.data;

import java.util.Collection;

import org.jetel.data.DataRecord;
import org.jetel.data.primitive.CloverInteger;
import org.jetel.metadata.DataRecordMetadata;

public class TLRecordValue extends TLContainerValue {

	private DataRecord record;
	
	public TLRecordValue(DataRecordMetadata metadata){
		super(TLValueType.RECORD);
		record=new DataRecord(metadata);
		record.init();
	}
	
	
	public TLRecordValue(DataRecord record){
		super(TLValueType.RECORD);
		this.record=record;
	}
	
	@Override
	public Collection<TLValue> getCollection() {
		// TODO Auto-generated method stub
		return null;
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
		return TLValue.convertValue(record.getField(key.toString()));
	}

	@Override
	public void setStoredValue(int index, TLValue _value) {
		_value.copyToDataField(record.getField(index));
	}

	@Override
	public void setStoredValue(TLValue key, TLValue _value) {
		_value.copyToDataField(record.getField(key.toString()));
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
		return new CloverInteger(record.getNumFields());
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return record.toString();
	}

}
