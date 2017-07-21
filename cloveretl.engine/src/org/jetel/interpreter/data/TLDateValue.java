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

import java.sql.Timestamp;
import java.util.Date;

import org.jetel.data.DataField;
import org.jetel.data.primitive.Numeric;
import org.jetel.metadata.DataFieldMetadata;

public class TLDateValue extends TLValue {

	private Date value;
	
	
	public TLDateValue(){
		super(TLValueType.DATE);
		value=new Date();
	}
	
	public TLDateValue(Date value){
		super(TLValueType.DATE);
		this.value=value;
	}
	
	@Override
	public void setValue(Object _value) {
		if (_value instanceof Date){
			setValue((Date)_value);
		}else if (_value instanceof Timestamp )
			this.value.setTime(((Timestamp)_value).getTime());
		else if (_value instanceof java.sql.Date)
			this.value.setTime(((java.sql.Date)_value).getTime());
		else if (_value instanceof Numeric)
			this.value.setTime(((Numeric)_value).getLong());
		else
			throw new IllegalArgumentException("Can't assign value " + _value + " to value type: "+type);
		
	}
	
	public void setValue(Date _value){
		if (_value!=null)
			this.value.setTime(_value.getTime());
		else
			throw new IllegalArgumentException("Can't assign value null to value type: " + type);
	}
	
	@Override
	public void setValue(TLValue _value){
		if (_value.type==type)
			setValue(_value.getDate());
		else if (_value.type==TLValueType.LONG)
			setValue(_value.getNumeric());
		else
			throw new IllegalArgumentException("Can't assign value " + _value + " to value type: "+type);
	}
	
	@Override
	public void setValue(DataField field) {
		if (field.getType()==DataFieldMetadata.DATE_FIELD)
			this.value=(Date)field.getValueDuplicate();
		else
			throw new IllegalArgumentException("Can't assign value of field "+field.getMetadata().getName()+
					" (type " + field.getMetadata().getTypeAsString()+") to Date value");
		
	}
	
	
	@Override
	public Object getValue(){
		return value;
	}
	
	@Override
	public Date getDate(){
		return value;
	}
	
	 @Override public int compareTo(TLValue o) {
	        if (this.value==null) return -1;
	        else if (o.getValue()==null) return 1;
	        
	        if (o.type!=TLValueType.DATE) throw new IllegalArgumentException("Can't compare value type: " + type + " with type: "+o.type);

	        return this.value.compareTo((Date)o.getValue());
	        

	    }
	 
	 @Override
	public void copyToDataField(DataField field) {
			if (value == null) {
				field.setNull(true);
			} else if (field.getMetadata().getType() == DataFieldMetadata.DATE_FIELD) {
						field.setValue(value.getTime());
					}else{
						field.fromString(value.toString());
			}
		}

	@Override
	public TLValue duplicate() {
		return new TLDateValue((Date)value.clone());
	}

	@Override public String toString(){
		return value.toString();
	}
	
	@Override public boolean equals(Object val){
		if (this==val) return true;
		if (val instanceof TLDateValue){
			return value.equals(((TLDateValue)val).value);
		}
		return false;
	}
	
	@Override public int hashCode(){
		return value.hashCode();
	}
}
