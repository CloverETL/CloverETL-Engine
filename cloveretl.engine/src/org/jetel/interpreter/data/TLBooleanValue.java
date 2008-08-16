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
 * Created on 6.11.2007 by dadik
 *
 */

package org.jetel.interpreter.data;

import javolution.text.TypeFormat;

import org.jetel.data.BooleanDataField;
import org.jetel.data.ByteDataField;
import org.jetel.data.DataField;
import org.jetel.data.StringDataField;
import org.jetel.data.primitive.Numeric;
import org.jetel.metadata.DataFieldMetadata;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;

public class TLBooleanValue extends TLValue {

	public static final TLBooleanValue TRUE=new TLBooleanValue(true);
	public static final TLBooleanValue FALSE=new TLBooleanValue(false);
	
	private boolean value;
	
	private TLBooleanValue() {
		this(false);
	}

	private TLBooleanValue(boolean value){
		super(TLValueType.BOOLEAN);
		this.value=value;
	}

	public static TLBooleanValue getInstance(boolean val){
		return val ? TRUE : FALSE;
	}
	

	@Override
	public int compareTo(TLValue arg0) {
	    if (! (arg0 instanceof TLBooleanValue)){
	    	throw new IllegalArgumentException("Can't compare value type: " + type + " with type: "+arg0.type);
	    }

        if (this==arg0) return 0;
        else if (arg0==FALSE) return 1;
        else return -1;
	}

	@Override
	@SuppressWarnings("BC")
	public void copyToDataField(DataField field) {
		switch(field.getType()){
		case DataFieldMetadata.INTEGER_FIELD:
		case DataFieldMetadata.NUMERIC_FIELD:
		case DataFieldMetadata.DECIMAL_FIELD:
		case DataFieldMetadata.LONG_FIELD:
			((Numeric)field).setValue( value ? 1 : 0);
			break;
		case DataFieldMetadata.STRING_FIELD:
			field.fromString(toString());
			break;
		case DataFieldMetadata.BYTE_FIELD:
			((ByteDataField)field).setValue( value ? 1 : 0);
			break;
		case DataFieldMetadata.BOOLEAN_FIELD:
			((BooleanDataField)field).setValue( value);
			break;
		default:
			throw new IllegalArgumentException("Can't popula DateField type: " + field.getMetadata().getTypeAsString() + " from type: "+type);
		}
	}

	@Override
	public TLValue duplicate() {
		return this;
	}

	@Override
	public Object getValue() {
		return (value) ? Boolean.TRUE : Boolean.FALSE;
	}

	
	public boolean getBoolean(){
		return value;
	}
	
	@Override
	public void setValue(Object _value) {
		if (_value instanceof Boolean){
			if (((Boolean)_value).booleanValue()!=value){
				throw new IllegalArgumentException("Can't assign "+_value);
			}
		}else{
			throw new IllegalArgumentException("Can't assign value " + _value + " to value type: "+type);
		}
		
	}

	@Override
	public void setValue(TLValue _value) {
		if (!(_value instanceof TLBooleanValue)){
			throw new IllegalArgumentException("Can't assign value " + _value + " to value type: "+type);
		}
		if (_value != this) {
			value = ((TLBooleanValue)_value).getBoolean();
		}
	}
	
	@Override
	@SuppressWarnings("BC")
	public void setValue(DataField field) {
		switch(field.getType()){
		case DataFieldMetadata.INTEGER_FIELD:
		case DataFieldMetadata.NUMERIC_FIELD:
		case DataFieldMetadata.DECIMAL_FIELD:
		case DataFieldMetadata.LONG_FIELD:
			value = ((Numeric)field).getInt() != 0;
			break;
		case DataFieldMetadata.STRING_FIELD:
			value = TypeFormat.parseBoolean((StringDataField)field);
			break;
		case DataFieldMetadata.BYTE_FIELD:
			value = ((ByteDataField)field).getByte(0)!= 0;
			break;
		case DataFieldMetadata.BOOLEAN_FIELD:
			value = ((BooleanDataField)field).getBoolean();
			break;
		default:
			throw new IllegalArgumentException("Can't populate "+type+" from  DateField type: " + field.getMetadata().getTypeAsString());
		}
		
	}

	@Override
	public String toString() {
		return Boolean.toString(value);
	}
	
	@Override public boolean equals(Object val){
		// taking advantage of private constructor of TLBooleanValue
		if (val instanceof TLBooleanValue){
			return this==val;
		}
		return false;
	}

}