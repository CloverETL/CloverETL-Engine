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


import org.jetel.data.BooleanDataField;
import org.jetel.data.ByteDataField;
import org.jetel.data.DataField;
import org.jetel.data.primitive.Numeric;
import org.jetel.metadata.DataFieldMetadata;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;

public class TLBooleanValue extends TLValue {

	public static final TLBooleanValue TRUE=new TLBooleanValue(true);
	public static final TLBooleanValue FALSE=new TLBooleanValue(false);
	
	private boolean value;
	
	public TLBooleanValue() {
		this(false);
	}

	public TLBooleanValue(boolean value){
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

        if (this.value==((TLBooleanValue)arg0).value) return 0;
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
		return new TLBooleanValue(value);
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
		if (this==TRUE || this == FALSE)
			throw new UnsupportedOperationException("Can't set value of "+TLValueType.BOOLEAN+" - static class !");
		
		if (_value instanceof Boolean){
			this.value = ((Boolean)_value).booleanValue();
		}else if (_value instanceof Numeric){
			this.value = ((Numeric)_value).getInt() == 0 ? false : true;
		}else if (_value instanceof CharSequence){
			this.value = ((CharSequence)_value).charAt(0) == 'T' || ((CharSequence)_value).charAt(0)=='t';
		}else{
			throw new IllegalArgumentException("Can't set TLBoolean value from "+_value.getClass());
		}
	}

	@Override
	public void setValue(TLValue _value) {
		if (this==TRUE || this == FALSE)
			throw new UnsupportedOperationException("Can't set value of "+TLValueType.BOOLEAN+" - static class !");
		if (_value.type==TLValueType.BOOLEAN){
			this.value=((TLBooleanValue)_value).value;
		}else if (_value.type.isNumeric()){
			this.value= _value.compareTo(TLNumericValue.ZERO)==0 ? false : true; 
		}else if (_value.type==TLValueType.STRING){
			this.value = _value.toString().startsWith("T") || _value.toString().startsWith("t");
		}else{
			throw new IllegalArgumentException("Can't set TLBoolean value from "+_value.type);
		}
	}
	
	@Override
	@SuppressWarnings("BC")
	public void setValue(DataField field) {
		if (this==TRUE || this == FALSE)
			throw new UnsupportedOperationException("Can't set value of "+TLValueType.BOOLEAN+" - static class !");
		switch(field.getType()){
		case DataFieldMetadata.BOOLEAN_FIELD:
			this.value=((BooleanDataField)field).getBoolean();
			break;
		case DataFieldMetadata.INTEGER_FIELD:
		case DataFieldMetadata.NUMERIC_FIELD:
		case DataFieldMetadata.LONG_FIELD:
		case DataFieldMetadata.DECIMAL_FIELD:
			this.value=((Numeric)field).getInt()==0 ? false : true;
			break;
		case DataFieldMetadata.STRING_FIELD:
			this.value=field.toString().startsWith("T") || field.toString().startsWith("t");
			break;
			default:
				throw new IllegalArgumentException("Can't set TLBoolean value from "+field.getMetadata().getTypeAsString());
		}
	}

	@Override
	public String toString() {
		return Boolean.toString(value);
	}
	
	@Override public boolean equals(Object val){
		// taking advantage of private constructor of TLBooleanValue
		if (val instanceof TLBooleanValue){
			return this.value==((TLBooleanValue)val).value;
		}
		return false;
	}

}