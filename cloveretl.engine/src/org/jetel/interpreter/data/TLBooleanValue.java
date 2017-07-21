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


import org.jetel.data.BooleanDataField;
import org.jetel.data.ByteDataField;
import org.jetel.data.DataField;
import org.jetel.data.primitive.Numeric;
import org.jetel.metadata.DataFieldMetadata;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;

public class TLBooleanValue extends TLValue {

	public static final TLBooleanValue TRUE=new TLBooleanValue(true);
	public static final TLBooleanValue FALSE=new TLBooleanValue(false);
	
	private final boolean value;
	
	private TLBooleanValue(boolean value){
		// do not instantiate me - use getInstance() instead
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
		return getInstance(value);
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
		throw new UnsupportedOperationException(TLValueType.BOOLEAN+" is immutable, setValue(Object) not allowed");
	}

	@Override
	/**
	 * Immutable class, setValue() not available
	 * Get corresponding reference via getInstance()
	 */
	public void setValue(TLValue _value) {
		throw new UnsupportedOperationException(TLValueType.BOOLEAN + " is immutable, setValue(TLValue) not allowed");
	}
	
	/**
	 * Immutable class, setValue() not available
	 * Get corresponding reference via getInstance()
	 */
	@Override
	public void setValue(DataField field) {
		throw new UnsupportedOperationException(TLValueType.BOOLEAN + " is immutable, setValue(DataField) not allowed");
	}

	/**
	 * Immutable class, setValue() not available
	 * Get corresponding reference via getInstance()
	 */
	@Override
	public String toString() {
		return Boolean.toString(value);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (value ? 1231 : 1237);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!(obj instanceof TLBooleanValue))
			return false;
		final TLBooleanValue other = (TLBooleanValue) obj;
		if (value != other.value)
			return false;
		return true;
	}
	
}