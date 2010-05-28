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

import org.jetel.data.DataField;
import org.jetel.data.StringDataField;
import org.jetel.util.string.Compare;
import org.jetel.util.string.StringUtils;

public class TLStringValue extends TLValue implements CharSequence {

    public static final TLValue EMPTY = new TLStringValue("");
    
	StringBuilder value;
	
	public TLStringValue(){
		super(TLValueType.STRING);
		value=new StringBuilder();
	}
	
	
	public TLStringValue(CharSequence value){
		super(TLValueType.STRING);
		this.value=new StringBuilder(value.length());
		this.value.append(value);
	}
	
	public TLStringValue(StringBuilder value){
		super(TLValueType.STRING);
		this.value=value;
	}
	
	public void setValue(TLValue value){
		// this is fix for s=s assignment - if the value is actually us, we don't do anything
		if (this == value) {
			return;
		}

		// otherwise we set the value :)
		this.value.setLength(0);
		if (value instanceof TLStringValue){
			this.value.append(((TLStringValue)value).value);
		}else{
			this.value.append(value.toString());
		}
	}
	
	
	public Object getValue(){
		return value;
	}
	
	public CharSequence getCharSequence(){
		return value;
	}
	
	@Override
	public void copyToDataField(DataField field) {
		if (field instanceof StringDataField) {
			field.setValue(value);
		}else{
			field.fromString(value);
		}
	}

	@Override
	public void setValue(Object _value) {
		this.value.setLength(0);
		if (_value instanceof CharSequence){
			this.value.append((CharSequence)_value);
		}else if (_value instanceof char[]){
			this.value.append((char[])_value);
		}else{
			this.value.append(_value.toString());
		}
	}


	@Override
	public void setValue(DataField field) {
		if (!field.isNull()){
			this.value.setLength(0);
			if (field instanceof StringDataField) {
				this.value.append(((StringDataField) field).getCharSequence());
			} else {
				this.value.append(field.toString());
			}
		}else{
			throw new IllegalArgumentException("Can't assign value of field "+field.getMetadata().getName()+
					" (type " + field.getMetadata().getTypeAsString()+") to String value");
		}
		
	}


	@Override public int compareTo(TLValue val){
		if (val instanceof TLStringValue){
			return Compare.compare(value, ((TLStringValue)val).value);
		}
		return Compare.compare(value, val.toString());
	}
	
	@Override
	public TLValue duplicate() {
		return new TLStringValue(new StringBuilder(value));
	}
	
	@Override public String toString(){
		return value.toString();
	}


	public char charAt(int arg0) {
		return value.charAt(arg0);
	}


	public int length() {
		return value.length();
	}


	public CharSequence subSequence(int arg0, int arg1) {
		return value.subSequence(arg0, arg1);
	}
	
	@Override public int hashCode(){
		return StringUtils.hashCode(value);
	}

	@Override public boolean equals(Object obj){
		if (this==obj) return true;
		if (obj instanceof TLStringValue){
			return org.jetel.util.string.Compare.equals(value, ((TLStringValue)obj).value);
		}
		return false;
	}
}
