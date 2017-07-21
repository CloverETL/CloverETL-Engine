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

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.jetel.data.ByteDataField;
import org.jetel.data.DataField;
import org.jetel.data.primitive.ByteArray;

public class TLByteArrayValue extends TLContainerValue {

	ByteArray value;
	TLNumericValue numValue;
	
	public TLByteArrayValue(int size){
		super(TLValueType.BYTE);
		value=new ByteArray(size);
		numValue=new TLNumericValue(TLValueType.INTEGER);
	}
	
	public TLByteArrayValue(){
		super(TLValueType.BYTE);
		value=new ByteArray();
		numValue=new TLNumericValue(TLValueType.INTEGER);
	}
	
	
	public TLByteArrayValue(CharSequence value){
		super(TLValueType.BYTE);
		this.value=new ByteArray(value.length()*2);
		this.value.append(value);
		numValue=new TLNumericValue(TLValueType.INTEGER);
	}
	
	public TLByteArrayValue(ByteArray value){
		super(TLValueType.BYTE);
		this.value=new ByteArray();
		this.value.append(value);
		numValue=new TLNumericValue(TLValueType.INTEGER);
	}
	
	public TLByteArrayValue(byte[] value){
		super(TLValueType.BYTE);
		this.value=new ByteArray(value);
		numValue=new TLNumericValue(TLValueType.INTEGER);
	}
	
	@Override
	public void setValue(TLValue value){
		if (value == TLNullValue.getInstance()){
			this.value.reset();
		}else if (value instanceof TLByteArrayValue){
			this.value.reset();
			this.value.append(((TLByteArrayValue)value).value);
		}else if (value instanceof TLContainerValue){
			this.value.reset();
			for(TLValue val : ((TLContainerValue)value).getCollection()){
				append(val);
			}
		}else{
			this.value.append(value.toString());
		}
	}
	
	
	@Override
	public Object getValue(){
		return value;
	}
	
	public ByteArray getByteAraray(){
		return value;
	}
	
	public CharSequence getCharSequence(){
		return value.toString();
	}
	
	@Override
	public void copyToDataField(DataField field) {
		if (field instanceof ByteDataField) {
			//TODO:suboptimal performance, but internal bytearray may have array.length>length
			field.setValue(value.getValueDuplicate());
		}else{
			field.fromString(((ByteArray)value).toString());
		}
	}

	@Override
	public void setValue(Object _value) {
		this.value.reset();
		if (_value instanceof ByteArray){
			this.value.append((ByteArray)_value);
		}else if (_value instanceof byte[]){
			this.value.append((byte[])_value);
		}else if (_value instanceof CharSequence){
			this.value.append((CharSequence)_value);
		}else{
			this.value.append(_value.toString());
		}
	}


	@Override
	public void setValue(DataField field) {
		if (!field.isNull()){
			this.value.reset();
			if (field instanceof ByteDataField){
				this.value.append((byte[])field.getValue());
			}else{
				this.value.append(field.toString());
			}
		}else{
			throw new IllegalArgumentException("Can't assign value of field "+field.getMetadata().getName()+
					" (type " + field.getMetadata().getTypeAsString()+") to TLByteArray value");
		}
		
	}


	@Override public int compareTo(TLValue val){
		if (val instanceof TLByteArrayValue){
			return this.value.compareTo(((TLByteArrayValue)val).value);
		}else{
			byte max=this.value.max();
			return max-(byte)val.getNumeric().getInt();
		}
	}
	
	@Override
	public TLValue duplicate() {
		return new TLByteArrayValue(new ByteArray(value.getValueDuplicate()));
	}
	
	@Override public String toString(){
		//return Arrays.toString(value.getValue());
		return value.toHexString();
	}


	@Override
	public void clear() {
		value.reset();
	}


	@Override
	public Collection<TLValue> getCollection() {
		List<TLValue> list=new ArrayList<TLValue>(value.length());
		byte[] bvals = value.getValue();
		for(int i=0; i<value.length(); i++){
			numValue.setInt(bvals[i]);
			list.add(numValue.duplicate());
		}
		return list;
	}


	@Override
	public int getLength() {
		return value.length();
	}


	@Override
	public TLValue getStoredValue(int index) {
		numValue.setInt(value.getByte(index));
		return numValue;
	}


	@Override
	public TLValue getStoredValue(TLValue key) {
		   if (key.type.isNumeric()){
	        	return getStoredValue(key.getNumeric().getInt());
	        }else{
	        	throw new InvalidParameterException("invalid index - type: "+key.type);
	        }
	}


	@Override
	public void setStoredValue(int index, TLValue _value) {
		if (_value instanceof TLNumericValue){
			if (index<0){
				value.append((byte)_value.getNumeric().getInt());
			}else{
				value.setValue((byte)_value.getNumeric().getInt(), index);
			}
				
		}else{
			throw new InvalidParameterException("invalid value - type: "+_value.type);
		}
	}


	@Override
	public void setStoredValue(TLValue key, TLValue _value) {
		 if (key.type.isNumeric() && _value instanceof TLNumericValue){
			 		int index=key.getNumeric().getInt();
			 		if (index<0){
			 			value.append((byte)_value.getNumeric().getInt());
			 		}else{
			 			value.setValue((byte)_value.getNumeric().getInt(), index);
			 		}
	        }else{
	        	throw new InvalidParameterException("invalid index - type: "+key.type);
	        }
		
	}
	
	public void append(TLValue _value) {
		if (_value instanceof TLNumericValue){
			value.append((byte)_value.getNumeric().getInt());
		}else{
			throw new InvalidParameterException("invalid value - type: "+_value.type);
		}
	}

}
