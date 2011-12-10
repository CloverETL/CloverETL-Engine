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

import java.util.Collection;

import org.jetel.data.DataField;


public abstract class TLContainerValue extends TLValue {


	public TLContainerValue(TLValueType type) {
		super(type);
	}

	public abstract TLValue getStoredValue(int index);
	
	public abstract TLValue getStoredValue(TLValue key);
	
	public abstract void setStoredValue(int index,TLValue _value);

	public abstract void setStoredValue(TLValue key,TLValue _value);
	
	public abstract Collection<TLValue> getCollection();
	
	 @Override
	public void copyToDataField(DataField field) {
		 throw new UnsupportedOperationException("Can't assign ContainerValue to DataField");
	 }
	
	 public abstract int getLength();
	 
	 @Override
	public void setValue(Object _value){
		 throw new UnsupportedOperationException("Can't assign Object value to ContainerValue");
	 }
	 
	 @Override
	public void setValue(TLValue _value){
		 throw new UnsupportedOperationException("Can't assign TLValue value to ContainerValue");
	 }
	 
	 @Override
	public void setValue(DataField fieldValue) {
	        throw new UnsupportedOperationException();
	 }
	 
	 public boolean contains(TLValue value){
	        throw new UnsupportedOperationException();
	 }
	 
	 /**
	 * clear/remove all items stored in container
	 */
	public abstract void clear();
}
