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

public class TLObjectValue extends TLValue {

	Object value;
	
	public TLObjectValue(){
		super(TLValueType.OBJECT);
	}
	
	public TLObjectValue(Object value){
		super(TLValueType.OBJECT);
		this.value=value;
	}
	
	@Override
	public int compareTo(TLValue arg0) {
		return 0;
	}

	@Override
	public void copyToDataField(DataField field) {
		// TODO Auto-generated method stub

	}

	@Override
	public TLValue duplicate() {
		return new TLObjectValue(value);
	}

	@Override
	public Object getValue() {
		return value;
	}

	@Override
	public void setValue(Object _value) {
		this.value=_value;
	}
	
	@Override
	public void setValue(TLValue _value) {
		this.value=_value.getValue();
	}

	@Override
	public void setValue(DataField field) {
		// TODO Auto-generated method stub
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return value.toString();
	}

}
