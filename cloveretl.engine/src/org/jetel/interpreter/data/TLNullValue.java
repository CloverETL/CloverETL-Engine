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

import java.util.Date;

import org.jetel.data.DataField;
import org.jetel.data.primitive.Numeric;

public class TLNullValue extends TLValue {

	
	private static TLNullValue instance= new TLNullValue();
	
	public static TLNullValue getInstance(){
		return instance;
	}
	
	private TLNullValue(){
		super(TLValueType.NULL);
	}
	
	@Override
	public void copyToDataField(DataField field) {
		field.setNull(true);
	}

	@Override
	public Object getValue() {
		return null;
	}
	
	@Override
	public Date getDate(){
		throw new RuntimeException("this is a TLNull value");
	}
	    
	@Override
	public Numeric getNumeric(){
	    throw new RuntimeException("this is a TLNull value");
	}
	  
	@Override
	public void setValue(Object _value) {
		throw new IllegalArgumentException("Can NOT assign value to TLValue type NULL !");
	}

	@Override
	public void setValue(TLValue _value) {
		throw new IllegalArgumentException("Can NOT assign value to TLValue type NULL !");
	}
	
	@Override
	public String toString(){
		return "<null>";
	}

	@Override
	public void setValue(DataField field) {
		throw new IllegalArgumentException("Can NOT assign value to TLValue type NULL !");
	}

	@Override
	public int compareTo(TLValue arg0) {
		return -1;
	}

	@Override
	public TLValue duplicate() {
		return this;
	}
	
	@Override
	public int hashCode(){
		return 0;
	}
	
	@Override
	public boolean equals(Object obj){
		if (this==obj) return true;
		return false;
	}
}
