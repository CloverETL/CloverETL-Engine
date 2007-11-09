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
 * Created on 1.11.2007 by dadik
 *
 */

package org.jetel.interpreter.data;

import org.jetel.data.DataField;

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
	public void setValue(Object _value) {
		//do nothing
	}

	@Override
	public String toString(){
		return "<null>";
	}

	@Override
	public void setValue(DataField field) {
		// nothing to do
		
	}

	@Override
	public int compareTo(TLValue arg0) {
		return -1;
	}

	@Override
	public TLValue duplicate() {
		return this;
	}
	
}
