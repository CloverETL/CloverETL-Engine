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
package org.jetel.ctl.debug;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.jetel.ctl.DebugTransformLangExecutor;
import org.jetel.ctl.data.TLType;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;

public class Variable implements Cloneable, Serializable {
	
	private static final long serialVersionUID = 1L;
	
	protected String name;
	protected Object value;
	protected boolean global;
	protected TLType type;
	protected long id;
	
	public Variable(){
		name=null;
		value=null;
		type=TLType.UNKNOWN;
	}
	
	public Variable(String name){
		this.name=name;
		value=null;
		type=TLType.UNKNOWN;
	}
	
	
	public Variable(String name, TLType type, boolean global, Object value, long id){
		this.name=name;
		this.type=type;
		this.global=global;
		this.value=value;
		this.id = id;
	}
	
	public TLType getType() {
		return type;
	}
	public void setType(TLType type) {
		this.type = type;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public Object getValue() {
		return value;
	}
	public void setValue(Object value) {
		this.value = value;
	}
	
	public long getId() {
		return id;
	}
	
	@Override
	public String toString(){
		return this.name + ":" + this.type + ":" + value;
	}
	
	public Variable serializableCopy() {
		try {
			Variable copy = (Variable)super.clone();
			copy.value = deepCopy(value);
			return copy;
		} catch (CloneNotSupportedException e) {
			throw new RuntimeException(e);
		}
	}
	
	public static Object deepCopy(Object value) {
		if (value instanceof Date) {
			return new Date(((Date)value).getTime());
		} else if (value instanceof List<?>) {
			return deepCopy((List<?>)value); 
		} else if (value instanceof Map<?, ?>) {
			return deepCopy((Map<?, ?>)value);
		} else if (value instanceof DataRecord) {
			return SerializedDataRecord.fromDataRecord((DataRecord)value);
		} else if (value instanceof DataField) {
			return SerializedDataField.fromDataField((DataField)value);
		} else if (value == DebugTransformLangExecutor.VOID_RESULT_MARKER) {
			Variable voidVariable = new Variable();
			voidVariable.setType(TLType.VOID);
			return voidVariable;
		} else if (value instanceof Variable){
			return ((Variable)value).serializableCopy();
		} else {
			return value; // immutable objects
		}
	}
	
	private static List<?> deepCopy(List<?> list) {
		List<Object> result = new ArrayList<>(list.size());
		for (Object o : list) {
			result.add(deepCopy(o));
		}
		return result;
	}
	
	private static Map<?, ?> deepCopy(Map<?, ?> map) {
		Map<Object, Object> result = new LinkedHashMap<>(map.size());
		for (Entry<?, ?> e : map.entrySet()) {
			result.put(deepCopy(e.getKey()), deepCopy(e.getValue()));
		}
		return result;
	}
}