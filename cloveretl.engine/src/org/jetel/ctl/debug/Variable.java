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

import org.jetel.ctl.TransformLangExecutor;
import org.jetel.ctl.data.TLType;

public class Variable implements Cloneable, Serializable {
	
	private static final long serialVersionUID = 1L;
	
	protected String name;
	protected Object value;
	protected boolean global;
	protected TLType type;
	
	public Variable(){
		name=null;
		value=null;
		global=false;
		type=TLType.UNKNOWN;
	}
	
	public Variable(String name){
		this.name=name;
		value=null;
		global=false;
		type=TLType.UNKNOWN;
	}
	
	
	public Variable(String name,TLType type,boolean global,Object value){
		this.name=name;
		this.type=type;
		this.global=global;
		this.value=value;
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
	public boolean isGlobal() {
		return global;
	}
	public void setGlobal(boolean global) {
		this.global = global;
	}
	
	@Override
	public String toString(){
		return this.name + ":" + this.type + ":" + value;
	}
	
	@Override
	public Variable clone() {
		return new Variable(name, type, global, TransformLangExecutor.getDeepCopy(value));
	}
}