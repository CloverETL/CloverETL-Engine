/*
 *    jETeL/Clover - Java based ETL application framework.
 *    Copyright (C) 2002-07  David Pavlis <david.pavlis@centrum.cz> and others.
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
 * Created on 20.3.2007
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package org.jetel.interpreter.data;

import org.jetel.data.DataField;


/**
 * Named Container for TLLanguage values
 * 
 * 
 * @author dpavlis <david.pavlis@javlinconsulting.cz>
 *
 */
public class TLVariable {
      
    TLValue value;
    protected String name;
    TLValueType type;
    boolean isNull;		// does this variable represents NULL value ?
    boolean nullable;	// can this variable be assigned with null ?
    
    public TLVariable(String name,TLValue value) {
        this.value=value;
        this.type=value.getType();
        this.name=name;
        this.isNull= (value==TLValue.NULL_VAL) ? false :true;
        this.nullable=true;
    }
    
    public TLVariable(String name,TLValueType type) {
        this.name=name;
        this.type=type;
        this.value=TLValue.create(type);
        this.isNull=true;
        this.nullable=true;
    }
    
    public TLValueType getType() {
        return type;
    }
    
    public final boolean isPrimitive() {
        return type.isPrimitive();
    }
 
    public final boolean isArray() {
        return type.isArray();
    }
    
    public final  boolean isMap() {
        return type==TLValueType.MAP;
    }
    
    public final boolean isObject() {
        return value.type==TLValueType.OBJECT;
    }
    
    public boolean isNULL() {
        return isNull;
    }
    
    public String getName() {
        return name;
    }
    
    
    public TLValue getTLValue() {
        return isNull ? TLValue.NULL_VAL:value;
    }
    
    public boolean setTLValue(TLValue value) {
    	if (value==TLValue.NULL_VAL){
    		if (!nullable) return false; // optionally throw exception
    		isNull=true;
    		return true;
    	}
   		this.value.setValue(value);
   		return true;
    }
    
    public boolean setTLValue(TLVariable variable) {
    	if (variable.isNull){
    		if (!nullable) return false; // optionally throw exception
    		isNull=true;
    		return true;
    	}
   		this.value.setValue(variable.value);
    	return false;
    }
    
        
    public boolean setTLValueStrict(TLValue value) {
    	if (value==TLValue.NULL_VAL){
    		if (!nullable) return false; // optionally throw exception
    		isNull=true;
    		return true;
    	}
    	if (this.value.type == value.type) {
            this.value.setValue(value);
            return true;
        } else {
            return false;
        }
    }
    
    public boolean setTLValue(int index,TLValue value) {
    	if (value==TLValue.NULL_VAL && !nullable) return false; // optionally throw exception
    	((TLContainerValue)this.value).setStoredValue(index,value);
    	return true;
    }
    
    public boolean setTLValue(TLValue key,TLValue value) {
    	if (value==TLValue.NULL_VAL && !nullable) return false; // optionally throw exception
    	((TLContainerValue)this.value).setStoredValue(key,value);
   		return true;
    }
    
    public boolean setTLValue(DataField fieldValue) {
    	if (fieldValue.isNull() && !nullable) return false; // optionally throw exception
    	this.value.setValue(fieldValue);
    	return true;
    }
    
    @Override
    public int hashCode() {
        return name.hashCode();
    }
    
    @Override public boolean equals(Object obj) {
        if (obj instanceof TLVariable) {
            return ((TLVariable)obj).name.equals(name) && ((TLVariable)obj).type==type;
        }
        return false;
    }
    
    @Override public String toString() {
        return name+" : "+type.getName()+" : "+value.toString();
    }

	public boolean isNullable() {
		return nullable;
	}

	public void setNullable(boolean nullable) {
		this.nullable = nullable;
	}
}
