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

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jetel.data.DataField;
import org.jetel.data.primitive.Numeric;

import com.sun.org.apache.xpath.internal.operations.Equals;

public class TLVariable {
      
    TLValue value;
    protected String name;
    
    public TLVariable(String name,TLValue value) {
        this.value=value;
        this.name=name;
    }
    
    public TLVariable(String name,TLValueType type) {
        this.name=name;
        this.value=new TLValue(type);
    }
    
    protected TLVariable(String name) {
        this.name=name;
        this.value=null;
    }
    
    public TLValueType getType() {
        return value.type;
    }
    
    public final boolean isPrimitive() {
        return value.type.isPrimitive();
    }
 
    public final boolean isArray() {
        return value.type.isArray();
    }
    
    public final  boolean isMap() {
        return value.type==TLValueType.MAP;
    }
    
    public final boolean isObject() {
        return value.type==TLValueType.OBJECT;
    }
    
    public boolean isNULL() {
        return value.isNull();
    }
    
    public String getName() {
        return name;
    }
    
    public int getLength() {
        return -1;
    }
    
    public TLValue getValue() {
        return value;
    }
    
    public TLValue getValue(int index) {
     throw new UnsupportedOperationException() ;
            
    }
    
    public TLValue getValue(String key) {
        throw new UnsupportedOperationException() ;
    }
    
    public void setValue(TLValue value) {
        if (this.value.type!=value.type&&this.value.type.isNumeric()) {
            this.value.getNumeric().setValue(value.getNumeric());
        }else {
            this.value=value;
        }
    }
    
    public boolean setValueStrict(TLValue value) {
        if (this.value.type == value.type) {
            this.value = value;
            return true;
        } else {
            return false;
        }
    }
    
    public void setValue(int index,TLValue value) {
        throw new UnsupportedOperationException() ;
    }
    
    public void setValue(String key,TLValue value) {
        throw new UnsupportedOperationException() ;
    }
    
    public void setValue(DataField fieldValue) {
       setValue(new TLValue(fieldValue));
    }
    
    public void setValueStrict(DataField fieldValue) {
        setValueStrict(new TLValue(fieldValue));
     }
   
    public void setValue(int index,DataField fieldValue) {
        throw new UnsupportedOperationException() ;
    }
    
    public void setValue(String key,DataField fieldValue) {
        throw new UnsupportedOperationException() ;
    }
 
    @Override
    public int hashCode() {
        return name.hashCode();
    }
    
    @Override public boolean equals(Object obj) {
        if (obj instanceof TLVariable) {
            return ((TLVariable)obj).name.equals(name);
        }
        return false;
    }
    
    @Override public String toString() {
        return name+" : "+value.type.getName()+" : "+value.toString();
    }
}
