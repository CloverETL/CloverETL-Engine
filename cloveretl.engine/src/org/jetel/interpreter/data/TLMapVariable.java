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
 * Created on 21.3.2007
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package org.jetel.interpreter.data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.jetel.data.DataField;

public class TLMapVariable extends TLVariable {

    
    public TLMapVariable(String name) {
        super(name,TLValueType.MAP);
        value.setValue(new HashMap<String,TLValue>());      
    }
    
    public TLMapVariable(String name,int initialSize) {
        super(name,TLValueType.MAP);
        value.setValue(new HashMap<String,TLValue>(initialSize)); 
    }
    
    public TLValue getValue() {
        return value;
    }
    
    public TLValue getValue(int index) {
        throw new UnsupportedOperationException();
    }
    
    public TLValue getValue(String key) {
        TLValue val=(TLValue)value.getMap().get(key);
        return val!=null ? val : TLValue.NULL_VAL;
    }
    
    public void setValue(TLValue value) {
        if (value.type==this.value.type) {
            this.value.getMap().putAll(value.getMap());
        }else {
            throw new RuntimeException("incompatible value type: "+value.type);
        }
    }
    
    public void setValue(int index,TLValue value) {
        throw new UnsupportedOperationException();
    }
    
    public void setValue(String key,TLValue value) {
        if (value.isNull()) {
            this.value.getMap().remove(key);
        }else {
            this.value.getMap().put(key, value);
        }
    }
    
    public void setValue(DataField fieldValue) {
        throw new UnsupportedOperationException();
    }
   
    public void setValue(int index,DataField fieldValue) {
        throw new UnsupportedOperationException();
    }
    
    public void setValue(String key,DataField fieldValue) {
        this.value.getMap().put(key, new TLValue(fieldValue));
    }
 
    @Override public boolean isNULL() {
        return value.getMap().size()==0;
    }
    
    
    public int getLength() {
        return value.getMap().size();
    }
   
    
    @Override public String toString() {
        return name+" : "+value.type.getName()+" : "+value.getMap().toString();
    }
    
    public Map getMap() {
        return value.getMap();
    }

}
