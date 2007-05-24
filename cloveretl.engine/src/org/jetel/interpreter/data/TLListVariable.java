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
import java.util.List;

import org.jetel.data.DataField;

public class TLListVariable extends TLVariable {

    public TLListVariable(String name) {
        super(name,TLValueType.LIST);
        value.setValue(new ArrayList<TLValue>());        
    }
    
    public TLListVariable(String name,int initialSize) {
        super(name,TLValueType.LIST);
        value.setValue(new ArrayList<TLValue>(initialSize));        
    }
    
    public TLValue getValue() {
        return value;
    }
    
    public TLValue getValue(int index) {
      return (TLValue) value.getList().get(index);
    }
    
    public TLValue getValue(String key) {
        throw new UnsupportedOperationException();
    }
    
    public void setValue(TLValue value) {
        if (value.type==this.value.type) {
            this.value.getList().addAll(value.getList());
        }else {
            throw new RuntimeException("incompatible value type: "+value.type);
        }
    }
    
    public void setValue(int index,TLValue value) {
        if (index<0) {
            this.value.getList().add(value);
        }else {
            this.value.getList().set(index, value);
        }
    }
    
    public void setValue(String key,TLValue value) {
        throw new UnsupportedOperationException();
    }
    
    public void setValue(DataField fieldValue) {
        throw new UnsupportedOperationException();
    }
   
    public void setValue(int index,DataField fieldValue) {
        value.getList().set(index, new TLValue(fieldValue));
    }
    
    public void setValue(String key,DataField fieldValue) {
        throw new UnsupportedOperationException();
    }
 
    @Override public boolean isNULL() {
        return value.getList().size()==0;
    }
    
    
    public int getLength() {
        return value.getList().size();
    }
   
    
    @Override public String toString() {
        return name+" : "+value.type.getName()+" : "+value.getList().toString();
    }
    
    public List getList() {
        return value.getList();
    }
}
