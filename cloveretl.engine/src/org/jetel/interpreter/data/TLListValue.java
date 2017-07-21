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

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class TLListValue extends TLContainerValue {

	private List<TLValue> valueList;	
	
    public TLListValue() {
        super(TLValueType.LIST);
        valueList=new ArrayList<TLValue>();        
    }
    
    public TLListValue(int initialSize) {
        super(TLValueType.LIST);
        valueList=new ArrayList<TLValue>(initialSize);      
    }
    
    
    @Override
	public Object getValue() {
    	return valueList;
    }
    
    public List<TLValue> getList(){
    	return valueList;
    }
    
    @Override
	public TLValue getStoredValue(int index) {
      return valueList.get(index);
    }
    
    
    @Override
	public TLValue getStoredValue(TLValue key) {
        if (key.type.isNumeric()){
        	return getStoredValue(key.getNumeric().getInt());
        }else{
        	throw new InvalidParameterException("invalid index - type: "+key.type);
        }
    }
    
    public void setStoredValue(TLValue value) {
    	// this is fix for s=s assignment - if the value is actually us, we don't do anything
		if (this == value) {
			return;
		}
		
    	if (value instanceof TLContainerValue){
    		for(TLValue val : ((TLContainerValue)value).getCollection()){
    			valueList.add(val.duplicate());
    		}
        }else if (value==TLNullValue.getInstance()){
        	valueList.clear();
        }else{
            throw new RuntimeException("incompatible value assigned - type: "+value.type);
        }
    }
    
    
    @Override
	public void setValue(TLValue value){
    	setStoredValue(value);
    }
    
    
    @Override
	public Collection<TLValue> getCollection(){
    	return valueList;
    }
    
    /* (non-Javadoc)
     * Following operation is performed based on value type and index value
     * 
     * Type=anyother, index = -1 -> append value at the end of the list
     * Type=any, index >= 0 -> save the value at specified index of the list
     * Value is null, index = -1 -> remove last item from the list
     * Value is null, index >=0 -> remove item at location
     * 
     */
    @Override
	public void setStoredValue(int index, TLValue value) {
        if (value==TLNullValue.getInstance()) {
        		if (index<0)
        			valueList.remove(valueList.size()-1);
        		else
        			valueList.remove(index);
        } else {
            		if (index<0)
            			valueList.add(value.duplicate());
            		else
            			valueList.set(index, value.duplicate());
        }
    }
    
    
    
    @Override
	public void setStoredValue(TLValue key, TLValue value) {
    	if (key.type.isNumeric()){
        	setStoredValue(key.getNumeric().getInt(),value);
        }else{
        	throw new InvalidParameterException("invalid index - type: "+key.type);
        }
    }
    
    @Override
	public int getLength() {
        return valueList.size();
    }
   
    
    @Override public String toString() {
        return TLValueType.LIST+" : "+valueList.toString();
    }
    
    
    public void fill(TLValue value,int count) {
        if (value instanceof TLContainerValue){
        	valueList.addAll(((TLContainerValue)value).getCollection());
        }else{
        	for (int i=0;i<count;i++) valueList.add(value);
        }
    }
    
    @Override
	public TLValue duplicate(){
    	TLListValue newVal=new TLListValue(valueList.size());
    	for(TLValue item : valueList){
    		newVal.valueList.add(item.duplicate());
    	}
    	return newVal;
    }
    
    
    @Override
	public int compareTo(TLValue value){
    	if (value instanceof TLContainerValue){
    		TLValue maxVal1=Collections.max(valueList);
    		TLValue maxVal2=Collections.max(((TLContainerValue)value).getCollection());
    		return maxVal1.compareTo(maxVal2);
    	}else{
    		TLValue maxVal1=Collections.max(valueList);
    		return maxVal1.compareTo(value);
    	}
    }
    
    @Override public int hashCode(){
    	long hash=0;
    	for(TLValue val:valueList){
    		hash+=val.hashCode();
    	}
    	return (int)(hash&0x7FFFFFFF);
    }
    
    @Override public boolean equals(Object value){
    	if (value instanceof TLListValue){
    		Iterator<TLValue> val2=((TLListValue)value).valueList.iterator();
    		for(TLValue val1:valueList){
    			if (val2.hasNext()){
    				if (! val1.equals(val2.next())) return false;
    			}else{
    				return false;
    			}
    		}	
    		if (valueList.size()>0) return true;
    	}
    	return false;
    }
    
    @Override public void clear(){
    	valueList.clear();
    }
    
    @Override public boolean contains(TLValue value){
    	return valueList.contains(value);
    }
}
