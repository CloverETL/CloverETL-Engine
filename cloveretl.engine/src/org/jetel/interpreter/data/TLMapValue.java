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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.jetel.data.DataField;

public class TLMapValue extends TLContainerValue {

	private Map<TLValue,TLValue> valueMap;
	
	
    public TLMapValue() {
        super(TLValueType.MAP);
        valueMap=new HashMap<TLValue,TLValue>();
    }
    
    public TLMapValue(int initialSize) {
        super(TLValueType.MAP);
        valueMap=new HashMap<TLValue,TLValue>(initialSize); 
    }
    
    /* (non-Javadoc)
     * @see org.jetel.interpreter.data.TLValue#getValue()
     * 
     * Returns CloverInteger containing length/size of map
     */
    @Override
	public Object getValue() {
    	return valueMap;
    }
    
    public Map getMap(){
    	return valueMap;
    }
    
    public TLValue getValue(int index) {
        throw new UnsupportedOperationException();
    }
    
    public TLValue getValue(TLValue key) {
        TLValue val=valueMap.get(key);
        return val!=null ? val : TLNullValue.getInstance();
    }
    
    @Override
	public void setValue(TLValue value) {
    	setStoredValue(value);
    }
    
    
    public void setStoredValue(TLValue value){
    	// this is fix for s=s assignment - if the value is actually us, we don't do anything
		if (this == value) {
			return;
		}
    	
    	if (value==TLNullValue.getInstance()){
        	valueMap.clear();
    	}else if (value instanceof TLMapValue){
    		putAll(((TLMapValue)value).valueMap);
    	}else if (value instanceof TLContainerValue){
    		putCollection(((TLContainerValue)value).getCollection());
    	}else{
            throw new RuntimeException("can't store into Map value type: "+value.type+" without specified key");
        }
    }
    
    @Override
	public void setStoredValue(int index,TLValue value) {
        throw new UnsupportedOperationException();
    }
    
    @Override
	public void setStoredValue(TLValue key,TLValue value) {
    	if(value==TLNullValue.getInstance()){
    		valueMap.remove(key);
    	}else if (value instanceof TLMapValue){
    			putAll(((TLMapValue)value).valueMap);
    	}else{
        	valueMap.put(key.duplicate(), value.duplicate());
        }
    }
    
    private final void putAll(Map<TLValue,TLValue> pairs){
    	for (Map.Entry<TLValue,TLValue> entry: pairs.entrySet()){
    		valueMap.put(entry.getKey().duplicate(), entry.getValue().duplicate());
    	}
    	
    }
    
    private final void putCollection(Collection<TLValue> col){
    	Iterator<TLValue> iter=col.iterator();
    	while(iter.hasNext()){
    		TLValue key=iter.next();
    		if (iter.hasNext()){
    			TLValue val=iter.next();
    			valueMap.put(key.duplicate(), val.duplicate());
    		}else{
    			break;
    		}
    	}
    }
    
   
    public void setValue(int index,DataField fieldValue) {
        throw new UnsupportedOperationException();
    }
    
    public void setValue(TLValue key,DataField fieldValue) {
        setStoredValue(key, TLValue.convertValue(fieldValue));
    }
 
    
    @Override
	public int getLength() {
        return valueMap.size();
    }
   
    
    @Override public String toString() {
         return TLValueType.MAP.toString()+" : "+valueMap.toString();
    }

	@Override
	public Collection<TLValue> getCollection() {
		return valueMap.values();
	}

	@Override
	public TLValue getStoredValue(int index) {
		throw new UnsupportedOperationException();
	}

	@Override
	public TLValue getStoredValue(TLValue key) {
		return valueMap.get(key);
	}

	

	@Override
	public int compareTo(TLValue value) {
		if (value instanceof TLContainerValue){
    		TLValue maxVal1=Collections.max(valueMap.values());
    		TLValue maxVal2=Collections.max(((TLContainerValue)value).getCollection());
    		return maxVal1.compareTo(maxVal2);
    	}else{
    		TLValue maxVal1=Collections.max(valueMap.values());
    		return maxVal1.compareTo(value);
    	}
	}

	@Override
	public TLValue duplicate() {
		TLMapValue newVal=new TLMapValue(valueMap.size());
		for (Map.Entry<TLValue,TLValue> entry: valueMap.entrySet()){
			newVal.valueMap.put(entry.getKey(), entry.getValue().duplicate());
		}
		return newVal;
	}

	@Override public int hashCode(){
	   	long hash=0;
	   	for(TLValue val:valueMap.values()){
	   		hash+=val.hashCode();
	   	}
    	return (int)(hash&0x7FFFFFFF);
	}
	
	@Override public boolean equals(Object value){
		if (value instanceof TLMapValue){
			Iterator<TLValue> val2=((TLMapValue)value).valueMap.values().iterator();
    		for(TLValue val1:valueMap.values()){
    			if (val2.hasNext()){
    				if (! val1.equals(val2.next())) return false;
    			}else{
    				return false;
    			}
    		}	
    		if (valueMap.size()>0) return true;
		}
		return false;
	}
	
	@Override public void clear(){
		valueMap.clear();
	}
	
	@Override public boolean contains(TLValue value){
    	return valueMap.containsValue(value);
    }
	
}
