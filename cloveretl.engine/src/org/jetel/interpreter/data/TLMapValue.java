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

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.jetel.data.DataField;
import org.jetel.data.primitive.CloverInteger;

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
    public Object getValue() {
    	return new CloverInteger(valueMap.size());
    }
    
    public TLValue getValue(int index) {
        throw new UnsupportedOperationException();
    }
    
    public TLValue getValue(TLValue key) {
        TLValue val=valueMap.get(key);
        return val!=null ? val : TLValue.NULL_VAL;
    }
    
    public void setValue(TLValue value) {
    	if (value instanceof TLContainerValue){
    		putCollection(((TLContainerValue)value).getCollection());
    	}else{
            throw new RuntimeException("incompatible value type: "+value.type);
        }
    }
    
    
    public void setStoredValue(int index,TLValue value) {
        throw new UnsupportedOperationException();
    }
    
    public void setStoredValue(TLValue key,TLValue value) {
    	if(value==TLValue.NULL_VAL){
    		valueMap.remove(key);
    	}else{
        	valueMap.put(key, value);
        }
    }
    
//    private final void putAll(Map<TLValue,TLValue> pairs){
//    	for (Map.Entry<TLValue,TLValue> entry: pairs.entrySet()){
//    		valueMap.put(entry.getKey(), entry.getValue().duplicate());
//    	}
//    	
//    }
    
    private final void putCollection(Collection<TLValue> col){
    	Iterator<TLValue> iter=col.iterator();
    	while(iter.hasNext()){
    		TLValue key=iter.next();
    		if (iter.hasNext()){
    			TLValue val=iter.next();
    			valueMap.put(key, val);
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
	public int compareTo(TLValue arg0) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public TLValue duplicate() {
		// TODO Auto-generated method stub
		return null;
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
	
}
