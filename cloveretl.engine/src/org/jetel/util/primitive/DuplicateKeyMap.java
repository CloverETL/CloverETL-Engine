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
package org.jetel.util.primitive;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Wrapper/Decorator around Map object which allows duplicate values stored
 * under the same key.<br>
 * Values are stored in ArrayList, modified getters/setters are defined.
 * The memory footprint is higher than normal HashMap/TreeMap since
 * each value/object is wrapped in ArrayList with size at least 1.
 * 
 * @author david pavlis
 * @since  15.5.2006
 * @deprecated use {@link MultiValueMap} instead
 */
@Deprecated
@SuppressWarnings("all")
public class DuplicateKeyMap implements Map {

    private static final int DEFAULT_CONTAINER_INITIAL_SIZE=1; /* used when 1st value
    is stored under specific key*/
    private static final int DEFAULT_CONTAINER_NEXT_SIZE=10; /* used when 2nd and next value
    is stored under specific key*/
    
    protected Map map;
    protected Object savedKey;
    protected boolean savedKeyIsNull = false;
    protected int savedIndex;
    protected ArrayList savedData;
    protected static final Iterator emptyIterator = new Iterator() {
        @Override
		public boolean hasNext() { return false;}
        @Override
		public Object next() { return null;}
        @Override
		public void remove() {}
    };
    
    /**
     * 
     */
    public DuplicateKeyMap(Map map) {
        this.map=map;
        this.savedKey=this.savedData=null;
        this.savedIndex=-1;
    }

    /** 
     * Returns number of key-value pairs.<br>
     * The total number of stored values may be greater
     * since there may be multiple values stored for single key.<br>
     * 
     * @see java.util.Map#size()
     */
    @Override
	public int size() {
        return map.size();
    }
    
    /**
     * Returns total size (total number of values)
     * contaned in this map.<br>
     * May be slow as it walks over all key-value pairs
     * (using Map.values() method call internally).
     * 
     * @return total size
     */
    public int totalSize(){
        int size=0;
        for(Iterator iter=map.values().iterator();iter.hasNext();){
            size+=((ArrayList)iter.next()).size();
        }
        return size;
    }

    /* (non-Javadoc)
     * @see java.util.Map#clear()
     */
    @Override
	public void clear() {
        savedKey=savedData=null;
        savedIndex=-1;
        map.clear();
    }

    /* (non-Javadoc)
     * @see java.util.Map#isEmpty()
     */
    @Override
	public boolean isEmpty() {
        return map.isEmpty();
    }

    /* (non-Javadoc)
     * @see java.util.Map#containsKey(java.lang.Object)
     */
    @Override
	public boolean containsKey(Object key) {
        return map.containsKey(key);
    }

    /* (non-Javadoc)
     * @see java.util.Map#containsValue(java.lang.Object)
     */
    @Override
	public boolean containsValue(Object value) {
        for(Iterator iter=map.values().iterator();iter.hasNext();){
            ArrayList container=(ArrayList)iter.next();
            if (container.contains(value)){
                return true;
            }
        }
        return false;
    }

    /** 
     * This implementation breaks the standard Map.values() contract !!<br>
     * Modifications to returned collection does not change values
     * stored in Map. The collection returned is dynamically generated
     * each time this method is called.<br>
     * The returned collection contains ALL stored values.
     * 
     * @see java.util.Map#values()
     */
    @Override
	public Collection values() {
        Collection col=new ArrayList((int)(1.6*(double)map.size()));
        for(Iterator iter=map.values().iterator();iter.hasNext();){
            ArrayList container=(ArrayList)iter.next();
            col.addAll(container);
        }
        return col;
    }

    /* (non-Javadoc)
     * @see java.util.Map#putAll(java.util.Map)
     */
    @Override
	public void putAll(Map arg0) {
        Map.Entry entry;
      for(Iterator iter=arg0.entrySet().iterator();iter.hasNext();){
          entry=(Map.Entry)iter.next();
          put(entry.getKey(),entry.getValue());
      }
      
    }

    /**
     * This method always returns null !<br> 
     * Since this Map allows duplicate values, they can't be hold in Set
     * object.
     * 
     * @see java.util.Map#entrySet()
     */
    @Override
	public Set entrySet() {
        return map.entrySet();
    }

    /* (non-Javadoc)
     * @see java.util.Map#keySet()
     */
    @Override
	public Set keySet() {
       return map.keySet();
    }

    /* (non-Javadoc)
     * @see java.util.Map#get(java.lang.Object)
     */
    @Override
	public Object get(Object key) {
        ArrayList data=(ArrayList)map.get(key);
        savedKeyIsNull = key == null;
        if (data!=null){
            savedKey=key;
            savedIndex=0;
            savedData=data;
            return data.get(savedIndex);
        }else{
            savedKey=null;
        }
        return null;
    }

    /**
     * This method returns next value stored under the same key
     * which was used for the last get() method call.<br>
     * 
     * @return next value or null if no more values stored
     * @see org.jetel.util.DuplicateKeyMap#get(java.lang.Object)
     */
    public Object getNext() {
        if (savedKey!=null || savedKeyIsNull){
            savedIndex++;
            if (savedData.size()>savedIndex){
                return savedData.get(savedIndex);
            }
        }
        return null;
    }
    
    /**
     * This method returns next value stored under the key
     * passed - provided the key is the same as the one used for the last get() 
     * method call.<br>
     * Otherwise this method behaves like get(java.lang.Object).<br>
     * 
     * 
     * @return next value 
     * @see org.jetel.util.DuplicateKeyMap#get(java.lang.Object)
     */
    public Object getNext(Object key) {
        if (savedKey!=key){
            return get(key);
        }else{
            return getNext();
        }
    }
    
    
    /**
     * Returns array of  all values stored under
     * specified key.<br>
     * If no-value is associated with specified key in the
     * map, null is returned.
     * 
     * @param key   key which is searched for
     * @param returnType type of the returned array
     * @return      array of values stored under specified key or null if
     * no values exist
     * @since 7.1.2007
     */
    public Object[] getAll(Object key,Object[] returnType){
        List data=(List)map.get(key);
        if (data!=null){
            return data.toArray(returnType);
        }else{
            return null;
        }
    }
    
    public List getAll(Object key){
    	return (List) map.get(key);
    }
    
    /* (non-Javadoc)
     * @see java.util.Map#remove(java.lang.Object)
     */
    @Override
	public Object remove(Object key) {
        savedKey=null;
        return map.remove(key);
    }
    
    public boolean remove(Object key, Object value){
        ArrayList data=(ArrayList)map.get(key);
    	if (data == null) {
    		return false;
    	}
    	boolean removed = data.remove(value);
    	if (data.size() == 0) {
    		remove(key);
    	}
    	return removed;
    }

    /* (non-Javadoc)
     * @see java.util.Map#put(java.lang.Object, java.lang.Object)
     */
    @Override
	public Object put(Object arg0, Object arg1) {
        ArrayList container=(ArrayList)map.get(arg0);
        if (container!=null){
            container.ensureCapacity(DEFAULT_CONTAINER_NEXT_SIZE);
            container.add(arg1);
            return container;
        }else{
            container=new ArrayList(DEFAULT_CONTAINER_INITIAL_SIZE);
            container.add(arg1);
            return map.put(arg0,container);
        }
        
    }

    /**
     * Determines how many values were found for specified
     * key.<br>
     * This method should be called after get(Object key) method.
     * 
     * @return number of found values or 0 if nothing was found
     */
    public int getNumFound(){
        if (savedKey!=null && savedData!=null){
            return savedData.size();
        }else{
            return 0;
        }
    }
    
    /**
     * Returns the Map object wrapped by this class (passed-in)
     * to constructor.
     * 
     * @return wrapped Map object
     */
    public Map getMapObject(){
        return this.map;
    }
}
