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
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;


/**
 * Simple cache based on LinkedHashMap.<br>
 * It keeps LRU order of accessed items. When maximum capacity
 * is reached and new item is to be stored, the Eldest entry is
 * automatically removed. 
 * When simple cache is created it can be only one entry upon one key, if you 
 * want to have more entries upon one key (using of ArrayList) after creating 
 * the object call method enableDuplicity()
 * 
 * @author david pavlis
 * @author avackova <agata.vackova@javlinconsulting.cz> ; 
* (c) JavlinConsulting s.r.o.
*	www.javlinconsulting.cz
 * @since  24.11.2005
 *
 */
public class SimpleCache {
    
    protected Map map;
    protected DuplicateKeyMap keyMap = null;
    
    protected transient int totalSize = 0;
    protected int maxSize;
    
    protected Object savedKey;
    protected Object[] result = new Object[1];
    protected List resultList = new ArrayList(1);

    /**
     * Creates cache with initial size of 16 entries.
     * Maximum size is defaulted to 100.
     */
    public SimpleCache() {
		StoreMap tmp = new StoreMap();
		map = Collections.synchronizedMap(tmp);
		maxSize = tmp.getMaxEntries();
	}
    
    /**
     * Creates cache with initial size equal to parameter.
     * Maximum size is defaulted to 100.
     * 
     * @param initialCapacity
     */
    public SimpleCache(int initialCapacity) {
		StoreMap tmp = new StoreMap(initialCapacity);
		map = Collections.synchronizedMap(tmp);
		maxSize = tmp.getMaxEntries();
	}
    
    /**
     * Creates cache with initial capacity and maximum capacity
     * equal to specified parameters
     * 
     * @param initialCapacity
     * @param maxCapacity		
     */
    public SimpleCache(int initialCapacity, int maxCapacity) {
		StoreMap tmp = new StoreMap(initialCapacity, maxCapacity);
		map = Collections.synchronizedMap(tmp);
		maxSize = tmp.getMaxEntries();
	}
    
    /**
     * This method turns on duplicity entries upon the same key
     * 
     */
    public void enableDuplicity(){
    	keyMap = new DuplicateKeyMap(map);
    }
    
    public Object[] getAll(Object key, Object[] returnType){
    	if (keyMap != null) {
    		return keyMap.getAll(key, returnType);
    	}
    	result[0] = map.get(key);
    	return result;
    }
    
    public List getAll(Object key){
    	if (keyMap != null) {
    		return keyMap.getAll(key);
    	}
    	if (!map.containsKey(key)) return null;
    	resultList.clear();
    	resultList.add(map.get(key));
    	return resultList;
    }
    
    /**
     * 
     * @param key
     * @return first entry upon the given key
     */
    public Object get(Object key){
    	savedKey = key;
    	return (keyMap == null ? map.get(key) : keyMap.get(key) );
    }
    
    /**
     * @return next entry upon the key from last used method get
     */
    public Object getNext(){
    	return (keyMap == null ? null :keyMap.getNext() );
    }
    
    /**
     * Stores the given value under the given key in the cache. If the cache is full, the eldest entry or entries
     * (in case when duplicity is enabled) are removed so that storing of the new value succeeds.
     *
     * @param key the key under which the value should be stored
     * @param value the value to be stored
     *
     * @return <code>true</code> if the value was successfully stored in the cache, <code>false</code> otherwise 
     */
    public boolean put(Object key, Object value) {
		if (keyMap != null) {
			if (totalSize >= maxSize) {
				// Check if there is a mapping for the current key and if it is the only mapping present.
				// The call to the get() method here ensures that the order of entries is updated properly
				// in the underlying linked hash map and thus the LRU algorithm works properly.
				if (keyMap.get(key) != null && keyMap.size() == 1) {
					// If there is only a single mapping, all data records share a single key. Adding another
					// entry would require the size of the cache to be grown and that is not supported.
					return false;
				}

				// Remove the eldest entry now. If the get() method was not called earlier, we could remove
				// previously stored data records for the current key.
				Iterator iterator = map.entrySet().iterator();
				Entry eldestEntry = (Entry) iterator.next();
				iterator.remove();

				totalSize -= ((List) eldestEntry.getValue()).size();
			}

			keyMap.put(key, value);
			totalSize++;
		} else {
			map.put(key, value);

			if (map.size() > maxSize) {
				Iterator iterator = map.entrySet().iterator();
				iterator.next();
				iterator.remove();
	    	}
		}

		savedKey = key;

		return true;
    }

    /**
     * @return number of records found for last used key
     */
    public int getNumFound(){
    	if (keyMap==null){
    		return map.containsKey(savedKey) ? 1 : 0; 
    	}else{
    		return keyMap.getNumFound();
    	}
    }
    
    /**
     * Returns <tt>true</tt> if this map contains a mapping for the
     * specified key.
     *
     * @param   key   The key whose presence in this map is to be tested
     * @return <tt>true</tt> if this map contains a mapping for the specified
     * key.
     */
    public boolean containsKey(Object key){
    	return map.containsKey(key);
    }
    
    public void clear(){
    	map.clear();
    	savedKey = null;
    	totalSize = 0;
    }
    
    /**
     * Class for storing limited numbers of entries
     * 
     * @author avackova
     *
     */
    @SuppressWarnings("Se")
    class StoreMap extends LinkedHashMap{
    	
		private static final long serialVersionUID = 7137792243985321904L;
		
		private static final int DEFAULT_MAX_ENTRIES = 100;
        private static final boolean ACCESS_ORDER=true;
        
        int max_entries;
        
        //Following variables are not actually used, because method removeEldestEntry always returns false
        boolean remove;
        Object tmp;
        int eldestSize;

        /**
         * Creates cache with initial size of 16 entries.
         * Maximum size is defaulted to 100.
         */
    	StoreMap(){
            super(16,(float)0.75,ACCESS_ORDER);
            max_entries=DEFAULT_MAX_ENTRIES;
    	}
    	
        /**
         * Creates cache with initial size equal to parameter.
         * Maximum size is defaulted to 100.
         * 
         * @param initialCapacity
         */
    	StoreMap(int initialCapacity){
            super(initialCapacity,(float)0.75,ACCESS_ORDER);
            max_entries= (initialCapacity > DEFAULT_MAX_ENTRIES ? initialCapacity : DEFAULT_MAX_ENTRIES);
        }
    	
	    /**
	     * Creates cache with initial capacity and maximum capacity
	     * equal to specified parameters
	     * 
	     * @param initialCapacity
	     * @param maxCapacity
	     */
	   	StoreMap(int initialCapacity,int maxCapacity){
	        super((initialCapacity > 16 ? initialCapacity : 16),(float)0.75,ACCESS_ORDER);
	        max_entries= maxCapacity;
	    }
    	
        /* (non-Javadoc)
         * @see java.util.LinkedHashMap#removeEldestEntry(java.util.Map.Entry)
         * 
         * In present implementation this method always returns false, because
         * total size is checked before calling method LinkedHashMap.put and, 
         * when it is greater then maximal size the eldest entry is removed
         */
        @Override
		protected boolean removeEldestEntry(Map.Entry eldest) {
         	remove = totalSize > max_entries;
//        	if (remove) {
//	        	tmp = eldest.getValue();
//	        	if (tmp instanceof ArrayList) {
//					eldestSize = ((ArrayList)tmp).size();
//				}
//	       		totalSize = totalSize - eldestSize;
//        	}
	        return remove;
         }
     	
        int getMaxEntries(){
        	return max_entries;
        }
    }
}

