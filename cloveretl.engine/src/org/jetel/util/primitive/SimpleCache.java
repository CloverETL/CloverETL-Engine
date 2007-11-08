/*
 *    jETeL/Clover - Java based ETL application framework.
 *    Copyright (C) 2002-05  David Pavlis <david_pavlis@hotmail.com> and others.
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
 * Created on 24.11.2005
 *
 */
package org.jetel.util.primitive;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;


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
    
    protected LinkedHashMap map;
    protected DuplicateKeyMap keyMap = null;
    
    protected transient int totalSize = 0;
    protected int maxSize;
    
    protected Iterator iterator;
    protected Entry eldest;
    protected Object savedKey;

    /**
     * Creates cache with initial size of 16 entries.
     * Maximum size is defaulted to 100.
     */
    public SimpleCache(){
    	map = new StoreMap();
    	maxSize = ((StoreMap)map).getMaxEntries();
    }
    
    /**
     * Creates cache with initial size equal to parameter.
     * Maximum size is defaulted to 100.
     * 
     * @param initialCapacity
     */
    public SimpleCache(int initialCapacity){
    	map = new StoreMap(initialCapacity);
       	maxSize = ((StoreMap)map).getMaxEntries();
    }
    
    /**
     * Creates cache with initial capacity and maximum capacity
     * equal to specified parameters
     * 
     * @param initialCapacity
     * @param maxCapacity		
     */
    public SimpleCache(int initialCapacity,int maxCapacity){
    	map = new StoreMap(initialCapacity,maxCapacity);
       	maxSize = ((StoreMap)map).getMaxEntries();
   }
    
    /**
     * This method turns on duplicity entries upon the same key
     * 
     */
    public void enableDuplicity(){
    	keyMap = new DuplicateKeyMap(map);
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
     * This method puts the value upon the given key. When there is reached 
     * maximal capacity eldest object (when duplicity are enabled it is the
     * eldest ArrayList) is removed. In present implementation method 
     * StoreMap.removeEldestEntry always returns false
     * 
     * @param key
     * @param value
     * @return
     */
    public Object put(Object key, Object value){
    	savedKey = key;
		totalSize++;
      	if (totalSize<=maxSize){
    		return (keyMap == null ? 
    				map.put(key,value) : keyMap.put(key,value) );
    	}else if (keyMap==null){
     		return map.put(key,value);
    	}
      	iterator = map.entrySet().iterator();
      	eldest = (Entry)iterator.next();
      	map.remove(eldest.getKey());
    	totalSize = totalSize - ((ArrayList)eldest.getValue()).size();
    	return keyMap.put(key,value);

//    	totalSize++;
//		return (keyMap == null ? map.put(key,value) : keyMap.put(key,value) );
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
    
    /**
     * Class for storing limited numbers of entries
     * 
     * @author avackova
     *
     */
    class StoreMap extends LinkedHashMap{
    	
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

