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
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package org.jetel.util;

import java.util.LinkedHashMap;
import java.util.*;

/**
 * Simple cached based on LinkedHashMap.<br>
 * It keeps LRU order of accessed items. When maximum capacity
 * is reached and new item is to be stored, the Eldest entry is
 * automatically removed. 
 * 
 * @author david pavlis
 * @since  24.11.2005
 *
 */
public class SimpleCache extends LinkedHashMap {
    
    private static final int DEFAULT_MAX_ENTRIES = 100;
    private static final boolean ACCESS_ORDER=true;
    
    protected int max_entries;

    /**
     * Creates cache with initial size of 16 entries.
     * Maximum size is defaulted to 100.
     */
    public SimpleCache(){
        super(16,(float)0.75,ACCESS_ORDER);
        max_entries=DEFAULT_MAX_ENTRIES;
    }
    
    /**
     * Creates cache with initial size equal to parameter.
     * Maximum size is defaulted to 100.
     * 
     * @param initialCapacity
     */
    public SimpleCache(int initialCapacity){
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
    public SimpleCache(int initialCapacity,int maxCapacity){
        super((initialCapacity > 16 ? initialCapacity : 16),(float)0.75,ACCESS_ORDER);
        max_entries= maxCapacity;
    }
    
    
    protected boolean removeEldestEntry(Map.Entry eldest) {
       return size() > max_entries;
    }


}
