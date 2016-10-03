/*
 * Copyright (c) 2004-2005 Javlin Consulting s.r.o. All rights reserved.
 * 
 * $Header$
 */
package org.jetel.util;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * Utilities for easier work with collections.
 * 
 * @author vitaz
 */
public class CollectionUtils {
    
    public static void addAll(Collection col, Iterator it) {
        while (it.hasNext()) {
        	col.add(it.next());
        }
    }
    
    public static int[] toIntegerArray(Collection<Integer> col) {
        final int[] ret = new int[col.size()];
        int idx = 0;
        for (Iterator<Integer> it = col.iterator(); it.hasNext();) {
            ret[idx++] = it.next().intValue();
        }
        return ret;
    }
    
    public static boolean[] toBooleanArray(Collection<Boolean> col) {
        final boolean[] ret = new boolean[col.size()];
        int idx = 0;
        for (Iterator<Boolean> it = col.iterator(); it.hasNext();) {
            ret[idx++] = it.next().booleanValue();
        }
        return ret;
    }

    public static <T> T[] toArray(Iterator<T> iterator, T[] arr) {
        final List<T> ret = new ArrayList<T>();
        while (iterator.hasNext()) {
        	ret.add(iterator.next());
        }
        
        return ret.toArray(arr);
    }
    
    /**
     * Wraps an array of int[] as List&lt;Integer&gt;
     * Used to bypass the problem that arrays aren't autoboxed/unboxed
     * @param arr arrray of int[]
     * @return A List&lt;Integer&gt; backed by given array. Supports operations supported by AbstractList.
     */
    public static List<Integer> fromIntArray(final int[] arr){
    	return new AbstractList<Integer>() {

			@Override
			public Integer get(int index) {
				return arr[index];
			}

			@Override
			public int size() {
				return arr.length;
			}
		};
    }
}
