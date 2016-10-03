/*
 * Copyright (c) 2004-2005 Javlin Consulting s.r.o. All rights reserved.
 * 
 * $Header$
 */
package org.jetel.util;

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
}
