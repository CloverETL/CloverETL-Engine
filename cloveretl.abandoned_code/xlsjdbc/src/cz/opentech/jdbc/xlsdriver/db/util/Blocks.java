/*
 * Copyright (c) 2004-2005 OpenTech s.r.o. All rights reserved.
 * 
 * $Header$
 */
package cz.opentech.jdbc.xlsdriver.db.util;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;

/**
 * Implementation of a set of integer intervals.
 * 
 * @author vitek
 */
public class Blocks {
    
    public static final int INF = Integer.MAX_VALUE;
    public static final int NEG_INF = Integer.MIN_VALUE;
    
    private final LinkedList list = new LinkedList();
    private long size; // cardinality of intervals
    private boolean isInf, isNInf;
    
    /**
     * Tests if the setis empty.
     * 
     * @return
     */
    public boolean isEmpty() {
        return list.isEmpty();
    }
    
    /**
     * Tests if a number is in this set of intervals.
     * 
     * @param i
     * @return
     */
    public boolean contains(int i) {
        for (Iterator it = list.iterator(); it.hasNext();) {
            int[] pair = (int[])it.next();
            if (i >= pair[0] && i <= pair[1]) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Gets the count of all numbers in this set of intervals.
     * 
     * @return
     */
    public int size() {
        return isInf || isNInf ? INF : (int)Math.min(size, INF);
    }
    
    /**
     * 
     * @param b
     */
    public void union(Blocks b) {
        for (Iterator it = b.list.iterator(); it.hasNext();) {
            int[] pair = (int[]) it.next();
            add(pair[0], pair[1]);
        }
    }
    
    /**
     * 
     * @param number
     */
    public void add(int number) {
        add(number, number);
    }
    
    /**
     * 
     * @param from
     * @param to
     */
    public void add(int from, int to) {
        add(new int[] {from, to});
    }
    private void add(int[] pair) {
        if (pair[1] < pair[0]) {
            throw new IllegalArgumentException(pair[0] + "<=" + pair[1] + " failed");
        }
        
        isNInf = (pair[0] == NEG_INF);
        isInf = (pair[1] == INF);        
                
        for (ListIterator it = list.listIterator(); it.hasNext();) {
            int[] p = (int[]) it.next();
            int cmp = compare(pair, p);
            if (cmp == -1) {
                it.previous();
                it.add(pair);
                size += pair[1] - pair[0] + 1;
                return;
            } else if (cmp == 0
                    || (pair[1] + 1 == p[0] || p[1] + 1 == pair[0])) {
                it.remove();
                size -= p[1] - p[0] + 1;
                internalUnion(pair, p);
            }
        }
        list.add(pair);
        size += pair[1] - pair[0] + 1;
    }
    public void remove(int from, int to) {

        isNInf = (from == NEG_INF);
        isInf = (to == INF);                
        
    	for (ListIterator it = list.listIterator(); it.hasNext();) {
    		int[] pair = (int[]) it.next();
    		int lo = pair[0], hi = pair[1];
    		if (from <= lo && hi <= to) {
    			it.remove();
    			size -= hi - lo + 1;
    		} else {
    		    int psize = 0;
	    		if (lo < from && from <= hi) {
	    			pair[1] = from - 1;
	    			psize += pair[1] - pair[0] + 1;
	    			pair = null;
	    		}
	    		if (lo <= to && to < hi) {
	    			if (pair == null) {
	    				pair = new int[2];
		    			it.add(pair);
	    			}
	    			pair[0] = to + 1;
	    			pair[1] = hi;
	    			psize += pair[1] - pair[0] + 1;
	    		}
	    		size -= (hi - lo + 1) - psize;
    		}
    	}
    }
    
    /**
     * Iterates over all numbers in this set of intervals.
     * 
     * @see java.lang.Object#toString()
     */
    public Iterator iterator() {
        final int listSize = list.size();
        final int maxNum = listSize > 0 ? ((int[]) list.get(listSize - 1))[1] : -1;
        return new Iterator() {
            int pairIdx = 0;
            Integer curr = null;
            public boolean hasNext() {
                return pairIdx < listSize - 1 || (pairIdx == listSize - 1
                        && (curr == null || curr.intValue() < maxNum));
            }
            public Object next() {
                if (hasNext() == false) {
                    throw new NoSuchElementException();
                }
                int[] pair = (int[]) list.get(pairIdx);
                if (curr != null) {
	                int lastVal = curr.intValue();
	                if (lastVal < pair[1]) {
	                    return curr = new Integer(++lastVal);
	                } else {
	                    pairIdx += 1;
	                    pair = (int[]) list.get(pairIdx);
	                    return curr = new Integer(pair[0]);
	                }
                } else {
                    return curr = new Integer(pair[0]);                    
                }
            }
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
    
    /**
     * 
     * @return
     */
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (this == o) {
            return true;
        }
        if (o instanceof Blocks) {
            return internalEquals(this, (Blocks) o);
        }
        return false;
    }
    private static boolean internalEquals(Blocks b1, Blocks b2) {
        if (b1.size() != b2.size()) {
            return false;
        }
        if (b1.list.size() != b2.list.size()) {
            return false;
        }
        Iterator it1 = b1.list.iterator();
        Iterator it2 = b2.list.iterator();
        while (it1.hasNext()) { // and it2.hasNext()
            int[] p1 = (int[]) it1.next();
            int[] p2 = (int[]) it2.next();
            if(p1[0] != p2[0] || p1[1] != p2[1]) {
                return false;
            }
        }
        return true;
    }
    
    /**
     * Gets the string reprezentation of this set of intervals.
     * 
     * @see java.lang.Object#toString()
     */
    public String toString() {
        StringBuffer sb = new StringBuffer();
        for (Iterator it = list.iterator(); it.hasNext();) {
            if (sb.length() > 0) {
                sb.append(",");
            }
            int[] pair = (int[])it.next();
            if (pair[0] > Integer.MIN_VALUE) {
                sb.append(pair[0]);
            }
            if (pair[0] != pair[1]) {
                sb.append('-');
                if (pair[1] < Integer.MAX_VALUE) {
                    sb.append(pair[1]);
                }
            }
        }
        return sb.toString();
    }
    
    /**
     * 
     * @param str
     * @return
     */
    public static Blocks fromString(String str) {
        return fromString(str, new Blocks());
    }
    
    /**
     * Parses the string to a set of intervals.
     * 
     * @param str
     * @param b
     * @return
     */
    public static Blocks fromString(String str, Blocks b) {
        StringTokenizer tok = new StringTokenizer(str, ",");
        while (tok.hasMoreTokens()) {
            String pstr = tok.nextToken();
            int idx = pstr.indexOf('-');
            String[] sa = new String[2];
            if (idx != -1) {
                sa[0] = pstr.substring(0, idx);
                sa[1] = pstr.substring(idx + 1);
            } else {
                sa[0] = sa[1] = pstr;
            }
            
            int[] pair = new int[2];
            for (int i = 0; i < sa.length; i++) {
                String s = sa[i].trim();
                if (s.length() > 0) {
                    pair[i] = Integer.parseInt(s);
                } else {
                    pair[i] = i%2==0 ? Integer.MIN_VALUE : Integer.MAX_VALUE;
                    sa[i] = null;
                }
            }
            if (sa[0] != null || sa[1] != null) {
                b.add(pair);
            }
        }
        return b;
    }
    
    // -1 if p1 < p2, 1 iff p1 > p2, 0 if p1 intersects p2
    private static int compare(int[] p1, int[] p2) {
        if (p1[1] < p2[0]) return -1;
        if(p1[0] > p2[1]) return 1;
        return 0;
    }
    // result in p1
    private static void internalUnion(int[] p1, int[] p2) {
        p1[0] = Math.min(p1[0], p2[0]);
        p1[1] = Math.max(p1[1], p2[1]);
    }
}