/*
 * Copyright (c) 2004-2005 OpenTech s.r.o. All rights reserved.
 * 
 * $Header$
 */
package cz.opentech.jdbc.xlsdriver;

import java.util.Iterator;

import junit.framework.TestCase;

import cz.opentech.jdbc.xlsdriver.db.util.Blocks;

/**
 * @author vitaz
 */
public class BlocksTest extends TestCase {

    public void testAdd() {
        Blocks b1 = new Blocks();
        b1.add(1);
        b1.add(2, 3);
        b1.add(5, 8);
        b1.add(9, Blocks.INF);
        
        Blocks b2 = new Blocks();
        b2.add(1,3);
        b2.add(5, 10022);
        b2.add(324, Blocks.INF);
        
        assertEquals(b1, b2);
    }

    public void testRemove() {
        Blocks b1 = new Blocks();
        b1.add(1, 10);
        
        b1.remove(5, 6);
        
        Blocks b2 = new Blocks();
        b2.add(1, 4);
        b2.add(7, 10);
        
        assertEquals(b1, b2);
    }
    
    public void testParse() {
        Blocks b1 = Blocks.fromString("1,2-3, 5 - 8 , 9    -");
        
        Blocks b2 = new Blocks();
        b2.add(1, 3);
        b2.add(5, Blocks.INF);
        
        assertEquals(b1, b2);
    }
    
    public void testSize() {
        int SIZE = 0;
        Blocks b = new Blocks();
        
        b.add(1);
        assertEquals(b.size(), SIZE += 1);
        
        b.add(3, 6);
        assertEquals(b.size(), SIZE += 4);
        
        b.add(7, 7);
        assertEquals(b.size(), SIZE += 1);
        
        b.add(2, 9);
        assertEquals(b.size(), SIZE = 9);
        
        b.add(20, Blocks.INF);
        assertEquals(b.size(), SIZE = Blocks.INF);
    }
    
    public static void testIterator() {
        Blocks b = new Blocks();
        
        b.add(1);
        b.add(4, 6);
        b.add(23, 25);
        b.add(5, 7);
        
        Iterator it = b.iterator();
        
        assertEquals(it.next(), new Integer(1));
        assertEquals(it.next(), new Integer(4));
        assertEquals(it.next(), new Integer(5));
        assertEquals(it.next(), new Integer(6));
        assertEquals(it.next(), new Integer(7));
        assertEquals(it.next(), new Integer(23));
        assertEquals(it.next(), new Integer(24));
        assertEquals(it.next(), new Integer(25));
        assertFalse(it.hasNext());
    }    
}
