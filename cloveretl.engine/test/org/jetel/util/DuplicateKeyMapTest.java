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
 * Created on 15.5.2006
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package org.jetel.util;

import java.util.HashMap;

import org.jetel.test.CloverTestCase;
import org.jetel.util.primitive.DuplicateKeyMap;

public class DuplicateKeyMapTest extends CloverTestCase {

    DuplicateKeyMap map;
    @Override
	protected void setUp() throws Exception {
        super.setUp();
        map=new DuplicateKeyMap(new HashMap());
        
        map.put("1","one");
        map.put("1","jedna");
        map.put("1","uno");
        map.put("2","two");
        map.put("3","three");
        map.put("3","tri");
        map.put("3","drei");
        map.put("3","troix");
        
        
    }

    @Override
	protected void tearDown() throws Exception {
        super.tearDown();
        map.clear();
    }

    /*
     * Test method for 'org.jetel.util.DuplicateKeyMap.size()'
     */
    public void testSize() {
        assertEquals(map.size(),3);
    }

    /*
     * Test method for 'org.jetel.util.DuplicateKeyMap.totalSize()'
     */
    public void testTotalSize() {
        assertEquals(map.totalSize(),8);
    }

    /*
     * Test method for 'org.jetel.util.DuplicateKeyMap.containsKey(Object)'
     */
    public void testContainsKey() {
        assertTrue(map.containsKey("3"));
    }

    /*
     * Test method for 'org.jetel.util.DuplicateKeyMap.get(Object)'
     */
    public void testGet() {
        assertEquals(map.get("1"),"one");
    }

    /*
     * Test method for 'org.jetel.util.DuplicateKeyMap.getNext()'
     */
    public void testGetNext() {
        map.get("3");
        assertEquals(map.getNext(),"tri");
        map.get("2");
        assertNull(map.getNext());
    }

    /*
     * Test method for 'org.jetel.util.DuplicateKeyMap.put(Object, Object)'
     */
    public void testPut() {
        map.put("3","tres");
        map.get("3");
        map.getNext();
        map.getNext();
        map.getNext();
        assertEquals(map.getNext(),"tres");
    }

    /*
     * Test method for 'org.jetel.util.DuplicateKeyMap.getNumFound()'
     */
    public void testGetNumFound() {
        map.get("2");
        assertEquals(map.getNumFound(),1);
        map.get("1");
        assertEquals(map.getNumFound(),3);

    }

    public void testContainsValue(){
        assertTrue(map.containsValue("troix"));
        assertFalse(map.containsValue("xxx"));
    }
    
    public void testClear() {
        map.clear();
        assertNull(map.get("2"));
    }

}
