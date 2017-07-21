/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2006 Javlin Consulting <info@javlinconsulting>
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
*/
package org.jetel.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.jetel.test.CloverTestCase;

/**
 * Unit test for the data generator.
 *  
 * @author Jan Ausperger (jan.ausperger@javlin.eu)
 *         (c) Javlin, a.s. (www.javlin.eu)
 */
public class DataGeneratorTest extends CloverTestCase {

	DataGenerator dataGenerator = new DataGenerator();
	
    @Override
	protected void setUp() throws Exception {
        super.setUp();
    }

    public void test_randomLong(){
    	long date;
    	long dFrom;
    	long dTo;
    	Map<Long, Integer> aggr = new HashMap<Long, Integer>();
    	Integer res;
		dFrom = -10;
		dTo = 10;
    	for (long i=0; i<1000000; i++) {
    		date = dataGenerator.nextLong(dFrom, dTo);
    		
    		res = aggr.get(Long.valueOf(date));
    		if (res == null) aggr.put(new Long(date), new Integer(1));
    		else aggr.put(new Long(date), new Integer(res.intValue() + 1));
    		
    		assertTrue(date >= dFrom && date<=dTo);
    	}
    	for (Entry<Long, Integer> e: aggr.entrySet()) {
    		System.out.println(e.getKey() + " " + e.getValue());
    	}
    }
    
    @Override
	protected void tearDown(){
    }

}
