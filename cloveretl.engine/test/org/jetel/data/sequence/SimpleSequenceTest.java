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
 * Created on 31.5.2005
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package org.jetel.data.sequence;


import java.io.File;

import org.jetel.graph.TransformationGraph;
import org.jetel.test.CloverTestCase;

/**
 * @author david
 * @since  31.5.2005
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
public class SimpleSequenceTest extends CloverTestCase {
    
    Sequence sequence;
    
    /* (non-Javadoc)
     * @see junit.framework.TestCase#setUp()
     */
	private final static String SEQUENCE_FILE = "sequence4.dat";

	@Override
	protected void setUp() throws Exception {
		super.setUp();
	    
        sequence = SequenceFactory.createSequence(null, "SIMPLE_SEQUENCE", 
        		new Object[]{"",null,"Test",SEQUENCE_FILE,0,1,17}, 
        		new Class[]{String.class,TransformationGraph.class,String.class,String.class,long.class,int.class,int.class});
        sequence.init();
    }

    public void test_1(){
    	System.out.println("Test 1:");
        sequence.resetValue();
        assertEquals("different",sequence.currentValueInt(),0);
        assertEquals("different",sequence.nextValueInt(),0);
        assertEquals("different",sequence.nextValueInt(),1);
        for (int i=0;i<30;i++){
            System.out.println(sequence.nextValueInt());
        }
    }
    
    public void test_2(){
       // sequence.init();
    	System.out.println("Test 2:");
        for (int i=0;i<30;i++){
            System.out.println(sequence.nextValueInt());
        }
            
    }
    
    @Override
	protected void tearDown(){
        sequence.free();
        (new File(SEQUENCE_FILE)).delete();
    }
    
}
