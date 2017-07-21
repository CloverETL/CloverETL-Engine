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

import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.TransformationGraph;
import org.jetel.test.CloverTestCase;

/**
 * @author david, salamonp
 * @since  31.5.2005
 *
 */
public class SimpleSequenceTest extends CloverTestCase {
    
    Sequence sequence;
    
    /* (non-Javadoc)
     * @see junit.framework.TestCase#setUp()
     */
	private final static String SEQUENCE_FILE = "sequence4.dat";
	private final static String SEQUENCE_FILE_2 = "sequenceFile2.dat";
	
	private final static int ITERATIONS_SEQUENCE_RUNNER = 101751;

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		
		long start = 0;
		long step = 1;
		long numCachedValues = 17;
	    
        sequence = createSequence(SEQUENCE_FILE, start, step, numCachedValues);
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
    	System.out.println("Test 2:");
        for (int i=0;i<30;i++){
            System.out.println(sequence.nextValueInt());
        }
    }
    
    /**
     * Created after implementing CLO-5365
     */
    public void test_concurrent_1() {
    	System.out.println("test_concurrent_1:");
		Sequence s1 = null, s2 = null, s3 = null;
		try {
			s1 = createSequence(SEQUENCE_FILE_2, 10, 2, 123);
			s2 = createSequence(SEQUENCE_FILE_2, 20, 1, 174);
	    	s3 = createSequence(SEQUENCE_FILE_2, 30, 7, 135);
		} catch (ComponentNotReadyException e) {
			fail("Failed while creating sequences in test: test_concurrent_1");
		}
    	
    	Runnable r1 = getSequenceRunner(s1);
    	Runnable r2 = getSequenceRunner(s2);
    	Runnable r3 = getSequenceRunner(s3);
    	
    	r1.run();
    	r2.run();
    	r3.run();
    	
    	// no check, just no exceptions expected
    }
    
    /**
     * also checks range returning
     */
    public void test_inits_when_file_exists() {
    	System.out.println("test_inits_when_file_exists:");
    	assertEquals(sequence.nextValueInt(),0);
    	assertEquals(sequence.nextValueInt(),1);
    	assertEquals(sequence.nextValueInt(),2);
    	assertEquals(sequence.nextValueInt(),3);
    	
    	sequence.free();
    	try {
			sequence = createSequence(SEQUENCE_FILE, 101010, 4, 11);
			sequence.init();
		} catch (ComponentNotReadyException e) {
			fail();
		}
        
        assertEquals(sequence.currentValueInt(),4);
        assertEquals(sequence.nextValueInt(),4);
        assertEquals(sequence.nextValueInt(),8);
        assertEquals(sequence.nextValueInt(),12);
        
        sequence.free();
    	try {
			sequence = createSequence(SEQUENCE_FILE, 101010, 2, 1);
			sequence.init();
		} catch (ComponentNotReadyException e) {
			fail();
		}
        
        assertEquals(sequence.currentValueInt(),16);
        assertEquals(sequence.nextValueInt(),16);
        assertEquals(sequence.nextValueInt(),18);
        assertEquals(sequence.nextValueInt(),20);
        
        sequence.free();
    	try {
			sequence = createSequence(SEQUENCE_FILE, 101010, 1, 3);
			sequence.init();
		} catch (ComponentNotReadyException e) {
			fail();
		}
        
        assertEquals(sequence.currentValueInt(),22);
        assertEquals(sequence.nextValueInt(),22);
        assertEquals(sequence.nextValueInt(),23);
        assertEquals(sequence.nextValueInt(),24);
    }
    
    public void test_resets_in_middle() {
    	System.out.println("test_reset_in_middle:");
    	assertEquals(sequence.nextValueInt(),0);
    	assertEquals(sequence.nextValueInt(),1);
    	assertEquals(sequence.nextValueInt(),2);
    	assertEquals(sequence.nextValueInt(),3);
    	
		sequence.resetValue();
		assertEquals(sequence.nextValueInt(),0);
    	assertEquals(sequence.nextValueInt(),1);
    	sequence.resetValue();
    	sequence.free();

    	try {
			sequence = createSequence(SEQUENCE_FILE, 101010, 4, 11);
			sequence.init();
		} catch (ComponentNotReadyException e) {
			fail();
		}
    	
    	assertEquals(sequence.nextValueInt(),0);
    	assertEquals(sequence.nextValueInt(),4);
    }
    
    private Runnable getSequenceRunner(final Sequence seq) {
    	return new Runnable() {
			@Override
			public void run() {
				for (int i = 0; i < ITERATIONS_SEQUENCE_RUNNER; i++) {
					seq.nextValueLong();
				}
				seq.free();
			}
		};
    }
    
    private Sequence createSequence(String filePath, long start, long step, long numCachedValues) throws ComponentNotReadyException {
    	Sequence seq = SequenceFactory.createSequence(null, "SIMPLE_SEQUENCE", 
        		new Object[]{"",null,"Test",filePath,start,step,numCachedValues}, 
        		new Class[]{String.class,TransformationGraph.class,String.class,String.class,long.class,long.class,long.class});
        seq.init();
        return seq;
    }
    
    @Override
	protected void tearDown(){
        sequence.free();
        (new File(SEQUENCE_FILE)).delete();
        (new File(SEQUENCE_FILE_2)).delete();
    }
    
}
