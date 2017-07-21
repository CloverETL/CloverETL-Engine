package org.jetel.util;

import java.util.NoSuchElementException;

import org.jetel.test.CloverTestCase;

public class NumberIteratorTest extends CloverTestCase {
	
	NumberIterator numbers;
	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
    	numbers = new NumberIterator("*--9,-7--5,-3,-1,1-3,5,7,9-*",-10,10);
	}
	
	
    public void test_1(){
    	while (numbers.hasNext()){
    		System.out.print(numbers.next() + ",");
    	}
     }

    public void test_2(){
    	for (int i=0;i<14;i++){
    		System.out.print(numbers.next() + ",");
    	}
    	try{
    		numbers.next();
    		fail("There is not next elemet!!!");
    	}catch(NoSuchElementException e){
     	}
    	System.out.println();
    	numbers.reset();
    	for (int i=0;i<14;i++){
    		System.out.print(numbers.next() + ",");
    	}
     }
    
    public void test_3(){
    	numbers = new NumberIterator("*",0,10);
    	int i=0;
    	while (numbers.hasNext()){
    		System.out.print(numbers.next() + ",");
    		i++;
    	}
    	assertEquals(i, 11);
     }

    @Override
    protected void tearDown() throws Exception {
    	super.tearDown();
    }
}
