package org.jetel.util;

import junit.framework.TestCase;

public class WcardTest extends TestCase {

	private WcardPattern fgen;
	
    protected void setUp() throws Exception {
        super.setUp();
        fgen = new WcardPattern();
    }

    public void test_1(){
    	fgen.addPattern("/etc/.*");
    	fgen.addPattern("/etc/*mt?b");
    	fgen.addPattern("/etc/fstab");
    	fgen.addPattern("/etc/*tab*");
    	fgen.addPattern("/?????*");
        System.out.println(fgen.filenames().toString());
    }
    
    protected void tearDown(){
    }

}
