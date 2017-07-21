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

import java.io.IOException;

import org.jetel.test.CloverTestCase;
import org.jetel.util.file.WcardPattern;

/**
 * @author Jan Hadrava, Javlin Consulting (www.javlinconsulting.cz) 
 * 
 * Unit test for wildcard filename matching. 
 */
public class WcardTest extends CloverTestCase {

	private WcardPattern fgen;
	
    @Override
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
        try {
			System.out.println(fgen.filenames().toString());
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
    
    @Override
	protected void tearDown(){
    }

}
