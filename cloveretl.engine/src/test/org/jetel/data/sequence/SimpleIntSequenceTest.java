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
package test.org.jetel.data.sequence;

import org.jetel.data.sequence.SimpleIntSequence;

import junit.framework.TestCase;

/**
 * @author david
 * @since  31.5.2005
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
public class SimpleIntSequenceTest extends TestCase {
    
    SimpleIntSequence sequence;
    
    /* (non-Javadoc)
     * @see junit.framework.TestCase#setUp()
     */
    protected void setUp() throws Exception {
        // TODO Auto-generated method stub
        super.setUp();
        sequence=new SimpleIntSequence("Test","c:\\tmp\\sequence2.dat",0,1,20);
        sequence.init();
    }

    public void test_1(){
        assertEquals("different",sequence.currentValueInt(),0);
        assertEquals("different",sequence.nextValueInt(),1);
        sequence.close();
    }
    
    public void test_2(){
        sequence.init();
        assertEquals("different",sequence.currentValueInt(),1);
        sequence.close();
    }
    
    protected void tearDown(){
        sequence.close();
    }
    
}
