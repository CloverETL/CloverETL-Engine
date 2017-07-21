/*
 *    jETeL/Clover - Java based ETL application framework.
 *    Copyright (C) 2002-06  David Pavlis <david.pavlis@centrum.cz> and others.
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
 * Created on 17.10.2006
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package org.jetel.util;

import java.text.RuleBasedCollator;

import org.jetel.test.CloverTestCase;
import org.jetel.util.string.Compare;

public class CompareTest extends CloverTestCase {

    
    private RuleBasedCollator col=(RuleBasedCollator)RuleBasedCollator.getInstance();
    
    @Override
	protected void setUp() throws Exception {
        super.setUp();
    }

    public void testCompareCharSequenceCharSequenceRuleBasedCollator() {
        int cmp;
        
        cmp=Compare.compare("abcd", "abcd", col);
        assertEquals(cmp, 0);
        cmp=Compare.compare("abčd", "abcd", col);
        assertTrue(cmp != 0);
        cmp=Compare.compare("abcd", "abcď", col);
        assertTrue(cmp != 0);
    
    }

}
