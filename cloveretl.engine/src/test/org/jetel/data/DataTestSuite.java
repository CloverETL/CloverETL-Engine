/*
 *  jETeL/Clover - Java based ETL application framework.
 *  Created on Apr 10, 2003
 *  Copyright (C) 2003, 2002  David Pavlis, Wes Maciorowski
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

package test.org.jetel.data;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * @author maciorowski
 *
 */
public class DataTestSuite  extends TestSuite  {

	public static Test suite() {
	
		TestSuite suite= new TestSuite("All org.jetel.data Tests");
	
		suite.addTest(new TestSuite(test.org.jetel.data.ByteDataFieldTest.class));
		suite.addTest(new TestSuite(test.org.jetel.data.DateDataFieldTest.class));
		suite.addTest(new TestSuite(test.org.jetel.data.IntegerDataFieldTest.class));
		suite.addTest(new TestSuite(test.org.jetel.data.NumericDataFieldTest.class));
		suite.addTest(new TestSuite(test.org.jetel.data.StringDataFieldTest.class));
		suite.addTest(new TestSuite(test.org.jetel.data.FixLenDataParser2Test.class));
		suite.addTest(new TestSuite(test.org.jetel.data.OneRecordPerLinePolicyTest.class));
		suite.addTest(new TestSuite(test.org.jetel.data.FixLenDataFormatterTest.class));
		suite.addTest(new TestSuite(test.org.jetel.data.FixLenDataParserTest.class));
		return suite;
	
	}

	public static void main (String[] args) {
		junit.textui.TestRunner.run(suite());
	}
}
