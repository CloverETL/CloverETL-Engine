/*
 *  jETeL/Clover - Java based ETL application framework.
 *  Created on Apr 23, 2003
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

package org.jetel.exception;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * @author maciorowski
 *
 */
public class ExceptionTestSuite   extends TestSuite  {

public static Test suite() {
	TestSuite suite= new TestSuite("All org.jetel.exception Tests");
	suite.addTest(new TestSuite(org.jetel.exception.BadDataFormatExceptionHandler_FixLenDataParser2_Test.class));
	suite.addTest(new TestSuite(org.jetel.exception.BadDataFormatExceptionHandler_DelimitedDataParserNIO_Test.class));
		
	return suite;
}

	

public static void main (String[] args) {
	junit.textui.TestRunner.run(suite());
	}
}

