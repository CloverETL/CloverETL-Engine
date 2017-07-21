/*
 * jETeL/CloverETL - Java based ETL application framework.
 * Copyright (c) Javlin, a.s. (info@cloveretl.com)
 *  
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.jetel.logger;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.spi.LoggingEvent;
import org.jetel.test.CloverTestCase;

/**
 * @author salamonp (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 8. 12. 2014
 */
public class SafeLogLayoutTest extends CloverTestCase {
	
	private SafeLogLayout layout;
	private Logger logger = Logger.getLogger(SafeLogLayoutTest.class);
	
	@Override
	protected void setUp() throws Exception {
		layout = new SafeLogLayout();
	}

	@Override
	protected void tearDown() throws Exception {
		layout = null;
	}

	public void testNonStringMessage() {
		Integer message = new Integer(17);
		Exception e = new Exception("testExceptionMessage");
		StackTraceElement[] stacktrace = new StackTraceElement[2];
		stacktrace[0]= new StackTraceElement("org.thisTest.SafeLogLayoutTest", "methodName1", "fileName1", 1);
		stacktrace[1]= new StackTraceElement("org.thisTest.SafeLogLayoutTest", "methodName2", "fileName2", 2);
		e.setStackTrace(stacktrace);
		LoggingEvent event = new LoggingEvent("org.jetel.logger.SafeLogLayoutTest", logger, Level.ERROR, message, e);
		
		String safeValue = layout.format(event);
		PatternLayout ptlay = new PatternLayout();
		String expectedValue = ptlay.format(event);
		expectedValue += "java.lang.Exception: testExceptionMessage\n"+
				"	at org.thisTest.SafeLogLayoutTest.methodName1(fileName1:1)\n"+
				"	at org.thisTest.SafeLogLayoutTest.methodName2(fileName2:2)\n";

		assertEquals(safeValue, expectedValue);
	}
}
