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
package org.jetel.util;

import org.jetel.test.CloverTestCase;
import org.jetel.util.string.CommentsProcessor;

/**
 * @author Martin Zatopek (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 11.6.2010
 */
public class CommentsProcessorTest extends CloverTestCase {

	private String transform = "//comment\n"
		+ "function a() { /* comment */\n"
		+ "// comment\n"
		+ "		metoda(A);\n"
		+ "/*\n"
		+ "	multiline comment\n"
		+ " one more line\n"
		+ " end of comment */\n"
		+ "}\n";

	private String expected = "\n"
		+ "function a() { \n"
		+ "\n"
		+ "		metoda(A);\n"
		+ "\n"
		+ "}\n";
		
	public void testStripComments() {
		assertEquals(expected, CommentsProcessor.stripComments(transform));
	}

}
