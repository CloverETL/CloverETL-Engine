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
package org.jetel.connection.jdbc;

import java.io.File;
import java.io.IOException;

import org.jetel.test.CloverTestCase;
import org.jetel.util.file.FileUtils;
import org.jetel.util.stream.StreamUtils;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 26 Jan 2012
 */
public class AnalyzeDBTest extends CloverTestCase {

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		initEngine();
	}
	
	public void testBasicSmokeTest() throws IOException {
		AnalyzeDB.main(new String[] { "-database", "MYSQL", "-dbURL", "jdbc:mysql://koule:3306/test",
				"-user", "test", "-password", "test", "-o", "analyzeDb.output.fmt", "-q", "select * from customer" });
		String result = FileUtils.getStringFromURL(null, "analyzeDb.output.fmt", "UTF-8");
		
		String expected = StreamUtils.convertStreamToString(
				AnalyzeDBTest.class.getResourceAsStream("analyzeDb.output.fmt"),
				"UTF-8", true);
		
		assertEquals(expected, result);
		
		//remove the temporary file
		File outputFile = new File("analyzeDb.output.fmt");
		outputFile.delete();
	}
	
	public void testBasicSmokeTestOfScript() throws IOException, InterruptedException {
		//TODO it is not really straightforward to write this type of junit test
	}
	
}
