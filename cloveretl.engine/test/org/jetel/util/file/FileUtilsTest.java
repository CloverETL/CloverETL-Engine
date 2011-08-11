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
package org.jetel.util.file;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;

import org.jetel.test.CloverTestCase;
import org.jetel.util.file.FileUtils.ArchiveURLStreamHandler;

/**
 * @author Martin Zatopek (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 13.6.2011
 */
public class FileUtilsTest extends CloverTestCase {

	public void testGetJavaFile() throws MalformedURLException {
		File file = FileUtils.getJavaFile(FileUtils.getFileURL("./kokon/"), "neco/data.txt");
		assertEquals(file.getAbsolutePath(), new File("kokon/neco/data.txt").getAbsolutePath());
	}

	public void testGetFileURL() throws MalformedURLException {
    	String contextURLString = "file:/c:/project/";
    	String fileURL = "/c:/otherProject/data.txt";
		
		URL contextURL = FileUtils.getFileURL(contextURLString);
		assertEquals(new URL("file:/c:/project/"), contextURL);

		String result1 = FileUtils.getFile(contextURL, fileURL);
		assertEquals("/c:/otherProject/data.txt", result1);
		
		URL result2 = FileUtils.getFileURL(contextURLString, fileURL);
		assertEquals(new URL("file:/c:/otherProject/data.txt"), result2);
		
		URL result3 = FileUtils.getFileURL(contextURL, fileURL);
		assertEquals(new URL("file:/c:/otherProject/data.txt"), result3);
		
		fileURL = "zip:(./data-out/ordersByCountry.zip)#ordersByCountry.xls";
		URL result4 = FileUtils.getFileURL(contextURLString, fileURL);
		assertEquals(new URL(null, "zip:(file:/c:/project/data-out/ordersByCountry.zip)#ordersByCountry.xls", new ArchiveURLStreamHandler()), result4);
	}
	
}
