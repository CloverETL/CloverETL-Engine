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
import org.jetel.util.exec.PlatformUtils;
import org.jetel.util.file.FileUtils.ArchiveURLStreamHandler;
import org.jetel.util.protocols.proxy.ProxyHandler;
import org.jetel.util.protocols.sftp.SFTPStreamHandler;

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
    	String fileURL = "c:/otherProject/data.txt";
		
    	if (PlatformUtils.isWindowsPlatform()) {
			URL contextURL = FileUtils.getFileURL(contextURLString);
			assertEquals(new URL("file:/c:/project/"), contextURL);
	
			String result1 = FileUtils.getFile(contextURL, fileURL);
			assertEquals("c:/otherProject/data.txt", result1);
			
			URL result2 = FileUtils.getFileURL(contextURLString, fileURL);
			assertEquals(new URL("file:/c:/otherProject/data.txt"), result2);
			
			URL result3 = FileUtils.getFileURL(contextURL, fileURL);
			assertEquals(new URL("file:/c:/otherProject/data.txt"), result3);
			
			fileURL = "zip:(./data-out/ordersByCountry.zip)#ordersByCountry.xls";
			URL result4 = FileUtils.getFileURL(contextURLString, fileURL);
			assertEquals(new URL(null, "zip:(file:/c:/project/data-out/ordersByCountry.zip)#ordersByCountry.xls", new ArchiveURLStreamHandler()), result4);
	
			URL result5 = FileUtils.getFileURL(new URL("file:/c:/project/"), "c:/otherProject/data.txt");
			assertEquals(new URL("file:/c:/otherProject/data.txt"), result5);
	
			URL result6 = FileUtils.getFileURL(new URL("file:c:/project/"), "c:/otherProject/data.txt");
			assertEquals(new URL("file:/c:/otherProject/data.txt"), result6);
			
			//assertEquals("c:/project and project/", FileUtils.convertUrlToFile(new File("c:/project and project/").toURI().toURL()).toString());
	
			String result7 = FileUtils.normalizeFilePath(FileUtils.getFile(new URL("file:c:/project/"), "c:/other Project/data.txt"));
			assertEquals("c:/other Project/data.txt", result7);
			
			URL result8 = FileUtils.getFileURL("C:/Windows", "C:/Users");
			assertEquals(new URL("file:/C:/Users"), result8);

    	}

    	try {
			FileUtils.getFileURL("C:/Windows", "unknownprotocol://home/agad/fileManipulation/graph/");
			fail("MalformedURLException expected");
		} catch (MalformedURLException ex) {}

		try {
			FileUtils.getFileURL("unknownprotocol://home/agad/fileManipulation/graph/", "home/user");
			fail("MalformedURLException expected");
		} catch (MalformedURLException ex) {}

		try {
			FileUtils.getFileURL("C:/Windows", "unknownprotocol:(C:/Users/file.txt)");
			fail("MalformedURLException expected");
		} catch (MalformedURLException ex) {}
	}
	
	public void testGetFileURLLinux() throws MalformedURLException {
		URL contextURL = FileUtils.getFileURL("/home/user/workspace/myproject/");
		assertEquals(new URL("file:/home/user/workspace/myproject/"), contextURL);

		contextURL = FileUtils.getFileURL("/home/user/workspace/myproject");
		assertEquals(new URL("file:/home/user/workspace/myproject"), contextURL);

		contextURL = FileUtils.getFileURL("/home/user/workspace/my project");
		assertEquals(new URL("file:/home/user/workspace/my project"), contextURL);

		contextURL = FileUtils.getFileURL("file:/home/user/workspace/myproject");
		assertEquals(new URL("file:/home/user/workspace/myproject"), contextURL);

		//relative path
		URL fileURL = FileUtils.getFileURL(new URL("file:/home/user/workspace/myproject/"), "data-in/myfile.txt");
		assertEquals(new URL("file:/home/user/workspace/myproject/data-in/myfile.txt"), fileURL);

		fileURL = FileUtils.getFileURL("file:/home/user/workspace/myproject/", "data-in/myfile.txt");
		assertEquals(new URL("file:/home/user/workspace/myproject/data-in/myfile.txt"), fileURL);

		//absolute path
		fileURL = FileUtils.getFileURL(new URL("file:/home/user/workspace/myproject/"), "/home/user/location/myfile.txt");
		assertEquals(new URL("file:/home/user/location/myfile.txt"), fileURL);

		fileURL = FileUtils.getFileURL("/home/user/workspace/myproject/", "/home/user/location/myfile.txt");
		assertEquals(new URL("file:/home/user/location/myfile.txt"), fileURL);

		fileURL = FileUtils.getFileURL(new URL("file:/home/user/workspace/myproject/"), "file:/home/user/location/myfile.txt");
		assertEquals(new URL("file:/home/user/location/myfile.txt"), fileURL);

		fileURL = FileUtils.getFileURL("/home/user/workspace/myproject/", "file:/home/user/location/myfile.txt");
		assertEquals(new URL("file:/home/user/location/myfile.txt"), fileURL);

		//file with white space
		fileURL = FileUtils.getFileURL(new URL("file:/home/user/workspace/myproject/"), "data in/myfile.txt");
		assertEquals(new URL("file:/home/user/workspace/myproject/data in/myfile.txt"), fileURL);
	
		fileURL = FileUtils.getFileURL("/home/user/workspace/myproject/", "data in/myfile.txt");
		assertEquals(new URL("file:/home/user/workspace/myproject/data in/myfile.txt"), fileURL);
	
		fileURL = FileUtils.getFileURL(new URL("file:/home/user/workspace/my project/"), "data-in/myfile.txt");
		assertEquals(new URL("file:/home/user/workspace/my project/data-in/myfile.txt"), fileURL);

		fileURL = FileUtils.getFileURL("/home/user/workspace/my project/", "data-in/myfile.txt");
		assertEquals(new URL("file:/home/user/workspace/my project/data-in/myfile.txt"), fileURL);

		fileURL = FileUtils.getFileURL(new URL("file:/home/user/workspace/my project/"), "data in/myfile.txt");
		assertEquals(new URL("file:/home/user/workspace/my project/data in/myfile.txt"), fileURL);
	
		fileURL = FileUtils.getFileURL("/home/user/workspace/my project/", "data in/myfile.txt");
		assertEquals(new URL("file:/home/user/workspace/my project/data in/myfile.txt"), fileURL);
	
		fileURL = FileUtils.getFileURL(new URL("file:/home/user/workspace/myproject/"), "/home/user/another location/myfile.txt");
		assertEquals(new URL("file:/home/user/another location/myfile.txt"), fileURL);

		fileURL = FileUtils.getFileURL("/home/user/workspace/myproject/", "/home/user/another location/myfile.txt");
		assertEquals(new URL("file:/home/user/another location/myfile.txt"), fileURL);

		fileURL = FileUtils.getFileURL(new URL("file:/home/user/workspace/my project/"), "/home/user/another location/myfile.txt");
		assertEquals(new URL("file:/home/user/another location/myfile.txt"), fileURL);

		fileURL = FileUtils.getFileURL("/home/user/workspace/my project/", "/home/user/another location/myfile.txt");
		assertEquals(new URL("file:/home/user/another location/myfile.txt"), fileURL);

		//without context
		fileURL = FileUtils.getFileURL("data in/myfile.txt");
		assertEquals(new URL("file:data in/myfile.txt"), fileURL);
	
		fileURL = FileUtils.getFileURL("data-in/myfile.txt");
		assertEquals(new URL("file:data-in/myfile.txt"), fileURL);

		fileURL = FileUtils.getFileURL("file:data-in/myfile.txt");
		assertEquals(new URL("file:data-in/myfile.txt"), fileURL);

		fileURL = FileUtils.getFileURL("/home/user/another location/myfile.txt");
		assertEquals(new URL("file:/home/user/another location/myfile.txt"), fileURL);

		//ftp
		fileURL = FileUtils.getFileURL(new URL("file:/home/user/workspace/myproject/"), "ftp://user@server/myfile.txt");
		assertEquals(new URL("ftp://user@server/myfile.txt"), fileURL);

		fileURL = FileUtils.getFileURL("ftp://user@server/myfile.txt");
		assertEquals(new URL("ftp://user@server/myfile.txt"), fileURL);

		fileURL = FileUtils.getFileURL(new URL("file:/home/user/workspace/myproject/"), "ftp://user:password@server/myfile.txt");
		assertEquals(new URL("ftp://user:password@server/myfile.txt"), fileURL);

		fileURL = FileUtils.getFileURL(new URL("file:/home/user/workspace/myproject/"), "ftp://user:password@server:123/myfile.txt");
		assertEquals(new URL("ftp://user:password@server:123/myfile.txt"), fileURL);

		fileURL = FileUtils.getFileURL(new URL("file:/home/user/workspace/myproject/"), "proxy://user:password@proxyserver:1234");
		assertEquals(new URL(null, "proxy://user:password@proxyserver:1234", new ProxyHandler()), fileURL);

//		TODO: is it possible to check full URL with proxy?		
//		fileURL = FileUtils.getFileURL(new URL("file:/home/user/workspace/myproject/"), "ftp:(proxy://user:password@proxyserver:1234)//seznam.cz");
//		assertEquals(new URL("ftp://user:password@server/myfile.txt"), fileURL);

		//sftp
		fileURL = FileUtils.getFileURL(new URL("file:/home/user/workspace/myproject/"), "sftp://user@server/myfile.txt");
		assertEquals(new URL(null, "sftp://user@server/myfile.txt", new SFTPStreamHandler()), fileURL);

		fileURL = FileUtils.getFileURL("sftp://user@server/myfile.txt");
		assertEquals(new URL(null, "sftp://user@server/myfile.txt", new SFTPStreamHandler()), fileURL);

		fileURL = FileUtils.getFileURL(new URL("file:/home/user/workspace/myproject/"), "sftp://user:password@server/myfile.txt");
		assertEquals(new URL(null, "sftp://user:password@server/myfile.txt", new SFTPStreamHandler()), fileURL);

		fileURL = FileUtils.getFileURL(new URL("file:/home/user/workspace/myproject/"), "sftp://user:password@server:123/myfile.txt");
		assertEquals(new URL(null, "sftp://user:password@server:123/myfile.txt", new SFTPStreamHandler()), fileURL);
		
		fileURL = FileUtils.getFileURL(new URL("jar:file:/clover-executor/clover-executor-86.0.0-SNAPSHOT.jar!/com/gooddata/clover"), "/home/test");
		assertEquals(new URL("file:/home/test"), fileURL);

		fileURL = FileUtils.getFileURL(new URL("http://www.cloveret.com/clover"), "/home/test");
		assertEquals(new URL("file:/home/test"), fileURL);

		try {
			FileUtils.getFileURL("/home/user", "unknownprotocol://home/agad/fileManipulation/graph/");
			fail("MalformedURLException expected");
		} catch (MalformedURLException ex) {}

		try {
			FileUtils.getFileURL("unknownprotocol://home/agad/fileManipulation/graph/", "home/user");
			fail("MalformedURLException expected");
		} catch (MalformedURLException ex) {}

		try {
			FileUtils.getFileURL("/home/user", "unknownprotocol:(/home/test)");
			fail("MalformedURLException expected");
		} catch (MalformedURLException ex) {}
	}

// this issue was commented out due low priority and non-trivial possible fix
//	public void testInvalidURL() {
//		
//		String urlString;
//		Exception ex = null;
//		
//		try {
//			urlString = "ftp://user@server:myfile.txt";
//			FileUtils.getFileURL(new URL("file://home/user/workspace/myproject/"), urlString);
//			try {
//				new URL(urlString);
//			}catch (MalformedURLException e) {
//				ex = e;
//			}
//			fail("Should fail for url: " + urlString + " with reason: " + ex);
//		} catch (MalformedURLException e) {
//			//OK
//		}
//	}
	
	public void testNormalizeFilePath() {
		if (PlatformUtils.isWindowsPlatform()) {
			assertEquals("c:/project/data.txt", FileUtils.normalizeFilePath("c:\\project\\data.txt"));
			
			assertEquals("c:/project/data.txt",  FileUtils.normalizeFilePath("/c:/project/data.txt"));
			
			assertEquals("c:/project/data.txt",  FileUtils.normalizeFilePath("\\c:\\project\\data.txt"));
		}	
			assertEquals("../project/data.txt",  FileUtils.normalizeFilePath("../project/data.txt"));
	}
	
	public void testIsMultiURL() {
		assertFalse(FileUtils.isMultiURL(null));
		assertFalse(FileUtils.isMultiURL(""));
		assertFalse(FileUtils.isMultiURL("abc"));
		assertTrue(FileUtils.isMultiURL("abc;"));
		assertTrue(FileUtils.isMultiURL("abc*defg"));
		assertTrue(FileUtils.isMultiURL("?defg"));
		assertTrue(FileUtils.isMultiURL(";abc?defg*"));
	}
	
	public void testGetFileURLWeblogicHandler() throws MalformedURLException {
		final String credentials = "user:pass@";
		final String fileSpec = "http://" + credentials + "javlin.eu/file";
		final URL contexUrl = null;
		
		URL urlWithCredentials = FileUtils.getFileURL(contexUrl, fileSpec);
		String serializedUrl = urlWithCredentials.toExternalForm();
		String toStringUrl = urlWithCredentials.toString();
		
		assertNotNull(serializedUrl);
		assertTrue(serializedUrl.contains(credentials));
		assertNotNull(toStringUrl);
		assertTrue(toStringUrl.contains(credentials));
	}
}
