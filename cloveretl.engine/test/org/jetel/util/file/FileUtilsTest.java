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

import org.jetel.exception.JetelRuntimeException;
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
		
		try {
			FileUtils.getJavaFile(null, "dict:filename");
			fail();
		} catch (JetelRuntimeException jre) {}

		try {
			FileUtils.getJavaFile(null, "port:$0.field1:discrete");
			fail();
		} catch (JetelRuntimeException jre) {}
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
			
			{
				URL result;
				
//				result = FileUtils.getFileURL("jar:file:/home/duke/duke.jar!/", "baz/entry.txt");
//				assertEquals(new URL("jar:file:/home/duke/duke.jar!/baz/entry.txt"), result);
//
//				result = FileUtils.getFileURL((URL) null, "jar:file:/home/duke/duke.jar!/");
//				assertEquals(new URL("jar:file:/home/duke/duke.jar!/"), result);
				
				result = FileUtils.getFileURL((URL) null, "jar:file:/C:/proj/parser/jar/parser.jar!/test");
				assertEquals(new URL("jar:file:/C:/proj/parser/jar/parser.jar!/test"), result);

				result = FileUtils.getFileURL(new URL("jar:file:/C:/proj/parser/jar/parser.jar!/"), "test");
				assertEquals(new URL("jar:file:/C:/proj/parser/jar/parser.jar!/test"), result);
			}

    	}

    	testInvalidURL("C:/Windows", "unknownprotocol://home/agad/fileManipulation/graph/");

    	testInvalidURL("C:/Windows", "unknownprotocol:(C:/Users/file.txt)");
		
		{
			URL result;
			result = FileUtils.getFileURL("https://user%40gooddata.com:password@secure-di.gooddata.com/");
			assertEquals(new URL("https://user%40gooddata.com:password@secure-di.gooddata.com/"), result);
		}
		
		testInvalidURL(null, "https://user@gooddata.com:password@secure-di.gooddata.com/");
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
		SFTPStreamHandler sftpHandler = FileUtils.sFtpStreamHandler;
		fileURL = FileUtils.getFileURL(new URL("file:/home/user/workspace/myproject/"), "sftp://user@server/myfile.txt");
		assertEquals(new URL(null, "sftp://user@server/myfile.txt", sftpHandler), fileURL);

		fileURL = FileUtils.getFileURL("sftp://user@server/myfile.txt");
		assertEquals(new URL(null, "sftp://user@server/myfile.txt", sftpHandler), fileURL);

		fileURL = FileUtils.getFileURL(new URL("file:/home/user/workspace/myproject/"), "sftp://user:password@server/myfile.txt");
		assertEquals(new URL(null, "sftp://user:password@server/myfile.txt", sftpHandler), fileURL);

		fileURL = FileUtils.getFileURL(new URL("file:/home/user/workspace/myproject/"), "sftp://user:password@server:123/myfile.txt");
		assertEquals(new URL(null, "sftp://user:password@server:123/myfile.txt", sftpHandler), fileURL);
		
		fileURL = FileUtils.getFileURL(new URL("jar:file:/clover-executor/clover-executor-86.0.0-SNAPSHOT.jar!/com/gooddata/clover"), "/home/test");
		assertEquals(new URL("file:/home/test"), fileURL);

		fileURL = FileUtils.getFileURL(new URL("http://www.cloveret.com/clover"), "/home/test");
		assertEquals(new URL("file:/home/test"), fileURL);

		testInvalidURL("/home/user", "unknownprotocol://home/agad/fileManipulation/graph/");

		testInvalidURL("unknownprotocol://home/agad/fileManipulation/graph/", "home/user");

		testInvalidURL("/home/user", "unknownprotocol:(/home/test)");

		testInvalidURL("file://home/user/workspace/myproject/", "ftp://user@server:myfile.txt");

		{
			URL result;
			
			result = FileUtils.getFileURL("jar:file:/home/duke/duke.jar!/", "baz/entry.txt");
			assertEquals(new URL("jar:file:/home/duke/duke.jar!/baz/entry.txt"), result);

			result = FileUtils.getFileURL((URL) null, "jar:file:/home/duke/duke.jar!/");
			assertEquals(new URL("jar:file:/home/duke/duke.jar!/"), result);
			
			result = FileUtils.getFileURL("port:$0.field1:discrete"); // must not fail
			assertEquals("port:$0.field1:discrete", result.toString());
			
			result = FileUtils.getFileURL("dict:path:source"); // must not fail
			assertEquals("dict:path:source", result.toString());

			result = FileUtils.getFileURL("dict:path"); // must not fail
			assertEquals("dict:path", result.toString());
		}
	}
	
	private void testInvalidURL(String urlString) {
		testInvalidURL(null, urlString);
	}
	
	private void testInvalidURL(String context, String urlString) {
		Exception ex = null;
		
		try {
			try {
				if (context != null) {
					FileUtils.getFileURL(context); // first check the contextURL, if any
				}
				new URL(urlString);
				// both URLs are valid, unexpected
				throw new RuntimeException("Valid URL: " + urlString);
			} catch (MalformedURLException e) {
				ex = e;
			}
			if (context != null) { 
				FileUtils.getFileURL(context, urlString);
			} else {
				FileUtils.getFileURL(urlString);
			}
			fail("Should fail for url: " + urlString + " with reason: " + ex);
		} catch (MalformedURLException e) {
			assertEquals(ex.getClass(), e.getClass());
			assertEquals(ex.getMessage(), e.getMessage());
			// same exception class, same message
		}
	}

	public void testInvalidURL() throws MalformedURLException {
		testInvalidURL("ftp://user@server:myfile.txt");
		testInvalidURL("https://user@gooddata.com:password@secure-di.gooddata.com/");
	}
	
	public void testIsLocalFile() {
		assertFalse(FileUtils.isLocalFile(null, "dict:filename"));
		assertFalse(FileUtils.isLocalFile(null, "port:$0.field1:discrete"));
	}
	
	public void testIsRemoteFile() {
		assertFalse(FileUtils.isRemoteFile("dict:filename"));
		assertFalse(FileUtils.isRemoteFile("port:$0.field1:discrete"));
	}
	
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
	
	public void testRemoveFinalSlashIfNecessary() {
		assertNull(FileUtils.removeTrailingSlash(null));
		assertEquals("", FileUtils.removeTrailingSlash(""));
		assertEquals("c:/project", FileUtils.removeTrailingSlash("c:/project/"));
		assertEquals("c:/project", FileUtils.removeTrailingSlash("c:/project"));
	}
}
