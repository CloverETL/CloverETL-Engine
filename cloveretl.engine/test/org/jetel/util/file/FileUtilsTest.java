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
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import org.jetel.enums.ProcessingType;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.test.CloverTestCase;
import org.jetel.util.exec.PlatformUtils;
import org.jetel.util.file.FileUtils.ArchiveURLStreamHandler;
import org.jetel.util.file.FileUtils.PortURL;
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
		
		try {
			FileUtils.getJavaFile(null, "request:body");
			fail();
		} catch (JetelRuntimeException jre) {}
		
		try {
			FileUtils.getJavaFile(null, "request:part:name");
			fail();
		} catch (JetelRuntimeException jre) {}
		
		try {
			FileUtils.getJavaFile(null, "response:body");
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

		fileURL = FileUtils.getFileURL(new URL("jar:file:/clover-executor/clover-executor-86.0.0-SNAPSHOT.jar!/com/gooddata/clover/"), "home/test");
		assertEquals(new URL("jar:file:/clover-executor/clover-executor-86.0.0-SNAPSHOT.jar!/com/gooddata/clover/home/test"), fileURL);
		
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
			
			result = FileUtils.getFileURL("request:body"); // must not fail
			assertEquals("request:body", result.toString());
			
			result = FileUtils.getFileURL("request:part:name"); // must not fail
			assertEquals("request:part:name", result.toString());
			
			result = FileUtils.getFileURL("response:body"); // must not fail
			assertEquals("response:body", result.toString());
		}
		
		// CLO-978
		try {
			FileUtils.getFileURL("smb://virt-orange%3BSMBTest:p%40ss{/}@VIRT-ORANGE/SMBTestPub/test.txt");
			fail();
		} catch (MalformedURLException ex) {
			assertTrue(ex.getMessage().contains("p%40ss{"));
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
		assertFalse(FileUtils.isLocalFile(null, "s3://accessKey:secretKey@s3.amazonaws.com"));
		assertFalse(FileUtils.isLocalFile(null, "hdfs://CONNECTION0/"));
		assertFalse(FileUtils.isLocalFile(null, "request:body"));
		assertFalse(FileUtils.isLocalFile(null, "request:part:name"));
		assertFalse(FileUtils.isLocalFile(null, "response:body"));
	}
	
	public void testIsRemoteFile() {
		assertFalse(FileUtils.isRemoteFile("dict:filename"));
		assertFalse(FileUtils.isRemoteFile("port:$0.field1:discrete"));
		assertTrue(FileUtils.isRemoteFile("s3://accessKey:secretKey@s3.amazonaws.com"));
		assertTrue(FileUtils.isRemoteFile("hdfs://CONNECTION0/"));
		assertFalse(FileUtils.isRemoteFile("request:body"));
		assertFalse(FileUtils.isRemoteFile("request:part:name"));
		assertFalse(FileUtils.isRemoteFile("response:body"));
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
		
		URL urlWithCredentials = new URL(contexUrl, fileSpec); //FileUtils.getFileURL(contexUrl, fileSpec);
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
	
	public void testGetAbsoluteURL() throws IOException {
		// context URL is absolute file URL
		URL contextUrl = new URL("file:/C:/somedir/subdir/");
		String input;
		String result;
		
		if (PlatformUtils.isWindowsPlatform()) {
			input = "C:/Project/dir";
			result = FileUtils.getAbsoluteURL(contextUrl, input);
			assertEquals("file:/C:/Project/dir", result);
		}
		
		input = "ftp://test:test@ftp.javlin.eu/file.txt";
		result = FileUtils.getAbsoluteURL(contextUrl, input);
		assertEquals(input, result);
		
		input = "./data-in/file.txt";
		result = FileUtils.getAbsoluteURL(contextUrl, input);
		assertEquals("file:/C:/somedir/subdir/data-in/file.txt", result);
		
		input = "sandbox://mysandbox/dir/myfile.txt";
		result = FileUtils.getAbsoluteURL(contextUrl, input);
		assertEquals(input, result);

		input = "./data-in/file.txt;./data-in/file.txt";
		result = FileUtils.getAbsoluteURL(contextUrl, input);
		assertEquals("file:/C:/somedir/subdir/data-in/file.txt;file:/C:/somedir/subdir/data-in/file.txt", result);
		
		input = "zip:(./data-in/file.zip)#dir/myfile.txt";
		result = FileUtils.getAbsoluteURL(contextUrl, input);
		assertEquals("zip:(file:/C:/somedir/subdir/data-in/file.zip)#dir/myfile.txt", result);

		input = "./data-in/*.txt";
		result = FileUtils.getAbsoluteURL(contextUrl, input);
		assertEquals("file:/C:/somedir/subdir/data-in/*.txt", result);
		
		input = "./data-in/?es?.txt";
		result = FileUtils.getAbsoluteURL(contextUrl, input);
		assertEquals("file:/C:/somedir/subdir/data-in/?es?.txt", result);
		
		input = "./path/filename$.out";
		result = FileUtils.getAbsoluteURL(contextUrl, input);
		assertEquals("file:/C:/somedir/subdir/path/filename$.out", result);

		input = "./path/filename#.out";
		result = FileUtils.getAbsoluteURL(contextUrl, input);
		assertEquals("file:/C:/somedir/subdir/path/filename#.out", result);

		input = "./path/filename%20space.out";
		result = FileUtils.getAbsoluteURL(contextUrl, input);
		assertEquals("file:/C:/somedir/subdir/path/filename%20space.out", result);

		// context URL is sandbox URL
		contextUrl = FileUtils.getFileURL("sandbox://mysandbox/dir/");
		input = "./data-in/file.txt";
		result = FileUtils.getAbsoluteURL(contextUrl, input);
		assertEquals("sandbox://mysandbox/dir/data-in/file.txt", result);
		
		input = "./data-in/file.txt;./data-in/file.txt";
		result = FileUtils.getAbsoluteURL(contextUrl, input);
		assertEquals("sandbox://mysandbox/dir/data-in/file.txt;sandbox://mysandbox/dir/data-in/file.txt", result);
		
		input = "zip:(./data-in/file.zip)#dir/myfile.txt";
		result = FileUtils.getAbsoluteURL(contextUrl, input);
		assertEquals("zip:(sandbox://mysandbox/dir/data-in/file.zip)#dir/myfile.txt", result);

		input = "./data-in/*.txt";
		result = FileUtils.getAbsoluteURL(contextUrl, input);
		assertEquals("sandbox://mysandbox/dir/data-in/*.txt", result);
		
		input = "./data-in/?es?.txt";
		result = FileUtils.getAbsoluteURL(contextUrl, input);
		assertEquals("sandbox://mysandbox/dir/data-in/?es?.txt", result);
		
		input = "./path/filename$.out";
		result = FileUtils.getAbsoluteURL(contextUrl, input);
		assertEquals("sandbox://mysandbox/dir/path/filename$.out", result);

		input = "./path/filename#.out";
		result = FileUtils.getAbsoluteURL(contextUrl, input);
		assertEquals("sandbox://mysandbox/dir/path/filename#.out", result);

		input = "./path/filename%20space.out";
		result = FileUtils.getAbsoluteURL(contextUrl, input);
		assertEquals("sandbox://mysandbox/dir/path/filename%20space.out", result);

		// context URL is null
		contextUrl = null;
		String realContext = FileUtils.appendSlash(new File(".").getCanonicalFile().toURI().toString());
		
		input = "./data-in/file.txt";
		result = FileUtils.getAbsoluteURL(contextUrl, input);
		assertEquals(realContext + "data-in/file.txt", result);
		
		input = "sandbox://mysandbox/dir/myfile.txt";
		result = FileUtils.getAbsoluteURL(contextUrl, input);
		assertEquals(input, result);

		input = "./data-in/file.txt;./data-in/file.txt";
		result = FileUtils.getAbsoluteURL(contextUrl, input);
		assertEquals(realContext + "data-in/file.txt;" + realContext + "data-in/file.txt", result);
		
		input = "zip:(./data-in/file.zip)#dir/myfile.txt";
		result = FileUtils.getAbsoluteURL(contextUrl, input);
		assertEquals("zip:(" + realContext + "data-in/file.zip)#dir/myfile.txt", result);

		input = "./data-in/*.txt";
		result = FileUtils.getAbsoluteURL(contextUrl, input);
		assertEquals(realContext + "data-in/*.txt", result);
		
		input = "./data-in/?es?.txt";
		result = FileUtils.getAbsoluteURL(contextUrl, input);
		assertEquals(realContext + "data-in/?es?.txt", result);
		
		input = "./path/filename$.out";
		result = FileUtils.getAbsoluteURL(contextUrl, input);
		assertEquals(realContext + "path/filename$.out", result);

		input = "./path/filename#.out";
		result = FileUtils.getAbsoluteURL(contextUrl, input);
		assertEquals(realContext + "path/filename#.out", result);

		input = "./path/filename%20space.out";
		result = FileUtils.getAbsoluteURL(contextUrl, input);
		assertEquals(realContext + "path/filename%20space.out", result);

//		{
//			// SFTP relative path starting with a slash
//			URL context = FileUtils.getFileURL("sftp://localhost/home/user/");
//			
//			input = "/var/log/mongodb/mongo.log";
//			result = FileUtils.getAbsoluteURL(context, input);
//			assertEquals("sftp://localhost/var/log/mongodb/mongo.log", result);
//			
//			input = "var/log/mongodb/mongo.log";
//			result = FileUtils.getAbsoluteURL(context, input);
//			assertEquals("sftp://localhost/home/user/var/log/mongodb/mongo.log", result);
//		}
//		
//		{
//			// FTP relative path starting with a slash
//			URL context = FileUtils.getFileURL("ftp://localhost/home/user/");
//			
//			input = "/var/log/mongodb/mongo.log";
//			result = FileUtils.getAbsoluteURL(context, input);
//			assertEquals("ftp://localhost/var/log/mongodb/mongo.log", result);
//			
//			input = "var/log/mongodb/mongo.log";
//			result = FileUtils.getAbsoluteURL(context, input);
//			assertEquals("ftp://localhost/home/user/var/log/mongodb/mongo.log", result);
//		}
		
		// paths starting with a slash are considered absolute (even on Windows, a bug in FileUtils.getFileURL())
		input = "/home/krivanekm";
		result = FileUtils.getAbsoluteURL(contextUrl, input);
		assertEquals("file:/home/krivanekm", result);

		input = "/home/krivanekm";
		result = FileUtils.getAbsoluteURL(FileUtils.getFileURL("sandbox://mysandbox/"), input);
		assertEquals("file:/home/krivanekm", result);

		contextUrl = FileUtils.getFileURL("/home/user/");
		input = "path/file.txt";
		result = FileUtils.getAbsoluteURL(contextUrl, input);
		if (PlatformUtils.isWindowsPlatform()) {
			String rc = FileUtils.appendSlash(new File("/home/user").getCanonicalFile().toURI().toString());
			assertEquals(rc + input, result);
		} else {
			assertEquals("file:/home/user/" + input, result);
		}

		// context URL is relative file URL of the current working directory
		contextUrl = FileUtils.getFileURL(".");

		input = "./data-in/file.txt";
		result = FileUtils.getAbsoluteURL(contextUrl, input);
		assertEquals(realContext + "data-in/file.txt", result);
		
		input = "sandbox://mysandbox/dir/myfile.txt";
		result = FileUtils.getAbsoluteURL(contextUrl, input);
		assertEquals(input, result);

		input = "./data-in/file.txt;./data-in/file.txt";
		result = FileUtils.getAbsoluteURL(contextUrl, input);
		assertEquals(realContext + "data-in/file.txt;" + realContext + "data-in/file.txt", result);
		
		input = "zip:(./data-in/file.zip)#dir/myfile.txt";
		result = FileUtils.getAbsoluteURL(contextUrl, input);
		assertEquals("zip:(" + realContext + "data-in/file.zip)#dir/myfile.txt", result);

		input = "./data-in/*.txt";
		result = FileUtils.getAbsoluteURL(contextUrl, input);
		assertEquals(realContext + "data-in/*.txt", result);
		
		input = "./data-in/?es?.txt";
		result = FileUtils.getAbsoluteURL(contextUrl, input);
		assertEquals(realContext + "data-in/?es?.txt", result);
		
		input = "./path/filename$.out";
		result = FileUtils.getAbsoluteURL(contextUrl, input);
		assertEquals(realContext + "path/filename$.out", result);

		input = "./path/filename#.out";
		result = FileUtils.getAbsoluteURL(contextUrl, input);
		assertEquals(realContext + "path/filename#.out", result);

		input = "./path/filename%20space.out";
		result = FileUtils.getAbsoluteURL(contextUrl, input);
		assertEquals(realContext + "path/filename%20space.out", result);

		// context URL is relative (non-existing) file URL
		contextUrl = FileUtils.getFileURL("graph");
		realContext = FileUtils.appendSlash(new File("graph").getCanonicalFile().toURI().toString());

		input = "./data-in/file.txt";
		result = FileUtils.getAbsoluteURL(contextUrl, input);
		assertEquals(realContext + "data-in/file.txt", result);
		
		input = "sandbox://mysandbox/dir/myfile.txt";
		result = FileUtils.getAbsoluteURL(contextUrl, input);
		assertEquals(input, result);

		input = "./data-in/file.txt;./data-in/file.txt";
		result = FileUtils.getAbsoluteURL(contextUrl, input);
		assertEquals(realContext + "data-in/file.txt;" + realContext + "data-in/file.txt", result);
		
		input = "zip:(./data-in/file.zip)#dir/myfile.txt";
		result = FileUtils.getAbsoluteURL(contextUrl, input);
		assertEquals("zip:(" + realContext + "data-in/file.zip)#dir/myfile.txt", result);

		input = "./data-in/*.txt";
		result = FileUtils.getAbsoluteURL(contextUrl, input);
		assertEquals(realContext + "data-in/*.txt", result);
		
		input = "./data-in/?es?.txt";
		result = FileUtils.getAbsoluteURL(contextUrl, input);
		assertEquals(realContext + "data-in/?es?.txt", result);
		
		input = "./path/filename$.out";
		result = FileUtils.getAbsoluteURL(contextUrl, input);
		assertEquals(realContext + "path/filename$.out", result);

		input = "./path/filename#.out";
		result = FileUtils.getAbsoluteURL(contextUrl, input);
		assertEquals(realContext + "path/filename#.out", result);

		input = "./path/filename%20space.out";
		result = FileUtils.getAbsoluteURL(contextUrl, input);
		assertEquals(realContext + "path/filename%20space.out", result);

		// standard in/out
		input = "-";
		result = FileUtils.getAbsoluteURL(contextUrl, input);
		assertEquals(input, result);
		
		// port
		input = "port:$0.fieldName:discrete";
		result = FileUtils.getAbsoluteURL(contextUrl, input);
		assertEquals(input, result);
		
		// dictionary
		input = "dict:entryName:source";
		result = FileUtils.getAbsoluteURL(contextUrl, input);
		assertEquals(input, result);

		// proxy
		input = "http:(proxy://juzr:heslou@koule:3128)//www.google.com";
		result = FileUtils.getAbsoluteURL(contextUrl, input);
		assertEquals(input, result);

		// zip http proxy wildcards
		input = "zip:(http:(proxy://juzr:heslou@koule:3128)//www.google.com/path/*.zip)#entry/fil?.txt";
		result = FileUtils.getAbsoluteURL(contextUrl, input);
		assertEquals(input, result);
		
		// zip http proxy wildcards context
		contextUrl = FileUtils.getFileURL("http:(proxy://juzr:heslou@koule:3128)//www.google.com/");
		input = "zip:(./path/*.zip)#entry/fil?.txt";
		result = FileUtils.getAbsoluteURL(contextUrl, input);
		assertEquals("zip:(http:(proxy://juzr:heslou@koule:3128)//www.google.com/path/*.zip)#entry/fil?.txt", result);
		
		// HTTP request
		input = "request:body";
		result = FileUtils.getAbsoluteURL(contextUrl, input);
		assertEquals(input, result);
		
		// HTTP request
		input = "request:part:name";
		result = FileUtils.getAbsoluteURL(contextUrl, input);
		assertEquals(input, result);
		
		// HTTP response
		input = "response:body";
		result = FileUtils.getAbsoluteURL(contextUrl, input);
		assertEquals(input, result);
	}
	
	private static String[][] paths = new String[][] {
		new String[] {
				null, // input
				null, // path
				null, // filename
				null, // basename
				null, // extension
				null, // normalized
		},

		new String[] {
				"", // input
				"", // path
				"", // filename
				"", // basename
				"", // extension
				"", // normalized
		},

		new String[] {
				"/foo/../bar/../baz/out5.txt", // input
				"/foo/../bar/../baz/", // path
				"out5.txt", // filename
				"out5", // basename
				"txt", // extension
				"/baz/out5.txt", // normalized
		},
		
		new String[] {
				"/cloveretl.test.scenarios/./data-in/fileOperation/input.xlsx", // input
				"/cloveretl.test.scenarios/./data-in/fileOperation/", // path
				"input.xlsx", // filename
				"input", // basename
				"xlsx", // extension
				"/cloveretl.test.scenarios/data-in/fileOperation/input.xlsx", // normalized
		},
		
		new String[] {
				"/data/./file.dat", // input
				"/data/./", // path
				"file.dat", // filename
				"file", // basename
				"dat", // extension
				"/data/file.dat", // normalized
		},
		
		new String[] {
				"C:/./a\\b/c.cdf", // input
				"C:/./a/b/", // path
				"c.cdf", // filename
				"c", // basename
				"cdf", // extension
				"C:/a/b/c.cdf", // normalized
		},
		
		new String[] {
				"C:\\a\\b\\c.xml", // input
				"C:/a/b/", // path
				"c.xml", // filename
				"c", // basename
				"xml", // extension
				"C:/a/b/c.xml", // normalized
		},
		
		new String[] {
				"a/b/c.ab.jpg", // input
				"a/b/", // path
				"c.ab.jpg", // filename
				"c.ab", // basename
				"jpg", // extension
				"a/b/c.ab.jpg", // normalized
		},
		
		new String[] {
				"file:/C:/Users/krivanekm/workspace/Experiments/", // input
				"file:/C:/Users/krivanekm/workspace/Experiments/", // path
				"", // filename
				"", // basename
				"", // extension
				"file:/C:/Users/krivanekm/workspace/Experiments/", // normalized
		},
		
		new String[] {
				"file:/C:/", // input
				"file:/C:/", // path
				"", // filename
				"", // basename
				"", // extension
				"file:/C:/", // normalized
		},
		
		new String[] {
				"file:/C:/..", // input
				"file:/C:/", // path
				"..", // filename
				".", // basename
				"", // extension
				null, // normalized
		},
		
		new String[] {
				"sandbox://cloveretl.test.scenarios/", // input
				"sandbox://cloveretl.test.scenarios/", // path
				"", // filename
				"", // basename
				"", // extension
				"sandbox://cloveretl.test.scenarios/", // normalized
		},
		
		new String[] {
				"sandbox://cloveretl.test.scenarios/a/b/c.dbf", // input
				"sandbox://cloveretl.test.scenarios/a/b/", // path
				"c.dbf", // filename
				"c", // basename
				"dbf", // extension
				"sandbox://cloveretl.test.scenarios/a/b/c.dbf", // normalized
		},
		
		new String[] {
				"ftp://user:pass%40word@hostname.com/a/b", // input
				"ftp://user:pass%40word@hostname.com/a/", // path
				"b", // filename
				"b", // basename
				"", // extension
				"ftp://user:pass%40word@hostname.com/a/b", // normalized
		},
		
		new String[] {
				"ftp://user:password@hostname.com/a/../b/c.gz", // input
				"ftp://user:password@hostname.com/a/../b/", // path
				"c.gz", // filename
				"c", // basename
				"gz", // extension
				"ftp://user:password@hostname.com/b/c.gz", // normalized
		},
		
		new String[] {
				"s3://user:password@hostname.com/a/b/c.Z", // input
				"s3://user:password@hostname.com/a/b/", // path
				"c.Z", // filename
				"c", // basename
				"Z", // extension
				"s3://user:password@hostname.com/a/b/c.Z", // normalized
		},
		
		new String[] {
				"sftp://user:password@hostname.com/../a/b", // input
				"sftp://user:password@hostname.com/../a/", // path
				"b", // filename
				"b", // basename
				"", // extension
				null, // normalized
		},
		
		new String[] {
				"sandbox://cloveretl.test.scenarios", // input
				"sandbox://cloveretl.test.scenarios", // path
				"", // filename
				"", // basename
				"", // extension
				"sandbox://cloveretl.test.scenarios", // normalized
		},
		
		new String[] {
				"sandbox://cloveretl.test.scenarios/dir 1/file_with_query?and#hash.txt", // input
				"sandbox://cloveretl.test.scenarios/dir 1/", // path
				"file_with_query?and#hash.txt", // filename
				"file_with_query?and#hash", // basename
				"txt", // extension
				"sandbox://cloveretl.test.scenarios/dir 1/file_with_query?and#hash.txt", // normalized
		},
		
		new String[] {
				"zip:(C:/a/b/c.zip)", // input
				"zip:(C:/a/b/c.zip)", // path
				"", // filename
				"", // basename
				"", // extension
				"zip:(C:/a/b/c.zip)", // normalized
		},
		
		new String[] {
				"zip:(C:/a/b/c.zip)#zipEntry/file.txt", // input
				"zip:(C:/a/b/c.zip)#zipEntry/", // path
				"file.txt", // filename
				"file", // basename
				"txt", // extension
				"zip:(C:/a/b/c.zip)#zipEntry/file.txt", // normalized
		},
		
		new String[] {
				"http://seznam.cz", // input
				"http://seznam.cz", // path
				"", // filename
				"", // basename
				"", // extension
				"http://seznam.cz", // normalized
		},
		
		new String[] {
				"http:(proxy://user:password@212.93.193.82:443)//seznam.cz", // input
				"http:(proxy://user:password@212.93.193.82:443)//seznam.cz", // path
				"", // filename
				"", // basename
				"", // extension
				"http:(proxy://user:password@212.93.193.82:443)//seznam.cz", // normalized
		},
		
		new String[] {
				"http:(proxy://user:password@212.93.193.82:443)//seznam.cz/", // input
				"http:(proxy://user:password@212.93.193.82:443)//seznam.cz/", // path
				"", // filename
				"", // basename
				"", // extension
				"http:(proxy://user:password@212.93.193.82:443)//seznam.cz/", // normalized
		},
		
		new String[] {
				"http:(proxy://user:password@212.93.193.82:443)//seznam.cz/index.html?query", // input
				"http:(proxy://user:password@212.93.193.82:443)//seznam.cz/", // path
				"index.html?query", // filename
				"index", // basename
				"html?query", // extension
				"http:(proxy://user:password@212.93.193.82:443)//seznam.cz/index.html?query", // normalized
		},
		
		new String[] {
				"zip:(C:\\a\\b\\..\\c.zip)#zip Entry\\file.txt", // input
				"zip:(C:/a/b/../c.zip)#zip Entry/", // path
				"file.txt", // filename
				"file", // basename
				"txt", // extension
				"zip:(C:/a/c.zip)#zip Entry/file.txt", // normalized
		},
		
		new String[] {
				"gzip:(C:\\a\\b\\..\\c%.gz)", // input
				"gzip:(C:/a/b/../c%.gz)", // path
				"", // filename
				"", // basename
				"", // extension
				"gzip:(C:/a/c%.gz)", // normalized
		},
		
		new String[] {
				"http://illegal%escape%sequence/f%ile.tmp", // input
				"http://illegal%escape%sequence/", // path
				"f%ile.tmp", // filename
				"f%ile", // basename
				"tmp", // extension
				"http://illegal%escape%sequence/f%ile.tmp", // normalized
		},
		
		new String[] {
				"http://illegal%escape%sequence/../f%ile.tmp", // input
				"http://illegal%escape%sequence/../", // path
				"f%ile.tmp", // filename
				"f%ile", // basename
				"tmp", // extension
				null, // normalized
		},
		
		new String[] {
				"nonexisting://hostname/a/b/c.d.e.avi", // input
				"nonexisting://hostname/a/b/", // path
				"c.d.e.avi", // filename
				"c.d.e", // basename
				"avi", // extension
				"nonexisting://hostname/a/b/c.d.e.avi", // normalized
		},
		
		new String[] {
				"dict:filename", // input
				"", // path
				"dict:filename", // filename
				"dict:filename", // basename
				"", // extension
				"dict:filename", // normalized
		},
		
		new String[] {
				"mailto:milan.krivanek@cloveretl.com", // input
				"", // path
				"mailto:milan.krivanek@cloveretl.com", // filename
				"mailto:milan.krivanek@cloveretl", // basename
				"com", // extension
				"mailto:milan.krivanek@cloveretl.com", // normalized
		},
		
		new String[] {
				"port:$0.FieldName:source", // input
				"", // path
				"port:$0.FieldName:source", // filename
				"port:$0", // basename
				"FieldName:source", // extension
				"port:$0.FieldName:source", // normalized
		},
		
		new String[] {
				"ldap://localhost:389/ou=People,o=JNDITutorial", // input
				"ldap://localhost:389/", // path
				"ou=People,o=JNDITutorial", // filename
				"ou=People,o=JNDITutorial", // basename
				"", // extension
				"ldap://localhost:389/ou=People,o=JNDITutorial", // normalized
		},
		
		new String[] {
				"ldap://localhost:389/ou=People,o=JNDITutorial??sub?(sn=Geisel)", // input
				"ldap://localhost:389/", // path
				"ou=People,o=JNDITutorial??sub?(sn=Geisel)", // filename
				"ou=People,o=JNDITutorial??sub?(sn=Geisel)", // basename
				"", // extension
				"ldap://localhost:389/ou=People,o=JNDITutorial??sub?(sn=Geisel)", // normalized
		},
		
		new String[] {
				"file:/home/krivanekm/file.doc", // input
				"file:/home/krivanekm/", // path
				"file.doc", // filename
				"file", // basename
				"doc", // extension
				"file:/home/krivanekm/file.doc", // normalized
		},
		
		new String[] {
				"/path/filename.txt", // input
				"/path/", // path
				"filename.txt", // filename
				"filename", // basename
				"txt", // extension
				"/path/filename.txt", // normalized
		},
		
		new String[] {
				"/path/filename?.txt", // input
				"/path/", // path
				"filename?.txt", // filename
				"filename?", // basename
				"txt", // extension
				"/path/filename?.txt", // normalized
		},
		
		new String[] {
				"/path/./*", // input
				"/path/./", // path
				"*", // filename
				"*", // basename
				"", // extension
				"/path/*", // normalized
		},
		
		new String[] {
				"tar:(/path/../file.tar)#innerfolder/../filename.txt", // input
				"tar:(/path/../file.tar)#innerfolder/../", // path
				"filename.txt", // filename
				"filename", // basename
				"txt", // extension
				"tar:(/file.tar)#filename.txt", // normalized
		},

		new String[] {
				"zip:(zip:(/path\\..\\name?.zip)#innerfolder/./file.zip)#innermostfolder?\\../filename*.html", // input
				"zip:(zip:(/path/../name?.zip)#innerfolder/./file.zip)#innermostfolder?/../", // path
				"filename*.html", // filename
				"filename*", // basename
				"html", // extension
				"zip:(zip:(/name?.zip)#innerfolder/file.zip)#filename*.html", // normalized
		},
		
		new String[] {
				"http://access_key_id:secret_access_key@bucketname.s3.amazonaws.com/file%20name*.out", // input
				"http://access_key_id:secret_access_key@bucketname.s3.amazonaws.com/", // path
				"file%20name*.out", // filename
				"file%20name*", // basename
				"out", // extension
				"http://access_key_id:secret_access_key@bucketname.s3.amazonaws.com/file%20name*.out", // normalized
		},
		
		new String[] {
				"hdfs://CONN_ID/path/file#name.dat.tmp", // input
				"hdfs://CONN_ID/path/", // path
				"file#name.dat.tmp", // filename
				"file#name.dat", // basename
				"tmp", // extension
				"hdfs://CONN_ID/path/file#name.dat.tmp", // normalized
		},
		
		new String[] {
				"smb://domain%3Buser:pa55word@server/path/file name.txt", // input
				"smb://domain%3Buser:pa55word@server/path/", // path
				"file name.txt", // filename
				"file name", // basename
				"txt", // extension
				"smb://domain%3Buser:pa55word@server/path/file name.txt", // normalized
		},
		
		new String[] {
				"sftp:(proxy://66.11.122.193:443)//user:password@server/pa@th/.././file$.dat", // input
				"sftp:(proxy://66.11.122.193:443)//user:password@server/pa@th/.././", // path
				"file$.dat", // filename
				"file$", // basename
				"dat", // extension
				"sftp:(proxy://66.11.122.193:443)//user:password@server/file$.dat", // normalized
		},
		
		new String[] {
				"sandbox://data/path to/file/.project", // input
				"sandbox://data/path to/file/", // path
				".project", // filename
				"", // basename
				"project", // extension
				"sandbox://data/path to/file/.project", // normalized
		},
		
		new String[] {
				"sftp:(proxy://66.11.122.193:443)//user:password@server/.././file$.dat", // input
				"sftp:(proxy://66.11.122.193:443)//user:password@server/.././", // path
				"file$.dat", // filename
				"file$", // basename
				"dat", // extension
				null, // normalized
		},
		
		new String[] {
				"file:///home/user1/my.documents/log", // input
				"file:///home/user1/my.documents/", // path
				"log", // filename
				"log", // basename
				"", // extension
				"file:///home/user1/my.documents/log", // normalized
		},
		
		new String[] {
				"zip:(C:\\Data\\..\\archive.zip)#inner1/../inner2/./data.txt", // input
				"zip:(C:/Data/../archive.zip)#inner1/../inner2/./", // path
				"data.txt", // filename
				"data", // basename
				"txt", // extension
				"zip:(C:/archive.zip)#inner2/data.txt", // normalized
		},
		
		new String[] {
				"request:body", // input
				"", // path
				"request:body", // filename
				"request:body", // basename
				"", // extension
				"request:body", // normalized
		},
		
		new String[] {
				"request:part:name", // input
				"", // path
				"request:part:name", // filename
				"request:part:name", // basename
				"", // extension
				"request:part:name", // normalized
		},
		
		new String[] {
				"response:body", // input
				"", // path
				"response:body", // filename
				"response:body", // basename
				"", // extension
				"response:body", // normalized
		},
	};


	public void testGetFilePath() {
		for (int i = 0; i < paths.length; i++) {
			String[] row = paths[i];
			assertEquals(row[0], row[1], FileUtils.getFilePath(row[0]));
		}
	}

	public void testGetFileName() {
		for (int i = 0; i < paths.length; i++) {
			String[] row = paths[i];
			assertEquals(row[0], row[2], FileUtils.getFileName(row[0]));
		}
	}

	public void testGetBaseName() {
		for (int i = 0; i < paths.length; i++) {
			String[] row = paths[i];
			assertEquals(row[0], row[3], FileUtils.getBaseName(row[0]));
		}
	}

	public void testGetFileExtension() {
		for (int i = 0; i < paths.length; i++) {
			String[] row = paths[i];
			assertEquals(row[0], row[4], FileUtils.getFileExtension(row[0]));
		}
	}

	public void testNormalize() {
		for (int i = 0; i < paths.length; i++) {
			String[] row = paths[i];
			assertEquals(i + " - " + row[0], row[5], FileUtils.normalize(row[0]));
		}
	}
	
	public void testGetPortURL() {
		assertPortUrl("0", "field1", ProcessingType.SOURCE, FileUtils.getPortURL("port:$0.field1:source"));
		assertPortUrl("record1", "field1", ProcessingType.SOURCE, FileUtils.getPortURL("port:$record1.field1:source"));
		assertPortUrl("record1", "a123", ProcessingType.SOURCE, FileUtils.getPortURL("port:$record1.a123:source"));
		assertPortUrl("1", "field1", ProcessingType.DISCRETE, FileUtils.getPortURL("port:$1.field1:discrete"));
		assertPortUrl("1", "field1", ProcessingType.STREAM, FileUtils.getPortURL("port:$1.field1:stream"));
		assertPortUrl("1", "field1", ProcessingType.DISCRETE, FileUtils.getPortURL("port:$1.field1:"));
		assertPortUrl("1", "field1", ProcessingType.DISCRETE, FileUtils.getPortURL("port:$1.field1"));
		assertPortUrl(null, "field1", ProcessingType.DISCRETE, FileUtils.getPortURL("port:$field1"));
		assertPortUrl(null, "field1", ProcessingType.DISCRETE, FileUtils.getPortURL("port:$field1:discrete"));
		assertPortUrl(null, "field1", ProcessingType.SOURCE, FileUtils.getPortURL("port:$field1:source"));
		assertPortUrl(null, "field1", ProcessingType.STREAM, FileUtils.getPortURL("port:$field1:stream"));
		assertInvalidPortUrl("port:");
		assertInvalidPortUrl("port::");
		assertInvalidPortUrl("port:a:");
		assertInvalidPortUrl("port::abc");
		assertInvalidPortUrl("port:$");
		assertInvalidPortUrl("port:$:abc");
		assertInvalidPortUrl("port:$:source");
		assertInvalidPortUrl("port:$.");
		assertInvalidPortUrl("port:$.:");
		assertInvalidPortUrl("port:$.:stream");
		assertInvalidPortUrl("port:$.ab:stream");
		assertInvalidPortUrl("port:$1");
		assertInvalidPortUrl("port:$1:stream");
		assertInvalidPortUrl("port:$1.:stream");
		assertInvalidPortUrl("port:$asd.:stream");
		assertInvalidPortUrl("port:$asd.123");
		assertInvalidPortUrl("port:$asd.123:stream");
		assertInvalidPortUrl("port:asd.123:stream");
		assertInvalidPortUrl("port:asd123:stream");
		assertInvalidPortUrl("port:$$asd:stream");
		assertInvalidPortUrl("Port:$0.field1:source");
	}
	
	public void testCanWrite() throws Exception {
		URL contextURL = FileUtils.getFileURL("file:/c:/project/");
		
		try {
			assertFalse(FileUtils.canWrite(contextURL, "request:body"));
			fail("Cannot read to a HTTP request");
		} catch(ComponentNotReadyException e) {
			
		}
		try {
			FileUtils.canWrite(contextURL, "request:part:name");
			fail("Cannot read to a HTTP request");
		} catch(ComponentNotReadyException e) {
			
		}
		assertTrue(FileUtils.canWrite(contextURL, "response:body"));
		
	}
	
	private void assertPortUrl(String expectedRecordName, String expectedFieldName, ProcessingType expectedProcessingType, PortURL portUrl) {
		assertEquals(expectedRecordName, portUrl.getRecordName());
		assertEquals(expectedFieldName, portUrl.getFieldName());
		assertEquals(expectedProcessingType, portUrl.getProcessingType());
	}

	private void assertInvalidPortUrl(String portUrlStr) {
		try {
			FileUtils.getPortURL(portUrlStr);
			assertTrue("Port URL '" + portUrlStr + "' is considered as valid, which is not expected.", false);
		} catch (JetelRuntimeException e) {
			//ok
		}
	}
	
}
