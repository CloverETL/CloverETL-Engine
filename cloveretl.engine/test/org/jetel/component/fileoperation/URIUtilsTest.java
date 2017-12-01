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
package org.jetel.component.fileoperation;

import java.net.URI;

import junit.framework.TestCase;

public class URIUtilsTest extends TestCase {

	private void testGetChildURI(String parent, String child, String expected) {
		URI parentUri = URI.create(parent);
		URI childUri = URI.create(URIUtils.urlEncode(child));
		assertEqualUris(expected, URIUtils.getChildURI(parentUri, child));
		assertEqualUris(expected, URIUtils.getChildURI(parentUri, childUri));
	}

	private static void assertEqualUris(String expectedUri, URI actualUri) {
		assertEquals(URI.create(expectedUri), actualUri);
		assertEquals(expectedUri, actualUri.toString()); // URI.equals() compares individual fields - compare the whole strings in addition
	}
	
	public void testGetChildURI() {
		testGetChildURI("ftp://hostname/dir/", "fileName", "ftp://hostname/dir/fileName");
		String parent = "ftp://hostname/dir";
		testGetChildURI(parent, "fileName", "ftp://hostname/dir/fileName");
		testGetChildURI(parent, "file name", "ftp://hostname/dir/file%20name");

		String child = "file/name";
		testGetChildURI(parent, child, "ftp://hostname/dir/file%2Fname");
		testGetChildURI("ftp://hostname/.", child, "ftp://hostname/file%2Fname");
		testGetChildURI("ftp://hostname/dir/..", child, "ftp://hostname/file%2Fname");
		testGetChildURI("ftp://hostname/dir/../", child, "ftp://hostname/file%2Fname");
		testGetChildURI("ftp://hostname/dir/file.txt", child, "ftp://hostname/dir/file.txt/file%2Fname");
		
		// the trailing slash is not required, but should not break anything
		// we assume the context URL is a directory
		child = "";
		testGetChildURI("ftp://hostname/dir", child, "ftp://hostname/dir/");
		testGetChildURI("ftp://hostname/dir/", child, "ftp://hostname/dir/");
	}

	private void testGetFileName(String uri, String expectedFileName) {
		assertEquals(expectedFileName, URIUtils.getFileName(URI.create(uri)));
	}
	
	public void testGetFileName() {
		testGetFileName("ftp://hostname/dir/subdir/../file.txt", "file.txt");
		testGetFileName("ftp://hostname/dir/subdir/./file.txt", "file.txt");
		testGetFileName("ftp://hostname/dir/subdir/..", "dir");
		testGetFileName("ftp://hostname/dir/.", "dir");
		testGetFileName("ftp://hostname/file%20name", "file name");
		testGetFileName("ftp://hostname/file%2Fname", "file/name");
		testGetFileName("ftp://hostname/", "");
		testGetFileName("ftp://hostname", "");
		testGetFileName("relative/path", "path");
		testGetFileName("relative/file%2Fname", "file/name");
		testGetFileName("relative/../../../file%20name", "file name");
		testGetFileName("relative/./././file%20name", "file name");
		testGetFileName("relative/../../../", "");
	}

	private void testUrlEncode(String expected, String input) {
		assertEquals(expected, URIUtils.urlEncode(input));
	}
	
	public void testUrlEncode() {
		testUrlEncode("file%20name", "file name");
		testUrlEncode("file%2Fname", "file/name");
		testUrlEncode("milan.krivanek%40javlin.eu", "milan.krivanek@javlin.eu");
		testUrlEncode("path%3Fquery", "path?query");
		testUrlEncode("path%23fragment", "path#fragment");
		testUrlEncode("file%25name", "file%name"); // escape % sign
		testUrlEncode("", "");
	}
	
	private void testUrlDecode(String expected, String input) {
		assertEquals(expected, URIUtils.urlDecode(input));
	}

	public void testUrlDecode() {
		testUrlDecode("file name", "file%20name");
		testUrlDecode("file+name", "file+name"); // preserve + sign
		testUrlDecode("file/name", "file%2Fname");
		testUrlDecode("milan.krivanek@javlin.eu", "milan.krivanek%40javlin.eu");
		testUrlDecode("path?query", "path%3Fquery");
		testUrlDecode("path#fragment", "path%23fragment");
		testUrlDecode("path%23fragment", "path%2523fragment"); // no double unescaping
		testUrlDecode("", "");

		testUrlDecode("file name", "file name");
		testUrlDecode("file name", "file name");
		testUrlDecode("file/name", "file/name");
		testUrlDecode("milan.krivanek@javlin.eu", "milan.krivanek@javlin.eu");
		testUrlDecode("path?query", "path?query");
		testUrlDecode("path#fragment", "path#fragment");
		
		// CLO-9981: URIUtils.urlDecode() should be consistent with java.net.URI
		URI uri = URI.create("s3://AKIAIG6EFMJBH6F7WYZQ:A+B%2FC@s3.eu-central-1.amazonaws.com/cloveretl.test.eu/test+fo/?a+b+b");
		testUrlDecode(uri.getUserInfo(), uri.getRawUserInfo());
		testUrlDecode(uri.getPath(), uri.getRawPath());
		testUrlDecode(uri.getQuery(), uri.getRawQuery());
	}
	
	private void testResolve(String base, String pathUri, String expected) {
		URI baseUri = URI.create(base);
		assertEqualUris(expected, URIUtils.resolve(baseUri, pathUri));
		assertEqualUris(expected, URIUtils.resolve(baseUri, URI.create(pathUri)));
	}

	public void testResolve() {
		testResolve(
				"s3:(proxy://user:password@hostname.com:8080)//s3.amazonaws.com/cloveretl.engine.test/",
				"file.txt",
				"s3:(proxy://user:password@hostname.com:8080)//s3.amazonaws.com/cloveretl.engine.test/file.txt"
		);

		testResolve(
				"s3://s3.amazonaws.com/cloveretl.engine.test/",
				"file.txt",
				"s3://s3.amazonaws.com/cloveretl.engine.test/file.txt"
		);

		testResolve(
				"s3://s3.amazonaws.com/cloveretl.engine.test/",
				"http://seznam.cz",
				"http://seznam.cz"
		);

		testResolve(
				"s3:(proxy://user:password@hostname.com:8080)//s3.amazonaws.com/cloveretl.engine.test/",
				"http://seznam.cz",
				"http://seznam.cz"
		);

		testResolve(
				"http://seznam.cz",
				"s3:(proxy://user:password@hostname.com:8080)//s3.amazonaws.com/cloveretl.engine.test/",
				"s3:(proxy://user:password@hostname.com:8080)//s3.amazonaws.com/cloveretl.engine.test/"
		);
	}
	
	private void testInsertProxyString(String uri, String proxyString, String expected) {
		assertEqualUris(expected, URIUtils.insertProxyString(URI.create(uri), proxyString));
	}
	
	public void testInsertProxyString() {
		testInsertProxyString("sftp://hostname/path", "proxy://user:password@proxyhost:8080", "sftp:(proxy://user:password@proxyhost:8080)//hostname/path");
		testInsertProxyString("sftp://hostname/path", "proxy://user:p%3A%20%3B%23%25%26*%2B%24ssword@proxyhost:8080", "sftp:(proxy://user:p%3A%20%3B%23%25%26*%2B%24ssword@proxyhost:8080)//hostname/path");
		testInsertProxyString("s3://s3.eu-central-1.amazonaws.com/cloveretl.test.eu/test-fo/*.csv", "proxy://user:p%3A%20%3B%23%25%26*%2B%24ssword@proxyhost:8080", "s3:(proxy://user:p%3A%20%3B%23%25%26*%2B%24ssword@proxyhost:8080)//s3.eu-central-1.amazonaws.com/cloveretl.test.eu/test-fo/*.csv");
		testInsertProxyString("sftp://hostname/path", "proxy://proxyhost:8080", "sftp:(proxy://proxyhost:8080)//hostname/path");
		testInsertProxyString("sftp://hostname/path", "proxy://proxyhost", "sftp:(proxy://proxyhost)//hostname/path");
		testInsertProxyString("sftp://hostname/path", "proxysocks://user:password@proxyhost:8080", "sftp:(proxysocks://user:password@proxyhost:8080)//hostname/path");
		testInsertProxyString("sftp://hostname/path", "direct:", "sftp:(direct:)//hostname/path");
		testInsertProxyString("sftp://hostname/path", null, "sftp://hostname/path");
	}
	
	private void testRemoveProxyString(String expected, String uri) {
		assertEqualUris(expected, URIUtils.removeProxyString(URI.create(uri)));
	}

	public void testRemoveProxyString() {
		testRemoveProxyString("sftp://hostname/path", "sftp:(proxy://user:password@proxyhost:8080)//hostname/path");
		testRemoveProxyString("sftp://hostname/path", "sftp:(proxy://user:p%3A%20%3B%23%25%26*%2B%24ssword@proxyhost:8080)//hostname/path");
		testRemoveProxyString("s3://s3.eu-central-1.amazonaws.com/cloveretl.test.eu/test-fo/*.csv", "s3:(proxy://user:p%3A%20%3B%23%25%26*%2B%24ssword@proxyhost:8080)//s3.eu-central-1.amazonaws.com/cloveretl.test.eu/test-fo/*.csv");
		testRemoveProxyString("sftp://hostname/path", "sftp:(proxy://proxyhost:8080)//hostname/path");
		testRemoveProxyString("sftp://hostname/path", "sftp:(proxy://proxyhost)//hostname/path");
		testRemoveProxyString("sftp://hostname/path", "sftp:(proxysocks://user:password@proxyhost:8080)//hostname/path");
		testRemoveProxyString("sftp://hostname/path", "sftp:(direct:)//hostname/path");
		testRemoveProxyString("sftp://hostname/path", "sftp://hostname/path");
		testRemoveProxyString("ftp://test:p%40ssword@hostname", "ftp://test:p%40ssword@hostname");
	}
	
	private void testGetPath(String expected, String uri) {
		assertEquals(expected, URIUtils.getPath(URI.create(uri)));
	}
	
	public void testGetPath() {
		testGetPath("/path", "sftp:(proxy://user:password@proxyhost:8080)//hostname/path");
		testGetPath("/path", "sftp:(proxy://user:p%3A%20%3B%23%25%26*%2B%24ssword@proxyhost:8080)//hostname/path");
		testGetPath("/cloveretl.test.eu/test-fo/*.csv", "s3:(proxy://user:p%3A%20%3B%23%25%26*%2B%24ssword@proxyhost:8080)//s3.eu-central-1.amazonaws.com/cloveretl.test.eu/test-fo/*.csv");
		testGetPath("/path", "sftp:(proxy://proxyhost:8080)//hostname/path");
		testGetPath("/path", "sftp:(proxy://proxyhost)//hostname/path");
		testGetPath("/path", "sftp:(proxysocks://user:password@proxyhost:8080)//hostname/path");
		testGetPath("/path", "sftp:(direct:)//hostname/path");
		testGetPath("/path", "sftp://hostname/path");
		testGetPath("", "ftp://test:p%40ssword@hostname");
	}
	
}
