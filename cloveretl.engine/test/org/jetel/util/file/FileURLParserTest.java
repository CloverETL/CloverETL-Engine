/*
 *    jETeL/Clover - Java based ETL application framework.
 *    Copyright (C) 2004-08 Javlin Consulting <info@javlinconsulting.cz>
 *
 *    This library is free software; you can redistribute it and/or
 *    modify it under the terms of the GNU Lesser General Public
 *    License as published by the Free Software Foundation; either
 *    version 2.1 of the License, or (at your option) any later version.
 *
 *    This library is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *    Lesser General Public License for more details.
 *
 *    You should have received a copy of the GNU Lesser General Public
 *    License along with this library; if not, write to the Free Software
 *    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 */


package org.jetel.util.file;

import org.jetel.test.CloverTestCase;

/**
 * @author Jan Ausperger (jan.ausperger@javlin.eu)
 *         (c) Javlin, a.s. (www.javlin.eu)
 */

public class FileURLParserTest extends CloverTestCase {

	private String[] simpleURLs = new String[] {
			"/path/filename.txt",
			"ftp://user:password@server/path/name.txt",
			"http://server/path/name.txt",
			"port:$0.fieldName:source",
			"dict:keyName:discrete",
			"-"
	};
	
	private String[] complexURLs = new String[] {
			"zip:/path/filename.zip",
			"gzip:/path/filename.gz",
			"zip:/path/filename.zip#name.txt",
			"zip:(http://server/path/name.zip)#filename.txt",
			"zip:(ftp://user:password@server/path/name.zip)#filename.txt",
			"zip:(zip:(http://server/path/name.zip)#filename.zip)#name.txt",
			"zip:(/path/name.*)#filename?.zip)#name.*",
			"zip:/path/name.*#filename?.zip#name.*",
			"gzip:(http://server/path/name.gz)",
			"gzip:(ftp://user:password@server/path/name.gz)",
			"tar:(path/name.tar)#path/filename.txt",
			"tar:(gzip:path/name.tar.gz)#path/filename.txt",
			"tar:(ftp://user:password@server/path/name.tar)#filename.txt",
			"tar:(gzip:(/path/name.g?)#filename.zi*)#name.???"
	};
	
	private String[] complexMostURLs = new String[] {
			"/path/filename.zip",
			"/path/filename.gz",
			"/path/filename.zip",
			"http://server/path/name.zip",
			"ftp://user:password@server/path/name.zip",
			"http://server/path/name.zip",
			"/path/name.*)#filename?.zip",
			"/path/name.*#filename?.zip",
			"http://server/path/name.gz",
			"ftp://user:password@server/path/name.gz",
			"path/name.tar",
			"path/name.tar.gz",
			"ftp://user:password@server/path/name.tar",
			"/path/name.g?"
	};

	/* (non-Javadoc)
	 * @see junit.framework.TestCase#setUp()
	 */
	@Override
	protected void setUp() throws Exception {
		super.setUp();
	}

	public void testMostInnerAddress() {
		for (String file: simpleURLs) {
			assertEquals(file, FileURLParser.getMostInnerAddress(file));
		}
		
		for (int i=0; i<complexURLs.length; i++) {
			assertEquals(complexMostURLs[i], FileURLParser.getMostInnerAddress(complexURLs[i]));
		}
	}

	public void testMostInnerAddress2() {
		assertEquals("proxy://user:password@hostname", FileURLParser.getMostInnerAddress("sftp:(proxy://user:password@hostname)//sftp.sftphost.com/path/to/file.txt", true));
		assertEquals("sftp:(proxy://user:password@hostname)//sftp.sftphost.com/path/to/file.txt", FileURLParser.getMostInnerAddress("sftp:(proxy://user:password@hostname)//sftp.sftphost.com/path/to/file.txt", false));
		assertEquals("/path/name.g?", FileURLParser.getMostInnerAddress("tar:(gzip:(/path/name.g?)#filename.zi*)#name.???", true));
	}

	/* (non-Javadoc)
	 * @see junit.framework.TestCase#tearDown()
	 */
	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
	}

}
