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

	public void testGetChildURI() throws Exception {
		URI parent;
		String child;
		URI childUri;
		URI result;
		URI expected;

		parent = URI.create("ftp://hostname/dir/");
		child = "fileName";
		childUri = new URI(null, child, null);
		expected = URI.create("ftp://hostname/dir/fileName");
		result = URIUtils.getChildURI(parent, child);
		assertEquals(expected, result);
		result = URIUtils.getChildURI(parent, childUri);
		assertEquals(expected, result);

		parent = URI.create("ftp://hostname/dir");
		result = URIUtils.getChildURI(parent, child);
		assertEquals(expected, result);
		result = URIUtils.getChildURI(parent, childUri);
		assertEquals(expected, result);

		child = "file name";
		childUri = new URI(null, child, null);
		expected = URI.create("ftp://hostname/dir/file%20name");
		result = URIUtils.getChildURI(parent, child);
		result = URIUtils.getChildURI(parent, childUri);
		assertEquals(expected, result);

		child = "file/name";
		childUri = new URI("file%2Fname");
		expected = URI.create("ftp://hostname/dir/file%2Fname");
		result = URIUtils.getChildURI(parent, child);
		result = URIUtils.getChildURI(parent, childUri);
		assertEquals(expected, result);

		parent = URI.create("ftp://hostname/.");
		expected = URI.create("ftp://hostname/file%2Fname");
		result = URIUtils.getChildURI(parent, child);
		assertEquals(expected, result);
		result = URIUtils.getChildURI(parent, childUri);
		assertEquals(expected, result);

		parent = URI.create("ftp://hostname/dir/..");
		expected = URI.create("ftp://hostname/file%2Fname");
		result = URIUtils.getChildURI(parent, child);
		assertEquals(expected, result);
		result = URIUtils.getChildURI(parent, childUri);
		assertEquals(expected, result);

		parent = URI.create("ftp://hostname/dir/../");
		expected = URI.create("ftp://hostname/file%2Fname");
		result = URIUtils.getChildURI(parent, child);
		assertEquals(expected, result);
		result = URIUtils.getChildURI(parent, childUri);
		assertEquals(expected, result);

		parent = URI.create("ftp://hostname/dir/file.txt");
		expected = URI.create("ftp://hostname/dir/file.txt/file%2Fname");
		result = URIUtils.getChildURI(parent, child);
		assertEquals(expected, result);
		result = URIUtils.getChildURI(parent, childUri);
		assertEquals(expected, result);
		
		child = "";
		childUri = URI.create(child);
		parent = URI.create("ftp://hostname/dir");
		// the trailing slash is not required, but should not break anything
		// we assume the context URL is a directory
		expected = URI.create("ftp://hostname/dir/");
		result = URIUtils.getChildURI(parent, child);
		assertEquals(expected, result);
		result = URIUtils.getChildURI(parent, childUri);
		assertEquals(expected, result);
		parent = URI.create("ftp://hostname/dir/");
		result = URIUtils.getChildURI(parent, child);
		assertEquals(expected, result);
		result = URIUtils.getChildURI(parent, childUri);
		assertEquals(expected, result);
	}

	public void testGetFileName() {
		URI uri;
		
		uri = URI.create("ftp://hostname/dir/subdir/../file.txt");
		assertEquals("file.txt", URIUtils.getFileName(uri));

		uri = URI.create("ftp://hostname/dir/subdir/./file.txt");
		assertEquals("file.txt", URIUtils.getFileName(uri));

		uri = URI.create("ftp://hostname/dir/subdir/..");
		assertEquals("dir", URIUtils.getFileName(uri));

		uri = URI.create("ftp://hostname/dir/.");
		assertEquals("dir", URIUtils.getFileName(uri));

		uri = URI.create("ftp://hostname/file%20name");
		assertEquals("file name", URIUtils.getFileName(uri));

		uri = URI.create("ftp://hostname/file%2Fname");
		assertEquals("file/name", URIUtils.getFileName(uri));

		uri = URI.create("ftp://hostname/");
		assertTrue(URIUtils.getFileName(uri).isEmpty());

		uri = URI.create("ftp://hostname");
		assertTrue(URIUtils.getFileName(uri).isEmpty());

		uri = URI.create("relative/path");
		assertEquals("path", URIUtils.getFileName(uri));

		uri = URI.create("relative/file%2Fname");
		assertEquals("file/name", URIUtils.getFileName(uri));

		uri = URI.create("relative/../../../file%20name");
		assertEquals("file name", URIUtils.getFileName(uri));

		uri = URI.create("relative/./././file%20name");
		assertEquals("file name", URIUtils.getFileName(uri));

		uri = URI.create("relative/../../../");
		assertEquals("", URIUtils.getFileName(uri));

	}

	public void testUrlEncode() {
		assertEquals("file%20name", URIUtils.urlEncode("file name"));
		assertEquals("file%2Fname", URIUtils.urlEncode("file/name"));
		assertEquals("milan.krivanek%40javlin.eu", URIUtils.urlEncode("milan.krivanek@javlin.eu"));
		assertEquals("path%3Fquery", URIUtils.urlEncode("path?query"));
		assertEquals("path%23fragment", URIUtils.urlEncode("path#fragment"));
		assertEquals("file%25name", URIUtils.urlEncode("file%name")); // escape % sign
		assertTrue(URIUtils.urlEncode("").isEmpty());
	}

	public void testUrlDecode() {
		assertEquals("file name", URIUtils.urlDecode("file%20name"));
		assertEquals("file name", URIUtils.urlDecode("file+name")); // decode + sign as space
		assertEquals("file/name", URIUtils.urlDecode("file%2Fname"));
		assertEquals("milan.krivanek@javlin.eu", URIUtils.urlDecode("milan.krivanek%40javlin.eu"));
		assertEquals("path?query", URIUtils.urlDecode("path%3Fquery"));
		assertEquals("path#fragment", URIUtils.urlDecode("path%23fragment"));
		assertEquals("path%23fragment", URIUtils.urlDecode("path%2523fragment")); // no double unescaping
		assertTrue(URIUtils.urlDecode("").isEmpty());

		assertEquals("file name", URIUtils.urlDecode("file name"));
		assertEquals("file name", URIUtils.urlDecode("file name"));
		assertEquals("file/name", URIUtils.urlDecode("file/name"));
		assertEquals("milan.krivanek@javlin.eu", URIUtils.urlDecode("milan.krivanek@javlin.eu"));
		assertEquals("path?query", URIUtils.urlDecode("path?query"));
		assertEquals("path#fragment", URIUtils.urlDecode("path#fragment"));
	}

}
