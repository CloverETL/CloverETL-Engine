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
package org.jetel.util.file.stream;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.jetel.data.Defaults;
import org.jetel.test.CloverTestCase;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 26. 9. 2016
 */
public class WilcardDirectoryStreamTest extends CloverTestCase {
	
	private URL contextUrl = null;
	private WildcardDirectoryStream stream;
	private Iterator<DirectoryStream<Input>> it;

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		stream = newInstance();
	}

	private WildcardDirectoryStream newInstance() {
		return newInstance(contextUrl, "*");
	}

	private WildcardDirectoryStream newInstance(URL contextUrl, String input) {
		String[] parts = input.isEmpty() ? new String[0] : input.split(Defaults.DEFAULT_PATH_SEPARATOR_REGEX);
		return new WildcardDirectoryStream(contextUrl, Arrays.asList(parts));
	}

	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
		stream.close();
		stream = null;
		it = null;
	}

	/**
	 * Test method for {@link org.jetel.util.file.stream.WildcardDirectoryStream#close()}.
	 */
	public void testClose() throws IOException {
		stream.close();
		stream.close(); // close() should be idempotent
	}

	/**
	 * Test method for {@link org.jetel.util.file.stream.WildcardDirectoryStream#WildcardDirectoryStream(java.net.URL, java.lang.Iterable)}.
	 */
	public void testConstructor() throws IOException {
		try (WildcardDirectoryStream s = new WildcardDirectoryStream(null, Collections.<String>emptyList())) {
			
		}
		
		URL contextUrl = new File(".").toURI().toURL();
		try (WildcardDirectoryStream s = new WildcardDirectoryStream(contextUrl, Collections.singletonList("*"))) {
		}
		
		try (WildcardDirectoryStream s = new WildcardDirectoryStream(null, null)) {
		} catch (NullPointerException npe) {}
	}

	/**
	 * Test method for {@link org.jetel.util.file.stream.WildcardDirectoryStream#hasNext()}.
	 */
	public void testHasNext() throws IOException {
		int count = 0;
		for (it = stream.iterator(); it.hasNext(); ) {
			DirectoryStream<Input> s = it.next();
			count++;
			s.close();
		}
		assertEquals(1, count);
		stream.close();
		
		stream = newInstance(contextUrl, "");
		it = stream.iterator();
		assertFalse(it.hasNext());
	}

	/**
	 * Test method for {@link org.jetel.util.file.stream.WildcardDirectoryStream#next()}.
	 */
	public void testNext() throws IOException {
		it = stream.iterator();
		it.next();
		try {
			it.next();
			fail();
		} catch (NoSuchElementException ex) {
		}
		stream.close();
		
		stream = newInstance(contextUrl, "");
		it = stream.iterator();
		try {
			it.next();
			fail();
		} catch (NoSuchElementException ex) {
		}
	}

	/**
	 * Test method for {@link org.jetel.util.file.stream.AbstractDirectoryStream#remove()}.
	 */
	public void testRemove() {
		try {
			stream.remove();
			fail();
		} catch (UnsupportedOperationException ex) {
		}
	}

	/**
	 * Test method for {@link org.jetel.util.file.stream.AbstractDirectoryStream#iterator()}.
	 */
	public void testIterator() throws IOException {
		it = stream.iterator(); 
		try {
			it = stream.iterator();
			fail();
		} catch (IllegalStateException ex) {
		}
		assertTrue(it.hasNext());
		stream.close();
		
		stream = newInstance();
		stream.close();
		try {
			it = stream.iterator();
			fail();
		} catch (IllegalStateException ex) {}
	}

}
