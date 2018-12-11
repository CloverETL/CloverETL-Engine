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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeNoException;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.Arrays;

import org.junit.Test;

/**
 * @author Milan (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 7. 12. 2018
 */
public class LenientZipInputStreamTest {

	private static final byte[] ZIP_LOCAL = {0x50, 0x4b, 0x03, 0x04};

	@Test
	public void testConstructorBuffered() throws Exception {
		Field in = null;
		try {
			in = FilterInputStream.class.getDeclaredField("in");
			in.setAccessible(true);
		} catch (Exception e) {
			assumeNoException("Access using reflection failed", e);
		}

		LenientZipInputStream is = fromEmptyStream();
		assertTrue(in.get(is) instanceof BufferedInputStream);
	}

	@Test
	public void testConstructorDefaultLimit() throws IOException {
		try (LenientZipInputStream is = fromEmptyStream()) {
			assertEquals(5 * 1024 * 1024, is.getLimit());
		}
	}
	
	@Test
	public void testReadEmptyFile1() throws IOException {
		try (
			LenientZipInputStream is = new LenientZipInputStream(newEmptyStream()) {

				@Override
				protected int limitReached() throws IOException {
					throw new IOException("Limit reached");
				}
				
			};
		) {
			assertEquals(-1, is.read()); // EOF
			
			// test partial buffered read after single byte read
			assertEquals(0, is.read(new byte[4], 0, 0)); // limit is 0
			assertEquals(-1, is.read(new byte[4], 0, 1)); // EOF, limit is > 0
		}

	}

	@Test
	public void testReadEmptyFile2() throws IOException {
		try (
			ByteArrayInputStream bais = newEmptyStream(); // empty file
			LenientZipInputStream is = new LenientZipInputStream(bais) {

				@Override
				protected int limitReached() throws IOException {
					throw new IOException("Limit reached");
				}
				
			};
		) {
			byte[] buffer = new byte[10];
			assertEquals(-1, is.read(buffer, 4, 100));
		}
	}
	
	@Test
	public void testLimitReachedEmptyFile() throws IOException {
		try (
			LenientZipInputStream is = new LenientZipInputStream(newEmptyStream(), 10); // set limit to 10
		) {
			for (int i = 0; i < 20; i++) { // 20 > limit
				assertEquals(-1, is.read()); // no exception is thrown
			}
		}
	}

	@Test
	public void testReadInvalidFile() throws IOException {
		byte[] bytes = new byte[20];
		Arrays.fill(bytes, (byte) 5); // no local file header signature
		try (
			ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
			LenientZipInputStream is = new LenientZipInputStream(bais); // default limit = 1 MB
		) {
			assertEquals(-1, is.read()); // first call returns EOF, no exception is thrown
		}
	}

	@Test
	public void testReadInvalidFileLimit() throws IOException {
		byte[] bytes = new byte[20];
		Arrays.fill(bytes, (byte) 5); // no local file header signature
		try (
			ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
			LenientZipInputStream is = new LenientZipInputStream(bais, 10); // set limit to 10 bytes
		) {
			assertEquals(-1, is.read()); // first call returns EOF, no exception is thrown
		}
	}

	@Test
	public void testUnlimitedAfterHeader() throws IOException {
		
		final int fileSize = 2 * LenientZipInputStream.DEFAULT_LIMIT;
		try (
			InputStream bigFile = new InputStream() {
				
				private int counter = 0;

				@Override
				public int read() throws IOException {
					if (counter < ZIP_LOCAL.length) { // header signature
						return ZIP_LOCAL[counter++];
					} else if (counter < fileSize) { // dummy data
						counter++;
						return 5;
					}
					
					return -1; // EOF
				}
				
			};
				
			LenientZipInputStream is = new LenientZipInputStream(bigFile);
		) {
			for (int i = 0; i < ZIP_LOCAL.length; i++) { // header signature
				assertEquals(ZIP_LOCAL[i], is.read());
			}
			int limit = fileSize - ZIP_LOCAL.length;
			for (int i = 0; i < limit; i++) { // dummy data
				assertEquals(5, is.read());
			}
			assertEquals(-1, is.read()); // EOF
		}
	}

	@Test
	public void testRead() throws IOException {
		try (LenientZipInputStream is = fromHeaderStream()) {
			for (int i = 0; i < ZIP_LOCAL.length; i++) {
				assertEquals(ZIP_LOCAL[i], is.read());
			}
			
			assertEquals(-1, is.read()); // EOF
			assertEquals(0, is.read(new byte[4], 0, 0)); // limit is 0
			assertEquals(-1, is.read(new byte[4], 0, 1)); // EOF, limit is > 0
			assertEquals(0, is.read(new byte[0])); // limit is 0
		}
	}

	@Test
	public void testReadBuffered() throws IOException {
		try (LenientZipInputStream is = fromHeaderStream()) {
			byte[] buffer = new byte[10];
			assertEquals(4, is.read(buffer));
			assertTrue(Arrays.equals(new byte[] { 0x50, 0x4b, 0x03, 0x04, 0, 0, 0, 0, 0, 0 }, buffer));
			
			assertEquals(-1, is.read()); // EOF
			assertEquals(0, is.read(new byte[4], 0, 0)); // limit is 0
			assertEquals(-1, is.read(new byte[4], 0, 1)); // EOF, limit is > 0
			assertEquals(0, is.read(new byte[0])); // limit is 0
		}
	}

	@Test
	public void testReadBufferedOffset() throws IOException {
		try (LenientZipInputStream is = fromHeaderStream()) {
			byte[] buffer = new byte[10];
			assertEquals(4, is.read(buffer, 4, 100));
			assertTrue("Wrong offset: " + Arrays.toString(buffer), Arrays.equals(new byte[] {0, 0, 0, 0, 0x50, 0x4b, 0x03, 0x04, 0, 0}, buffer));
			assertEquals(-1, is.read(new byte[10]));
			assertEquals(-1, is.read());
		}
	}

	@Test
	public void testReadBufferedLimit() throws IOException {
		try (LenientZipInputStream is = fromHeaderStream()) {
			byte[] buffer = new byte[10];
			assertEquals(2, is.read(buffer, 0, 2));
			assertTrue("Wrong limit: " + Arrays.toString(buffer), Arrays.equals(new byte[] {0x50, 0x4b, 0, 0, 0, 0, 0, 0, 0, 0}, buffer));
			assertEquals(ZIP_LOCAL[2], is.read());
			buffer = new byte[10];
			assertEquals(1, is.read(buffer));
			assertTrue("Wrong limit: " + Arrays.toString(buffer), Arrays.equals(new byte[] {0x04, 0, 0, 0, 0, 0, 0, 0, 0, 0}, buffer));
			assertEquals(-1, is.read());
		}
	}

	@Test
	public void testReadOffsetPartial() throws IOException {
		try (LenientZipInputStream is = fromHeaderStream()) {
			byte[] buffer = new byte[10];
			assertEquals(ZIP_LOCAL[0], is.read());
			assertEquals(ZIP_LOCAL[1], is.read());
			assertEquals(2, is.read(buffer, 4, 100));
			assertTrue("Wrong offset", Arrays.equals(new byte[] {0, 0, 0, 0, 0x03, 0x04, 0, 0, 0, 0}, buffer));
			assertEquals(-1, is.read(new byte[10]));
			assertEquals(-1, is.read());
		}
	}

	@Test
	public void testClose() throws IOException {
		MockInputStream mockInputStream = new MockInputStream(new byte[0]);
		LenientZipInputStream is = new LenientZipInputStream(mockInputStream);
		assertFalse(mockInputStream.isClosed());
		is.close();
		assertTrue(mockInputStream.isClosed());
	}

	private ByteArrayInputStream newEmptyStream() {
		return new ByteArrayInputStream(new byte[0]);
	}
	
	private LenientZipInputStream fromEmptyStream() {
		return new LenientZipInputStream(newEmptyStream());
	}
	
	private ByteArrayInputStream newHeaderStream() {
		return new ByteArrayInputStream(ZIP_LOCAL);
	}

	private LenientZipInputStream fromHeaderStream() {
		return new LenientZipInputStream(newHeaderStream());
	}
	
}
