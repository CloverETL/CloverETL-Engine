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
package org.jetel.data.parser;

import java.io.ByteArrayInputStream;
import java.nio.channels.Channels;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Random;

import org.jetel.data.parser.CharByteInputReader.RobustInputReader;
import org.jetel.test.CloverTestCase;
import org.jetel.util.string.StringUtils;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 10.2.2012
 */
public class RobustInputReaderTest extends CloverTestCase {
	
	private static final String INPUT = "Žluťoučký kůň úpěl ďábelské ódy";
	
	private static final Charset defaultCharset = Charset.forName("UTF-8");
	
	private static final int SEED = 0;
	
	private RobustInputReader reader;

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		initEngine();
		reader = createReader();
	}
	
	protected RobustInputReader createReader() {
		RobustInputReader reader = new RobustInputReader(defaultCharset, 0);
		reader.setInputSource(Channels.newChannel(new ByteArrayInputStream(INPUT.getBytes(defaultCharset))));
		return reader;
	}

	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
		reader = null;
	}
	
	private boolean isEndOfInput(int chr) {
		switch (chr) {
		case CharByteInputReader.END_OF_INPUT:
		case CharByteInputReader.BLOCKED_BY_MARK:
		case CharByteInputReader.DECODING_FAILED:
			return true;
		}
		return false;
	}
	
	/**
	 * Test method for {@link org.jetel.data.parser.CharByteInputReader#readChar()}.
	 */
	public void testReadChar() throws Exception {
		reader.mark(); // FIXME why is this necessary?
		StringBuilder sb = new StringBuilder();
		for (int chr = reader.readChar(); !isEndOfInput(chr); chr = reader.readChar()) {
			sb.append((char) chr);
		}
		assertEquals(INPUT, sb.toString());
	}

	/**
	 * Test method for {@link org.jetel.data.parser.CharByteInputReader#readByte()}.
	 */
	public void testReadByte() throws Exception {
		byte[] expected = INPUT.getBytes(defaultCharset);
		byte[] actual = new byte[expected.length];
		int i = 0;
		for (int b = reader.readByte(); !isEndOfInput(b); b = reader.readByte()) {
			actual[i++] = (byte) b;
		}
		assertTrue(Arrays.equals(expected, actual));
	}

	private String read(int count) throws Exception {
		StringBuilder sb = new StringBuilder();
		reader.mark();
		int i = 0;
		for (int isym = reader.readChar(); !isEndOfInput(isym) && i < count; isym = reader.readChar()) {
			sb.append((char) isym);
			i++;
		}
		reader.revert();
		String result = sb.toString();
		System.out.println(result.length() + ": " + StringUtils.specCharToString(result));
		return result;
	}

	private String readAll() throws Exception {
		return read(Integer.MAX_VALUE);
	}
	
	/**
	 * Test method for {@link org.jetel.data.parser.CharByteInputReader#revert()}.
	 */
	public void testMarkRevert() throws Exception {
		String data = null;
		
		data = readAll();
		assertEquals(INPUT, data);
		data = readAll();
		assertEquals(INPUT, data);
		
		data = readAll();
		assertEquals(INPUT, data);

		int ATTEMPTS = 1000;
		
		Random r = new Random(SEED);
		for (int i = 0; i < ATTEMPTS; i++) {
			int length = r.nextInt(INPUT.length() + 1);
			data = read(length);
			assertEquals(INPUT.substring(0, length), data);
			checkReadBoth(length);
		}

		data = readAll();
		assertEquals(INPUT, data);
		
	}
	
	private byte[] readBytes(int count) throws Exception {
		byte[] result = new byte[count];
		for (int i = 0; i < count; i++) {
			result[i] = (byte) reader.readByte();
		}
		return result;
	}
	
	private String readBoth(int count) throws Exception {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < count; i++) {
			if ((i % 2) == 0) { // read bytes
				char nextChar = INPUT.charAt(i);
				int length = String.valueOf(nextChar).getBytes(defaultCharset).length;
				byte[] bytes = readBytes(length);
				String decoded = new String(bytes, defaultCharset);
				sb.append(decoded);
			} else { // read a char
				sb.append((char) reader.readChar());
			}
		}
		
		return sb.toString();
	}

	private void checkReadBoth(int length) throws Exception {
		String expected = INPUT.substring(0, length);
		reader.mark();
		String actual = readBoth(length);
		reader.revert();
		assertEquals(expected, actual);
	}
	
	public void testReadBoth() throws Exception {
		readBoth(INPUT.length());
	}

}
