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

import org.jetel.exception.PolicyType;

public class SingleByteCharsetInputReaderTest extends CharByteReaderTestCase {
	
	private static final int MAX_BACK_MARK = 20;
	private static final Charset CHARSET = Charset.forName("ASCII");
	
	@Override
	protected ICharByteInputReader getReader() {
		return new CharByteInputReader.SingleByteCharsetInputReader(CHARSET, MAX_BACK_MARK, PolicyType.STRICT);
	}
	
	@Override
	public void testRevert() throws Exception {
		String input = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
		reader.setInputSource(Channels.newChannel(new ByteArrayInputStream(input.getBytes(CHARSET))));

		assertEquals('A', reader.readChar());
		assertEquals('B', reader.readByte());
		reader.mark();
		assertEquals('C', reader.readChar());
		reader.revert();
		assertEquals('C', reader.readByte());
		reader.mark();
		while (reader.readChar() >= 0) {}
		assertTrue(reader.isEndOfInput());
		reader.revert();
		assertFalse(reader.isEndOfInput());
		assertEquals('D', reader.readChar());
		assertEquals('E', reader.readByte());
		while (reader.readChar() >= 0) {}
		assertTrue(reader.isEndOfInput());
	}

}
