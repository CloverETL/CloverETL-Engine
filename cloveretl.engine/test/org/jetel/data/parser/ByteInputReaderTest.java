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

public class ByteInputReaderTest extends CharByteReaderTestCase {
	
	private static final int MAX_BACK_MARK = 20;
	
	@Override
	protected ICharByteInputReader getReader() {
		return new CharByteInputReader.ByteInputReader(MAX_BACK_MARK);
	}
	
	@Override
	public void testRevert() throws Exception {
		byte[] bytes = new byte[MAX_BACK_MARK];
		for (int i = 0; i < bytes.length; i++) {
			bytes[i] = (byte) i;
		}
		reader.setInputSource(Channels.newChannel(new ByteArrayInputStream(bytes)));

		assertEquals(0, reader.readByte());
		assertEquals(1, reader.readByte());
		reader.mark();
		assertEquals(2, reader.readByte());
		reader.revert();
		assertEquals(2, reader.readByte());
		reader.mark();
		while (reader.readByte() >= 0) {}
		assertTrue(reader.isEndOfInput());
		reader.revert();
		assertFalse(reader.isEndOfInput());
		assertEquals(3, reader.readByte());
		while (reader.readByte() >= 0) {}
		assertTrue(reader.isEndOfInput());
	}

}
