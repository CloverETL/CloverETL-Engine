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

public class CharInputReaderTest extends CharByteReaderTestCase {
	
	private static final int MAX_BACK_MARK = 20;
	private static final Charset CHARSET = Charset.forName("UTF-8");
	
	@Override
	protected ICharByteInputReader getReader() {
		return new CharByteInputReader.CharInputReader(CHARSET, MAX_BACK_MARK, PolicyType.STRICT);
	}
	
	@Override
	public void testRevert() throws Exception {
		String input = "p\u0159\u00EDli\u0161 \u017Elu\u0165ou\u010Dk\u00FD k\u016F\u0148 \u00FAp\u011Bl \u010F\u00E1belsk\u00E9 \u00F3dy";
		reader.setInputSource(Channels.newChannel(new ByteArrayInputStream(input.getBytes(CHARSET))));

		assertEquals('p', reader.readChar());
		assertEquals('\u0159', reader.readChar());
		reader.mark();
		assertEquals('\u00ED', reader.readChar());
		reader.revert();
		assertEquals('\u00ED', reader.readChar());
		reader.mark();
		while (reader.readChar() >= 0) {}
		assertTrue(reader.isEndOfInput());
		reader.revert();
		assertFalse(reader.isEndOfInput());
		assertEquals('l', reader.readChar());
		while (reader.readChar() >= 0) {}
		assertTrue(reader.isEndOfInput());
	}

}
