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
package org.jetel.util.string;

import junit.framework.TestCase;

/**
 * @author tkramolis (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 9 Feb 2011
 */
public class QuotingDecoderTest extends TestCase {

	private QuotingDecoder decoder = new QuotingDecoder();
	
	/**
	 * Test method for {@link org.jetel.util.string.QuotingDecoder#decode(java.lang.CharSequence)}.
	 */
	public void testDecode() {
		decoder.setQuoteChar('\"');
		assertEquals("Hi there!", decoder.decode("\"Hi there!\"").toString());
		assertEquals("She said: \"What she said?\"", decoder.decode("\"She said: \"\"What she said?\"\"\"").toString());
		assertEquals("'Other qoutes musn't be removed'", decoder.decode("'Other qoutes musn't be removed'").toString());
		decoder.setQuoteChar(null);
		assertEquals("\"Bla'", decoder.decode("'\"Bla''").toString());
	}

	/**
	 * Test method for {@link org.jetel.util.string.QuotingDecoder#encode(java.lang.CharSequence)}.
	 */
	public void testEncode() {
		decoder.setQuoteChar('\"');
		assertEquals("\"Hi there!\"", decoder.encode("Hi there!").toString());
		assertEquals("\"She said: \"\"What she said?\"\"\"", decoder.encode("She said: \"What she said?\"").toString());
	}

}
