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

import org.jetel.test.CloverTestCase;

/**
 * @author reichman (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Oct 17, 2015
 */
public class TagNameTest extends CloverTestCase {

	public void testEncodeAndDecode() {
		encodeAndDecode("@Funny*Tag-Name()");
		encodeAndDecode("url_facebook");
		encodeAndDecode("url_");
		encodeAndDecode("url_xclass");
		encodeAndDecode("_x0040Funny_x002aTag-Name_x0028_x0029");
		encodeAndDecode("");
		encodeAndDecode("url_x005fxclass");
		encodeAndDecode("{http://test.com/}url_facebook");
		encodeAndDecode("{http://test.com/}url_x0040_c");
	}
	
	private void encodeAndDecode(String text) {
		String encoded = TagName.encode(text);
		String decoded = TagName.decode(encoded);
		assertEquals(text, decoded);
	}
	
	public void testDecode() {
		decode("url_facebook", "url_facebook");
		decode("url_", "url_");
		decode("url_xclass", "url_xclass");
		decode("url_xface", "url龜");
		decode("url_x0040", "url@");
		decode("urlx", "urlx");
		decode("", "");
		decode("_x", "_x");
		decode("_", "_");
		decode("_x0040Funny_x002aTag-Name_x0028_x0029", "@Funny*Tag-Name()");
		decode("url_x005fxclass", "url_xclass");
		decode("{http://test.com/}url_facebook", "{http://test.com/}url_facebook");
		decode("{http://test.com/}url_x0040c", "{http://test.com/}url@c");
	}
	
	private void decode(String encoded, String expected) {
		String decoded = TagName.decode(encoded);
		assertEquals(expected, decoded);
	}
}