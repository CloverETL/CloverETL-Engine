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
package org.jetel.data;

import java.io.UnsupportedEncodingException;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.Arrays;

import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.test.CloverTestCase;
import org.jetel.util.bytes.CloverBuffer;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 9. 4. 2014
 */
public class BooleanDataFieldTest extends CloverTestCase {

	public void testFromStringNullValue() {
		DataFieldMetadata fieldMetadata = new DataFieldMetadata("field", DataFieldType.BOOLEAN, ";");
		BooleanDataField field = new BooleanDataField(fieldMetadata);

		field.setValue(true);
		field.fromString("");
		assertTrue(field.isNull());

		field.setValue(true);
		field.fromString("false");
		assertTrue(field.isNull() == false);
		assertTrue(field.getValue() == false);
		
		fieldMetadata.setNullValues(Arrays.asList("abc", "", "xxx"));

		field.setValue(true);
		field.fromString("abc");
		assertTrue(field.isNull());

		field.setValue(true);
		field.fromString("");
		assertTrue(field.isNull());

		field.setValue(true);
		field.fromString("xxx");
		assertTrue(field.isNull());

		field.setValue(true);
		field.fromString("false");
		assertTrue(!field.isNull());
	}

	public void testFromByteByfferNullValue() throws CharacterCodingException, UnsupportedEncodingException {
		DataFieldMetadata fieldMetadata = new DataFieldMetadata("field", DataFieldType.BOOLEAN, ";");
		BooleanDataField field = new BooleanDataField(fieldMetadata);

		CharsetDecoder decoder = Charset.forName("US-ASCII").newDecoder();
		
		field.setValue(true);
		field.fromByteBuffer(getCloverBuffer(""), decoder);
		assertTrue(field.isNull());

		field.setValue(true);
		field.fromByteBuffer(getCloverBuffer("false"), decoder);
		assertTrue(field.isNull() == false);
		assertTrue(field.getValue() == false);
		
		fieldMetadata.setNullValues(Arrays.asList("abc", "", "xxx"));

		field.setValue(true);
		field.fromByteBuffer(getCloverBuffer("abc"), decoder);
		assertTrue(field.isNull());

		field.setValue(true);
		field.fromByteBuffer(getCloverBuffer(""), decoder);
		assertTrue(field.isNull());

		field.setValue(true);
		field.fromByteBuffer(getCloverBuffer("xxx"), decoder);
		assertTrue(field.isNull());

		field.setValue(true);
		field.fromByteBuffer(getCloverBuffer("false"), decoder);
		assertTrue(!field.isNull());
	}

	public static CloverBuffer getCloverBuffer(String s) throws UnsupportedEncodingException {
		CloverBuffer buffer = CloverBuffer.allocate(100);
		buffer.put(s.getBytes("US-ASCII"));
		buffer.flip();
		return buffer;
	}
	
}
