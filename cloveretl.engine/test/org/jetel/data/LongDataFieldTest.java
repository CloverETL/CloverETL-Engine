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

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 9. 4. 2014
 */
public class LongDataFieldTest extends CloverTestCase {

	public void testFromStringNullValue() {
		DataFieldMetadata fieldMetadata = new DataFieldMetadata("field", DataFieldType.LONG, ";");
		LongDataField field = new LongDataField(fieldMetadata);

		field.setValue(123);
		field.fromString("");
		assertTrue(field.isNull());

		field.setValue(123);
		field.fromString("456");
		assertTrue(field.isNull() == false);
		assertTrue(field.getValue() == 456);
		
		fieldMetadata.setNullValues(Arrays.asList("abc", "", "xxx"));

		field.setValue(123);
		field.fromString("abc");
		assertTrue(field.isNull());

		field.setValue(123);
		field.fromString("");
		assertTrue(field.isNull());

		field.setValue(123);
		field.fromString("xxx");
		assertTrue(field.isNull());

		field.setValue(123);
		field.fromString("456");
		assertTrue(!field.isNull());
	}

	public void testFromByteByfferNullValue() throws CharacterCodingException, UnsupportedEncodingException {
		DataFieldMetadata fieldMetadata = new DataFieldMetadata("field", DataFieldType.LONG, ";");
		LongDataField field = new LongDataField(fieldMetadata);

		CharsetDecoder decoder = Charset.forName("US-ASCII").newDecoder();
		
		field.setValue(123);
		field.fromByteBuffer(BooleanDataFieldTest.getCloverBuffer(""), decoder);
		assertTrue(field.isNull());

		field.setValue(123);
		field.fromByteBuffer(BooleanDataFieldTest.getCloverBuffer("456"), decoder);
		assertTrue(field.isNull() == false);
		assertTrue(field.getValue() == 456);
		
		fieldMetadata.setNullValues(Arrays.asList("abc", "", "xxx"));

		field.setValue(123);
		field.fromByteBuffer(BooleanDataFieldTest.getCloverBuffer("abc"), decoder);
		assertTrue(field.isNull());

		field.setValue(123);
		field.fromByteBuffer(BooleanDataFieldTest.getCloverBuffer(""), decoder);
		assertTrue(field.isNull());

		field.setValue(123);
		field.fromByteBuffer(BooleanDataFieldTest.getCloverBuffer("xxx"), decoder);
		assertTrue(field.isNull());

		field.setValue(123);
		field.fromByteBuffer(BooleanDataFieldTest.getCloverBuffer("456"), decoder);
		assertTrue(!field.isNull());
	}

}
