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

import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.test.CloverTestCase;
import org.jetel.util.bytes.CloverBuffer;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 14.5.2012
 */
public class TokenTest extends CloverTestCase {

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		initEngine();
	}

	private Token createToken() {
		DataRecordMetadata metadata = new DataRecordMetadata("metadata");
		metadata.addField(new DataFieldMetadata("field", ";"));
		return DataRecordFactory.newToken(metadata);
	}
	
	public void testTokenId() {
		Token token = createToken();
		
		assertEquals(-1, token.getTokenId());
		
		token.setTokenId(0);
		assertEquals(0, token.getTokenId());
	}
	
	public void testSerialization() {
		Token tokenSource = createToken();
		Token tokenTarget = createToken();
		CloverBuffer cloverBuffer = CloverBuffer.allocate(Defaults.Record.RECORD_INITIAL_SIZE);
		
		tokenSource.getField(0).setValue("kokon");
		
		tokenSource.serialize(cloverBuffer);
		cloverBuffer.flip();
		tokenTarget.deserialize(cloverBuffer);
		
		assertEquals("kokon", tokenTarget.getField(0).getValue().toString());
		assertEquals(-1, tokenTarget.getTokenId());

		////
		tokenSource.setTokenId(123);

		cloverBuffer.clear();
		tokenSource.serialize(cloverBuffer);
		cloverBuffer.flip();
		tokenTarget.deserialize(cloverBuffer);
		
		assertEquals("kokon", tokenTarget.getField(0).getValue().toString());
		assertEquals(123, tokenTarget.getTokenId());
	}

	public void testGetSizeSerialized() {
		Token token = createToken();
		assertEquals(2, token.getSizeSerialized());
		
		token.setTokenId(123);
		assertEquals(10, token.getSizeSerialized());
	}
	
	public void testCopyFrom() {
		Token tokenSource = createToken();
		Token tokenTarget = createToken();
		
		tokenSource.getField(0).setValue("kokon");
		tokenSource.setTokenId(123);
		
		tokenTarget.copyFrom(tokenSource);
		assertEquals("kokon", tokenTarget.getField(0).getValue().toString());
		assertEquals(123, tokenTarget.getTokenId());
	}

	public void testDuplicate() {
		Token tokenSource = createToken();
		
		tokenSource.getField(0).setValue("kokon");
		tokenSource.setTokenId(123);
		
		DataRecord tokenTarget = tokenSource.duplicate();
		
		assertTrue(tokenTarget instanceof Token);
		assertEquals("kokon", tokenTarget.getField(0).getValue().toString());
		assertEquals(123, ((Token) tokenTarget).getTokenId());
	}

}
