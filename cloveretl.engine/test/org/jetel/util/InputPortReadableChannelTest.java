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
package org.jetel.util;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.Edge;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.DataRecordParsingType;
import org.jetel.test.CloverTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Martin Slama (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 1.10.2012
 */
public class InputPortReadableChannelTest extends CloverTestCase {

	private static final int BUFFER_SIZE = 128;
	private static final String FIELD_NAME = "field1";
	
	private enum SampleDataType {SHORT, EXACT, NULL_FIRST, NULL_SECOND};
	
	private InputPortReadableChannel channel;
	
	@Before
	public void setUp() throws Exception {
		initEngine();
	}
	
	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
		channel = null;
	}

	/**
	 * Test method for {@link org.jetel.util.InputPortReadableChannel#isOpen()}.
	 * @throws IOException 
	 */
	@Test
	public final void testIsOpen() throws IOException {
		channel = new InputPortReadableChannel(new InputPortMock(new String[3]), FIELD_NAME, "UTF-8");
		assertTrue(channel.isOpen());
		channel.close();
		assertFalse(channel.isOpen());
		try {
			channel.read(ByteBuffer.allocate(1));
			fail("ClosedChannelException expected when calling read on closed channel!");
		} catch (ClosedChannelException e) {
		}
	}

	/**
	 * Test method for {@link org.jetel.util.InputPortReadableChannel#read(java.nio.ByteBuffer)}.
	 * @throws IOException 
	 */
	@Test
	public final void testReadWithoutNull1() throws IOException {
		
		//read short records (|record| << BUFFER_SIZE - all data fits buffer size
		String[] data = getSampleData(SampleDataType.SHORT);
		ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
		channel = new InputPortReadableChannel(new InputPortMock(data), FIELD_NAME, "UTF-8");
		assertTrue(channel.isOpen());
		assertFalse(channel.isEOF());
		assertTrue(channel.read(buffer) > 0);
		assertTrue(channel.isOpen());
		assertFalse(channel.isEOF());
		assertTrue(channel.read(buffer) > 0);
		assertTrue(channel.isOpen());
		assertFalse(channel.isEOF());
		assertTrue(channel.read(buffer) > 0);
		assertTrue(channel.isOpen());
		assertFalse(channel.isEOF());
		assertTrue(channel.read(buffer) <= 0);
		assertTrue(channel.isOpen());
		assertTrue(channel.isEOF());
		buffer.flip();
		byte[] byteArray = new byte[buffer.limit()];
		buffer.get(byteArray);
		assertEquals(true, (new String(byteArray)).equals(data[0] + data[1] + data[2]));
	}
	
	/**
	 * Test method for {@link org.jetel.util.InputPortReadableChannel#read(java.nio.ByteBuffer)}.
	 * @throws IOException 
	 */
	@Test
	public final void testReadWithoutNull2() throws IOException {
		
		//read records (|record| == BUFFER_SIZE)
		String[] data = getSampleData(SampleDataType.EXACT);
		ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE * 3);
		channel = new InputPortReadableChannel(new InputPortMock(data), FIELD_NAME, "UTF-8");
		
		while (channel.read(buffer) > 0) {}
		
		buffer.flip();
		byte[] byteArray = new byte[buffer.limit()];
		buffer.get(byteArray);
		assertEquals(true, (new String(byteArray)).equals(data[0] + data[1] + data[2]));
		assertTrue(channel.isEOF());
	}
	
	/**
	 * Test method for {@link org.jetel.util.InputPortReadableChannel#read(java.nio.ByteBuffer)}.
	 * @throws IOException 
	 */
	@Test
	public final void testReadWithoutNull3() throws IOException {
		
		//read record and use small buffer (smaller than field size)
		String[] data = getSampleData(SampleDataType.EXACT);
		InputPort port = new InputPortMock(data);
		StringBuilder sb = new StringBuilder();
		channel = new InputPortReadableChannel(port, FIELD_NAME, "UTF-8");
		do {
			ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE / 2 - 1);
			channel.read(buffer);
			buffer.flip();
			byte[] byteArray = new byte[buffer.limit()];
			buffer.get(byteArray);
			sb.append(new String(byteArray));
		} while (!channel.isEOF());
		
		assertEquals(true, sb.toString().equals(data[0] + data[1] + data[2]));
		assertTrue(channel.isEOF());
	}
	
	/**
	 * Test method for {@link org.jetel.util.InputPortReadableChannel#read(java.nio.ByteBuffer)}.
	 * @throws IOException 
	 */
	@Test
	public final void testReadNullFirst() throws IOException {
		
		//null field is read first, some field with value as second
		String[] data = getSampleData(SampleDataType.NULL_FIRST);
		ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
		channel = new InputPortReadableChannel(new InputPortMock(data), FIELD_NAME, "UTF-8");
		assertTrue(channel.isOpen());
		assertTrue(channel.isEOF());
		channel.close();
		
		channel = new InputPortReadableChannel(new InputPortMock(data), FIELD_NAME, "UTF-8");
		assertTrue(channel.isOpen());
		assertTrue(channel.isEOF());
		assertTrue(channel.read(buffer) > 0);
		buffer.flip();
		byte[] byteArray = new byte[buffer.limit()];
		buffer.get(byteArray);
		assertEquals(true, (new String(byteArray)).equals(data[1]));
	}
	
	/**
	 * Test method for {@link org.jetel.util.InputPortReadableChannel#read(java.nio.ByteBuffer)}.
	 * @throws IOException 
	 */
	@Test
	public final void testReadNullLast() throws IOException {
		
		//send (and last) field has null value
		String[] data = getSampleData(SampleDataType.NULL_SECOND);
		ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
		channel = new InputPortReadableChannel(new InputPortMock(data), FIELD_NAME, "UTF-8");
		assertTrue(channel.isOpen());
		assertFalse(channel.isEOF());
		assertTrue(channel.read(buffer) > 0);
		assertTrue(channel.read(buffer) <= 0);
		assertTrue(channel.isOpen());
		assertTrue(channel.isEOF());
		channel.close();
		
		buffer.flip();
		byte[] byteArray = new byte[buffer.limit()];
		buffer.get(byteArray);
		assertEquals(true, (new String(byteArray)).equals(data[0]));
	}
	
	@Test
	public final void testReadNullRecord() throws IOException {
		
		//send (and last) field has null value
		channel = new InputPortReadableChannel(new InputPortMock(new String[1]), FIELD_NAME, "UTF-8");
		assertTrue(channel.isOpen());
		assertTrue(channel.isEOF());
		channel.close();
	}
	
	private String[] getSampleData(SampleDataType type) throws UnsupportedEncodingException {
		
		List<String> data = new ArrayList<String>();
		
		switch (type) {
		case SHORT:
			for (int i = 0; i < 3; i++) {
				data.add(i + " " + "TEST");
			}
			return Arrays.copyOf(data.toArray(), data.toArray().length, String[].class);
		case EXACT:
			for (int i = 0; i < 3; i++) {
				StringBuilder sb = new StringBuilder(8192);
				for (int j = 0; j < BUFFER_SIZE; j++) {
					sb.append(String.valueOf(i));
				}
				data.add(sb.toString());
			}
			return Arrays.copyOf(data.toArray(), data.toArray().length, String[].class);
		case NULL_FIRST:
			String[] result = new String[2];
			result[1] = "TEST MESSAGE";
			return result;
		case NULL_SECOND:
			result = new String[2];
			result[0] = "TEST MESSAGE";
			return result;
		}
		
		return null;
	}

	/**
	 * Mock implementation of <code>InputPort</code> for usage by this JUnit test only!
	 *
	 * @author Martin Slama (info@cloveretl.com)
	 *         (c) Javlin, a.s. (www.cloveretl.com)
	 *
	 * @created 2.10.2012
	 */
	private class InputPortMock implements InputPort {

		private DataRecordMetadata metadata;
		private int i = 0;
		private String[] data;
		
		public InputPortMock(String[] data) {
			metadata = new DataRecordMetadata("recordName1", DataRecordParsingType.DELIMITED);
			metadata.addField(new DataFieldMetadata(FIELD_NAME, DataFieldType.STRING, "|"));
			this.data = data;
		}
		
		@Override
		public void connectReader(Node _reader, int portNum) {
		}

		@Override
		public DataRecord readRecord(DataRecord record) throws IOException, InterruptedException {
			if (record != null && i < 3) {
				record.getField(FIELD_NAME).setValue(data[i]);
				i++;
				return record;
			}
			return null;
		}

		@Override
		public boolean isOpen() {
			return false;
		}

		@Override
		public boolean isEOF() {
			return false;
		}

		@Override
		public DataRecordMetadata getMetadata() {
			return metadata;
		}

		@Override
		public Node getWriter() {
			return null;
		}

		@Override
		public int getRecordCounter() {
			return 0;
		}

		@Override
		public int getInputRecordCounter() {
			return 0;
		}

		@Override
		public long getByteCounter() {
			return 0;
		}

		@Override
		public long getInputByteCounter() {
			return 0;
		}

		@Override
		public boolean hasData() {
			return false;
		}

		@Override
		public int getInputPortNumber() {
			return 0;
		}

		@Override
		public void reset() throws ComponentNotReadyException {
		}

		@Override
		public int getUsedMemory() {
			return 0;
		}

		@Override
		public Edge getEdge() {
			return null;
		}
	}
	
}
