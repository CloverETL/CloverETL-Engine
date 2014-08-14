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
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.Objects;

import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.data.formatter.CloverDataFormatter;
import org.jetel.data.parser.CloverDataParser.FileConfig;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.DataRecordParsingType;
import org.jetel.util.bytes.CloverBuffer;
import org.jetel.util.primitive.BitArray;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 28. 5. 2014
 */
public class CloverDataParser35Test extends AbstractParserTestCase {

	private DataRecordMetadata metadata = null;
	protected byte[] bytes = null;
	
	protected DataRecordMetadata getMetadata() {
		if (metadata == null) {
			metadata = new DataRecordMetadata("metadata", DataRecordParsingType.DELIMITED);
			metadata.addField(new DataFieldMetadata("field", DataFieldType.STRING, '|'));
		}
		
		return metadata;
	}
	
	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
		this.metadata = null;
		this.bytes = null;
	}
	
	@Override
	protected ICloverDataParser createParser() throws Exception {
		CloverDataParser35 parser = new CloverDataParser35(getMetadata());
		parser.setVersion(new FileConfig());
		parser.init();
		return parser;
	}
	
	private void writeRecord(DataOutputStream os, CloverBuffer buffer) throws IOException {
		os.writeInt(buffer.remaining());
		os.write(buffer.array(), 0, buffer.limit());
	}
	
	protected CloverBuffer serializeRecord(DataRecord record, CloverBuffer buffer) {
		buffer.clear();
		record.serializeUnitary(buffer);
		buffer.flip();
		return buffer;
	}

	@Override
	protected byte[] getBytes() {
		if (bytes == null) {
			try {
				DataRecordMetadata metadata = getMetadata();
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				DataOutputStream os = new DataOutputStream(baos);
				os.writeLong(CloverDataFormatter.CLOVER_DATA_HEADER);
				os.writeLong(CloverDataFormatter.CLOVER_DATA_COMPATIBILITY_HASH_3_5);
				os.writeByte(3);
				os.writeByte(5);
				os.writeByte(0);
				byte[] extraBytes = new byte[CloverDataFormatter.HEADER_OPTIONS_ARRAY_SIZE_3_5];
				if (Defaults.Record.USE_FIELDS_NULL_INDICATORS) {
					BitArray.set(extraBytes, 0);
				}
				os.write(extraBytes);
				CloverBuffer buffer = CloverBuffer.wrap(new byte[100]);
				metadata.serialize(buffer);
				buffer.flip();
				os.write(buffer.array(), 0, buffer.limit());
				DataRecord record = DataRecordFactory.newRecord(metadata);
				record.init();
				record.getField(0).setValue("test1");
				writeRecord(os, serializeRecord(record, buffer));
				record.getField(0).setValue("test2");
				writeRecord(os, serializeRecord(record, buffer));
				bytes = baos.toByteArray();
			} catch (Exception ex) {
				throw new RuntimeException(ex);
			}
		}
		
		return bytes;
	}

	protected static class LazyInputStream extends FilterInputStream {
		
		public LazyInputStream(InputStream in) {
			super(in);
		}

		@Override
		public int read(byte[] buffer, int off, int len) throws IOException {
	        if (buffer == null) {
	            throw new NullPointerException();
	        } else if (off < 0 || len < 0 || len > buffer.length - off) {
	            throw new IndexOutOfBoundsException();
	        } else if (len == 0) {
	            return 0;
	        }

	        int c = read();
	        if (c == -1) {
	            return -1;
	        }
	        buffer[off] = (byte)c;
	        return 1;
		}

	}
	
	protected void setDataSource(ICloverDataParser aParser, InputStream is) throws Exception {
		CloverDataParser35 parser = (CloverDataParser35) aParser;
		Field field = CloverDataParser35.class.getDeclaredField("metadata");
		field.setAccessible(true);
		DataRecordMetadata metadata = (DataRecordMetadata) field.get(parser);
		CloverDataParser newParser = new CloverDataParser(metadata);
		newParser.setDataSource(is);
		parser.setVersion(newParser.getVersion());
		parser.setDataSource(is);
	}

	public void testLazyStream() throws Exception {
		ICloverDataParser parser = createParser();
		InputStream is = new LazyInputStream(new ByteArrayInputStream(getBytes()));
		setDataSource(parser, is);
		DataRecordMetadata metadata = getMetadata();
		DataRecord record = DataRecordFactory.newRecord(metadata);
		record.init();
		
		testParse(parser, parser.isDirectReadingSupported());
	}
	
	public void testCompatibilityHeader() throws Exception {
		byte[] bytes = getBytes();
		InputStream is = new LazyInputStream(new ByteArrayInputStream(bytes));
		FileConfig version = CloverDataParser.checkCompatibilityHeader(is, getMetadata());
		assertEquals(CloverDataFormatter.DataCompressAlgorithm.NONE.getId(), version.compressionAlgorithm);
		assertEquals(3, version.majorVersion);
		assertEquals(5, version.minorVersion);
		assertEquals(0, version.revisionVersion);
		assertEquals(CloverDataFormatter.DataFormatVersion.VERSION_35, version.formatVersion);
	}
	
	protected void testParse(ICloverDataParser parser, boolean raw) throws Exception {
		DataRecordMetadata metadata = getMetadata();
		DataRecord record = DataRecordFactory.newRecord(metadata);
		record.init();
		
		if (raw) { // try direct reading
			CloverBuffer buffer = CloverBuffer.allocate(Defaults.Record.RECORD_INITIAL_SIZE, Defaults.Record.RECORD_LIMIT_SIZE);
			int result = parser.getNextDirect(buffer);
			if (result == -1) { // direct reading not supported
				raw = false;
			} else {
				record.deserializeUnitary(buffer);
				assertEquals("test1", Objects.toString(record.getField(0).getValue(), null));
				parser.getNextDirect(buffer);
				record.deserializeUnitary(buffer);
				assertEquals("test2", Objects.toString(record.getField(0).getValue(), null));
			}
		}
		
		if (!raw) {
			parser.getNext(record);
			assertEquals("test1", Objects.toString(record.getField(0).getValue(), null));
			parser.getNext(record);
			assertEquals("test2", Objects.toString(record.getField(0).getValue(), null));
		}
	}
	
	protected FileConfig testParse(InputStream is) throws Exception {
		ICloverDataParser parser = createParser();
		setDataSource(parser, is);
		
		testParse(parser, true); // always try direct reading first
		
		return parser.getVersion();
	}
	
	public void testParse() throws Exception {
		InputStream is;
		
		is = new ByteArrayInputStream(getBytes());
		testParse(is);
	}
	
}
