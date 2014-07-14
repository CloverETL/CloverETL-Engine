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
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.data.formatter.CloverDataFormatter;
import org.jetel.data.parser.CloverDataParser.FileConfig;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.bytes.CloverBuffer;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Mar 15, 2013
 */
public class CloverDataParserTest extends CloverDataParser35Test {

	@Override
	protected ICloverDataParser createParser() throws Exception {
		CloverDataParser parser = new CloverDataParser(getMetadata());
		parser.init();
		return parser;
	}
	
	private void writeRecord(CloverDataFormatter formatter, DataRecord record, CloverBuffer buffer) throws IOException {
		if (formatter.isRawData()) {
			serializeRecord(record, buffer);
			formatter.writeDirect(buffer);
		} else {
			formatter.write(record);
		}
	}

	protected byte[] getBytes(int compressLevel) {
		try {
			CloverDataFormatter formatter = new CloverDataFormatter("anything", true);
			formatter.setCompressLevel(compressLevel);
			DataRecordMetadata metadata = getMetadata();
			formatter.init(metadata);
			ByteArrayOutputStream os = new ByteArrayOutputStream();
			formatter.setDataTarget(os);
			formatter.writeHeader();
			DataRecord record = DataRecordFactory.newRecord(metadata);
			record.init();
			CloverBuffer buffer = null;
			if (formatter.isRawData()) {
				buffer = CloverBuffer.allocate(Defaults.Record.RECORD_INITIAL_SIZE, Defaults.Record.RECORD_LIMIT_SIZE);
			}
			record.getField(0).setValue("test1");
			writeRecord(formatter, record, buffer);
			record.getField(0).setValue("test2");
			writeRecord(formatter, record, buffer);
			formatter.writeFooter();
			formatter.flush();
			formatter.close();
			return os.toByteArray();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	protected byte[] getBytes() {
		if (bytes == null) {
			bytes = getBytes(-1);
		}
		
		return bytes;
	}

	@Override
	protected void setDataSource(ICloverDataParser parser, InputStream is) throws Exception {
		parser.setDataSource(is);
	}
	
	@Override
	public void testCompatibilityHeader() throws Exception {
		InputStream is;
		FileConfig version;
		DataRecordMetadata metadata = getMetadata();
		
		is = new LazyInputStream(new ByteArrayInputStream(getBytes()));
		version = CloverDataParser.checkCompatibilityHeader(is, metadata);
		assertEquals(CloverDataFormatter.DataFormatVersion.VERSION_40, version.formatVersion);
		assertEquals(CloverDataFormatter.DataCompressAlgorithm.LZ4.getId(), version.compressionAlgorithm);
		assertTrue(version.raw);
		
		is = new ByteArrayInputStream(getBytes(0));
		version = CloverDataParser.checkCompatibilityHeader(is, metadata);
		assertEquals(CloverDataFormatter.DataFormatVersion.VERSION_40, version.formatVersion);
		assertEquals(CloverDataFormatter.DataCompressAlgorithm.NONE.getId(), version.compressionAlgorithm);
		assertTrue(version.raw);

		is = new ByteArrayInputStream(getBytes(1));
		version = CloverDataParser.checkCompatibilityHeader(is, metadata);
		assertEquals(CloverDataFormatter.DataFormatVersion.VERSION_40, version.formatVersion);
		assertEquals(CloverDataFormatter.DataCompressAlgorithm.LZ4.getId(), version.compressionAlgorithm);
		assertTrue(version.raw);

		is = new ByteArrayInputStream(getBytes(2));
		version = CloverDataParser.checkCompatibilityHeader(is, metadata);
		assertEquals(CloverDataFormatter.DataFormatVersion.VERSION_40, version.formatVersion);
		assertEquals(CloverDataFormatter.DataCompressAlgorithm.LZ4.getId(), version.compressionAlgorithm);
		assertFalse(version.raw);

		is = new ByteArrayInputStream(getBytes(7));
		version = CloverDataParser.checkCompatibilityHeader(is, metadata);
		assertEquals(CloverDataFormatter.DataFormatVersion.VERSION_40, version.formatVersion);
		assertEquals(CloverDataFormatter.DataCompressAlgorithm.GZIP.getId(), version.compressionAlgorithm);
		assertFalse(version.raw);
		
		try {
			is = new ByteArrayInputStream(getBytes());
			DataRecordMetadata incorrectMetadata = new DataRecordMetadata("incorrectMetadata");
			metadata.addField(new DataFieldMetadata("field", DataFieldType.STRING, '|'));
			metadata.addField(new DataFieldMetadata("field2", DataFieldType.STRING, '|'));
			version = CloverDataParser.checkCompatibilityHeader(is, incorrectMetadata);
			fail();
		} catch (ComponentNotReadyException ex) {}
	}

	@Override
	public void testParse() throws Exception {
		super.testParse();

		InputStream is;
		FileConfig version;
		
		is = new ByteArrayInputStream(getBytes(0));
		version = testParse(is);
		assertTrue(version.raw);

		is = new ByteArrayInputStream(getBytes(1));
		version = testParse(is);
		assertTrue(version.raw);

		is = new ByteArrayInputStream(getBytes(2));
		version = testParse(is);
		assertFalse(version.raw); // may change in the future

		is = new ByteArrayInputStream(getBytes(7));
		version = testParse(is);
		assertFalse(version.raw); // may change in the future
	}
	
}
