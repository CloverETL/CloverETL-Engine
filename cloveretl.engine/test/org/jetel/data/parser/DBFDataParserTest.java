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

import java.io.File;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.jetel.database.dbf.DBFDataFormatter;
import org.jetel.database.dbf.DBFDataParser;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.DataRecordParsingType;
import org.jetel.util.stream.StreamUtils;

import de.schlichtherle.io.FileInputStream;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Mar 14, 2013
 */
public class DBFDataParserTest extends AbstractParserTestCase {

	@Override
	protected Parser createParser() throws Exception {
		TextParserConfiguration cfg = new TextParserConfiguration();
		DataRecordMetadata metadata = new DataRecordMetadata("metadata");
		metadata.addField(new DataFieldMetadata("field", DataFieldType.STRING, 1));
		cfg.setMetadata(metadata);
		DBFDataParser parser = new DBFDataParser(cfg.getMetadata());
		return parser;
	}

	private DataRecordMetadata metadata = null;
	private byte[] bytes = null;
	
	protected DataRecordMetadata getMetadata() {
		if (metadata == null) {
			metadata = new DataRecordMetadata("metadata", DataRecordParsingType.FIXEDLEN);
			metadata.addField(new DataFieldMetadata("field", DataFieldType.STRING, 1));
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
	protected byte[] getBytes() {
		if (bytes == null) {
			try {
				DBFDataFormatter formatter = new DBFDataFormatter("ISO-8859-1", (byte) 0x02);
				formatter.init(getMetadata());
				File tmpFile = File.createTempFile("DBFDataParserTest", ".tmp");
				formatter.setDataTarget(tmpFile);
				formatter.writeHeader();
				formatter.writeFooter();
				formatter.close();
				ByteArrayOutputStream os = new ByteArrayOutputStream();
				StreamUtils.copy(new FileInputStream(tmpFile), os, true, true);
				bytes = os.toByteArray();
				if (!tmpFile.delete()) {
					System.err.println("Failed to delete " + tmpFile);
				}
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		
		return bytes;
	}
}
