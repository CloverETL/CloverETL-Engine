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

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.jetel.data.formatter.CloverDataFormatter;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.DataRecordParsingType;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Mar 15, 2013
 */
public class CloverDataParserTest extends AbstractParserTestCase {

	@Override
	protected Parser createParser() throws Exception {
		DataRecordMetadata metadata = new DataRecordMetadata("metadata");
		metadata.addField(new DataFieldMetadata("field", DataFieldType.STRING, 1));
		return new CloverDataParser(metadata);
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
				CloverDataFormatter formatter = new CloverDataFormatter("anything", true);
				formatter.init(getMetadata());
				ByteArrayOutputStream os = new ByteArrayOutputStream();
				formatter.setDataTarget(os);
				formatter.writeHeader();
				formatter.writeFooter();
				formatter.flush();
				formatter.close();
				bytes = os.toByteArray();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		
		return bytes;
	}
}
