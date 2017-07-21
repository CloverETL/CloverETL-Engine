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

import java.nio.channels.Channels;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.jetel.data.formatter.XLSXDataFormatter;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Mar 15, 2013
 */
public class XLSXDataParserTest extends AbstractParserTestCase {

	@Override
	protected Parser createParser() throws Exception {
		XLSXDataParser parser = new XLSXDataParser(getMetadata());
		parser.setSheetNumber("*");
		return parser;
	}

	private DataRecordMetadata metadata = null;
	private byte[] bytes = null;
	
	protected DataRecordMetadata getMetadata() {
		if (metadata == null) {
			metadata = new DataRecordMetadata("metadata");
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
				XLSXDataFormatter formatter = new XLSXDataFormatter(false, false);
				formatter.init(getMetadata());
				ByteArrayOutputStream os = new ByteArrayOutputStream();
				formatter.setDataTarget(Channels.newChannel(os));
				formatter.prepareSheet();
				formatter.writeHeader();
				formatter.writeFooter();
				formatter.close();
				bytes = os.toByteArray();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		
		return bytes;
	}
}
