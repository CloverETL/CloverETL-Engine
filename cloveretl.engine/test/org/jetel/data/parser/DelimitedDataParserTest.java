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

import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.DataRecordParsingType;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Mar 14, 2013
 */
public class DelimitedDataParserTest extends AbstractParserTestCase {

	@Override
	protected Parser createParser() throws Exception {
		TextParserConfiguration cfg = new TextParserConfiguration();
		DataRecordMetadata metadata = new DataRecordMetadata("metadata");
		metadata.setParsingType(DataRecordParsingType.DELIMITED);
		metadata.addField(new DataFieldMetadata("field", DataFieldType.STRING, "|"));
		cfg.setMetadata(metadata);
		
		DelimitedDataParser parser = new DelimitedDataParser(cfg.getMetadata());
		return parser;
	}

}
