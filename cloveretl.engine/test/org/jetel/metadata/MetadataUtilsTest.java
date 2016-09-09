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
package org.jetel.metadata;

import java.util.Arrays;

import org.jetel.test.CloverTestCase;

/**
 * @author salamonp (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 2. 8. 2016
 */
public class MetadataUtilsTest extends CloverTestCase {
	
	DataRecordMetadata delim1;
	DataRecordMetadata delim2;
	
	DataRecordMetadata fixlen1;
	DataRecordMetadata fixlen2;
	
	@Override
	protected void setUp() throws Exception { 
		super.setUp();
		
		delim1 = new DataRecordMetadata("delim1", DataRecordParsingType.DELIMITED);
		delim1.setFieldDelimiter(";");
		delim1.addField(new DataFieldMetadata("field1", ";"));
		delim1.addField(new DataFieldMetadata("field2", ";"));
		
		delim2 = new DataRecordMetadata("delim2", DataRecordParsingType.DELIMITED);
		delim2.addField(new DataFieldMetadata("field1", ","));
		delim2.addField(new DataFieldMetadata("field2", ","));
		
		fixlen1 = new DataRecordMetadata("fixlen1", DataRecordParsingType.FIXEDLEN);
		fixlen1.addField(new DataFieldMetadata("field1", 5));
		fixlen1.addField(new DataFieldMetadata("field2", 6));
		
		fixlen2 = new DataRecordMetadata("fixlen2", DataRecordParsingType.FIXEDLEN);
		fixlen2.addField(new DataFieldMetadata("field1", 10));
		fixlen2.addField(new DataFieldMetadata("field2", 20));
	}
	
	public void testMetadataConcat_delim() {
		DataRecordMetadata[] input = { delim1, delim2 };
		DataRecordMetadata concatenatedMetadata = MetadataUtils.getConcatenatedMetadata(input, "resultName");
		
		assertEquals("resultName", concatenatedMetadata.getName());
		assertEquals(DataRecordParsingType.DELIMITED, concatenatedMetadata.getParsingType());
		assertEquals(4, concatenatedMetadata.getNumFields());
		assertEquals(true, Arrays.equals(concatenatedMetadata.getFieldNamesArray(), new String[] {"field1", "field2", "field1_1", "field2_1"}));
		
		assertEquals(";", concatenatedMetadata.getFieldDelimiter());
		assertEquals(null, concatenatedMetadata.getField(0).getDelimiter());
		assertEquals(null, concatenatedMetadata.getField(1).getDelimiter());
		assertEquals(null, concatenatedMetadata.getField(2).getDelimiter());
		assertEquals(null, concatenatedMetadata.getField(3).getDelimiter());
	}
	
	public void testMetadataConcat_fixlen() {
		DataRecordMetadata[] input = { fixlen1, fixlen2 };
		DataRecordMetadata concatenatedMetadata = MetadataUtils.getConcatenatedMetadata(input, "resultName");
		
		assertEquals("resultName", concatenatedMetadata.getName());
		assertEquals(DataRecordParsingType.FIXEDLEN, concatenatedMetadata.getParsingType());
		assertEquals(4, concatenatedMetadata.getNumFields());
		assertEquals(true, Arrays.equals(concatenatedMetadata.getFieldNamesArray(), new String[] {"field1", "field2", "field1_1", "field2_1"}));
		
		assertEquals(5, concatenatedMetadata.getField(0).getSize());
		assertEquals(6, concatenatedMetadata.getField(1).getSize());
		assertEquals(10, concatenatedMetadata.getField(2).getSize());
		assertEquals(20, concatenatedMetadata.getField(3).getSize());
	}
	
	public void testMetadataConcat_mixed() {
		DataRecordMetadata[] input = { fixlen1, delim1 };
		DataRecordMetadata concatenatedMetadata = MetadataUtils.getConcatenatedMetadata(input, "resultName");
		
		assertEquals("resultName", concatenatedMetadata.getName());
		assertEquals(DataRecordParsingType.DELIMITED, concatenatedMetadata.getParsingType());
		assertEquals(4, concatenatedMetadata.getNumFields());
		assertEquals(true, Arrays.equals(concatenatedMetadata.getFieldNamesArray(), new String[] {"field1", "field2", "field1_1", "field2_1"}));
		
		assertEquals(0, concatenatedMetadata.getField(0).getSize());
		assertEquals(0, concatenatedMetadata.getField(1).getSize());
		assertEquals(0, concatenatedMetadata.getField(2).getSize());
		assertEquals(0, concatenatedMetadata.getField(3).getSize());
		
		assertEquals("|", concatenatedMetadata.getFieldDelimiter());
		assertEquals(null, concatenatedMetadata.getField(0).getDelimiter());
		assertEquals(null, concatenatedMetadata.getField(1).getDelimiter());
		assertEquals(null, concatenatedMetadata.getField(2).getDelimiter());
		assertEquals(null, concatenatedMetadata.getField(3).getDelimiter());
	}

}
