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

import java.io.FileInputStream;

import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.test.CloverTestCase;

/**
 * @author tkramolis (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created Nov 24, 2011
 */
public class AbstractSpreadsheetParserTest extends CloverTestCase {

	private String mapping1 = 
			"<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>" +
			"<mapping>" +
			"    <globalAttributes>" +
			"        <orientation>VERTICAL</orientation>" +
			"        <step>1</step>" +
			"    </globalAttributes>" +
			"    <headerGroups>" +
			"        <headerGroup skip=\"1\">" +
			"            <autoMappingType>NAME</autoMappingType>" +
			"            <headerRanges>" +
			"                <headerRange begin=\"A1\"/>" +
			"                <headerRange begin=\"B1\"/>" +
			"                <headerRange begin=\"D2\"/>" +
			"                <headerRange begin=\"C2\"/>" +
			"            </headerRanges>" +
			"        </headerGroup>" +
			"    </headerGroups>" +
			"</mapping>";
	
	private String mapping2 =
			"<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>" +
			"<mapping>" +
			"    <globalAttributes>" +
			"        <orientation>VERTICAL</orientation>" +
			"        <step>2</step>" +
			"    </globalAttributes>" +
			"    <headerGroups>" +
			"        <headerGroup skip=\"2\">" +
			"            <cloverField>field1</cloverField>" +
			"            <headerRanges>" +
			"                <headerRange begin=\"A3\"/>" +
			"            </headerRanges>" +
			"        </headerGroup>" +
			"        <headerGroup skip=\"3\">" +
			"            <cloverField>field3</cloverField>" +
			"            <headerRanges>" +
			"                <headerRange begin=\"C6\"/>" +
			"            </headerRanges>" +
			"        </headerGroup>" +
			"        <headerGroup skip=\"3\">" +
			"            <cloverField>field2</cloverField>" +
			"            <headerRanges>" +
			"                <headerRange begin=\"B2\"/>" +
			"            </headerRanges>" +
			"        </headerGroup>" +
			"    </headerGroups>" +
			"</mapping>";


	private DataRecordMetadata metadata;
	
	@Override
	protected void setUp() throws Exception {
		initEngine();
		
		String[] cloverFieldNames = {"jedna", "dva", "tri", "ctyri"};
		
		metadata = new DataRecordMetadata("md", DataRecordMetadata.DELIMITED_RECORD);
		for (int i = 0; i < cloverFieldNames.length; i++) {
			metadata.addField(new DataFieldMetadata(cloverFieldNames[i], DataFieldMetadata.STRING_FIELD, ";"));
		}
	}

	public void testMappingInit() throws Exception {
		// Just try to process few mappings
		
		AbstractSpreadsheetParser xlsxStreamParser = new SpreadsheetStreamParser(metadata, XLSMapping.parse(mapping1, metadata));
		xlsxStreamParser.setSheet("0");
		xlsxStreamParser.init();
		xlsxStreamParser.preExecute();
		xlsxStreamParser.setDataSource(new FileInputStream("data/xls/multirow.xlsx")); // resolves name mapping
		xlsxStreamParser.close();
		
		xlsxStreamParser = new SpreadsheetStreamParser(metadata, XLSMapping.parse(mapping2, metadata));
		xlsxStreamParser.setSheet("0");
		xlsxStreamParser.init();
		xlsxStreamParser.preExecute();
		xlsxStreamParser.setDataSource(new FileInputStream("data/xls/multirow.xlsx"));	
		xlsxStreamParser.close();
	}
	
}
