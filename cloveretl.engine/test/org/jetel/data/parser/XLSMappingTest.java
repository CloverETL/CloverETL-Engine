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

import java.util.List;

import org.jetel.data.parser.XLSMapping.HeaderGroup;
import org.jetel.data.parser.XLSMapping.HeaderRange;
import org.jetel.data.parser.XLSMapping.SpreadsheetMappingMode;
import org.jetel.data.parser.XLSMapping.SpreadsheetOrientation;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.test.CloverTestCase;

/**
 * @author lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 29 Aug 2011
 */
public class XLSMappingTest extends CloverTestCase {
	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		initEngine();
	}

	public void testSimpleMapping() {
		String[] cloverFieldNames = { "A", "B", "C", "D", "E", "F", "G", "H" };
		DataRecordMetadata stringMetadata = new DataRecordMetadata("md", DataRecordMetadata.DELIMITED_RECORD);
		for (int i = 0; i < cloverFieldNames.length; i++) {
			stringMetadata.addField(new DataFieldMetadata(cloverFieldNames[i], DataFieldMetadata.STRING_FIELD, ";"));
		}

		String textMapping = "<?xml version=\"1.0\"?>"
				+ "<mapping>"
					+ "<globalAttributes>"
						+ "<step>1</step>"
						+ "<orientation>VERTICAL</orientation>"
						+ "<writeHeader>true</writeHeader>"
					+ "</globalAttributes>"
					+ "<headerGroups>"
						+ "<headerGroup>"
							+ "<autoMappingType>ORDER</autoMappingType>"
							+ "<headerRanges>"
								+ "<headerRange begin=\"C86\" end=\"AGZ86\" />"
							+ "</headerRanges>"
						+ "</headerGroup>"
					+ "</headerGroups>"
				+ "</mapping>";

		try {
			XLSMapping mapping = XLSMapping.parse(textMapping, stringMetadata);

			assertEquals(SpreadsheetOrientation.VERTICAL, mapping.getOrientation());
			assertEquals(1, mapping.getStep());
			assertEquals(true, mapping.isWriteHeader());

			List<HeaderGroup> groups = mapping.getHeaderGroups();
			assertNotNull(groups);
			assertEquals(1, groups.size());
			HeaderGroup group = groups.get(0);
			assertEquals(XLSMapping.UNDEFINED, group.getCloverField());
			assertEquals(XLSMapping.DEFAULT_SKIP, group.getSkip());
			assertEquals(SpreadsheetMappingMode.ORDER, group.getMappingMode());

			List<HeaderRange> ranges = group.getRanges();
			assertNotNull(ranges);
			assertEquals(1, ranges.size());
			HeaderRange range = ranges.get(0);
			assertEquals(85, range.getRowStart());
			assertEquals(85, range.getRowEnd());
			assertEquals(2, range.getColumnStart());
			assertEquals(883, range.getColumnEnd());
		} catch (ComponentNotReadyException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	public void testComplexMapping() {
		String[] cloverFieldNames = { "A", "B", "C", "D", "E", "F", "G", "H" };
		DataRecordMetadata stringMetadata = new DataRecordMetadata("md", DataRecordMetadata.DELIMITED_RECORD);
		for (int i = 0; i < cloverFieldNames.length; i++) {
			stringMetadata.addField(new DataFieldMetadata(cloverFieldNames[i], DataFieldMetadata.STRING_FIELD, ";"));
		}

		String textMapping = "<?xml version=\"1.0\"?>"
				+ "<mapping>"
					+ "<globalAttributes>"
						+ "<step>17</step>"
						+ "<orientation>VERTICAL</orientation>"
						+ "<writeHeader>false</writeHeader>"
					+ "</globalAttributes>"
					+ "<headerGroups>"
						+ "<headerGroup skip=\"5\">"
							+ "<autoMappingType>ORDER</autoMappingType>"
							+ "<headerRanges>"
								+ "<headerRange begin=\"C2\" end=\"F2\" />"
								+ "<headerRange begin=\"K2\" end=\"N2\" />"
								+ "<headerRange begin=\"H2\" />"
							+ "</headerRanges>"
						+ "</headerGroup>"
						+ "<headerGroup>"
							+ "<cloverField>F</cloverField>"
							+ "<headerRanges>"
								+ "<headerRange begin=\"A1\"/>"
							+ "</headerRanges>"
							+ "<formatField>G</formatField>"
						+ "</headerGroup>"
						+ "<headerGroup>"
							+ "<autoMappingType>NAME</autoMappingType>"
							+ "<headerRanges>"
								+ "<headerRange begin=\"F3\" end=\"G4\" />"
								+ "<headerRange begin=\"A5\" end=\"C5\" />"
							+ "</headerRanges>"
						+ "</headerGroup>"
					+ "</headerGroups>"
				+ "</mapping>";

		try {
			XLSMapping mapping = XLSMapping.parse(textMapping, stringMetadata);
			assertEquals(SpreadsheetOrientation.VERTICAL, mapping.getOrientation());
			assertEquals(17, mapping.getStep());
			assertEquals(false, mapping.isWriteHeader());

			List<HeaderGroup> groups = mapping.getHeaderGroups();
			assertNotNull(groups);
			assertEquals(3, groups.size());

			// Group 0
			HeaderGroup group = groups.get(0);
			assertEquals(XLSMapping.UNDEFINED, group.getCloverField());
			assertEquals(XLSMapping.UNDEFINED, group.getFormatField());
			assertEquals(5, group.getSkip());
			assertEquals(SpreadsheetMappingMode.ORDER, group.getMappingMode());

			List<HeaderRange> ranges = group.getRanges();
			assertNotNull(ranges);
			assertEquals(3, ranges.size());

			HeaderRange range = ranges.get(0);
			assertEquals(1, range.getRowStart());
			assertEquals(1, range.getRowEnd());
			assertEquals(2, range.getColumnStart());
			assertEquals(5, range.getColumnEnd());

			range = ranges.get(1);
			assertEquals(1, range.getRowStart());
			assertEquals(1, range.getRowEnd());
			assertEquals(10, range.getColumnStart());
			assertEquals(13, range.getColumnEnd());

			range = ranges.get(2);
			assertEquals(1, range.getRowStart());
			assertEquals(1, range.getRowEnd());
			assertEquals(7, range.getColumnStart());
			assertEquals(7, range.getColumnEnd());

			// Group 1
			group = groups.get(1);
			assertEquals(5, group.getCloverField());
			assertEquals(6, group.getFormatField());
			assertEquals(XLSMapping.DEFAULT_SKIP, group.getSkip());
			assertEquals(SpreadsheetMappingMode.AUTO, group.getMappingMode());

			ranges = group.getRanges();
			assertNotNull(ranges);
			assertEquals(1, ranges.size());
			range = ranges.get(0);
			assertEquals(0, range.getRowStart());
			assertEquals(0, range.getRowEnd());
			assertEquals(0, range.getColumnStart());
			assertEquals(0, range.getColumnEnd());

			// Group 2
			group = groups.get(2);
			assertEquals(XLSMapping.UNDEFINED, group.getCloverField());
			assertEquals(XLSMapping.UNDEFINED, group.getFormatField());
			assertEquals(XLSMapping.DEFAULT_SKIP, group.getSkip());
			assertEquals(SpreadsheetMappingMode.NAME, group.getMappingMode());

			ranges = group.getRanges();
			assertNotNull(ranges);
			assertEquals(2, ranges.size());
			range = ranges.get(0);
			assertEquals(2, range.getRowStart());
			assertEquals(3, range.getRowEnd());
			assertEquals(5, range.getColumnStart());
			assertEquals(6, range.getColumnEnd());

			range = ranges.get(1);
			assertEquals(4, range.getRowStart());
			assertEquals(4, range.getRowEnd());
			assertEquals(0, range.getColumnStart());
			assertEquals(2, range.getColumnEnd());
		} catch (ComponentNotReadyException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
