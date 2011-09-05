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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.jetel.data.DataRecord;
import org.jetel.data.RecordKey;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.test.CloverTestCase;

/**
 * @author tkramolis (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 29 Jul 2011
 */
public class SpreadsheetParserTest extends CloverTestCase {
	
	private static final String XLS_FILE = "data/xls/Szinkron Comedy 2011 03v2.xls";
	private static final String XLSX_FILE = "data/xls/Szinkron Comedy 2011 03v2.xlsx";
	
	private DataRecordMetadata stringMetadata;
	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		initEngine();
		
		String[] cloverFieldNames = {"A", "B", "C", "D", "E", "F", "G", "H"};
		
		stringMetadata = new DataRecordMetadata("md", DataRecordMetadata.DELIMITED_RECORD);
		for (int i = 0; i < cloverFieldNames.length; i++) {
			stringMetadata.addField(new DataFieldMetadata(cloverFieldNames[i], DataFieldMetadata.STRING_FIELD, ";"));
		}
	}

	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
		stringMetadata = null;
	}

	public void testSpreadsheetParsers() throws Exception {
		String sheetName = "0";
		
		/* Old ones */
		XLSParser xlsxParser = new XLSXDataParser(stringMetadata);
		xlsxParser.setSheetNumber(sheetName);
		xlsxParser.init();
		xlsxParser.preExecute();
		xlsxParser.setDataSource(new FileInputStream(XLSX_FILE));
		
		XLSParser xlsParser = new JExcelXLSDataParser(stringMetadata);
		xlsParser.setSheetNumber(sheetName);
		xlsParser.init();
		xlsParser.preExecute();
		xlsParser.setDataSource(new FileInputStream(XLS_FILE));
		
		/* New ones */
		AbstractSpreadsheetParser xlsxStreamParser = new SpreadsheetStreamParser(stringMetadata, null);
		xlsxStreamParser.setSheet(sheetName);
		xlsxStreamParser.init();
		xlsxStreamParser.preExecute();
		xlsxStreamParser.setDataSource(new FileInputStream(XLSX_FILE));
		
		AbstractSpreadsheetParser xlsStreamParser = new SpreadsheetStreamParser(stringMetadata, null);
		xlsStreamParser.setSheet(sheetName);
		xlsStreamParser.init();
		xlsStreamParser.preExecute();
		xlsStreamParser.setDataSource(new FileInputStream(XLS_FILE));
		
		AbstractSpreadsheetParser inMemoryParser1 = new SpreadsheetDOMParser(stringMetadata, null);
		inMemoryParser1.setSheet(sheetName);
		inMemoryParser1.init();
		inMemoryParser1.preExecute();
		inMemoryParser1.setDataSource(new FileInputStream(XLSX_FILE));
	
		AbstractSpreadsheetParser inMemoryParser2 = new SpreadsheetDOMParser(stringMetadata, null);
		inMemoryParser2.setSheet(sheetName);
		inMemoryParser2.init();
		inMemoryParser2.preExecute();
		inMemoryParser2.setDataSource(new FileInputStream(XLS_FILE));
		
		assertEqualOutput(stringMetadata, xlsParser, xlsxParser, xlsxStreamParser, xlsStreamParser, inMemoryParser1, inMemoryParser2);
	}
	
	private void assertEqualOutput(DataRecordMetadata metadata, Parser... parsers) throws JetelException, IOException, ComponentNotReadyException {
		RecordKey recordKey = new RecordKey(metadata.getFieldNamesArray(), metadata);
		recordKey.setEqualNULLs(true);
		recordKey.init();
		
		DataRecord[] records = new DataRecord[parsers.length];
		for (int i = 0; i < parsers.length; i++) {
			records[i] = new DataRecord(metadata);
			records[i].init();
		}
		
		boolean run = true;
		int recNum = 0;
		while (run) {
			for (int i = 0; i < parsers.length; i++) {
				if (parsers[i].getNext(records[i]) == null) {
					records[i] = null;
					run = false;
				}
			}

			for (int i = 0; i < parsers.length-1; i++) {
				System.out.println(parsers[i+1].getClass().getSimpleName());
				assertTrue(
						"Unequal record #" + recNum + " (0-based)\n" +
						"Parser " + i + " (" + parsers[i].getClass().getSimpleName() + "):\n" + records[i] +
						"Parser " + (i+1) + " (" + parsers[i+1].getClass().getSimpleName() + "):\n" + records[i+1],
						recordKey.compare(records[i], records[i+1]) == 0);
			}
			recNum++;
		}

		List<Integer> eof = new ArrayList<Integer>();
		List<Integer> notEof = new ArrayList<Integer>();
		for (int i = 0; i < parsers.length; i++) {
			if (records[i] == null) {
				eof.add(i);
			} else {
				notEof.add(i);
			}
		}
		assertTrue("\nParsers with EOF: " + eof + "\n Parsers without EOF: " + notEof, notEof.isEmpty());
		
		for (int i = 0; i < parsers.length; i++) {
			parsers[i].postExecute();
			parsers[i].free();
		}
	}
	
	private List<AbstractSpreadsheetParser> prepareParsers(DataRecordMetadata metadata, XLSMapping mapping, String sheet,
			String inputFile) throws Exception {
		List<AbstractSpreadsheetParser> parsers = new ArrayList<AbstractSpreadsheetParser>();
		parsers.add(new SpreadsheetStreamParser(metadata, mapping));
		parsers.add(new SpreadsheetDOMParser(metadata, mapping));
		for (AbstractSpreadsheetParser spreadsheetParser : parsers) {
			spreadsheetParser.setSheet(sheet);
			spreadsheetParser.init();
			spreadsheetParser.preExecute();
			spreadsheetParser.setDataSource(new FileInputStream(inputFile));
		}
		return parsers;
	}
	
	private List<AbstractSpreadsheetParser> prepareParsers(DataRecordMetadata metadata, XLSMapping mapping, String sheet,
			String xlsFile, String xlsxFile) throws Exception {
		List<AbstractSpreadsheetParser> toReturn = new ArrayList<AbstractSpreadsheetParser>();
		toReturn.addAll(prepareParsers(metadata, mapping, sheet, xlsFile));
		toReturn.addAll(prepareParsers(metadata, mapping, sheet, xlsxFile));
		return toReturn;
	}

	public void testGetHeader() throws Exception {
		String[][] expected = new String[4][];
		expected[0] = new String[] {"Időszak", "2011. március"};
		expected[1] = new String[] {};
		expected[2] = new String[] {};
		expected[3] = new String[] {"Formátum", "Epizód darabszám", "Összes hossz", "Percdíj", "Össz ktg"};
		for (AbstractSpreadsheetParser parser : prepareParsers(stringMetadata, null, "0", XLS_FILE, XLSX_FILE)) {
			String[][] actual = parser.getHeader(6, 1, 10, 6);
			assertEquals(parser.getClass().getSimpleName(), expected.length, actual.length);
			for (int i = 0; i < expected.length; i++) {
				String[] expectedRow = expected[i];
				String[] actualRow = actual[i];
				assertEquals(parser.getClass().getSimpleName() + " row " + i,
						expectedRow.length, actualRow.length);
				for (int j = 0; j < actualRow.length; j++) {
					assertEquals(parser.getClass().getSimpleName() + " row " + i + " column " + j,
							expectedRow[j], actualRow[j]);
				}
			}
		}
	}
	
	public void testGetSheetNames() throws Exception {
		List<String> expected = Arrays.asList(new String[] {"Telj igaz Comedy", "Comedy"});
		for (AbstractSpreadsheetParser parser : prepareParsers(stringMetadata, null, "0", XLS_FILE, XLSX_FILE)) {
			assertEquals(parser.getClass().getSimpleName(), expected, parser.getSheetNames());
		}
	}
}
