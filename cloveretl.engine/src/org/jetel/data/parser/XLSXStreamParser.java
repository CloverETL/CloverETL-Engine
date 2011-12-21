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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.openxml4j.exceptions.OpenXML4JException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.ss.usermodel.BuiltinFormats;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.xssf.eventusermodel.ReadOnlySharedStringsTable;
import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.apache.poi.xssf.eventusermodel.XSSFReader.SheetIterator;
import org.apache.poi.xssf.model.StylesTable;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.parser.SpreadsheetStreamParser.CellBuffers;
import org.jetel.data.parser.SpreadsheetStreamParser.RecordFieldValueSetter;
import org.jetel.data.parser.XSSFSheetXMLHandler.SheetContentsHandler;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.util.SpreadsheetUtils;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

/**
 * @author tkramolis (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 1 Aug 2011
 */
public class XLSXStreamParser implements SpreadsheetStreamHandler {

	private final static Log LOGGER = LogFactory.getLog(XLSXStreamParser.class);

	private SpreadsheetStreamParser parent;

	private OPCPackage opcPackage;
	private XSSFReader reader;
	private StylesTable stylesTable;
	private ReadOnlySharedStringsTable sharedStringsTable;
	private XMLStreamReader staxParser;
	private XMLInputFactory xmlInputFactory;
	private RecordFillingContentHandler sheetContentHandler;
	private XSSFSheetXMLHandler xssfContentHandler;

	private SheetIterator sheetIterator;
	private InputStream currentSheetInputStream;

	private CellBuffers<CellValue> cellBuffers;
//	private CellValue[][] cellBuffers;
//	private int nextPartial = -1;

	private int currentSheetIndex;

	private int nextRecordStartRow;

	/** the data formatter used to format cell values as strings */
	private final DataFormatter dataFormatter = new DataFormatter();

	public XLSXStreamParser(SpreadsheetStreamParser parent) {
		this.parent = parent;
	}

	@Override
	public void init() throws ComponentNotReadyException {
		xmlInputFactory = XMLInputFactory.newInstance();
		cellBuffers = parent.new CellBuffers<CellValue>();
	}

	@Override
	public DataRecord parseNext(DataRecord record) throws JetelException {
		sheetContentHandler.setRecordStartRow(nextRecordStartRow);
		sheetContentHandler.setRecord(record);
		try {
			while (staxParser.hasNext() && !sheetContentHandler.isRecordFinished()) {
				processParserEvent(staxParser, xssfContentHandler);
			}
			if (!staxParser.hasNext() && !sheetContentHandler.isRecordFinished()) {
				if (!sheetContentHandler.isRecordStarted()) {
					return null;
				}
				sheetContentHandler.finishRecord();
			}			
		} catch (XMLStreamException e) {
			throw new JetelException("Error occurred while reading XML of sheet " + currentSheetIndex, e);
		} catch (SAXException e) {
			throw new JetelException("Error occurred while reading XML of sheet " + currentSheetIndex, e);
		}

		nextRecordStartRow += parent.mappingInfo.getStep();
		return record;
	}

	@Override
	public int skip(int nRec) throws JetelException {
		sheetContentHandler.skipRecords(nRec);

		try {
			while (staxParser.hasNext() && sheetContentHandler.isSkipRecords()) {
				processParserEvent(staxParser, xssfContentHandler);
			}

			int skippedRecords = sheetContentHandler.getNumberOfSkippedRecords();
			nextRecordStartRow += skippedRecords * parent.mappingInfo.getStep();

			return skippedRecords;			
		} catch (XMLStreamException e) {
			throw new JetelException("Error occurred while reading XML of sheet " + currentSheetIndex, e);
		} catch (SAXException e) {
			throw new JetelException("Error occurred while reading XML of sheet " + currentSheetIndex, e);
		}
	}

	@Override
	public List<String> getSheetNames() {
		try {
			SheetIterator iterator = (SheetIterator) reader.getSheetsData();
			List<String> toReturn = new ArrayList<String>();
			while (iterator.hasNext()) {
				InputStream stream = iterator.next();
				toReturn.add(iterator.getSheetName());
				stream.close();
			}
			return toReturn;
		} catch (InvalidFormatException e) {
			throw new JetelRuntimeException(e);
		} catch (IOException e) {
			throw new JetelRuntimeException(e);
		}
	}

	@Override
	public int getCurrentRecordStartRow() {
		return nextRecordStartRow;
	}

	@Override
	public boolean setCurrentSheet(int sheetNumber) {
		if (currentSheetIndex >= sheetNumber) {
			closeCurrentInputStream();
			initializeSheetIterator();
		}
		while (currentSheetIndex < sheetNumber && sheetIterator.hasNext()) {
			closeCurrentInputStream();
			currentSheetInputStream = sheetIterator.next();
			currentSheetIndex++;
		}
		if (currentSheetIndex == sheetNumber) {
			try {
				staxParser = xmlInputFactory.createXMLStreamReader(currentSheetInputStream);
				xssfContentHandler = new XSSFSheetXMLHandler(sharedStringsTable, sheetContentHandler);
				sheetContentHandler.setRecordStartRow(parent.startLine);
				sheetContentHandler.prepareForNextSheet();
			} catch (Exception e) {
				throw new JetelRuntimeException("Failed to create XML parser for sheet " + sheetIterator.getSheetName(), e);
			}

			this.nextRecordStartRow = parent.startLine;
			cellBuffers.clear();
			return true;
		}

		return false;
	}

	@Override
	public String[][] getHeader(int startRow, int startColumn, int endRow, int endColumn) {
		if (opcPackage == null) {
			return null;
		}

		try {
			List<List<CellValue>> rows = readRows(startRow, startColumn, endRow, endColumn);
			String[][] result = new String[rows.size()][];

			int i = 0;
			for (List<CellValue> row : rows) {
				if (row != null && !row.isEmpty()) {
					result[i] = new String[row.get(row.size() - 1).columnIndex - startColumn + 1];
					for (CellValue cellValue : row) {
						result[i][cellValue.columnIndex - startColumn] = cellValue.value;
					}
				} else {
					result[i] = new String[0];
				}
				i++;
			}

			return result;
		} catch (ComponentNotReadyException e) {
			throw new JetelRuntimeException("Failed to provide preview", e);
		}
	}

	@Override
	public void prepareInput(InputStream inputStream) throws IOException, ComponentNotReadyException {
		try {
			opcPackage = OPCPackage.open(inputStream);
			reader = new XSSFReader(opcPackage);
			stylesTable = reader.getStylesTable();
			sharedStringsTable = new ReadOnlySharedStringsTable(opcPackage);
			sheetContentHandler = new RecordFillingContentHandler(stylesTable, dataFormatter, AbstractSpreadsheetParser.USE_DATE1904);
			cellBuffers.init(CellValue.class, sheetContentHandler, sheetContentHandler.getFieldValueToFormatSetter());
		} catch (InvalidFormatException e) {
			throw new ComponentNotReadyException("Error opening the XLSX workbook!", e);
		} catch (OpenXML4JException e) {
			throw new ComponentNotReadyException("Error opening the XLSX workbook!", e);
		} catch (SAXException e) {
			throw new ComponentNotReadyException("Error opening the XLSX workbook!", e);
		}

		initializeSheetIterator();
	}

	@Override
	public void close() throws IOException {
		try {
			staxParser.close();
		} catch (XMLStreamException e) {
			LOGGER.warn("Closing parser threw exception", e);
		}
		closeCurrentInputStream();
	}

	private static void processParserEvent(XMLStreamReader staxParser, XSSFSheetXMLHandler contentHandler)
			throws XMLStreamException, SAXException {
		staxParser.next();
		switch (staxParser.getEventType()) {
		case XMLStreamConstants.START_ELEMENT:
			AttributesImpl attrs = new AttributesImpl();
			for (int i = 0; i < staxParser.getAttributeCount(); i++) {
				attrs.addAttribute(staxParser.getAttributeNamespace(i), staxParser.getAttributeLocalName(i),
						qNameToStr(staxParser.getAttributeName(i)), staxParser.getAttributeType(i), staxParser.getAttributeValue(i));
			}
			contentHandler.startElement(staxParser.getNamespaceURI(), staxParser.getLocalName(), qNameToStr(staxParser.getName()), attrs);
			break;
		case XMLStreamConstants.CHARACTERS:
			contentHandler.characters(staxParser.getTextCharacters(), staxParser.getTextStart(), staxParser.getTextLength());
			break;
		case XMLStreamConstants.END_ELEMENT:
			contentHandler.endElement(staxParser.getNamespaceURI(), staxParser.getLocalName(), qNameToStr(staxParser.getName()));
			break;
		}
	}

	private static String qNameToStr(QName qName) {
		String prefix = qName.getPrefix();
		return prefix.isEmpty() ? qName.getLocalPart() : prefix + ":" + qName.getLocalPart();
	}

	private void closeCurrentInputStream() {
		if (currentSheetInputStream != null) {
			try {
				currentSheetInputStream.close();
				currentSheetInputStream = null;
			} catch (IOException e) {
				LOGGER.warn("Failed to close input stream", e);
			}
		}
	}

	private void initializeSheetIterator() {
		try {
			sheetIterator = (SheetIterator) reader.getSheetsData();
			currentSheetIndex = -1;
		} catch (Exception e) {
			throw new JetelRuntimeException(e);
		}
	}

	private List<List<CellValue>> readRows(int startRow, int startColumn, int endRow, int endColumn)
			throws ComponentNotReadyException {
		List<List<CellValue>> rows = new LinkedList<List<CellValue>>();
		XMLStreamReader parser = null;
		try {
			parser = xmlInputFactory.createXMLStreamReader(currentSheetInputStream);
			RawRowContentHandler rowContentHandler = new RawRowContentHandler(stylesTable, dataFormatter, startColumn, endColumn);
			XSSFSheetXMLHandler xssfContentHandler = new XSSFSheetXMLHandler(sharedStringsTable, rowContentHandler);
			int currentRow = startRow;
			while (currentRow < endRow) {
				rowContentHandler.setRecordStartRow(currentRow);
				while (parser.hasNext() && !rowContentHandler.isRecordFinished()) {
					processParserEvent(parser, xssfContentHandler);
				}
				if (!parser.hasNext() && !rowContentHandler.isRecordFinished()) {
					return rows;
				}
				rows.add(rowContentHandler.getCellValues());
				currentRow++;
			}
		} catch (XMLStreamException e) {
			throw new ComponentNotReadyException("Error occurred while reading XML of sheet " + currentSheetIndex, e);
		} catch (SAXException e) {
			throw new ComponentNotReadyException("Error occurred while reading XML of sheet " + currentSheetIndex, e);
		} finally {
			closeCurrentInputStream();
			if (parser != null) {
				try {
					parser.close();
				} catch (XMLStreamException e) {
					LOGGER.warn("Closing parser threw an exception", e);
				}
			}
		}
		setCurrentSheet(currentSheetIndex);
		return rows;
	}

	// based on org.apache.poi.ss.usermodel.DateUtil.isCellDateFormatted(Cell)
	// public boolean isCellDateFormatted(CellValue cell) {
	// boolean bDate = false;
	// double d = Double.parseDouble(cell.value);
	// if (DateUtil.isValidExcelDate(d) ) {
	// CellStyle style = stylesTable.getStyleAt(cell.styleIndex);
	// if (style == null) return false;
	// int i = style.getDataFormat();
	// String f = style.getDataFormatString();
	// if (f == null) {
	// f = BuiltinFormats.getBuiltinFormat(i);
	// }
	// bDate = DateUtil.isADateFormat(i, f);
	// }
	// return bDate;
	// }

	private abstract class SheetRowContentHandler implements SheetContentsHandler {

		/* row on which starts currently read record */
		protected int recordStartRow;
		/* row where current record finishes */
		protected int recordEndRow;
		/* actual row which is being parsed */
		protected int currentParseRow;
		/* flag indicating whether row where current record finishes was read */
		protected boolean recordStarted;
		protected boolean recordFinished;

		protected DataFormatter formatter;
		protected StylesTable stylesTable;

		public SheetRowContentHandler(StylesTable stylesTable, DataFormatter formatter) {
			this.stylesTable = stylesTable;
			this.formatter = formatter;
		}

		public void prepareForNextSheet() {
			recordStartRow = parent.startLine;
			recordEndRow = 0;
			currentParseRow = -1;
		}

		public void setRecordStartRow(int startRow) {
			recordStartRow = startRow;
			recordEndRow = startRow + parent.mapping.length - 1;
			recordStarted = false;
			recordFinished = false;
		}

		public boolean isRecordFinished() {
			return recordFinished;
		}

		public boolean isRecordStarted() {
			return recordStarted;
		}

		@Override
		public void startRow(int rowNum) {
			currentParseRow = rowNum;
		}

		@Override
		public void endRow() {
			if (currentParseRow >= recordEndRow) {
				recordFinished = true;
			}
		}

		@Override
		public void headerFooter(String text, boolean isHeader, String tagName) {
			// Do nothing, not interested
		}

		protected String getFormatString(int styleIndex) {
			XSSFCellStyle style = stylesTable.getStyleAt(styleIndex);
			String formatString = style.getDataFormatString();
			if (formatString == null) {
				formatString = BuiltinFormats.getBuiltinFormat(style.getDataFormat());
			}
			return formatString;
		}
		
		protected String formatNumericToString(String value, int styleIndex) {
			XSSFCellStyle style = stylesTable.getStyleAt(styleIndex);
			String formatString = getFormatString(styleIndex);
			return formatter.formatRawCellContents(Double.parseDouble(value), style.getDataFormat(), formatString);
		}

	}

	private class RawRowContentHandler extends SheetRowContentHandler {

		private List<CellValue> cellValues = new ArrayList<CellValue>();
		private final int firstColumn;
		private final int lastColumn;

		public RawRowContentHandler(StylesTable stylesTable, DataFormatter formatter, int firstColumn, int lastColumn) {
			super(stylesTable, formatter);
			this.firstColumn = firstColumn;
			this.lastColumn = lastColumn;
		}

		public List<CellValue> getCellValues() {
			return new ArrayList<XLSXStreamParser.CellValue>(cellValues);
		}

		@Override
		public void setRecordStartRow(int startRow) {
			recordStartRow = startRow;
			recordEndRow = startRow;
			recordStarted = false;
			recordFinished = false;
			cellValues.clear();
		}
		
		@Override
		public void cell(String cellReference, int cellType, int formulaType, String value, int styleIndex) {
			if (currentParseRow == recordStartRow) {
				int columnIndex = SpreadsheetUtils.getColumnIndex(cellReference);
				if (columnIndex < firstColumn || columnIndex >= lastColumn) {
					return;
				}

				String formattedValue = value;
				if (cellType == Cell.CELL_TYPE_NUMERIC || (cellType == Cell.CELL_TYPE_FORMULA && formulaType == Cell.CELL_TYPE_NUMERIC)) {
					formattedValue = formatNumericToString(value, styleIndex);
				}
				if (cellType != Cell.CELL_TYPE_BLANK) {
					cellValues.add(new CellValue(columnIndex, formattedValue, cellType, formulaType, styleIndex));
				}
			}
		}
	}

	private class RecordFillingContentHandler extends SheetRowContentHandler implements RecordFieldValueSetter<CellValue> {

		private DataRecord record;
		private final boolean date1904;

		private int lastColumn = -1;

		private int skipStartRow;
		private boolean skipRecords = false;

		private final RecordFieldValueSetter<CellValue> fieldValueToFormatSetter = new FieldValueToFormatSetter();
		
		
		public RecordFillingContentHandler(StylesTable stylesTable, DataFormatter formatter, boolean date1904) {
			super(stylesTable, formatter);
			this.date1904 = date1904;
		}

		public void finishRecord() {			
			for (int i = currentParseRow + 1; i <= recordEndRow; i++) {
				handleMissingCells(i - recordStartRow, lastColumn, parent.mapping[0].length);
			}
		}

		public void setRecord(DataRecord record) {
			this.record = record;
			cellBuffers.fillRecordFromBuffer(record);
		}

		public boolean isSkipRecords() {
			return skipRecords;
		}

		public int getNumberOfSkippedRecords() {
			int result = currentParseRow - skipStartRow + parent.mappingInfo.getStep();
			if (result > 0) {
				return result / parent.mappingInfo.getStep();
			}
			return 0;
		}

		public void skipRecords(int nRec) {
			skipStartRow = recordStartRow;
			record = null;

			int numberOfRows = nRec * parent.mappingInfo.getStep();
			setRecordStartRow(recordStartRow + numberOfRows);
			
			if (nRec != 1 || skipStartRow != currentParseRow) {
				skipRecords = true;
			}
		}

		@Override
		public void cell(String cellReference, int cellType, int formulaType, String value, int styleIndex) {
			if (currentParseRow < recordStartRow) {
				// not interested yet, skip
				return;
			}

			int columnIndex = SpreadsheetUtils.getColumnIndex(cellReference);
			if (columnIndex < parent.mappingMinColumn || columnIndex >= parent.mapping[0].length + parent.mappingMinColumn) {
				// not interested, skip
				return;
			}
			int shiftedColumnIndex = columnIndex - parent.mappingMinColumn;
			int mappingRow = currentParseRow - recordStartRow;

			handleMissingCells(mappingRow, lastColumn, shiftedColumnIndex);
			lastColumn = shiftedColumnIndex;
			
			CellValue cellValue = new CellValue(-1, value, cellType, formulaType, styleIndex);
			cellBuffers.setCellBufferValue(mappingRow, shiftedColumnIndex, cellValue);
			if (record != null) {
				if (parent.mapping[mappingRow][shiftedColumnIndex] != XLSMapping.UNDEFINED) {
					setFieldValue(parent.mapping[mappingRow][shiftedColumnIndex], cellValue);
				}
				if (parent.formatMapping != null) {
					if (parent.formatMapping[mappingRow][shiftedColumnIndex] != XLSMapping.UNDEFINED) {
						fieldValueToFormatSetter.setFieldValue(parent.formatMapping[mappingRow][shiftedColumnIndex], cellValue);
					}
				}
			}
			
		}
		
		@Override
		public void startRow(int rowNum) {
			super.startRow(rowNum);
			if (currentParseRow >= recordStartRow - parent.mappingInfo.getStep()) {
				skipRecords = false;
			}
		}

		@Override
		public void endRow() {
			super.endRow();
			if (!skipRecords && currentParseRow >= recordStartRow) {
				handleMissingCells(currentParseRow - recordStartRow, lastColumn, parent.mapping[0].length);
			}
			lastColumn = -1;
		}

		private void handleMissingCells(int mappingRow, int firstColumn, int lastColumn) {
			int[] mappingPart = parent.mapping[mappingRow];
			int cloverFieldIndex;
			for (int i = firstColumn + 1; i < lastColumn; i++) {
				cloverFieldIndex = mappingPart[i];
				if (cloverFieldIndex != XLSMapping.UNDEFINED) {
					try {
						record.getField(cloverFieldIndex).setNull(true);
					} catch (BadDataFormatException e) {
						parent.handleException(new BadDataFormatException("There is no data row for field. Moreover, cannot set default value or null", e), record, cloverFieldIndex, null, null);
					}
				}
			}
		}

		@Override
		public void setFieldValue(int fieldIndex, CellValue cell) {
			setFieldValue(fieldIndex, cell.type, cell.formulaType, cell.value, cell.styleIndex);
		}

		private void setFieldValue(int cloverFieldIndex, int cellType, int formulaType, String value, int styleIndex) {
			if (!recordStarted) {
				recordStarted = true;
			}
			
			DataField field = record.getField(cloverFieldIndex);
			try {
				switch (field.getType()) {
				case DataFieldMetadata.DATE_FIELD:
				case DataFieldMetadata.DATETIME_FIELD:
					if (cellType == Cell.CELL_TYPE_NUMERIC) {
						field.setValue(DateUtil.getJavaDate(Double.parseDouble(value), date1904));
					} else {
						throw new IllegalStateException("Cannot get Date value from type " + cellTypeToString(cellType) + " cell");
					}
					break;
				case DataFieldMetadata.BYTE_FIELD:
				case DataFieldMetadata.STRING_FIELD:
					field.fromString(cellType == Cell.CELL_TYPE_NUMERIC || (cellType == Cell.CELL_TYPE_FORMULA && formulaType == Cell.CELL_TYPE_NUMERIC)? formatNumericToString(value, styleIndex) : value);
					break;
				case DataFieldMetadata.DECIMAL_FIELD:
				case DataFieldMetadata.INTEGER_FIELD:
				case DataFieldMetadata.LONG_FIELD:
				case DataFieldMetadata.NUMERIC_FIELD:
					field.setValue(Double.parseDouble(value));
					break;
				case DataFieldMetadata.BOOLEAN_FIELD:
					if (cellType == Cell.CELL_TYPE_BOOLEAN) {
						field.setValue(XSSFSheetXMLHandler.CELL_VALUE_TRUE.equals(value));
					} else {
						throw new IllegalStateException("Cannot get Boolean value from type " + cellTypeToString(cellType) + " cell");
					}
					break;
				}
			} catch (RuntimeException ex1) { // exception when trying get date or number from a different cell type
				try {
//					field.fromString(value);
					field.setNull(true);
				} catch (Exception ex2) {
				}
				String cellCoordinates = SpreadsheetUtils.getColumnReference(lastColumn + parent.mappingMinColumn) + String.valueOf(currentParseRow);
				parent.handleException(new BadDataFormatException("All attempts to set value \""+ value +"\" into field \"" + field.getMetadata().getName() + "\" (" + field.getMetadata().getTypeAsString() + ") failed:\n1st try error: " + ex1 + "\n"), 
						record, cloverFieldIndex, cellCoordinates, value);
			}
		}

		private String cellTypeToString(int cellType) {
			switch (cellType) {
			case Cell.CELL_TYPE_BOOLEAN:
				return "BOOLEAN";
			case Cell.CELL_TYPE_STRING:
				return "STRING";
			case Cell.CELL_TYPE_NUMERIC:
				return "NUMERIC";
			default:
				return "UNKNOWN";
			}
		}
		
		protected RecordFieldValueSetter<CellValue> getFieldValueToFormatSetter() {
			return fieldValueToFormatSetter;
		}
		
		private class FieldValueToFormatSetter implements RecordFieldValueSetter<CellValue> {
			@Override
			public void setFieldValue(int cloverFieldIndex, CellValue cellValue) {
				DataField field = record.getField(cloverFieldIndex);
				String format = getFormatString(cellValue.styleIndex);
				field.fromString(format);
			}
		}
		
	}

	private static class CellValue {
		public final int columnIndex;
		public final String value;
		public final int type;
		public final int formulaType;
		public final int styleIndex;

		CellValue(int columnIndex, String value, int type, int formulaType, int styleIndex) {
			this.columnIndex = columnIndex;
			this.value = value;
			this.type = type;
			this.formulaType = formulaType;
			this.styleIndex = styleIndex;
		}
	}
}
