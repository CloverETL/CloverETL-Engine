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
package org.jetel.data.formatter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.RecordKey;
import org.jetel.data.parser.XLSMapping;
import org.jetel.data.parser.XLSMapping.HeaderGroup;
import org.jetel.data.parser.XLSMapping.HeaderRange;
import org.jetel.data.parser.XLSMapping.Stats;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.SpreadsheetUtils.SpreadsheetAttitude;
import org.jetel.util.SpreadsheetUtils.SpreadsheetFormat;
import org.jetel.util.file.FileUtils;
import org.jetel.util.string.StringUtils;

/**
 * @author lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 6 Sep 2011
 */
public class SpreadsheetFormatter implements Formatter {

	private SpreadsheetAttitude attitude;
	private SpreadsheetFormat formatterType;

	private Workbook templateWorkbook; // TODO: implement me
	private String sheet;
	private XLSMapping mappingInfo;
	private CellMapping[] mapping;

	private boolean append; // TODO: implement me
	private boolean insert; // TODO: implement me
	private boolean removeSheets;

	/** the currently open workbook */
	private Workbook workbook;
	/** the sheet that is being used */
	private Sheet currentSheet;
	/** the output stream used to output the workbook */
	private OutputStream outputStream;

	private RecordKey sheetNameKeyRecord;
	private DataRecordMetadata metadata;
	private Map<String, SheetData> sheetData;

	private int sheetIndex = -1;
	private String sheetName;
	/** the sheet data associated with the currently used sheet */
	private SheetData currentSheetData;
	/** the current row index used for writing */
	private int currentRowIndex;

	public void setAttitude(SpreadsheetAttitude attitude) {
		this.attitude = attitude;
	}

	public void setFormatterType(SpreadsheetFormat formatterType) {
		this.formatterType = formatterType;
	}

	public void setTemplateWorkbook(Workbook templateWorkbook) {
		this.templateWorkbook = templateWorkbook;
	}

	public void setSheet(String sheet) {
		this.sheet = sheet;
	}

	public void setMapping(XLSMapping mapping) {
		this.mappingInfo = mapping;
	}

	public void setAppend(boolean append) {
		this.append = append;
	}

	public void setInsert(boolean insert) {
		this.insert = insert;
	}

	public void setRemoveSheets(boolean removeSheets) {
		this.removeSheets = removeSheets;
	}

	@Override
	public void init(DataRecordMetadata metadata) throws ComponentNotReadyException {
		this.metadata = metadata;

		if (!StringUtils.isEmpty(sheet)) {
			if (sheet.startsWith(Defaults.CLOVER_FIELD_INDICATOR)) {
				String[] fields = sheet.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
				for (int i = 0; i < fields.length; i++) {// Remove clover field indicator from each field
					fields[i] = fields[i].substring(1);
				}

				sheetNameKeyRecord = new RecordKey(fields, metadata);
				sheetNameKeyRecord.init();
				sheetData = new HashMap<String, SheetData>();
			} else {
				int result = StringUtils.isInteger(sheet);
				if (result == 0 || result == 1) {
					sheetIndex = Integer.parseInt(sheet);
				} else {
					if (sheet.charAt(0) == XLSMapping.ESCAPE_START) {
						sheetName = sheet.substring(1, sheet.length() - 1);
					} else {
						sheetName = sheet;
					}
				}
			}
		}
		
		initMapping();
	}

	private void initMapping() { // TODO: implement me
		Stats stats = mappingInfo.getStats();
		Map<String, Integer> nameMap = metadata.getFieldNamesMap();
		
		List<Integer> unusedFields = new ArrayList<Integer>();
		for (int i = 0; i < metadata.getNumFields(); i++) {
			unusedFields.add(i);
		}

		for (HeaderGroup group : mappingInfo.getHeaderGroups()) {
			for (HeaderRange range : group.getRanges()) {
				if (group.getCloverField() != XLSMapping.UNDEFINED) {
					
				} else {
					switch (group.getMappingMode()) {
					case AUTO:
						if (stats.useAutoNameMapping()) {
							// name mapping
						} else {
							// order mapping
						}
						break;
					case NAME:
						// name mapping
						break;
					case ORDER:
						// order mapping
						break;
					}
				}
			}
		}
		
		
		// original XLSWriter code -- just for inspiration securing that all features XLSWriter had SpreadsheetWriter will have
		// prepare cell styles
//		cellStyles = new CellStyle[includedFieldIndices.length];
//		DataFormat dataFormat = workbook.createDataFormat();
//
//		for (int i = 0; i < includedFieldIndices.length; i++) {
//			DataFieldMetadata fieldMetadata = metadata.getField(includedFieldIndices[i]);
//
//			CellStyle cellStyle = workbook.createCellStyle();
//			cellStyle.setDataFormat(dataFormat.getFormat((fieldMetadata.getFormatStr() != null) ? fieldMetadata.getFormatStr() : GENERAL_FORMAT_STRING));
//
//			cellStyles[i] = cellStyle;
//		}

	}

	@Override
	public void setDataTarget(Object outputDataTarget) throws IOException {
		if (outputDataTarget == null) {
			throw new NullPointerException("dataTarget");
		}

		close();
		try {
			if (outputDataTarget instanceof Object[]) {
				URL url = (URL) ((Object[]) outputDataTarget)[0];
				String file = (String) ((Object[]) outputDataTarget)[1];

				if (templateWorkbook == null) {
					workbook = createWorkbook(url, file);
				} else {
					workbook = newWorkbook();
				}

				outputStream = FileUtils.getOutputStream(url, file, false, -1);
			} else if (outputDataTarget instanceof WritableByteChannel) {
				workbook = new XSSFWorkbook();
				outputStream = Channels.newOutputStream((WritableByteChannel) outputDataTarget);
			} else {
				throw new IllegalArgumentException(outputDataTarget.getClass() + " not supported as a data target");
			}
		} catch (IOException exception) {
			throw new IllegalArgumentException("Error opening/writing the XLS(X) workbook!", exception);
		}

		if (removeSheets) { // remove all sheets in a workbook
			// they must be removed from the last sheet to the first sheet, because Workbook
			// re-indexes sheets with a higher number than the removed one
			for (int i = workbook.getNumberOfSheets() - 1; i >= 0; --i) {
				workbook.removeSheetAt(i);
			}
		}
	}

	public void prepareSheet() {
		if (sheetName != null) {
			currentSheet = workbook.getSheet(sheetName);

			if (sheet == null) {
				currentSheet = workbook.createSheet(sheetName);
			} else if (!append) { // TODO: really?
				Iterator<Row> rowIterator = currentSheet.iterator();

				while (rowIterator.hasNext()) {
					rowIterator.next();
					rowIterator.remove();
				}
			}
		} else if (sheetIndex >= 0) {
			if (sheetIndex >= workbook.getNumberOfSheets()) {
				throw new IndexOutOfBoundsException("sheetNumber >= " + workbook.getNumberOfSheets());
			}

			currentSheet = workbook.getSheetAt(sheetIndex);
		} else {
			currentSheet = workbook.createSheet();
		}

		// save the current sheet
		if (sheetData != null) {
			currentSheetData = new SheetData(currentSheet, currentRowIndex);
			sheetData.put(currentSheet.getSheetName(), currentSheetData);
		}
	}

	public void prepareSheet(DataRecord record) {
		sheetName = sheetNameKeyRecord.getKeyString(record);

		if (sheetData.containsKey(sheetName)) {
			currentSheetData = sheetData.get(sheetName);
			currentRowIndex = currentSheetData.currentRow;
		} else {
			prepareSheet();
			writeSheetHeader();
		}
	}

	private void writeSheetHeader() {// TODO: implement me
		// original XLSWriter code -- just for inspiration securing that all features XLSWriter had SpreadsheetWriter will have
		
//		try {
//			firstColumn = XLSFormatter.getCellNum(firstColumnIndex);
//		} catch (InvalidNameException exception) {
//			throw new IllegalArgumentException("firstColumnIndex", exception);
//		}
//		
//		// determine the correct row and write the names row
//		
//		currentRowIndex = append ? sheet.getLastRowNum() + (sheet.getPhysicalNumberOfRows() > 0 ? 1 : 0) : 0;
//
//		if (namesRow > -1) {
//			if (!append || sheet.getLastRowNum() < 0) {
//				Row row = sheet.getRow(namesRow);
//
//				if (row == null) {
//					row = sheet.createRow(namesRow);
//				}
//
//				CellStyle cellStyle = workbook.createCellStyle();
//
//				Font boldFont = workbook.createFont();
//				boldFont.setBoldweight(Font.BOLDWEIGHT_BOLD);
//
//				cellStyle.setFont(boldFont);
//
//				for (int i = 0; i < includedFieldIndices.length; i++) {
//					Cell newCell = row.createCell(firstColumn + i);
//					newCell.setCellValue(metadata.getField(includedFieldIndices[i]).getName());
//					newCell.setCellStyle(cellStyle);
//				}
//
//				currentRowIndex = namesRow + 1;
//			}
//		}
//		if (firstRow > currentRowIndex) {
//			currentRowIndex = firstRow;
//		}
	}

	public int write(DataRecord record) throws IOException {// TODO: implement me
		if (record == null) {
			throw new NullPointerException("record");
		}

		if (sheetNameKeyRecord != null) { // perform L3 partitioning according to sheet attribute;
			prepareSheet(record);
		} else if (sheet == null) {
			prepareSheet();
		}

		return 0;
	}

	@Override
	public void reset() {// TODO: implement me
	}

	@Override
	public void close() throws IOException {// TODO: implement me
	}

	@Override
	public void flush() throws IOException {// TODO: implement me
	}

	@Override
	public int writeHeader() throws IOException {
		return 0;
	}

	@Override
	public int writeFooter() throws IOException {
		return 0;
	}

	@Override
	public void finish() throws IOException {
		flush();
	}

	private Workbook createWorkbook(URL contextURL, String file) {
		try {
			InputStream inputStream = FileUtils.getInputStream(contextURL, file);
			if (inputStream.available() > 0) {
				Workbook toReturn = WorkbookFactory.create(inputStream);
				inputStream.close();
				return toReturn;
			} else {
				return newWorkbook();
			}
		} catch (Throwable t) {
			return newWorkbook();
		}
	}

	private Workbook newWorkbook() {
		switch (formatterType) {
		case XLS:
			if (attitude == SpreadsheetAttitude.IN_MEMORY) {
				workbook = new XSSFWorkbook();
			} else {
				throw new IllegalArgumentException("Stream write for XLS files is not supported!");
			}
			return new HSSFWorkbook();
		case XLSX:
			if (attitude == SpreadsheetAttitude.IN_MEMORY) {
				return new XSSFWorkbook();
			} else {
				return new SXSSFWorkbook(mappingInfo.getStats().getRowCount());
			}
		}

		throw new IllegalArgumentException("Unsupported format");
	}

	// Copy of inner help class so that there is no dependency on old XLSWriter
	/**
	 * A structure used to save states when multiple sheet writing is enabled.
	 * 
	 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
	 * 
	 * @version 30th January 2009
	 * @since 30th January 2009
	 */
	private static final class SheetData {

		/** the sheet affected */
		private Sheet sheet;
		/** the current row within the sheet */
		private int currentRow;

		public SheetData(Sheet sheet, int currentRow) {
			this.sheet = sheet;
			this.currentRow = currentRow;
		}
	}

	private class CellMapping {
		final int row;
		final int column;
		final int cloverField;
		final CellStyle cellStyle;

		public CellMapping(int row, int column, int cloverField, CellStyle style) {
			this.row = row;
			this.column = column;
			this.cloverField = cloverField;
			this.cellStyle = style;
		}
	}
}
