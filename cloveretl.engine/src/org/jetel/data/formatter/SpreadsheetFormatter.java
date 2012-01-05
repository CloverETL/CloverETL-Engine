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

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.RecordKey;
import org.jetel.data.formatter.spreadsheet.CellOperations;
import org.jetel.data.formatter.spreadsheet.CellPosition;
import org.jetel.data.formatter.spreadsheet.CellStyleLibrary;
import org.jetel.data.formatter.spreadsheet.CoordsTransformations;
import org.jetel.data.formatter.spreadsheet.SheetData;
import org.jetel.data.formatter.spreadsheet.SheetDataLibrary;
import org.jetel.data.formatter.spreadsheet.XLSMappingStats;
import org.jetel.data.formatter.spreadsheet.XYRange;
import org.jetel.data.parser.XLSMapping;
import org.jetel.data.parser.XLSMapping.HeaderGroup;
import org.jetel.data.parser.XLSMapping.HeaderRange;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.SpreadsheetUtils.SpreadsheetAttitude;
import org.jetel.util.SpreadsheetUtils.SpreadsheetFormat;
import org.jetel.util.file.FileUtils;
import org.jetel.util.string.StringUtils;

/**
 * @author psimecek & lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 6 Sep 2011
 */
public class SpreadsheetFormatter implements Formatter {

    public static final String XLSX_FILE_PATTERN = "^.*\\.[Xx][Ll][Ss][Xx]$";
    public static final String XLTX_FILE_PATTERN = "^.*\\.[Xx][Ll][Tt][Xx]$";

	/** the value specifying that no data format is used/set */
	public static final String GENERAL_FORMAT_STRING = "General";
	private static final String CLOVER_FIELD_PREFIX = "$";
	private static final int DEFAULT_STREAM_WINDOW_SIZE = 10;

	private SpreadsheetAttitude attitude;
	private SpreadsheetFormat formatterType;

	
	private SheetDataLibrary sheetDataLibrary = new SheetDataLibrary();
	private Workbook templateWorkbook;
	private String sheet;
	private RecordKey sheetNameKeyRecord;
	private XLSMapping mappingInfo;
	private CoordsTransformations transformations;
	private XLSMappingStats mappingStats = new XLSMappingStats();
	private boolean mappingStatsInitialized = false;

	private boolean append;
	private boolean insert;
	private boolean createFile;
	private boolean removeSheets;

	/** the currently open workbook */
	private Workbook workbook;
	/** the sheet that is being used */
	private SheetData currentSheetData;
	/** the output stream used to output the workbook */
	private Object outputDataTarget;
	private boolean workbookNotFlushed = true;
	private InputStream workbookInputStream;
	private CellStyleLibrary cellStyleLibrary = new CellStyleLibrary();
	
	private DataRecordMetadata metadata;

	private int sheetIndex = -1;
	private String sheetName;

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

	public void setMapping(XLSMapping mappingInfo) {
		this.mappingInfo = mappingInfo;
		if (mappingInfo!=null) {
			transformations = new CoordsTransformations(mappingInfo.getOrientation());
		}
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

	private Integer getCloverFieldByHeaderX1andY1(HeaderRange range) {
		int x;
		int y;
		if (mappingInfo.getOrientation() == XLSMapping.HEADER_ON_TOP) {
			x = range.getColumnStart();
			y = range.getRowStart();
		} else {
			x = range.getRowStart();
			y = range.getColumnStart();
		}
		
		return mappingStats.getCloverFieldForHeaderPosition(x, y);
	}
	
	@Override
	public void init(DataRecordMetadata metadata) {
		mappingStatsInitialized = false;
		this.metadata = metadata;
		sheetDataLibrary.clear();
		
		if (!StringUtils.isEmpty(sheet)) {
			if (sheet.startsWith(Defaults.CLOVER_FIELD_INDICATOR)) {
				String[] fields = sheet.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
				for (int i = 0; i < fields.length; i++) {// Remove clover field indicator from each field
					fields[i] = fields[i].substring(1);
				}
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
		
		cellStyleLibrary.clear();
	}
	
	/**
	 * @return the createFile
	 */
	public boolean isCreateFile() {
		return createFile;
	}

	/**
	 * @param createFile the createFile to set
	 */
	public void setCreateFile(boolean createFile) {
		this.createFile = createFile;
	}

	
	

	/**
	 * Sets from which fields from input metadata will be created sheet name for different records
	 * 
	 * @param fieldNames
	 */
	public void setKeyFields(String[] fieldNames){
		sheetNameKeyRecord = new RecordKey(fieldNames, metadata);
		sheetNameKeyRecord.init();
	}

	
	@Override
	public void setDataTarget(Object outputDataTarget) throws IOException {
		if (outputDataTarget == null) {
			throw new NullPointerException("dataTarget");
		}

		if (workbookInputStream!=null) {
			close();
		}
		
		this.outputDataTarget = outputDataTarget;
		workbookNotFlushed = true;
		
		if (outputDataTarget instanceof Object[]) {
			URL url = (URL) ((Object[]) outputDataTarget)[0];
			String file = (String) ((Object[]) outputDataTarget)[1];
			OutputStream dataTargetOutputStream = (OutputStream) ((Object[]) outputDataTarget)[2];
			dataTargetOutputStream.close();
			
			createWorkbook(url, file);
		} else if (outputDataTarget instanceof WritableByteChannel) {
			workbook = new XSSFWorkbook();
		} else {
			throw new IllegalArgumentException(outputDataTarget.getClass() + " not supported as a data target");
		}

		if (removeSheets) { // remove all sheets in a workbook
			// they must be removed from the last sheet to the first sheet, because Workbook
			// re-indexes sheets with a higher number than the removed one
			for (int i = workbook.getNumberOfSheets() - 1; i >= 0; --i) {
				workbook.removeSheetAt(i);
			}
		}
		
		cellStyleLibrary.init(workbook);
		
		//
		// set up the formatter for writing multiple sheets
		//
		if (!StringUtils.isEmpty(sheet) && sheet.startsWith(CLOVER_FIELD_PREFIX)) {
        	String[] fields = sheet.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);

			for (int i = 0; i < fields.length; i++) {
				fields[i] = fields[i].substring(1);
			}

			setKeyFields(fields);
			sheetDataLibrary.clear();
        }

		
		if (sheetNameKeyRecord==null) {
			prepareSelectedOrDefaultSheet(sheetName);
		}
	}

	/**
	 * Under some circumstances there may not be enough rows to write a record 
	 */
	private void createInitialEmptyLines() {
 		if (mappingInfo.getOrientation() != XLSMapping.HEADER_ON_TOP) {
 			createEmptyRowsForHeaderOnLeft();
 		}
		int linesCreatedEachStep = mappingInfo.getStep();
		if (currentSheetData.getLastLineNumber(mappingInfo.getOrientation()) + linesCreatedEachStep + mappingStats.getMinRecordFieldYOffset() < 0) {
			currentSheetData.appendEmptyLines(-mappingStats.getMinRecordFieldYOffset());
		}
	}

	private void createEmptyRowsForHeaderOnLeft() {
		int firstRowToCreateNumber = currentSheetData.getLastRowNumber()+1;
		XYRange firstRecordXYRange = mappingStats.getFirstRecordXYRange();
		int lastRowToCreateNumber = transformations.translateXYtoRowNumber(firstRecordXYRange.x2, firstRecordXYRange.y2);
		for (int i=firstRowToCreateNumber; i<lastRowToCreateNumber; ++i) {
			currentSheetData.createRow(i);
		}
	}

	private void takeSheetOrPrepareSheet(DataRecord dataRecord) {
		String selectedSheetName = null;
		if (sheetNameKeyRecord!=null) {
			if (dataRecord == null) {
				throw new NullPointerException("dataRecord");
			}
			
			selectedSheetName = sheetNameKeyRecord.getKeyString(dataRecord);
		}

		if (selectedSheetName==null) {
			selectedSheetName = sheetName;
		}

		if (sheetDataLibrary.getSheetNames().contains(selectedSheetName)) {
			currentSheetData = sheetDataLibrary.getSheetData(selectedSheetName);
		} else {
			prepareSelectedOrDefaultSheet(selectedSheetName);
		}
	}

	
	private void prepareSelectedOrDefaultSheet(String selectedSheetName) {
		if (selectedSheetName==null) {
			selectedSheetName = sheetName;
		}
		Sheet newSheet = null;
		if (selectedSheetName != null) {
			newSheet = workbook.getSheet(selectedSheetName);
		} else if (sheetIndex >= 0) {
			while (workbook.getNumberOfSheets() <= sheetIndex) {
				workbook.createSheet(); //create enough sheets to make an index valid
			}
			newSheet = workbook.getSheetAt(sheetIndex);
			if (newSheet!=null) {
				sheetName = newSheet.getSheetName();
			}
		} 
		
		if (newSheet==null){
			if (selectedSheetName!=null) {
				newSheet = workbook.createSheet(selectedSheetName);
			} else {
				newSheet = workbook.createSheet();
				sheetName = newSheet.getSheetName();
			}
		}
		
		if (mappingInfo==null) {
			setMapping(new XLSMapping(metadata));
		}
		
		currentSheetData = new SheetData(newSheet, mappingInfo, mappingStats, 0, 0);
		sheetDataLibrary.addSheetData(newSheet.getSheetName(), currentSheetData);
		
		if (!mappingStatsInitialized) {
			cellStyleLibrary.init(workbook);
			
			mappingStats.init(mappingInfo, metadata, currentSheetData, insert, templateWorkbook!=null);
			
			mappingStatsInitialized = true;
		}
		
		
		if (!append && !insert) {
			if (mappingInfo.isWriteHeader()) {
				writeSheetHeader();
			} else {
				createHeaderRegion();
			}
			currentSheetData.setCurrentY(mappingStats.getFirstRecordXYRange().y2-mappingInfo.getStep());
		} else {
			createInitialEmptyLines();
			// check that appending does not overwrite anything
			if (append) {
				appendEmptyLinesToAvoidDataRewritting();
				currentSheetData.setCurrentY(currentSheetData.getLastLineNumber(mappingInfo.getOrientation()));
			} else {
				currentSheetData.setCurrentY(mappingStats.getInitialInsertionY());
				currentSheetData.setTemplateCopiedRegionY2(mappingStats.getInitialTemplateCopiedRegionY2());
				// check that insertion is not stopped by a missing rows at the end of file
				int currentLastLineNumber = currentSheetData.getLastLineNumber(mappingInfo.getOrientation());
				if (insert && currentSheetData.getCurrentY() > currentLastLineNumber) {
					currentSheetData.appendEmptyLines(currentSheetData.getCurrentY() - currentLastLineNumber);
				}
			}
		}

		
	}

	/**
	 * 
	 */
	private void appendEmptyLinesToAvoidDataRewritting() {
		int linesToAppend = 0; //a number of empty rows needed to append in order to avoid rewritting existing data
		XYRange firstRecordXYRange = mappingStats.getFirstRecordXYRange();
		for (Integer fieldIndex : mappingStats.getRegisteredCloverFields()) {
				Integer yOffset = mappingStats.getYOffsetForCloverField(fieldIndex);
				int x = firstRecordXYRange.x1 + mappingStats.getXOffsetForCloverField(fieldIndex);
				if (yOffset < 0) {
					for (int yDelta=-1; yDelta >= yOffset; --yDelta) {
						int currentLastLineNumber = currentSheetData.getLastLineNumber(mappingInfo.getOrientation());
						Cell cell = currentSheetData.getCellByXY(x, currentLastLineNumber + mappingInfo.getStep() + yDelta);
						if (cell!=null && !CellOperations.cellIsEmpty(cell)) {
							linesToAppend = transformations.maximum(linesToAppend, Math.abs(yOffset - yDelta)+1);
						}
					}
				}
		}
		currentSheetData.appendEmptyLines(linesToAppend);
	}

	private void createRegion(int firstRow, int firstColumn, int lastRow, int lastColumn) {
		for (int i=firstRow; i<=lastRow; ++i) {
			Row row = currentSheetData.getRow(i);
			if (row==null) {
				row = currentSheetData.createRow(i);
			}
			for (int j=firstColumn; j<=lastColumn; ++j) {
				Cell cell = row.getCell(j);
				if (cell==null) {
					cell = currentSheetData.createCellAndRefreshLastColumnNumber(row, j);
				}
			}
		}
	}

	private void writeSheetHeader() {

		if (!append && !insert) {
			createHeaderRegion();
			boolean boldStyleFound = false;
			short boldStyle = 0;
			
			for (HeaderGroup headerGroup : mappingInfo.getHeaderGroups()) {
				for (HeaderRange range : headerGroup.getRanges()) {
					if (range.getRowStart()!=range.getRowEnd() || range.getColumnStart()!=range.getColumnEnd()) {
						currentSheetData.addMergedRegion(new CellRangeAddress(range.getRowStart(), range.getRowEnd(), range.getColumnStart(), range.getColumnEnd()));
					}
					String dataLabel;
					Integer cloverField = getCloverFieldByHeaderX1andY1(range);
					if (cloverField!=null) {
						dataLabel = metadata.getField(cloverField).getLabel();
						if (dataLabel==null) {
							dataLabel = metadata.getField(cloverField).getName();
						}
						CellOperations.setStringToCellGivenByRowAndColumn(currentSheetData, range.getRowStart(), range.getColumnStart(), dataLabel);
						if (!boldStyleFound) {
							boldStyle = cellStyleLibrary.findOrCreateBoldStyle(workbook, currentSheetData, range.getRowStart(), range.getColumnStart());
							boldStyleFound = true;
						}
						CellOperations.setStyleToCellGivenByRowAndColumn(currentSheetData, range.getRowStart(), range.getColumnStart(), workbook.getCellStyleAt(boldStyle));
					}
				}
			}
		}
	}

	/**
	 * 
	 */
	private void createHeaderRegion() {
		XYRange headerXYRange = mappingStats.getHeaderXYRange();
		XYRange firstRecordXYRange = mappingStats.getFirstRecordXYRange();
		int headerY2PlusSkip = transformations.maximum(headerXYRange.y2, firstRecordXYRange.y2 - mappingInfo.getStep());
		int rows = transformations.translateXYtoRowNumber(headerXYRange.x2, headerY2PlusSkip);
		int columns = transformations.translateXYtoColumnNumber(headerXYRange.x2, headerY2PlusSkip);
		createRegion(0, 0, rows, columns);
	}
	
	private CellPosition createNextRecordRegion() {
 			int linesToCreate = mappingInfo.getStep();
			int recordOffsetY = currentSheetData.getCurrentY() + linesToCreate;
			int recordOffsetX = mappingStats.getFirstRecordXYRange().x1;
			if (insert) {
				currentSheetData.insertEmptyOrTemplateLines(currentSheetData.getCurrentY()+1, linesToCreate);
			} else {
				currentSheetData.insertEmptyOrPreserveLines(currentSheetData.getCurrentY()+1, linesToCreate);
			}
			currentSheetData.setCurrentY(currentSheetData.getCurrentY() + linesToCreate);
			
			return new CellPosition(recordOffsetX, recordOffsetY);
	}
	
	
	@Override
	public int write(DataRecord record) throws IOException {
		if (record == null) {
			throw new NullPointerException("record");
		}
		
		takeSheetOrPrepareSheet(record);
		
		CellPosition recordOffset = createNextRecordRegion();
		for (int i=0; i<record.getNumFields(); ++i) {
			DataField dataField = record.getField(i);
			Integer cellInRecordXOffset = mappingStats.getXOffsetForCloverField(dataField.getMetadata().getNumber());
			Integer cellInRecordYOffset = mappingStats.getYOffsetForCloverField(dataField.getMetadata().getNumber());
			if (cellInRecordXOffset!=null && cellInRecordYOffset!=null) {
				//normally either cellOffset.x or cellOffset.y should be zero depending on orientation
				int cellX = cellInRecordXOffset + recordOffset.x;
				int cellY = cellInRecordYOffset + recordOffset.y;
				Cell cell = currentSheetData.getCellByXY(cellX, cellY);
				if (cell==null) { //it may happen that existing rows with no data are read from XLS(X) file so that they contain no cells
					int lastLineNumber = currentSheetData.getLastLineNumber(mappingInfo.getOrientation());
					if (cellX<=mappingStats.getFirstRecordXYRange().x2 && cellY<=lastLineNumber) {
						cell = currentSheetData.createCellXY(cellX, cellY);
					} else {
						throw new IllegalStateException("Unexpectedly not found a cell for a new record at coordinates: [X: " + cellX + ", Y:" + cellY + "]");
					}
				}
				CellOperations.setCellValue(cell, dataField);
				SheetData templateSheet = null;
				if (mappingStats.getTemplateSheetName()!=null) {
					sheetDataLibrary.getSheetData(mappingStats.getTemplateSheetName());
				}
				CellStyle cellStyle = cellStyleLibrary.takeCellStyleOrPrepareCellStyle(workbook, mappingStats, currentSheetData, templateSheet, dataField.getMetadata(), cell, insert);
				if (cellStyle!=null) {
					cell.setCellStyle(cellStyle);
				}
			}
			
		}

		
		return 0;
	}


	
	
	@Override
	public void reset() {
		this.init(metadata);
	}

	private URL getContextUrlFromOutputDataTarget(Object outputDataTarget) {
		return (URL) ((Object[]) outputDataTarget)[0];
	}
	
	private String getFilePathFromOutputDataTarget(Object outputDataTarget) {
		return (String) ((Object[]) outputDataTarget)[1];
	}
	
	@Override
	public void close() throws IOException {
		flush();
		workbookInputStream=null;
	}

	@Override
	public void flush() throws IOException {
		
		if (outputDataTarget!=null && workbook!=null) {
			if (outputDataTarget instanceof Object[]) {
				if (workbookNotFlushed) {
					for (SheetData sheetData : sheetDataLibrary.getAllSheetData()) {
						sheetData.autosizeColumns();
						removeTemplateLinesIfNeeded(sheetData);
					}

					URL url = (URL) ((Object[]) outputDataTarget)[0];
					String file = (String) ((Object[]) outputDataTarget)[1];
					OutputStream outputStream = FileUtils.getOutputStream(url, file, false, -1);
					workbook.write(outputStream);
					outputStream.close();
					workbookNotFlushed=false;
				}
			} else if (outputDataTarget instanceof WritableByteChannel) {
				OutputStream workbookOutputStream = Channels.newOutputStream((WritableByteChannel) outputDataTarget);
				workbook.write(workbookOutputStream);
				workbookOutputStream.close();
			}

		}
	}


	/**
	 * @param sheetData
	 */
	private void removeTemplateLinesIfNeeded(SheetData sheetData) {
		if (insert & templateWorkbook!=null) {
			sheetData.removeTemplateLines();
		}
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
		close();
	}

	private void configureWorkbook() {
		workbook.setForceFormulaRecalculation(true);
		//in order to preserve default style
		if (workbook.getNumberOfFonts()==0) {
			workbook.createFont();
		}
		if (workbook.getNumCellStyles()==0) {
			workbook.createCellStyle();
		}
	}
	
	private void createWorkbook(URL contextURL, String file) {
		try {
			if (templateWorkbook!=null) {
				createSpreadsheetFileFromTemplate();
			}
			workbookInputStream = FileUtils.getInputStream(contextURL, file);
//			if (workbookInputStream.available() > 0) { //did not work reliably for ZIP files => workaround below
			int firstByte = workbookInputStream.read();
			workbookInputStream.close();
			workbookInputStream = FileUtils.getInputStream(contextURL, file); //re-open stream in order to return to the beginning
			if (firstByte>=0) {
				if (!createFile) {
					workbook = newWorkbook(workbookInputStream, formatterType, attitude, mappingInfo);
				} else {//ignore an existing file, rewrite it while flushing
					workbook = newWorkbook(null, formatterType, attitude, mappingInfo); 
				}
			} else {
				workbook = newWorkbook(null, formatterType, attitude, mappingInfo);
			}
			configureWorkbook();
		} catch (IOException ioex) {
			throw new JetelRuntimeException("Problem while reading Excel file " + file);
		}
	}

	/**
	 * @param contextURL
	 * @param file
	 * @throws IOException
	 */
	private void createSpreadsheetFileFromTemplate() throws IOException {
		if (outputDataTarget instanceof Object[]) {
			Object [] outputDataTargetArray = (Object[]) outputDataTarget;
			URL url = (URL) outputDataTargetArray[0];
			String file = (String) outputDataTargetArray[1];
			
			OutputStream workbookOutputStream = FileUtils.getOutputStream(url, file, false, -1);
			templateWorkbook.write(workbookOutputStream);
			workbookOutputStream.close();
		} else if (outputDataTarget instanceof WritableByteChannel) {
			OutputStream workbookOutputStream = Channels.newOutputStream((WritableByteChannel) outputDataTarget);
			workbook.write(workbookOutputStream);
			workbookOutputStream.close();
		}
	}

	private static HSSFWorkbook createXlsWorkbook(InputStream inputStream) throws IOException {
		if (inputStream == null) {
			return new HSSFWorkbook();
		} else {
			return new HSSFWorkbook(inputStream);
		}
	}
	
	private static XSSFWorkbook createXlsxWorkbook(InputStream inputStream) throws IOException {
		if (inputStream == null) {
			return new XSSFWorkbook();
		} else {
			return new XSSFWorkbook(inputStream);
		}
	}
	
	public static Workbook newWorkbook(InputStream inputStream, SpreadsheetFormat formatterType, SpreadsheetAttitude attitude, XLSMapping mappingInfo) throws IOException {
		switch (formatterType) {
		case XLS:
			if (attitude == SpreadsheetAttitude.IN_MEMORY) {
				return createXlsWorkbook(inputStream);
			} else {
				throw new IllegalArgumentException("Stream write for XLS files is not yet supported!");
			}
		case XLSX:
			if (attitude == SpreadsheetAttitude.IN_MEMORY) {
				return createXlsxWorkbook(inputStream);
			} else {
				if (inputStream==null) {
					int windowSize;
					if (mappingInfo!=null) {
						windowSize = mappingInfo.getStep() + mappingInfo.getStats().getRowCount() + 1;
					} else {
						windowSize = DEFAULT_STREAM_WINDOW_SIZE;
					}
					return new SXSSFWorkbook(windowSize);
				} else {
					throw new IllegalArgumentException("Streaming is not supported on existing files");
				}
			}
		default:
			throw new IllegalArgumentException("Unsupported format");
		}
			
	}

	
}
