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
import org.jetel.data.formatter.spreadsheet.FieldNamesForSheetPartitioningParser;
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
import org.jetel.util.string.CloverString;
import org.jetel.util.string.StringUtils;

/**
 * A class that handles writing of records (arguments of its method write()) into a given output stream in XLS(X)
 * format (the output stream can be given using the setDataTarget() method). 
 * 
 * @author psimecek & lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 6 Sep 2011
 */
public class SpreadsheetFormatter implements Formatter {

    /** A pattern matching XLSX file extension */
	public static final String XLSX_FILE_PATTERN = "^.*\\.[Xx][Ll][Ss][Xx]$";
    /** A pattern matching XLS file extension */
	public static final String XLS_FILE_PATTERN = "^.*\\.[Xx][Ll][Ss]$";
	/** A pattern matching XLTX file extension */
    public static final String XLTX_FILE_PATTERN = "^.*\\.[Xx][Ll][Tt][Xx]$";
	/** A pattern matching XLT file extension */
    public static final String XLT_FILE_PATTERN = "^.*\\.[Xx][Ll][Tt]$";

	/** A value specifying that no data format is used/set */
	public static final String GENERAL_FORMAT_STRING = "General";
	/** Default number of rows of an in-memory window, while streaming writign is used */ 
	private static final int DEFAULT_STREAM_WINDOW_SIZE = 10;

	/** sets whether the formatter operates data in-memory or with a streaming approach */
	private SpreadsheetAttitude attitude;
	/** Sets which format should be used (XLS, XLSX or automatic detection using file extensions */
	private SpreadsheetFormat formatterType;

	/** A library of SheetData affiliated to sheets that have been created or used during the current formatter run. */ 
	private SheetDataLibrary sheetDataLibrary = new SheetDataLibrary();
	/** A workbook containing template file to be used as a template for writing records */
	private Workbook templateWorkbook;
	/** A sheet name set in a SpreadsheetWriter component */
	private String sheet;
	/** A key for sheet partitioning */
	private RecordKey sheetNameKeyRecord;
	/** A model of mapping of metadata to table columns */ 
	private XLSMapping mappingInfo;
	/** A helper static class with transformation over table coordinates with respect to spreadsheet orientation */
	private CoordsTransformations transformations;
	/** A statistic computed from mappingInfo used for writing a record according to a specified mapping */
	private XLSMappingStats mappingStats = new XLSMappingStats();
	/** A flag preventing repeated computation of mappingStats */
	private boolean mappingStatsInitialized = false;

	/** A flag saying whether formatter works in an append mode (data are appended to the end of table) */
	private boolean append;
	/** A flag saying whether formatter works in an insertion mode (data are inserted just below the header) */
	private boolean insert;
	/** A flag saying whether a new file must be created (and an old file must be deleted) before writing begins */ 
	private boolean createFile;
	/** A flag saying whether all sheets should be removed before writing begins */
	private boolean removeSheets;
	/** A flag saying whether all rows should be removed from all used sheets before writing begins */
	private boolean removeRows;

	/** A workbook for a currently open spreadsheet */
	private Workbook workbook;
	/** SheetData for a sheet that is currently being used */
	private SheetData currentSheetData;
	/** A representation of an output stream used for flushing of the workbook in the end*/
	private Object outputDataTarget;
	/** A boolean flag preventing repeated flushing of the workbook */
	private boolean workbookNotFlushed = true;
	/** An input stream used for reading of a workbook into the memory */
	private InputStream workbookInputStream;
	/** A library of cell styles used for different metadata fields. When the insertion mode is on, the library is filled according to the first record under the header */
	private CellStyleLibrary cellStyleLibrary = new CellStyleLibrary();

	/** Record metadata of input data */
	private DataRecordMetadata metadata;

	/** Index of sheet set by user */
	private int sheetIndex = -1;
	/** Sheet name derived either from a sheet name given by user or from sheetIndex or by creation of a new sheet */ 
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

	public void setRemoveRows(boolean removeRows) {
		this.removeRows = removeRows;
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
	
	/**
	 * Initialization method.
	 * 
	 * Resets the formatter so that it is prepared for writing another file.
	 */
	@Override
	public void init(DataRecordMetadata metadata) {
		mappingStatsInitialized = false;
		this.metadata = metadata;
		sheetDataLibrary.clear();
		deriveSheetName();
		cellStyleLibrary.clear();
	}

	/**
	 * Derives sheet name with respect of setting in a SpreadsheetWriter component 
	 */
	private void deriveSheetName() {
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
				} else { //allows to read "[1]" as a sheet named "1"
					if (sheet.charAt(0) == XLSMapping.ESCAPE_START) {
						sheetName = sheet.substring(1, sheet.length() - 1);
					} else {
						sheetName = sheet;
					}
				}
			}
		}
	}
	
	/**
	 * @return whether a new file should be created or not
	 */
	public boolean isCreateFile() {
		return createFile;
	}

	/**
	 * @param createFile sets whether a new file should be created or not
	 */
	public void setCreateFile(boolean createFile) {
		this.createFile = createFile;
	}

	
	

	/**
	 * Sets from which fields from input metadata will be created sheet name for different records (enables so called sheet partitioning)
	 * 
	 * @param fieldNames
	 */
	public void setKeyFields(String[] fieldNames){
		sheetNameKeyRecord = new RecordKey(fieldNames, metadata);
		sheetNameKeyRecord.init();
	}

	/**
	 * Sets an output stream for writting data. Initializes a workbook representing a written spreadsheet
	 * and prepares the default sheet to write to.
	 * 
	 * It also resets some structures that may not be reset without having spreadsheet output stream specified.
	 */
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
		
		String [] fields = FieldNamesForSheetPartitioningParser.parseFieldNames(sheet);
		if (fields!=null) {
			setKeyFields(fields);
		}
		
		sheetDataLibrary.clear();

		//prepare the default sheet and initialize mapping statistic data
		if (sheetNameKeyRecord==null) {
			prepareSelectedOrDefaultSheet(sheetName);
		}
	}

	/**
	 * Under some circumstances there may not be enough rows to write a record (e.g. empty sheet, no header and the the record shape spread over several lines).
	 * In such a case, some initial empty lines must be created.  
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

	/**
	 * When table has a header on the left side (horizontal writing), this method is intended to create rows so that the header fits into them. 
	 */
	private void createEmptyRowsForHeaderOnLeft() {
		int firstRowToCreateNumber = currentSheetData.getLastRowNumber()+1;
		XYRange firstRecordXYRange = mappingStats.getFirstRecordXYRange();
		int lastRowToCreateNumber = transformations.translateXYtoRowNumber(firstRecordXYRange.x2, firstRecordXYRange.y2);
		for (int i=firstRowToCreateNumber; i<lastRowToCreateNumber; ++i) {
			currentSheetData.createRow(i);
		}
	}

	/**
	 * Either takes a prepared sheet from a SheetData library or prepares new SheetData (over an existing or a brand new sheet) and returns them
	 * @param dataRecord
	 * 		a data record used for sheet partitioning (a current sheet name is derived from it)
	 */
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

	/**
	 * Prepares new SheetData for a sheet (no matter if existing or not) of a given name.
	 * If a sheet does not exist, a new sheet of a given name is created.
	 * If the method is called for the first time, statistic data about mapping are initialized (with respect to a selected sheet).
	 * If the formatter does not work in an insertion or append mode, a header is written to the selected sheet at the end.
	 * In all modes, initial empty space can be created in order to allow writting of curved records. 
	 * 
	 * @param selectedSheetName
	 * 		A sheet name used for sheet selection. Can be null when a new sheet should be created or sheetIndex should be used instead.
	 */
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
		
		if (removeRows) {
			removeAllRowsAndMergedRegions(currentSheetData);
		}
		
		if (mappingStats.isMappingEmpty()) {
			return;
		}
		
		if (!append && !insert) {
			if (mappingInfo.isWriteHeader()) {
				writeSheetHeader();
			} else {
				createHeaderRegion();
			}
			currentSheetData.setCurrentY(mappingStats.getFirstRecordXYRange().y2-mappingInfo.getStep());
		} else {
			if (currentSheetData.getLastRowNumber()==-1 && mappingInfo.isWriteHeader()) {
				//even appending and insertion may write new header, when a target sheet is empty
				writeSheetHeader();
			} else {
				createInitialEmptyLines();
			}
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
	private void removeAllRowsAndMergedRegions(SheetData sheetData) {
		for (int i=0; i<sheetData.getMergedRegionsCount(); ++i) {
			sheetData.removeMergedRegion(i);
		}
		for (int i=0; i<=sheetData.getLastRowNumber(); ++i) {
			Row row = sheetData.getRow(i);
			if (row!=null) {
				sheetData.removeRow(row);
			}
		}
	}

	/**
	 * When appending a curved record we must be sure that we do not overwrite any existing data in a sheet.
	 * For that purpose we append empty lines and move y-coordinate or writing lower.
	 * In most of cases (flat records), this method will not do anything, this is really only for exotic cases. 
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

	/**
	 * Creates a region full of empty cells in a rectangle given by bounds on row and column numbers  
	 * 
	 * @param firstRow
	 * @param firstColumn
	 * @param lastRow
	 * @param lastColumn
	 */
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

	/**
	 * This method writes header to the sheet contained in currentSheetData.
	 */
	private void writeSheetHeader() {

		createHeaderRegion();
		boolean boldStyleFound = false;
		short boldStyle = 0;

		for (HeaderGroup headerGroup : mappingInfo.getHeaderGroups()) {
			for (HeaderRange range : headerGroup.getRanges()) {
				if (range.getRowStart() != range.getRowEnd() || range.getColumnStart() != range.getColumnEnd()) {
					currentSheetData.addMergedRegion(new CellRangeAddress(range.getRowStart(), range.getRowEnd(), range.getColumnStart(), range.getColumnEnd()));
				}
				String dataLabel;
				Integer cloverField = getCloverFieldByHeaderX1andY1(range);
				if (cloverField != null) {
					dataLabel = metadata.getField(cloverField).getLabel();
					if (dataLabel == null) {
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

	/**
	 * Creates a new region of empty cells for writing a header 
	 */
	private void createHeaderRegion() {
		XYRange headerXYRange = mappingStats.getHeaderXYRange();
		XYRange firstRecordXYRange = mappingStats.getFirstRecordXYRange();
		int headerY2PlusSkip = transformations.maximum(headerXYRange.y2, firstRecordXYRange.y2 - mappingInfo.getStep());
		int rows = transformations.translateXYtoRowNumber(headerXYRange.x2, headerY2PlusSkip);
		int columns = transformations.translateXYtoColumnNumber(headerXYRange.x2, headerY2PlusSkip);
		createRegion(0, 0, rows, columns);
	}
	
	/**
	 * Creates a new region of empty cells for writing a next record.
	 * 
	 * In case of normal or append modes, new only empty lines are added. This is a relatively trivial operation.
	 * 
	 * Insertion mode is very non-trivial in this case. 
	 * It is necessary to create empty lines so that they match a record shape. In a typical case of flat records,
	 * this would be trivial too. Nevertheless, since curved records are allowed too, swapping of cells in order to move
	 * newly created empty cells becomes non-trivial. Additionally, it is requested that cell styles are copied from a
	 * template line (i.e. from a line with the first record under the header of a default sheet when a workbook
	 * is being opened.    
	 * 
	 * @return a reference point for writing a record. In combination with relative coordinates of fields stored
	 * 		in mappingStats this value can be used for assigning 
	 */
	private CellPosition createNextRecordRegion() {
		int linesToCreate = mappingInfo.getStep();
		int recordOffsetY = currentSheetData.getCurrentY() + linesToCreate;
		int recordOffsetX = mappingStats.getFirstRecordXYRange().x1;
		if (insert) {
			currentSheetData.insertEmptyOrTemplateLines(currentSheetData.getCurrentY() + 1, linesToCreate);
		} else {
			currentSheetData.insertEmptyOrPreserveLines(currentSheetData.getCurrentY() + 1, linesToCreate);
		}
		currentSheetData.setCurrentY(currentSheetData.getCurrentY() + linesToCreate);

		return new CellPosition(recordOffsetX, recordOffsetY);
	}	
	
	/**
	 * Writes data record into a sheet.
	 * 
	 * Formatter mode may affect a place where records are written to.
	 * 
	 * Normally, records are written just below a header (any header is not written then just below a place where a virtual header would be placed)
	 * and overwrite existing data.
	 * 
	 * In an append mode, the end of table is detected when preparing sheet and records are than written under the end of the table
	 * 
	 * In an insertion mode, records are written just below a header (just like in a normal mode), but additionally, all previously existing data are
	 * shifted lower to prevent them from re-writing.
	 */
	@Override
	public int write(DataRecord record) throws IOException {
		if (record == null) {
			throw new NullPointerException("record");
		}
		
		takeSheetOrPrepareSheet(record);
		
		if (mappingStats.isMappingEmpty()) {
			//no write actions for empty mapping
			return 0;
		}
		
		CellPosition recordOffset = createNextRecordRegion();
		
		//after creation of new empty space it takes all data field and writes them into
		//position computed from a recordOffsert and mappingStats (mappingStats contain relative positions of cells for data fields in a record)
		for (int i=0; i<record.getNumFields(); ++i) {
			DataField dataField = record.getField(i);
			//cellInRecordXOffset is a positive value, in simple cases it will typically obtain values from a sequence 0, 1, 2, 3, 4, ...
			Integer cellInRecordXOffset = mappingStats.getXOffsetForCloverField(dataField.getMetadata().getNumber());
			//cellInRecordYOffset is never positive value and it is non-zero (i.e. negative value) only for multi-line or curved records
			//for one-line flat records with vertical orientation (header on top of a table) cellInRecordYOffset is always zero
			Integer cellInRecordYOffset = mappingStats.getYOffsetForCloverField(dataField.getMetadata().getNumber());
			//if there is no mapping on a field, no position is stored for it in mappingStats
			if (cellInRecordXOffset!=null && cellInRecordYOffset!=null) {
				int cellX = recordOffset.x + cellInRecordXOffset;
				int cellY = recordOffset.y + cellInRecordYOffset;
				Cell cell = currentSheetData.getCellByXY(cellX, cellY);
				if (cell==null) { //it may happen that existing rows with null cells are read from XLS(X) file - in such a case a cell must be created
					int lastLineNumber = currentSheetData.getLastLineNumber(mappingInfo.getOrientation());
					//we check, whether a null cell is in a region with existing or newly written data
					if (cellX<=mappingStats.getFirstRecordXYRange().x2 && cellY<=lastLineNumber) {
						cell = currentSheetData.createCellXY(cellX, cellY);
					} else {
						//if not, the position where the formatter tries to write is considered illegal (this allowed to
						//find a lot of bugs in a development time - really good to check).
						throw new IllegalStateException("Unexpectedly not found a cell for a new record at coordinates: [X: " + cellX + ", Y:" + cellY + "]");
					}
				}
				//finally, set the value
				CellOperations.setCellValue(cell, dataField);
				//Afterwards, set a cell style for a given field
				setProperStyleToCell(cell, dataField, record);
			}
			
		}

		
		return 0;
	}

	/**
	 * Searches, if a cell style has already been created for a specified dataField. If yes, it sets this
	 * style to a given cell. If not, it searches whether the needed style is already present in a workbook
	 * and creates a new one if no such style is found.
	 * 
	 * If a template line is specified, it takes template styles into account when creating new styles.
	 * 
	 * Performance note:
	 *   This method is called only from a write() method.
	 *   When writing the first record, takeCellStyleOrPrepareCellStyle will prepare all styles.
	 *   When writing later records, no style creation is performed in takeCellStyleOrPrepareCellStyle - it only returns previously created styles.
	 *   There is one exception to this rule: when a field format arrives in an special data field of a record, then formatStringFromRecord
	 *   will contain information about number/data format which has the following consequences:
	 *      (1) Computation costs raise, because cellStyleLibrary tries to search for a corresponding style with the same format.
	 *	    (2) If no such a style is found, it is created.
	 *
	 * @param cell
	 * @param dataField
	 * @param record
	 */
	private void setProperStyleToCell(Cell cell, DataField dataField, DataRecord record) {
		SheetData templateSheet = null;
		if (mappingStats.getTemplateSheetName()!=null) {
			templateSheet = sheetDataLibrary.getSheetData(mappingStats.getTemplateSheetName());
		}
		Integer formatFieldIndex = mappingStats.getFormatFieldIndexForDataField(dataField.getMetadata().getNumber());
		String formatStringFromRecord = null;
		if (formatFieldIndex!=null) {
			Object formatFieldValue = record.getField(formatFieldIndex).getValue();
			if (formatFieldValue!=null && formatFieldValue instanceof CloverString && !"".equals(((CloverString)formatFieldValue).toString())) {
				formatStringFromRecord = ((CloverString) formatFieldValue).toString();
			}
		}
		CellStyle cellStyle = cellStyleLibrary.takeCellStyleOrPrepareCellStyle(cell, formatStringFromRecord, dataField.getMetadata(), workbook, mappingStats, currentSheetData, templateSheet, insert);
		if (cellStyle!=null) {
			cell.setCellStyle(cellStyle);
		}
	}


	
	
	@Override
	public void reset() {
		this.init(metadata);
	}

	@Override
	public void close() throws IOException {
		flush();
		workbookInputStream=null;
	}

	@Override
	public void flush() throws IOException {
		//flush performs writing into a stream. Before that _no_data_ have been written yet. They are all written at once 
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
	 * If a formatter runs in an insertion mode and a template file  
	 * 
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

	/**
	 * Sets formula recalculation and creates one new font and one new style
	 * if no fonts and styles exist (in order to have them as a default font
	 * and style)  
	 */
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
	
	/**
	 * Creates a new workbook.
	 * If template file is specified, its content is copied to a given path as a new file.
	 * If the file exists, it is read in using an input stream.
	 * If the file does not exist, it created in the flush() method at the end of writing.
	 * 
	 * @param contextURL
	 * @param file
	 */
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
	
	/**
	 * Creates a new workbook of type given by a formatterType. When streaming is enabled, the input stream is null and
	 * formatter type is XLSX, it creates a streamed workbook with a proper window size computed from mappingInfo. 
	 * 
	 * @param inputStream
	 * @param formatterType
	 * @param attitude
	 * @param mappingInfo
	 * @return
	 * @throws IOException
	 */
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
