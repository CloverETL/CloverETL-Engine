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
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.RecordKey;
import org.jetel.data.parser.XLSMapping;
import org.jetel.data.parser.XLSMapping.HeaderGroup;
import org.jetel.data.parser.XLSMapping.HeaderRange;
import org.jetel.data.parser.XLSMapping.Stats;
import org.jetel.data.primitive.Decimal;
import org.jetel.data.primitive.Numeric;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.metadata.DataFieldMetadata;
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

//	private class IntPair {
//	final int x1;
//	final int x2;
//	public IntPair(int x1, int x2) {
//		this.x1 = x1;
//		this.x2 = x2;
//	}
//
//}

	private class CellPosition {
		final int row;
		final int col;

		public CellPosition(int row, int col) {
			this.row = row;
			this.col = col;
		}

	}
	
	private class Range {
		final int rowStart;
		final int colStart;
		final int rowEnd;
		final int colEnd;
		public Range(int rowStart, int colStart, int rowEnd, int colEnd) {
			this.rowStart = rowStart;
			this.colStart = colStart;
			this.rowEnd = rowEnd;
			this.colEnd = colEnd;
		}
	}

	private Workbook templateWorkbook; // TODO: implement me
	private String sheet;
	private XLSMapping mappingInfo;
	private List<CellMapping> headerMapping = new ArrayList<CellMapping>();
	private Map<Integer, Integer> xToCloverFieldMapping = new HashMap<Integer, Integer>();
	private Map<Integer, Range> cloverFieldToXMapping = new HashMap<Integer, Range>();

	private boolean append;
	private boolean insert; // TODO: implement me
	private boolean removeSheets;

	/** the currently open workbook */
	private Workbook workbook;
	/** the sheet that is being used */
	private Sheet currentSheet;
	/** the output stream used to output the workbook */
	private Object outputDataTarget;
	private InputStream workbookInputStream;

	private DataRecordMetadata metadata;

	private int sheetIndex = -1;
	private String sheetName;

	private int headerRowCount = 0;
	private int headerColumnCount = 0;
	private int headerRowIndent = 0;
	private int headerColumnIndent = 0;
	private int recordRowCount = 0;
	private int recordColumnCount = 0;
	private int recordRowIndent = 0;
	private int recordColumnIndent = 0;
	
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

	private Integer getCloverFieldByX1(HeaderRange range) {
		if (mappingInfo.getOrientation() == XLSMapping.HEADER_ON_TOP) {
			return xToCloverFieldMapping.get(range.getColumnStart());
		} else {
			return xToCloverFieldMapping.get(range.getRowEnd());
		}
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

	private int maximum(int value1, int value2) {
		return ((value1>value2) ? value1 : value2);
	}

	private int minimum(int value1, int value2) {
		return ((value1<value2) ? value1 : value2);
	}
	
	private int getY2fromRange(HeaderRange headerRange) {
		if (mappingInfo.getOrientation()==XLSMapping.HEADER_ON_TOP) {
			return headerRange.getRowEnd();
		} else {
			return headerRange.getColumnEnd();
		}
	}
	
	private int getX1fromRange(HeaderRange headerRange) {
		if (mappingInfo.getOrientation()==XLSMapping.HEADER_ON_TOP) {
			return headerRange.getColumnStart();
		} else {
			return headerRange.getRowStart();
		}
	}
	
	private int getY1fromRange(HeaderRange headerRange) {
		if (mappingInfo.getOrientation()==XLSMapping.HEADER_ON_TOP) {
			return headerRange.getRowStart();
		} else {
			return headerRange.getColumnStart();
		}
	}
	
	private int getX2fromRange(HeaderRange headerRange) {
		if (mappingInfo.getOrientation()==XLSMapping.HEADER_ON_TOP) {
			return headerRange.getColumnEnd();
		} else {
			return headerRange.getRowEnd();
		}
	}
	
	private int getMaxY(int y, HeaderRange headerRange, int skip) {
		return maximum(y, getY2fromRange(headerRange) + skip);
	}
	
	private int getMaxX(int x, HeaderRange headerRange) {
		return maximum(x, getX2fromRange(headerRange));
	}
	
	private int getMinY(int y, HeaderRange headerRange) {
		return minimum(y, getY1fromRange(headerRange));
	}
	
	private int getMinX(int x, HeaderRange headerRange) {
		return minimum(x, getX1fromRange(headerRange));
	}
	
	private int translateXYtoRowNumber(int x, int y) {
		if (mappingInfo.getOrientation()==XLSMapping.HEADER_ON_TOP) {
			return y;
		} else {
			return x;
		}
	}
	
	private int translateXYtoColumnNumber(int x, int y) {
		if (mappingInfo.getOrientation()==XLSMapping.HEADER_ON_TOP) {
			return x;
		} else {
			return y;
		}
	}
	
	private DataFieldMetadata takeNextFieldInOrder(LinkedHashSet<DataFieldMetadata> cloverFields) {
		if (cloverFields.size()!=0) {
			DataFieldMetadata dataFieldMetaData = cloverFields.iterator().next();
			cloverFields.remove(dataFieldMetaData);
			return dataFieldMetaData;
		} else {
			return null;
		}

	}
	
	private void computeHeaderAndRecordBounds() {
		int maxX = 0;
		int maxY = 0;
		int maxYIncluingSkip = 0;
		int minX = Integer.MAX_VALUE;
		int minY = Integer.MAX_VALUE;
		
		for (HeaderGroup group : mappingInfo.getHeaderGroups()) {
			for (HeaderRange range : group.getRanges()) {
				maxX = getMaxX(maxX, range);
				maxYIncluingSkip = getMaxY(maxY, range, group.getSkip());
				maxY = getMaxY(maxY, range, 0);
				minX = getMinX(minX, range);
				minY = getMinY(minY, range);
			}
		}
		
		if (minX==Integer.MAX_VALUE || minY==Integer.MAX_VALUE) {
			recordRowCount = 0;
			recordColumnCount = 0;
			recordRowIndent = 0;
			recordColumnIndent = 0;
			headerRowCount = 0;
			headerColumnCount = 0;
			headerRowIndent = 0;
			headerColumnIndent = 0;
		} else {
			int headerRowMin = translateXYtoRowNumber(minX, minY);
			int headerColumnMin = translateXYtoColumnNumber(minX, minY);
			int headerRowMaxWithoutSkip = translateXYtoRowNumber(maxX, maxY);
			int headerColumnMaxWithoutSkip = translateXYtoColumnNumber(maxX, maxY);
			
			recordRowCount = headerRowMaxWithoutSkip - headerRowMin + 1;
			recordColumnCount = headerColumnMaxWithoutSkip - headerColumnMin + 1;
			if (mappingInfo.getOrientation() == XLSMapping.HEADER_ON_TOP) {
				recordRowIndent = 0;
				recordColumnIndent = headerColumnMin;
				
			} else {
				recordRowIndent = headerRowMin;
				recordColumnIndent = 0;
			}
			
			headerRowCount = translateXYtoRowNumber(maxX, maxYIncluingSkip) - headerRowMin + 1;
			headerColumnCount = translateXYtoColumnNumber(maxX, maxYIncluingSkip) - headerColumnMin + 1;
			headerRowIndent = headerRowMin;
			headerColumnIndent = headerColumnMin;
		}

	}
	
	private void initMapping() { // TODO: implement me
		Stats stats = mappingInfo.getStats();
		Map<String, Integer> nameMap = metadata.getFieldNamesMap();
		headerMapping.clear();
		xToCloverFieldMapping.clear();
		cloverFieldToXMapping.clear();

		List<Integer> unusedFields = new ArrayList<Integer>();
		for (int i = 0; i < metadata.getNumFields(); i++) {
			unusedFields.add(i);
		}
		
		computeHeaderAndRecordBounds();
		
		for (HeaderGroup group : mappingInfo.getHeaderGroups()) {
			for (HeaderRange range : group.getRanges()) {
				LinkedHashSet<DataFieldMetadata> cloverFields = new LinkedHashSet<DataFieldMetadata>();
				cloverFields.addAll(cloverFields);
				
				int cloverField = XLSMapping.UNDEFINED;
				if (group.getCloverField() != XLSMapping.UNDEFINED) {
					cloverField = group.getCloverField();
				} else {
					switch (group.getMappingMode()) {
					case AUTO:
						if (stats.useAutoNameMapping()) {
							// name mapping
							//TODO we have to read it from a sheet first
						} else {
							// order mapping
							DataFieldMetadata dataFieldMetadata = takeNextFieldInOrder(cloverFields);
							if (dataFieldMetadata!=null) {
								cloverField = dataFieldMetadata.getNumber();
							}
						}
						break;
					case NAME:
						//TODO we have to read it from a sheet first
						break;
					case ORDER:
						// order mapping
						DataFieldMetadata dataFieldMetadata = takeNextFieldInOrder(cloverFields);
						if (dataFieldMetadata!=null) {
							cloverField = dataFieldMetadata.getNumber();
						}
						break;
					}
				}
				
				if (cloverField != XLSMapping.UNDEFINED) {
					for (int x = getX1fromRange(range); x<=getX2fromRange(range); ++x) {
						xToCloverFieldMapping.put(x, cloverField);
					}
					cloverFieldToXMapping.put(cloverField, new Range(range.getRowStart()-headerRowIndent, range.getColumnStart()-headerColumnIndent, range.getRowEnd()-headerRowIndent, range.getColumnEnd()-headerColumnIndent));
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

		if (workbookInputStream!=null) {
			close();
		}
		
		this.outputDataTarget = outputDataTarget;
		
		if (outputDataTarget instanceof Object[]) {
			URL url = (URL) ((Object[]) outputDataTarget)[0];
			String file = (String) ((Object[]) outputDataTarget)[1];

			if (templateWorkbook == null) {
				createWorkbook(url, file);
			} else {
				newWorkbook();
			}
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
		prepareSelectedOrDefaultSheet();
	}

	private void prepareSelectedOrDefaultSheet() {
		if (sheetName != null) {
			currentSheet = workbook.getSheet(sheetName);
		} else if (sheetIndex >= 0) {
			if (sheetIndex >= workbook.getNumberOfSheets()) {
				throw new IndexOutOfBoundsException("sheetNumber >= " + workbook.getNumberOfSheets());
			}

			currentSheet = workbook.getSheetAt(sheetIndex);
		} 
		
		if (currentSheet==null){
			if (sheetName!=null) {
				currentSheet = workbook.createSheet(sheetName);
			} else {
				currentSheet = workbook.createSheet();
				sheetName = currentSheet.getSheetName();
			}
		}
		
		if (!append) {
			for (int i=0; i<=currentSheet.getLastRowNum();++i) {
				Row row = currentSheet.getRow(i);
				if (row!=null) {
					currentSheet.removeRow(row);
				}
			}
			
			writeSheetHeader();
		}

	}
	
	private void chooseSelectedOrDefaultSheet() {
		if (sheetName != null) {
			currentSheet = workbook.getSheet(sheetName);
		} else if (sheetIndex >= 0) {
			if (sheetIndex >= workbook.getNumberOfSheets()) {
				throw new IndexOutOfBoundsException("sheetNumber >= " + workbook.getNumberOfSheets());
			}

			currentSheet = workbook.getSheetAt(sheetIndex);
		} else {
			throw new JetelRuntimeException("A sheet selected for writting not found.");
		}
	}

	private void createRegion(int rowCount, int columnCount) {
		for (int i=0; i<rowCount; ++i) {
			Row newRow = currentSheet.createRow(i);
			for (int j=0; j<columnCount; ++j) {
				newRow.createCell(j);
			}
		}
	}
	
	private Cell getCellByRowAndColumn(int rowIndex, int columnIndex) {
		Row row = currentSheet.getRow(rowIndex);
		if (row==null) {
			return null;
		}
		Cell cell = row.getCell(columnIndex);
		return cell;
	}
	
	private void setStringToCellGivenByRowAndColumn(int rowIndex, int columnIndex, String stringValue) {
		Cell cell = getCellByRowAndColumn(rowIndex, columnIndex);
		cell.setCellValue(stringValue);
	}
	
	private short findOrCreateBoldStyle(int rowIndex, int columnIndex) {
		Cell cell = getCellByRowAndColumn(rowIndex, columnIndex);
		CellStyle origStyle = cell.getCellStyle();
		short fontIndex = origStyle.getFontIndex();
		Font font = workbook.getFontAt(fontIndex);
		Font correspondingBoldFont = workbook.findFont(Short.MAX_VALUE, font.getColor(), font.getFontHeight(), font.getFontName(), font.getItalic(), font.getStrikeout(), font.getTypeOffset(), font.getUnderline());
		if (correspondingBoldFont==null) {
			correspondingBoldFont = workbook.createFont();
			correspondingBoldFont.setBoldweight(Short.MAX_VALUE);
			correspondingBoldFont.setColor(font.getColor());
			correspondingBoldFont.setFontHeight(font.getFontHeight());
			correspondingBoldFont.setFontName(font.getFontName());
			correspondingBoldFont.setItalic(font.getItalic());
			correspondingBoldFont.setStrikeout(font.getStrikeout());
			correspondingBoldFont.setTypeOffset(font.getTypeOffset());
			correspondingBoldFont.setUnderline(font.getUnderline());
		}
		
		CellStyle correspondingCellStyle=null;
		for (short i=0; i<workbook.getNumCellStyles() && correspondingCellStyle==null; ++i) {
			CellStyle cellStyle = workbook.getCellStyleAt(i);
			if (cellStyle.getFontIndex()==correspondingBoldFont.getIndex()) {
				correspondingCellStyle=cellStyle;
			}
		}
		
		if (correspondingCellStyle==null) {
			correspondingCellStyle = workbook.createCellStyle();
			correspondingCellStyle.cloneStyleFrom(origStyle);
			correspondingCellStyle.setFont(correspondingBoldFont);
		}
		
		return correspondingCellStyle.getIndex();
	}
	
	private void setStyleToCellGivenByRowAndColumn(int rowIndex, int columnIndex, short styleNumber) {
		Cell cell = getCellByRowAndColumn(rowIndex, columnIndex);
		CellStyle cellStyle = workbook.getCellStyleAt(styleNumber);
		cell.setCellStyle(cellStyle);
	}
	
	private void writeSheetHeader() {// TODO: implement me

//		if (mappingInfo.getOrientation() == XLSMapping.HEADER_ON_TOP) {
//			currentSheet.createRow(arg0)
//		} else {
//			
//		}
		
		if (!append) {
			createRegion(headerRowIndent + headerRowCount, headerColumnIndent + headerColumnCount);
			boolean boldStyleFound = false;
			short boldStyle = 0;
			
			for (HeaderGroup headerGroup : mappingInfo.getHeaderGroups()) {
				for (HeaderRange range : headerGroup.getRanges()) {
					if (range.getRowStart()!=range.getRowEnd() || range.getColumnStart()!=range.getColumnEnd()) {
						currentSheet.addMergedRegion(new CellRangeAddress(range.getRowStart(), range.getRowEnd(), range.getColumnStart(), range.getColumnEnd()));
					}
					String dataLabel;
					Integer cloverField = getCloverFieldByX1(range);
					if (cloverField!=null) {
						dataLabel = metadata.getField(cloverField).getLabel();
						if (dataLabel==null) {
							dataLabel = metadata.getField(cloverField).getName();
						}
						setStringToCellGivenByRowAndColumn(range.getRowStart(), range.getColumnStart(), dataLabel);
						if (!boldStyleFound) {
							boldStyle = findOrCreateBoldStyle(range.getRowStart(), range.getColumnStart());
							boldStyleFound = true;
						}
						setStyleToCellGivenByRowAndColumn(range.getRowStart(), range.getColumnStart(), boldStyle);
					}
				}
			}
		}
		
		
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

	Range getRangeForCloverField(int cloverFieldIndex) {
		return cloverFieldToXMapping.get(cloverFieldIndex);
	}
	
	private CellPosition createNextRecordRegion(int rowIndent, int colIndent, int rowCount, int columnCount) {
		if (mappingInfo.getOrientation() == XLSMapping.HEADER_ON_TOP) {
			int rowOffset = currentSheet.getLastRowNum() + rowIndent + 1;
			int colOffset = colIndent; 
			int rowsToCreateInSum = rowIndent + rowCount;
			Row [] newRows = new Row[rowsToCreateInSum];
			for (int i=0; i<rowsToCreateInSum; ++i) {
				newRows[i] = currentSheet.createRow(currentSheet.getLastRowNum()+1);
				int columnsToCreateInSum = colIndent + columnCount;
				for (int j=0; j<columnsToCreateInSum; ++j) {
					newRows[i].createCell(j);
				}
			}
			return new CellPosition(rowOffset, colOffset);
		} else {
			throw new UnsupportedOperationException("Not yet implemented"); //TODO
		}
	}
	
	
	public int write(DataRecord record) throws IOException {// TODO: implement me
		if (record == null) {
			throw new NullPointerException("record");
		}

		chooseSelectedOrDefaultSheet();
		
		CellPosition cellOffset = createNextRecordRegion(recordRowIndent, recordColumnIndent, recordRowCount, recordColumnCount);
		for (int i=0; i<record.getNumFields(); ++i) {
			DataField dataField = record.getField(i);
			Range range = getRangeForCloverField(dataField.getMetadata().getNumber());
			if (range!=null) {
				//normally either cellOffset.row or cellOffset.col should be zero depending on orientation
				int cellRowIndex = cellOffset.row + range.rowStart;
				int cellColumnIndex = cellOffset.col + range.colStart;
				Cell cell = getCellByRowAndColumn(cellRowIndex, cellColumnIndex);
				if (cell==null) {
					throw new IllegalStateException("Unexpectedly not found a cell for a new record at coordinates: [row " + cellRowIndex + ", col:" + cellColumnIndex + "]");
				}
				setCellValue(cell, dataField);
			}
			
		}

		
		return 0;
	}


	private void setCellValue(Cell cell, DataField dataField) {
		
		try {
			switch (dataField.getType()) {
			case DataFieldMetadata.DATE_FIELD:
			case DataFieldMetadata.DATETIME_FIELD:
				cell.setCellValue(DateUtil.getExcelDate((Date)dataField.getValue()));
				break;
			case DataFieldMetadata.BYTE_FIELD:
			case DataFieldMetadata.STRING_FIELD:
				cell.setCellValue(dataField.toString());
				break;
			case DataFieldMetadata.DECIMAL_FIELD:
				cell.setCellValue(((Decimal)dataField.getValue()).getDouble());
				break;
			case DataFieldMetadata.INTEGER_FIELD:
				cell.setCellValue((Integer)dataField.getValue());
				break;
			case DataFieldMetadata.LONG_FIELD:
				cell.setCellValue((Long)dataField.getValue());
				break;
			case DataFieldMetadata.NUMERIC_FIELD:
				cell.setCellValue(((Numeric)dataField.getValue()).getDouble());
				break;
			case DataFieldMetadata.BOOLEAN_FIELD:
				cell.setCellValue(((Boolean) (dataField.getValue())).booleanValue());
				break;
			default:
				throw new IllegalStateException("Unknown data field type " + DataFieldMetadata.type2Str(dataField.getType()));
					
			}
		} catch (ClassCastException ex) {
			throw new BadDataFormatException("Problems while converting data record to cell data", ex);
		}
	}

	
	@Override
	public void reset() {// TODO: implement me
	}

	private URL getContextUrlFromOutputDataTarget(Object outputDataTarget) {
		return (URL) ((Object[]) outputDataTarget)[0];
	}
	
	private String getFilePathFromOutputDataTarget(Object outputDataTarget) {
		return (String) ((Object[]) outputDataTarget)[1];
	}
	
	@Override
	public void close() throws IOException {// TODO: implement me
		flush();
		workbookInputStream=null;
	}

	@Override
	public void flush() throws IOException {// TODO: implement me
		
		if (outputDataTarget!=null) {
			if (outputDataTarget instanceof Object[]) {
				URL url = getContextUrlFromOutputDataTarget(outputDataTarget);
				String file = getFilePathFromOutputDataTarget(outputDataTarget);
				OutputStream workbookOutputStream = FileUtils.getOutputStream(url, file, false, -1);
				workbook.write(workbookOutputStream);
				workbookOutputStream.close();
			} else if (outputDataTarget instanceof WritableByteChannel) {
				OutputStream workbookOutputStream = Channels.newOutputStream((WritableByteChannel) outputDataTarget);
				workbook.write(workbookOutputStream);
				workbookOutputStream.close();
			}

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

	private void createWorkbook(URL contextURL, String file) {
		try {
			workbookInputStream = FileUtils.getInputStream(contextURL, file);
			if (workbookInputStream.available() > 0) {
				workbook = WorkbookFactory.create(workbookInputStream);
			} else {
				newWorkbook();
			}
		} catch (IOException ioex) {
			throw new JetelRuntimeException("Problem while reading Excel file " + file);
		} catch (InvalidFormatException e) {
			throw new JetelRuntimeException("Invalid format of Excel file " + file);
		}
	}

	private void newWorkbook() {
		switch (formatterType) {
		case XLS:
			if (attitude == SpreadsheetAttitude.IN_MEMORY) {
				workbook = new HSSFWorkbook();
			} else {
				throw new IllegalArgumentException("Stream write for XLS files is not yet supported!");
			}
		case XLSX:
			if (attitude == SpreadsheetAttitude.IN_MEMORY) {
				workbook = new XSSFWorkbook();
			} else {
				workbook = new SXSSFWorkbook(mappingInfo.getStats().getRowCount());
			}
		}

		throw new IllegalArgumentException("Unsupported format");
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
