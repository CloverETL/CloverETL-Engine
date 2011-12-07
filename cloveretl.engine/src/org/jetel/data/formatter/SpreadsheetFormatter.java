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

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Comment;
import org.apache.poi.ss.usermodel.DataFormat;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.Hyperlink;
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
import org.jetel.data.parser.XLSMapping;
import org.jetel.data.parser.XLSMapping.HeaderGroup;
import org.jetel.data.parser.XLSMapping.HeaderRange;
import org.jetel.data.parser.XLSMapping.SpreadsheetOrientation;
import org.jetel.data.parser.XLSMapping.Stats;
import org.jetel.data.primitive.Decimal;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.SpreadsheetUtils.SpreadsheetAttitude;
import org.jetel.util.SpreadsheetUtils.SpreadsheetFormat;
import org.jetel.util.file.FileUtils;
import org.jetel.util.string.StringUtils;

import com.googlecode.sardine.model.Getlastmodified;

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


	private class CellPosition {
		final int x;
		final int y;

		public CellPosition(int x, int y) {
			this.x = x;
			this.y = y;
		}

	}
	
	private class XYRange {
		final int x1;
		final int y1;
		final int x2;
		final int y2;
		
		public XYRange(int x1, int y1, int x2, int y2) {
			this.x1 = x1;
			this.y1 = y1;
			this.x2 = x2;
			this.y2 = y2;
		}
	}
	
	private Workbook templateWorkbook;
	private String sheet;
	private Map<String, SheetData> sheetNameToSheetDataMap = new HashMap<String, SheetData>();
	private RecordKey sheetNameKeyRecord;
	private XLSMapping mappingInfo;
	private boolean mappingInitialized = false;
	private Map<Integer, Integer> xToCloverFieldMapping = new HashMap<Integer, Integer>();
	private Map<Integer, Integer> cloverFieldToXOffsetMapping = new HashMap<Integer, Integer>();
	private Map<Integer, Integer> cloverFieldToYOffsetMapping = new HashMap<Integer, Integer>();
	private Map<Integer, CellStyle> cloverFieldToCellStyle = new HashMap<Integer, CellStyle>();
	private int minRecordFieldYOffset = 0;

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
	private DataFormat dataFormat;
	
	private DataRecordMetadata metadata;

	private int sheetIndex = -1;
	private String sheetName;

	private int headerRowCount = 0;
	private int headerColumnCount = 0;
	private int headerRowIndent = 0;
	private int headerColumnIndent = 0;
	private XYRange headerXYRange;
	private int firstRecordBelowHeaderX = 0;
	private int firstRecordBelowHeaderY = 0;
	private String templateSheetName;
	private int initialFirstFooterLineIndex;
	private int initialTemplateCopiedRegionY1;
	
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
	public void init(DataRecordMetadata metadata) {
		mappingInitialized = false;
		this.metadata = metadata;
		this.sheetNameToSheetDataMap.clear();
		
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
	
	private DataFieldMetadata takeNextFieldInOrder(LinkedHashMap<String,DataFieldMetadata> cloverFields) {
		if (cloverFields.size()!=0) {
			String dataFieldMetaDataKey = cloverFields.keySet().iterator().next();
			 DataFieldMetadata dataFieldMetaData = cloverFields.remove(dataFieldMetaDataKey);
			return dataFieldMetaData;
		} else {
			return null;
		}

	}

	private String getCellStringValueByRowAndColumn(int row, int column) {
		Cell cell = getCellByRowAndColumn(row, column);
		if (cell==null) {
			return null;
		}
		String result = null;
		try {
			result = cell.getStringCellValue();
		} catch (Exception ex) {
			return null;
		}
		return result;

	}
	
	private DataFieldMetadata takeNextFieldByName(LinkedHashMap<String,DataFieldMetadata> cloverFields, HeaderRange range) {
		if (cloverFields.size()!=0) {
			String dataFieldMetaDataKey = getCellStringValueByRowAndColumn(range.getRowStart(), range.getColumnStart());
			if (dataFieldMetaDataKey!=null) {
				DataFieldMetadata dataFieldMetaData = cloverFields.remove(dataFieldMetaDataKey);
				return dataFieldMetaData;
			} else {
				return null;
			}
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
				maxYIncluingSkip = getMaxY(maxYIncluingSkip, range, group.getSkip());
				maxY = getMaxY(maxY, range, 0);
				minX = getMinX(minX, range);
				minY = getMinY(minY, range);
			}
		}
		
		if (minX==Integer.MAX_VALUE || minY==Integer.MAX_VALUE) {
			headerRowCount = 0;
			headerColumnCount = 0;
			headerRowIndent = 0;
			headerColumnIndent = 0;
			firstRecordBelowHeaderX = 0;
			firstRecordBelowHeaderY = 0;
		} else {
			firstRecordBelowHeaderX = minX;
			firstRecordBelowHeaderY = maxYIncluingSkip;
			if (!mappingInfo.isWriteHeader()) {
				minY=0;
				maxYIncluingSkip=0;
			}
			int headerRowMin = translateXYtoRowNumber(minX, minY);
			int headerColumnMin = translateXYtoColumnNumber(minX, minY);
			
			headerRowCount = translateXYtoRowNumber(maxX, maxYIncluingSkip) - headerRowMin + 1;
			headerRowIndent = headerRowMin;
			headerColumnCount = translateXYtoColumnNumber(maxX, maxYIncluingSkip) - headerColumnMin + 1;
			headerColumnIndent = headerColumnMin;
			headerXYRange = new XYRange(minX, minY, maxX, maxYIncluingSkip);
		}

		if (insert) {
			initialFirstFooterLineIndex = headerXYRange.y2+1;
			initialTemplateCopiedRegionY1 = headerXYRange.y2+1;
		}
	}
	
	private void initMapping() {
		if (mappingInfo==null) {
			mappingInfo = new XLSMapping(metadata);
		}
		Stats stats = mappingInfo.getStats();
		computeHeaderAndRecordBounds();
		
		xToCloverFieldMapping.clear();
		cloverFieldToXOffsetMapping.clear();
		cloverFieldToYOffsetMapping.clear();
		minRecordFieldYOffset = 0;

		List<DataFieldMetadata> usedDataFields = new ArrayList<DataFieldMetadata>();
		
		LinkedHashMap<String, DataFieldMetadata> cloverFields = new LinkedHashMap<String, DataFieldMetadata>();
		for (DataFieldMetadata dataField : metadata.getFields()) {
			cloverFields.put(dataField.getName(), dataField);
		}
		
		for (HeaderGroup group : mappingInfo.getHeaderGroups()) {
			for (HeaderRange range : group.getRanges()) {
				
				int cloverField = XLSMapping.UNDEFINED;
				if (group.getCloverField() != XLSMapping.UNDEFINED) {
					cloverField = group.getCloverField();
				} else {
					switch (group.getMappingMode()) {
					case AUTO:
						if (stats.useAutoNameMapping()) {
							// name mapping
							DataFieldMetadata dataFieldMetadataByName = takeNextFieldByName(cloverFields, range);
							if (dataFieldMetadataByName!=null) {
								cloverField = dataFieldMetadataByName.getNumber();
							}
						} else {
							// order mapping
							DataFieldMetadata dataFieldMetadata = takeNextFieldInOrder(cloverFields);
							if (dataFieldMetadata!=null) {
								cloverField = dataFieldMetadata.getNumber();
							}
						}
						break;
					case NAME:
						// order mapping
						DataFieldMetadata dataFieldMetadataByName = takeNextFieldByName(cloverFields, range);
						if (dataFieldMetadataByName!=null) {
							cloverField = dataFieldMetadataByName.getNumber();
						}
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
					usedDataFields.add(metadata.getField(cloverField));
					for (int x = getX1fromRange(range); x<=getX2fromRange(range); ++x) {
						xToCloverFieldMapping.put(x, cloverField);
					}
					
					int recordFieldXOffset = getX1fromRange(range) - firstRecordBelowHeaderX; //x coordinate minus x-indentation
					cloverFieldToXOffsetMapping.put(cloverField, recordFieldXOffset);
					int headerBottomPlusSkip = getY2fromRange(range) + group.getSkip();
					int recordFieldYOffset = headerBottomPlusSkip - firstRecordBelowHeaderY; //y coordinate of cell under header of this column minus y bound to the entire header
					cloverFieldToYOffsetMapping.put(cloverField, recordFieldYOffset);
					minRecordFieldYOffset = minimum(minRecordFieldYOffset, recordFieldYOffset);
				}
			}
		}
		
		cloverFieldToCellStyle.clear();
		mappingInitialized = true;
		templateSheetName = currentSheetData.sheet.getSheetName();
	}
	
	private class CellStyleFilter {
		private short alignment;
		private short verticalAlignment;
		private short borderBottom;
		private short borderLeft;
		private short borderRight;
		private short borderTop;
		private short bottomBorderColor;
		private short leftBorderColor;
		private short rightBorderColor;
		private short topBorderColor;
		private short dataFormat;
		private short fillBackgroundColor;
		private short fillForegroundColor;
		private short fillPattern;
		private short fontIndex;
		private boolean hidden;
		private short indention;
		private boolean locked;
		private short rotation;
		private boolean wrapText;
		
		CellStyleFilter(CellStyle cellStyle) {
			setAlignment(cellStyle.getAlignment());
			setVerticalAlignment(cellStyle.getVerticalAlignment());
			setBorderBottom(cellStyle.getBorderBottom());
			setBorderLeft(cellStyle.getBorderLeft());
			setBorderRight(cellStyle.getBorderRight());
			setBorderTop(cellStyle.getBorderTop());
			setBottomBorderColor(cellStyle.getBottomBorderColor());
			setLeftBorderColor(cellStyle.getLeftBorderColor());
			setRightBorderColor(cellStyle.getRightBorderColor());
			setTopBorderColor(cellStyle.getRightBorderColor());
			setDataFormat(cellStyle.getDataFormat());
			setFillBackgroundColor(cellStyle.getFillBackgroundColor());
			setFillForegroundColor(cellStyle.getFillForegroundColor());
			setFillPattern(cellStyle.getFillPattern());
			setFontIndex(cellStyle.getFontIndex());
			setHidden(cellStyle.getHidden());
			setIndention(cellStyle.getIndention());
			setLocked(cellStyle.getLocked());
			setRotation(cellStyle.getRotation());
			setWrapText(cellStyle.getWrapText());
		}

		boolean matches(CellStyle cellStyle) {
			return (getAlignment() == cellStyle.getAlignment() &&
					getVerticalAlignment() == cellStyle.getVerticalAlignment() &&
					getBorderBottom() == cellStyle.getBorderBottom() &&
					
			getBorderLeft() == cellStyle.getBorderLeft() &&
			getBorderRight() == cellStyle.getBorderRight() &&
			getBorderTop() == cellStyle.getBorderTop() &&
			getBottomBorderColor() == cellStyle.getBottomBorderColor() &&
			getLeftBorderColor() == cellStyle.getLeftBorderColor() &&
			getRightBorderColor() == cellStyle.getRightBorderColor() &&
			getTopBorderColor() == cellStyle.getRightBorderColor() &&
			getDataFormat() == cellStyle.getDataFormat() &&
			getFillBackgroundColor() == cellStyle.getFillBackgroundColor() &&
			getFillForegroundColor() == cellStyle.getFillForegroundColor() &&
			getFillPattern() == cellStyle.getFillPattern() &&
			getFontIndex() == cellStyle.getFontIndex() &&
			getHidden() == cellStyle.getHidden() &&
			getIndention() == cellStyle.getIndention() &&
			getLocked() == cellStyle.getLocked() &&
			getRotation() == cellStyle.getRotation() &&
			getWrapText() == cellStyle.getWrapText());
		}

		public short getAlignment() {
			return alignment;
		}

		public void setAlignment(short alignment) {
			this.alignment = alignment;
		}

		public short getVerticalAlignment() {
			return verticalAlignment;
		}

		public void setVerticalAlignment(short verticalAlignment) {
			this.verticalAlignment = verticalAlignment;
		}

		public short getBorderBottom() {
			return borderBottom;
		}

		public void setBorderBottom(short borderBottom) {
			this.borderBottom = borderBottom;
		}

		public short getBorderLeft() {
			return borderLeft;
		}

		public void setBorderLeft(short borderLeft) {
			this.borderLeft = borderLeft;
		}

		public short getBorderRight() {
			return borderRight;
		}

		public void setBorderRight(short borderRight) {
			this.borderRight = borderRight;
		}

		public short getBorderTop() {
			return borderTop;
		}

		public void setBorderTop(short borderTop) {
			this.borderTop = borderTop;
		}

		public short getBottomBorderColor() {
			return bottomBorderColor;
		}

		public void setBottomBorderColor(short bottomBorderColor) {
			this.bottomBorderColor = bottomBorderColor;
		}

		public short getLeftBorderColor() {
			return leftBorderColor;
		}

		public void setLeftBorderColor(short leftBorderColor) {
			this.leftBorderColor = leftBorderColor;
		}

		public short getRightBorderColor() {
			return rightBorderColor;
		}

		public void setRightBorderColor(short rightBorderColor) {
			this.rightBorderColor = rightBorderColor;
		}

		public short getTopBorderColor() {
			return topBorderColor;
		}

		public void setTopBorderColor(short topBorderColor) {
			this.topBorderColor = topBorderColor;
		}

		public short getDataFormat() {
			return dataFormat;
		}

		public void setDataFormat(short dataFormat) {
			this.dataFormat = dataFormat;
		}

		public short getFillBackgroundColor() {
			return fillBackgroundColor;
		}

		public void setFillBackgroundColor(short fillBackgroundColor) {
			this.fillBackgroundColor = fillBackgroundColor;
		}

		public short getFillPattern() {
			return fillPattern;
		}

		public void setFillPattern(short fillPattern) {
			this.fillPattern = fillPattern;
		}

		public short getFillForegroundColor() {
			return fillForegroundColor;
		}

		public void setFillForegroundColor(short fillForegroundColor) {
			this.fillForegroundColor = fillForegroundColor;
		}

		public short getFontIndex() {
			return fontIndex;
		}

		public void setFontIndex(short fontIndex) {
			this.fontIndex = fontIndex;
		}

		public boolean getHidden() {
			return hidden;
		}

		public void setHidden(boolean hidden) {
			this.hidden = hidden;
		}

		public short getIndention() {
			return indention;
		}

		public void setIndention(short indention) {
			this.indention = indention;
		}

		public boolean getLocked() {
			return locked;
		}

		public void setLocked(boolean locked) {
			this.locked = locked;
		}

		public short getRotation() {
			return rotation;
		}

		public void setRotation(short rotation) {
			this.rotation = rotation;
		}

		public boolean getWrapText() {
			return wrapText;
		}

		public void setWrapText(boolean wrapText) {
			this.wrapText = wrapText;
		}
		
		
	}
	
	CellStyle findCellStyle(CellStyleFilter cellStyleFilter) {
		for (short i=0; i<workbook.getNumCellStyles(); ++i) {
			CellStyle cellStyle = workbook.getCellStyleAt(i);
			if (cellStyleFilter.matches(cellStyle)) {
				return cellStyle;
			}
		}
		return null;
	}
	
	private CellStyle takeCellStyleOrPrepareCellStyle(DataFieldMetadata fieldMetadata, Cell cell) {
		CellStyle cellStyle = cloverFieldToCellStyle.get(fieldMetadata.getNumber());
		if (cellStyle==null) {
			cellStyle = prepareCellStyle(fieldMetadata, cell);
			cloverFieldToCellStyle.put(fieldMetadata.getNumber(), cellStyle);
		}
		return cellStyle;
	}
	
	private CellStyle prepareCellStyle(DataFieldMetadata fieldMetadata, Cell cell) {
		int x = headerXYRange.x1 + cloverFieldToXOffsetMapping.get(fieldMetadata.getNumber());
		int y = currentSheetData.getTemplateCopiedRegionY1() + cloverFieldToYOffsetMapping.get(fieldMetadata.getNumber());
		Sheet templateSheet = sheetNameToSheetDataMap.get(templateSheetName).sheet;
		Cell templateCell = getCellByXY(templateSheet, x, y);

		CellStyle templateCellStyle;
		if (templateCell != null) {
			templateCellStyle = templateCell.getCellStyle();
		} else {
			if (workbook.getNumCellStyles() == 0) {
				templateCellStyle = workbook.createCellStyle();
			} else {
				templateCellStyle = cell.getCellStyle();
			}
		}

		short modifiedDataFormat = dataFormat.getFormat((fieldMetadata.getFormatStr() != null) ? fieldMetadata.getFormatStr() : GENERAL_FORMAT_STRING);

		CellStyleFilter cellStyleFilter = new CellStyleFilter(templateCellStyle);
		cellStyleFilter.setDataFormat(modifiedDataFormat);
		CellStyle cellStyle = findCellStyle(cellStyleFilter);
		if (cellStyle == null) {
			cellStyle = workbook.createCellStyle();
			cellStyle.cloneStyleFrom(templateCellStyle);
			cellStyle.setDataFormat(modifiedDataFormat);
		}
		
		return cellStyle;
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
		dataFormat = workbook.createDataFormat();

		if (removeSheets) { // remove all sheets in a workbook
			// they must be removed from the last sheet to the first sheet, because Workbook
			// re-indexes sheets with a higher number than the removed one
			for (int i = workbook.getNumberOfSheets() - 1; i >= 0; --i) {
				workbook.removeSheetAt(i);
			}
		}
		
		//
		// set up the formatter for writing multiple sheets
		//
		if (!StringUtils.isEmpty(sheet) && sheet.startsWith(CLOVER_FIELD_PREFIX)) {
        	String[] fields = sheet.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);

			for (int i = 0; i < fields.length; i++) {
				fields[i] = fields[i].substring(1);
			}

			setKeyFields(fields);
			sheetNameToSheetDataMap.clear();
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
		if (currentSheetData.getLastLineNumber(mappingInfo.getOrientation()) + linesCreatedEachStep + minRecordFieldYOffset < 0) {
			appendEmptyLines(-minRecordFieldYOffset);
		}
	}

	private void createEmptyRowsForHeaderOnLeft() {
		int firstRowToCreateNumber = currentSheetData.sheet.getLastRowNum()+1;
		int lastRowToCreateNumber = headerXYRange.y2;
		for (int i=firstRowToCreateNumber; i<lastRowToCreateNumber; ++i) {
			currentSheetData.sheet.createRow(i);
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

		if (sheetNameToSheetDataMap.containsKey(selectedSheetName)) {
			currentSheetData = sheetNameToSheetDataMap.get(selectedSheetName);
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
			if (sheetIndex >= workbook.getNumberOfSheets()) {
				if (workbook.getNumberOfSheets()==0) {
					throw new JetelRuntimeException("Sheet number cannot be specified when there are not existing sheets");
				} else {
					throw new JetelRuntimeException("Sheet index " + sheetIndex + " is bigger than a number of existing sheets (" + workbook.getNumberOfSheets() + ")");
				}
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
		
		currentSheetData = new SheetData(newSheet, 0, 0, 0);
		sheetNameToSheetDataMap.put(newSheet.getSheetName(), currentSheetData);
		
		if (!mappingInitialized) {
			initMapping();
		}
		
		if (append) {
			currentSheetData.setCurrentY(currentSheetData.getLastLineNumber(mappingInfo.getOrientation()));
		} else {
			if (mappingInfo.isWriteHeader()) {
				currentSheetData.setCurrentY(headerXYRange.y2);
			} else {
				currentSheetData.setCurrentY(-1);
			}
		}
		currentSheetData.setFirstFooterLineIndex(initialFirstFooterLineIndex);
		currentSheetData.setTemplateCopiedRegionY1(initialTemplateCopiedRegionY1);
		
		if (!append && !insert) {
			if (mappingInfo.isWriteHeader()) {
				writeSheetHeader();
			} else {
				createInitialEmptyLines();
			}
		} else {
			createInitialEmptyLines();
			// check that appending does not overwrite anything
			if (append) {
				appendEmptyLinesToAvoidDataRewritting();
			}
			// check that insertion is not stopped by a missing rows at the end of file
			int currentLastLineNumber = currentSheetData.getLastLineNumber(mappingInfo.getOrientation());
			if (insert && currentSheetData.getFirstFooterLineIndex() > (currentLastLineNumber+1)) {
				appendEmptyLines(currentSheetData.getFirstFooterLineIndex() - (currentLastLineNumber+1));
			}
		}

		
	}

	/**
	 * 
	 */
	private void appendEmptyLinesToAvoidDataRewritting() {
		int linesToAppend = 0; //a number of empty rows needed to append in order to avoid rewritting existing data
		for (int x=headerXYRange.x1; x<=headerXYRange.x2; ++x) {
			Integer fieldIndex = xToCloverFieldMapping.get(x);
			if (fieldIndex!=null) {
				Integer yOffset = cloverFieldToYOffsetMapping.get(fieldIndex);
				if (yOffset < 0) {
					for (int yDelta=-1; yDelta >= yOffset; --yDelta) {
						int currentLastLineNumber = currentSheetData.getLastLineNumber(mappingInfo.getOrientation());
						Cell cell = getCellByXY(x, currentLastLineNumber+1 + yDelta);
						if (cell!=null && !cellIsEmpty(cell)) {
							linesToAppend = maximum(linesToAppend, Math.abs(yOffset - yDelta)+1);
						}
					}
				}
			}
		}
		appendEmptyLines(linesToAppend);
		currentSheetData.setCurrentY(currentSheetData.getCurrentY() + linesToAppend);
	}
	
//	private void chooseSelectedOrDefaultSheet() {
//		if (sheetName != null) {
//			currentSheetData.sheet = workbook.getSheet(sheetName);
//		} else if (sheetIndex >= 0) {
//			if (sheetIndex >= workbook.getNumberOfSheets()) {
//				throw new IndexOutOfBoundsException("sheetNumber >= " + workbook.getNumberOfSheets());
//			}
//
//			currentSheetData.sheet = workbook.getSheetAt(sheetIndex);
//		} else {
//			throw new JetelRuntimeException("A sheet selected for writting not found.");
//		}
//	}

	private void createRegion(int firstRow, int firstColumn, int lastRow, int lastColumn) {
		for (int i=firstRow; i<=lastRow; ++i) {
			Row row = currentSheetData.sheet.getRow(i);
			if (row==null) {
				row = currentSheetData.sheet.createRow(i);
			}
			for (int j=firstColumn; j<=lastColumn; ++j) {
				Cell cell = row.getCell(j);
				if (cell==null) {
					cell = currentSheetData.createCellAndRefreshLastColumnNumber(row, j);
				}
			}
		}
	}

	
	private Cell getCellByXY(int x, int y) {
		return getCellByXY(currentSheetData.sheet, x, y);
	}
	
	private Cell getCellByXY(Sheet sheet, int x, int y) {
		return getCellByRowAndColumn(sheet, translateXYtoRowNumber(x, y), translateXYtoColumnNumber(x, y));
	}
	
	private void copyCell(Cell sourceCell, Cell targetCell) {
		if (sourceCell.getCellComment()!=null) {
			targetCell.setCellComment(sourceCell.getCellComment());
		}
		if (sourceCell.getHyperlink()!=null) {
			targetCell.setHyperlink(sourceCell.getHyperlink());
		}
		targetCell.setCellStyle(sourceCell.getCellStyle());
		switch (sourceCell.getCellType()) {
		case Cell.CELL_TYPE_BLANK:
		case Cell.CELL_TYPE_STRING:
			targetCell.setCellValue(sourceCell.getStringCellValue());
			break;
		case Cell.CELL_TYPE_BOOLEAN:
			targetCell.setCellValue(sourceCell.getBooleanCellValue());
			break;
		case Cell.CELL_TYPE_ERROR:
			targetCell.setCellValue(sourceCell.getErrorCellValue());
			break;
		case Cell.CELL_TYPE_FORMULA:
			targetCell.setCellFormula(sourceCell.getCellFormula());
			break;
		case Cell.CELL_TYPE_NUMERIC:
			targetCell.setCellValue(sourceCell.getNumericCellValue());
		}
	}
	
	private void swapCells(Cell sourceCell, Cell targetCell) {
		if (sourceCell.getCellComment()!=null) {
			Comment targetCellComment = targetCell.getCellComment();
			targetCell.setCellComment(sourceCell.getCellComment());
			sourceCell.setCellComment(targetCellComment);
		}
		if (sourceCell.getHyperlink()!=null) {
			Hyperlink targetCellHyperLink = targetCell.getHyperlink();
			targetCell.setHyperlink(sourceCell.getHyperlink());
			sourceCell.setHyperlink(targetCellHyperLink);
		}
		CellStyle targetCellStyle = targetCell.getCellStyle(); 
		targetCell.setCellStyle(sourceCell.getCellStyle());
		sourceCell.setCellStyle(targetCellStyle);
		
		Object targetCellValue = getCellValue(targetCell);
		Object sourceCellValue = getCellValue(sourceCell);
		int sourceCellType = sourceCell.getCellType();
		int targetCellType = targetCell.getCellType();
		setCellValue(targetCell, sourceCellType, sourceCellValue);
		setCellValue(sourceCell, targetCellType, targetCellValue);
	}

	private Object getCellValue(Cell cell) {
		switch (cell.getCellType()) {
		case Cell.CELL_TYPE_BLANK:
		case Cell.CELL_TYPE_STRING:
			return cell.getStringCellValue();
		case Cell.CELL_TYPE_BOOLEAN:
			return cell.getBooleanCellValue(); 
		case Cell.CELL_TYPE_ERROR:
			return cell.getErrorCellValue();
		case Cell.CELL_TYPE_FORMULA:
			return cell.getCellFormula();
		case Cell.CELL_TYPE_NUMERIC:
			return cell.getNumericCellValue();
		default:
			return null;
		}
	}
	
	private void setCellValue(Cell cell, int cellType, Object value) {
		switch (cellType) {
		case Cell.CELL_TYPE_BLANK:
		case Cell.CELL_TYPE_STRING:
			String stringValue = (String)value;
			cell.setCellValue(stringValue);
			break;
		case Cell.CELL_TYPE_BOOLEAN:
			boolean booleanValue = (Boolean)value; 
			cell.setCellValue(booleanValue);
			break;
		case Cell.CELL_TYPE_ERROR:
			byte errorValue = (Byte)value;
			cell.setCellErrorValue(errorValue);
			break;
		case Cell.CELL_TYPE_FORMULA:
			String cellFormula = (String)value;
			cell.setCellFormula(cellFormula);
			break;
		case Cell.CELL_TYPE_NUMERIC:
			double numericValue = (Double)value;
			cell.setCellValue(numericValue);
		}

	}
	
	private boolean cellIsEmpty(Cell cell) {
		switch (cell.getCellType()) {
		case Cell.CELL_TYPE_BLANK:
		case Cell.CELL_TYPE_STRING:
			return (cell.getStringCellValue().equals(""));
		case Cell.CELL_TYPE_BOOLEAN:
			return false;
		case Cell.CELL_TYPE_ERROR:
			return false;
		case Cell.CELL_TYPE_FORMULA:
			return false;
		case Cell.CELL_TYPE_NUMERIC:
			return false;
		default:
			return false;
		}
	}
	
	private Cell createCell(int rowNumber, int colNumber) {
		Row row = currentSheetData.sheet.getRow(rowNumber);
		if (row==null) {
			row = currentSheetData.sheet.createRow(rowNumber);
		}
		
		Cell cell = row.getCell(colNumber);
		if (cell==null) {
			cell = currentSheetData.createCellAndRefreshLastColumnNumber(row, colNumber);
		}
		
		return cell;
	}
	
	private Cell createCellXY(int x, int y) {
		int rowNumber = translateXYtoRowNumber(x, y);
		int colNumber = translateXYtoColumnNumber(x, y);
		return createCell(rowNumber, colNumber);
	}
	
	private void copyCell(int sourceX, int sourceY, int targetX, int targetY) {
		Cell sourceCell = getCellByXY(sourceX, sourceY);
		Cell targetCell = getCellByXY(targetX, targetY);
		if (targetCell==null) {
			targetCell = createCellXY(targetX, targetY);
		}
		if (sourceCell==null) {
			targetCell.setCellValue("");
		} else {
			copyCell(sourceCell, targetCell);
		}
	}

	private Cell getCellByRowAndColumn(int rowIndex, int columnIndex) {
		return getCellByRowAndColumn(currentSheetData.sheet, rowIndex, columnIndex);
	}
	private Cell getCellByRowAndColumn(Sheet sheet, int rowIndex, int columnIndex) {
		Row row = sheet.getRow(rowIndex);
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
		Font correspondingBoldFont = workbook.findFont(Font.BOLDWEIGHT_BOLD, font.getColor(), font.getFontHeight(), font.getFontName(), font.getItalic(), font.getStrikeout(), font.getTypeOffset(), font.getUnderline());
		if (correspondingBoldFont==null) {
			correspondingBoldFont = workbook.createFont();
			correspondingBoldFont.setBoldweight(Font.BOLDWEIGHT_BOLD);
			correspondingBoldFont.setColor(font.getColor());
			correspondingBoldFont.setFontHeight(font.getFontHeight());
			correspondingBoldFont.setFontName(font.getFontName());
			correspondingBoldFont.setItalic(font.getItalic());
			correspondingBoldFont.setStrikeout(font.getStrikeout());
			correspondingBoldFont.setTypeOffset(font.getTypeOffset());
			correspondingBoldFont.setUnderline(font.getUnderline());
		}
		
		CellStyleFilter cellStyleFilter = new CellStyleFilter(origStyle);
		cellStyleFilter.setFontIndex(correspondingBoldFont.getIndex());
		CellStyle correspondingCellStyle = findCellStyle(cellStyleFilter);
		
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
	
	private void writeSheetHeader() {

		if (!append && !insert) {
			createRegion(0, 0, headerRowIndent + headerRowCount-1, headerColumnIndent + headerColumnCount-1);
			boolean boldStyleFound = false;
			short boldStyle = 0;
			
			for (HeaderGroup headerGroup : mappingInfo.getHeaderGroups()) {
				for (HeaderRange range : headerGroup.getRanges()) {
					if (range.getRowStart()!=range.getRowEnd() || range.getColumnStart()!=range.getColumnEnd()) {
						currentSheetData.sheet.addMergedRegion(new CellRangeAddress(range.getRowStart(), range.getRowEnd(), range.getColumnStart(), range.getColumnEnd()));
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
	}

	private void insertEmptyOrPreserveLines(int index, int lineCount) {
		if (mappingInfo.getOrientation() == XLSMapping.HEADER_ON_TOP) {
			insertEmptyOrPreserveRows(index, lineCount);
		} else {
			insertEmptyOrPreserveColumns(index, lineCount);
		}
	}
	
	private void insertEmptyOrPreserveRows(int index, int rowCount) {
		int columnsToCreate = headerColumnIndent + headerColumnCount;
		for (int i = 0; i < rowCount; ++i) {
			int rowIndex = index + i;
			Row row = currentSheetData.sheet.getRow(rowIndex);
			if (row == null) {
				row = currentSheetData.sheet.createRow(rowIndex);
			}
			for (int j=0; j<columnsToCreate; ++j) {
				Cell cell = row.getCell(j);
				if (cell == null) {
					currentSheetData.createCellAndRefreshLastColumnNumber(row, j);
				}
			}
		}
	}
	
	private void insertEmptyOrPreserveColumns(int index, int columnCount) {
		for (int i = 0; i <= currentSheetData.getLastRowNumber(); ++i) {
			Row row = currentSheetData.sheet.getRow(i);
			if (row == null) {
				row = currentSheetData.sheet.createRow(i);
			}
			for (int j=0; j<columnCount; ++j) {
				Cell cell = row.getCell(index+j);
				if (cell == null) {
					currentSheetData.createCellAndRefreshLastColumnNumber(row, index+j);
				}
			}
		}
	}
	
	private void appendEmptyLines(int lineCount) {
		insertEmptyOrPreserveLines(currentSheetData.getLastLineNumber(mappingInfo.getOrientation())+1, lineCount);
	}
	
	void insertEmptyOrTemplateLines(int index, int lineCount) {
		
		if (mappingInfo.getOrientation()==XLSMapping.HEADER_ON_TOP) {
			int lastLineNum = currentSheetData.getLastLineNumber(mappingInfo.getOrientation());
			if (index <= lastLineNum) {
				currentSheetData.sheet.shiftRows(index, lastLineNum, lineCount);
				currentSheetData.setTemplateCopiedRegionY1(currentSheetData.getTemplateCopiedRegionY1() + lineCount);
			}

			
			int defaultColumnsToCreate = headerXYRange.x2+1;
			for (int i = 0; i < lineCount; ++i) {
				int newRowIndex = index + i;
				Row row = currentSheetData.sheet.getRow(newRowIndex);
				if (row == null) {
					row = currentSheetData.sheet.createRow(newRowIndex);
				}
				
				Row templateRow = currentSheetData.sheet.getRow(currentSheetData.getTemplateCopiedRegionY1() + i);
				if (insert && templateRow!=null) {
					
					// copy from a template row
					for (int j = 0; j < templateRow.getLastCellNum(); ++j) {
						Cell templateCell = findTemplateCell(j);
						if (templateCell != null) {
							Cell cell = currentSheetData.createCellAndRefreshLastColumnNumber(row, j); 
							copyCell(templateCell, cell);
						}
					}
				} else {
					//or create an empty row of default width
					for (int j=0; j<defaultColumnsToCreate; ++j) {
						currentSheetData.createCellAndRefreshLastColumnNumber(row, j);
					}
				}
			}
			if (minRecordFieldYOffset<0) {
				for (int x=headerXYRange.x1; x<headerXYRange.x2; ++x) {
					Integer cloverFieldIndex = xToCloverFieldMapping.get(x);
					if (cloverFieldIndex!=null) {
						Integer yOffset = cloverFieldToYOffsetMapping.get(cloverFieldIndex);
						if (yOffset!=null && yOffset<0) {
							for (int yDelta=-1; yDelta>=yOffset; --yDelta) {
								copyCell(x, index+yDelta, x, index+yDelta+lineCount);
							}
						}
						
					}
				}
			}

		}
		else {
			shiftColumns(currentSheetData, index, lineCount);
			if (insert) {
				for (int i = 0; i < lineCount; ++i) {
					for (int x=0; x<=currentSheetData.getLastRowNumber(); ++x) {
						int lineY = index + i;
						Cell cell = findCellOnLine(lineY, x);
						if (cell!=null) {
							Cell templateCell = findTemplateCell(x);
							if (templateCell != null) {
								copyCell(templateCell, cell);
							}
						}
					}
				}
			}
			
		}
		
	}

	/**
	 * @param x
	 */
	private Cell findTemplateCell(int x) {
		Cell templateCell = null;
		Integer cloverField = xToCloverFieldMapping.get(x);
		if (cloverField!=null) {
			Integer yOffset = cloverFieldToYOffsetMapping.get(cloverField);
			if (yOffset!=null) {
				templateCell = getCellByXY(x, currentSheetData.getTemplateCopiedRegionY1() + yOffset);
			}
		}
		return templateCell;
	}
	
	private Integer findCellYOnLine(int lineY, int x) {
		Integer dataFieldIndex = xToCloverFieldMapping.get(x);
		if (dataFieldIndex!=null) {
			DataFieldMetadata dataFieldMetadata = metadata.getField(dataFieldIndex);
			Integer yOffset = cloverFieldToYOffsetMapping.get(dataFieldMetadata.getNumber());
			if (yOffset!=null) {
				return (lineY + yOffset); 
			}
		}
		return null;
		
	}
	
	private Cell findCellOnLine(int lineY, int x) {
		Integer cellY = findCellYOnLine(lineY, x);
		if (cellY!=null) {
			return getCellByXY(x, cellY);
		}
		return null;
	}
	
	private CellPosition createNextRecordRegion() {
 			int linesToCreate = mappingInfo.getStep();
			int recordOffsetY = currentSheetData.getCurrentY() + linesToCreate;
			int recordOffsetX = headerXYRange.x1;
			if (insert) {
				int firstFooterLineIndex = currentSheetData.getFirstFooterLineIndex();
				insertEmptyOrTemplateLines(firstFooterLineIndex, linesToCreate);
				recordOffsetY = firstFooterLineIndex;
				currentSheetData.setFirstFooterLineIndex(firstFooterLineIndex + linesToCreate);
			} else {
				insertEmptyOrPreserveLines(currentSheetData.getCurrentY()+1, linesToCreate);
				currentSheetData.setCurrentY(currentSheetData.getCurrentY() + linesToCreate);
			}
			
			return new CellPosition(recordOffsetX, recordOffsetY);
	}
	
	
	public int write(DataRecord record) throws IOException {
		if (record == null) {
			throw new NullPointerException("record");
		}
		
		takeSheetOrPrepareSheet(record);
		
		CellPosition cellOffset = createNextRecordRegion();
		for (int i=0; i<record.getNumFields(); ++i) {
			DataField dataField = record.getField(i);
			Integer recordX = cloverFieldToXOffsetMapping.get(dataField.getMetadata().getNumber());
			Integer recordY = cloverFieldToYOffsetMapping.get(dataField.getMetadata().getNumber());
			if (recordX!=null && recordY!=null) {
				//normally either cellOffset.x or cellOffset.y should be zero depending on orientation
				int cellX = recordX + cellOffset.x;
				int cellY = recordY + cellOffset.y;
				Cell cell = getCellByXY(cellX, cellY);
				if (cell==null) { //it may happen that existing rows with no data are read from XLS(X) file so that they contain no cells
					int lastLineNumber = currentSheetData.getLastLineNumber(mappingInfo.getOrientation());
					if (cellX<=headerXYRange.x2 && cellY<=lastLineNumber) {
						cell = createCellXY(cellX, cellY);
					} else {
						throw new IllegalStateException("Unexpectedly not found a cell for a new record at coordinates: [X: " + cellX + ", Y:" + cellY + "]");
					}
				}
				setCellValue(cell, dataField);
				CellStyle cellStyle = takeCellStyleOrPrepareCellStyle(dataField.getMetadata(), cell);
				if (cellStyle!=null) {
					cell.setCellStyle(cellStyle);
				}
			}
			
		}

		
		return 0;
	}


	private void setCellValue(Cell cell, DataField dataField) {
		
		if (dataField.getValue()==null) {
			cell.setCellValue(dataField.getMetadata().getNullValue());
			return;
		}
		
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
				cell.setCellValue((Double)dataField.getValue());
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
					for (SheetData sheetData : sheetNameToSheetDataMap.values()) {
						autosizeColumns(sheetData.sheet);
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

	private void swapCells(SheetData sheetData, Row row, int sourceCellIndex, int targetCellIndex) {
		Cell sourceCell = row.getCell(sourceCellIndex);
		if (sourceCell==null) {
			sourceCell = sheetData.createCellAndRefreshLastColumnNumber(row, sourceCellIndex);
		}
		Cell targetCell = row.getCell(targetCellIndex);
		if (targetCell == null) {
			targetCell = sheetData.createCellAndRefreshLastColumnNumber(row, targetCellIndex);
		}
		swapCells(sourceCell, targetCell);
	}
	
	private void shiftColumns(SheetData sheetData, int index, int movementSize) {
		for (int x=0; x<=sheetData.getLastRowNumber(); ++x) {
			Row row = sheetData.sheet.getRow(x);
			if (row==null) {
				row = sheetData.sheet.createRow(x);
			}
			int lastColumnNumber = sheetData.getLastColumnNumber();
			
			if (lastColumnNumber < index) {
				appendEmptyLines(index - lastColumnNumber);
			} else {
				if (movementSize > 0) {
					for (int movementIndex = lastColumnNumber; movementIndex >= index; --movementIndex) {
						Integer cellY = findCellYOnLine(movementIndex, x);
						if (cellY == null) {
							cellY = movementIndex;
						}
						swapCells(sheetData, row, cellY, cellY + movementSize);
					}
				} else {
					for (int movementIndex = index; movementIndex <= lastColumnNumber; ++movementIndex) {
						Integer cellY = findCellYOnLine(movementIndex, x);
						if (cellY==null) {
							cellY=movementIndex;
						}
						swapCells(sheetData, row, cellY, cellY + movementSize);
					}
				}
			}
		}
	}
	
	/**
	 * @param sheetData
	 */
	private void removeTemplateLinesIfNeeded(SheetData sheetData) {
		if (insert & templateWorkbook!=null) {
			if (mappingInfo.getOrientation()==XLSMapping.HEADER_ON_TOP) {
				int deletedRows = 0;
				for (int i=0; i<mappingInfo.getStep(); ++i) {
					Row templateRowToRemove = sheetData.sheet.getRow(sheetData.getTemplateCopiedRegionY1()+i);
					if (templateRowToRemove!=null) {
						sheetData.sheet.removeRow(templateRowToRemove);
						deletedRows++;
					}
				}
				int rowsToShiftUpStart = sheetData.getTemplateCopiedRegionY1()+1;
				int rowsToShiftUpEnd = sheetData.getLastRowNumber();
				if (rowsToShiftUpStart <= rowsToShiftUpEnd) {
					sheetData.sheet.shiftRows(sheetData.getTemplateCopiedRegionY1()+1, sheetData.getLastRowNumber(), -deletedRows);
				}
			} else {
				shiftColumns(sheetData, sheetData.getTemplateCopiedRegionY1(), -mappingInfo.getStep());
			}
		}
	}

	/**
	 * @param sheetData
	 */
	private void autosizeColumns(Sheet sheet) {
		if (headerXYRange != null && mappingInfo.getOrientation() == XLSMapping.HEADER_ON_TOP) { // mapping initialized at least
			for (int x = headerXYRange.x1; x < headerXYRange.x2; ++x) {
				if (mappingInfo.getOrientation() == XLSMapping.HEADER_ON_TOP) {
					if (xToCloverFieldMapping.get(x) != null) {
						sheet.autoSizeColumn(x);
					}
				}
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

	private void configureWorkbook() {
		workbook.setForceFormulaRecalculation(true);
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
		private final Sheet sheet;
		/** the current row within the sheet */
		private int currentY;
		private int firstFooterLineIndex;
		private int templateCopiedRegionY1;
		
		final int COLUMN_NUMBER_NOT_INITIALIZED = Integer.MIN_VALUE; 
		final int NO_COLUMN = -1; 
		private int lastColumnNumber = COLUMN_NUMBER_NOT_INITIALIZED;
		private Map<Integer, Integer> lastCellNumberInRow = new HashMap<Integer, Integer>();

		public SheetData(Sheet sheet, int currentY, int firstFooterLineIndex, int templateCopiedRegionY1) {
			this.sheet = sheet;
			this.currentY = currentY;
			this.firstFooterLineIndex = firstFooterLineIndex;
			this.templateCopiedRegionY1 = templateCopiedRegionY1;
		}

		public int getCurrentY() {
			return currentY;
		}

		public void setCurrentY(int currentY) {
			this.currentY = currentY;
		}

		public int getFirstFooterLineIndex() {
			return firstFooterLineIndex;
		}

		public void setFirstFooterLineIndex(int firstFooterLineIndex) {
			this.firstFooterLineIndex = firstFooterLineIndex;
		}

		public int getTemplateCopiedRegionY1() {
			return templateCopiedRegionY1;
		}

		public void setTemplateCopiedRegionY1(int templateCopiedRegionY1) {
			this.templateCopiedRegionY1 = templateCopiedRegionY1;
		}

		private int initializeLastCellNumber(Row row) {
			int lastCellIndexUpperBound;
			if (row.getPhysicalNumberOfCells()==0) {
				lastCellIndexUpperBound = -1;
			} else {
				lastCellIndexUpperBound = row.getLastCellNum();
			}
			
			int lastNonNullCellIndex = lastCellIndexUpperBound;
			while (lastNonNullCellIndex >= 0 && row.getCell(lastNonNullCellIndex)==null) {
				lastNonNullCellIndex--;
			}
			
			lastCellNumberInRow.put(row.getRowNum(), lastNonNullCellIndex);
			
			return lastNonNullCellIndex;
		}
		
		private int getLastCellNumber(Row row) {
			Integer result = lastCellNumberInRow.get(row.getRowNum());
			if (result == null) {
				return initializeLastCellNumber(row);
			} else {
				return result;
			}
		}
		
		
		public int getLastColumnNumber() {
			if (lastColumnNumber==COLUMN_NUMBER_NOT_INITIALIZED) {
				lastColumnNumber = Integer.MIN_VALUE;
				boolean initialized = false;
				for (int i=0; i<=getLastRowNumber(); ++i) {
					Row row = sheet.getRow(i);
					if (row!=null) {
						if (getLastCellNumber(row) > lastColumnNumber) {
							lastColumnNumber = getLastCellNumber(row);
							initialized = true;
						}
					}
				}
				if (!initialized) {
					lastColumnNumber = NO_COLUMN;
				}
			}
			return lastColumnNumber;
		}

		public int getLastRowNumber() {
			if (sheet.getPhysicalNumberOfRows()==0) {
				return -1;
			} else {
				return sheet.getLastRowNum();
			}
		}
		

		public int getLastLineNumber(SpreadsheetOrientation oritentation) {
			if (oritentation==XLSMapping.HEADER_ON_TOP) {
				return getLastRowNumber();
			} else {
				return getLastColumnNumber();
			}
		}
		
		public void setLastColumnNumber(int lastColumnNumber) {
			this.lastColumnNumber = lastColumnNumber;
		}
		
		public Cell createCellAndRefreshLastColumnNumber(Row row, int cellNumber) {
			if (cellNumber > getLastColumnNumber()) {
				setLastColumnNumber(cellNumber);
			}
			return row.createCell(cellNumber);
		}

	}

	
}
