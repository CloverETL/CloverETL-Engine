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
package org.jetel.data.formatter.spreadsheet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.util.CellRangeAddress;
import org.jetel.data.parser.XLSMapping;
import org.jetel.data.parser.XLSMapping.SpreadsheetOrientation;

/**
 * A structure used to save states when multiple sheet writing is enabled.
 *
 * @author Pavel Simecek 
 *
 */
public final class SheetData {
	/** the affected sheet */
	private final Sheet sheet;
	/** the current line within the sheet */
	private int currentY;
	private int templateCopiedRegionY2;
	
	final int COLUMN_NUMBER_NOT_INITIALIZED = Integer.MIN_VALUE; 
	final int NO_COLUMN = -1; 
	private int lastColumnNumber = COLUMN_NUMBER_NOT_INITIALIZED;
	private Map<Integer, Integer> lastCellNumberInRow = new HashMap<Integer, Integer>();
	private XLSMapping mappingInfo;
	private XLSMappingStats mappingStats;
	private CoordsTransformations transformations;

	public SheetData(Sheet sheet, XLSMapping mappingInfo, XLSMappingStats mappingStats, int currentY, int templateCopiedRegionY2) {
		this.sheet = sheet;
		this.currentY = currentY;
		this.templateCopiedRegionY2 = templateCopiedRegionY2;
		this.transformations = new CoordsTransformations(mappingInfo.getOrientation());
		this.mappingInfo = mappingInfo;
		this.mappingStats = mappingStats;
	}
	
	public void removeTemplateLines() {
		if (mappingInfo.getOrientation()==XLSMapping.HEADER_ON_TOP) {
			int lastRowIndex = getLastRowNumber();
			shiftRows(getTemplateCopiedRegionY2()+1, -mappingInfo.getStep());
			for (int rowToDeleteIndex = lastRowIndex; rowToDeleteIndex > lastRowIndex-mappingInfo.getStep(); --rowToDeleteIndex) {
				Row row = getRow(rowToDeleteIndex);
				if (row!=null) {
					removeRow(row);
				}
			}
		} else {
			shiftColumns(getTemplateCopiedRegionY2()+1, -mappingInfo.getStep());
		}
	}

	Cell getCellByRowAndColumn(int rowIndex, int columnIndex) {
		Row row = sheet.getRow(rowIndex);
		if (row==null) {
			return null;
		}
		Cell cell = row.getCell(columnIndex);
		return cell;
	}
	
	
	public Cell getCellByXY(int x, int y) {
		return getCellByRowAndColumn(transformations.translateXYtoRowNumber(x, y), transformations.translateXYtoColumnNumber(x, y));
	}
	
	public String getSheetName() {
		return sheet.getSheetName();
	}
	
	public void removeMergedRegion(int mergedRegionIndex) {
		sheet.removeMergedRegion(mergedRegionIndex);
	}
	
	public int addMergedRegion(CellRangeAddress mergedRegion) {
		return sheet.addMergedRegion(mergedRegion);
	}
	
	public CellRangeAddress getMergedRegion(int mergedRegionIndex) {
		return sheet.getMergedRegion(mergedRegionIndex);
	}
	
	public int getMergedRegionsCount() {
		return sheet.getNumMergedRegions();
	}
	
	public void shiftRows(int indexFirst, int indexLast, int shiftSize) {
		sheet.shiftRows(indexFirst, indexLast, shiftSize);
	}
	
	public Row createRow(int rowIndex) {
		return sheet.createRow(rowIndex);
	}
	
	public Row getRow(int rowIndex) {
		return sheet.getRow(rowIndex);
	}
	
	public void removeRow(Row rowToRemove) {
		sheet.removeRow(rowToRemove);
	}
	
	public int getCurrentY() {
		return currentY;
	}

	public void setCurrentY(int currentY) {
		this.currentY = currentY;
	}

	public int getTemplateCopiedRegionY2() {
		return templateCopiedRegionY2;
	}

	public void setTemplateCopiedRegionY2(int templateCopiedRegionY1) {
		this.templateCopiedRegionY2 = templateCopiedRegionY1;
	}

	
	private Cell createCell(int rowNumber, int colNumber) {
		Row row = getRow(rowNumber);
		if (row==null) {
			row = createRow(rowNumber);
		}
		
		Cell cell = row.getCell(colNumber);
		if (cell==null) {
			cell = createCellAndRefreshLastColumnNumber(row, colNumber);
		}
		
		return cell;
	}
	
	public Cell createCellXY(int x, int y) {
		int rowNumber = transformations.translateXYtoRowNumber(x, y);
		int colNumber = transformations.translateXYtoColumnNumber(x, y);
		return createCell(rowNumber, colNumber);
	}
	
	public void swapCells(int sourceX, int sourceY, int targetX, int targetY) {
		Cell sourceCell = getCellByXY(sourceX, sourceY);
		Cell targetCell = getCellByXY(targetX, targetY);
		if (sourceCell==null) {
			sourceCell = createCellXY(sourceX, sourceY);
		}
		if (targetCell==null) {
			targetCell = createCellXY(targetX, targetY);
		}
		
		CellOperations.swapCells(sourceCell, targetCell);
	}
	
	public void copyCell(int sourceX, int sourceY, int targetX, int targetY) {
		Cell sourceCell = getCellByXY(sourceX, sourceY);
		Cell targetCell = getCellByXY(targetX, targetY);
		if (targetCell==null) {
			targetCell = createCellXY(targetX, targetY);
		}
		if (sourceCell==null) {
			targetCell.setCellValue("");
		} else {
			CellOperations.copyCell(sourceCell, targetCell);
		}
	}
	
	public int initializeLastCellNumber(Row row) {
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

	public void setLastCellNumber(Row row, int lastCellNumber) {
		lastCellNumberInRow.put(row.getRowNum(), lastCellNumber);
	}
	
	public int getLastCellNumber(Row row) {
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
		if (getLastCellNumber(row) < cellNumber) {
			setLastCellNumber(row, cellNumber);
		}
		Cell newCell = row.createCell(cellNumber);
		newCell.setCellType(Cell.CELL_TYPE_BLANK);
		return newCell;
	}
	
	public void appendEmptyLines(int lineCount) {
		insertEmptyOrPreserveLines(getLastLineNumber(mappingInfo.getOrientation())+1, lineCount);
	}
	

	public void insertEmptyOrPreserveLines(int index, int lineCount) {
		if (mappingInfo.getOrientation() == XLSMapping.HEADER_ON_TOP) {
			insertEmptyOrPreserveRows(index, lineCount);
		} else {
			insertEmptyOrPreserveColumns(index, lineCount);
		}
	}
	
	public void insertEmptyOrPreserveRows(int index, int rowCount) {
		XYRange firstRecordXYRange = mappingStats.getFirstRecordXYRange();
		int columnsToCreate = transformations.translateXYtoColumnNumber(firstRecordXYRange.x2, firstRecordXYRange.y2);
		for (int i = 0; i < rowCount; ++i) {
			int rowIndex = index + i;
			Row row = getRow(rowIndex);
			if (row == null) {
				row = createRow(rowIndex);
			}
			for (int j=0; j<columnsToCreate; ++j) {
				Cell cell = row.getCell(j);
				if (cell == null) {
					createCellAndRefreshLastColumnNumber(row, j);
				}
			}
		}
	}
	
	public void insertEmptyOrPreserveColumns(int index, int columnCount) {
		for (int i = 0; i <= getLastRowNumber(); ++i) {
			Row row = getRow(i);
			if (row == null) {
				row = createRow(i);
			}
			for (int j=0; j<columnCount; ++j) {
				Cell cell = row.getCell(index+j);
				if (cell == null) {
					createCellAndRefreshLastColumnNumber(row, index+j);
				}
			}
		}
	}
	
	public void insertEmptyOrTemplateLines(int index, int lineCount) {
		
		if (mappingInfo.getOrientation()==XLSMapping.HEADER_ON_TOP) {
			shiftRows(index, lineCount);
		}
		else {
			shiftColumns(index, lineCount);
		}
		int lastRecordLine = index + lineCount - 1;
		for (RelativeCellPosition relativeCellPosition : mappingStats.getTemplateCellsToCopy()) {
			int templateCellX = mappingStats.getFirstRecordXYRange().x1 + relativeCellPosition.relativeX;
			int templateCellY = this.getTemplateCopiedRegionY2() + relativeCellPosition.relativeY;
			int recordCellX = mappingStats.getFirstRecordXYRange().x1 + relativeCellPosition.relativeX;
			int recordCellY = lastRecordLine + relativeCellPosition.relativeY;
			copyCell(templateCellX, templateCellY, recordCellX, recordCellY);
		}
	}

	/**
	 * @param index
	 * @param lineCount
	 */
	public void shiftRows(int index, int lineCount) {
		int lastLineNum = getLastLineNumber(mappingInfo.getOrientation());
		XYRange firstRecordXYRange = mappingStats.getFirstRecordXYRange();
		if (index <= lastLineNum) {
			shiftRows(index, lastLineNum, lineCount);
			setTemplateCopiedRegionY2(getTemplateCopiedRegionY2() + lineCount);
		}
		for (int i = 0; i < lineCount; ++i) {
			int newRowIndex = index + i;
			Row row = getRow(newRowIndex);
			if (row == null) {
				row = createRow(newRowIndex);
			}
			for (int j = 0; j <=firstRecordXYRange.x2; ++j) {
				createCellAndRefreshLastColumnNumber(row, j);
			}
		}
		if (mappingStats.getMinRecordFieldYOffset()<0) {
			//when a record is not a straight line, newly created empty cells must be moved to positions defined by mapping
			int previousRecordBottom = index + mappingInfo.getStep() - 1;
			for (Entry<Integer, Interval> entry : mappingStats.getXOffsetToMinimalYIntervalMap().entrySet()) {
				int x = firstRecordXYRange.x1 + entry.getKey();
				int firstCellYToMove = index - 1;
				int lastCellYToMove = previousRecordBottom + entry.getValue().min;
				for (int y = firstCellYToMove; y >= lastCellYToMove; --y) {
					int emptyCellY = y + lineCount;;
					int previousRecordCellY = y;
					swapCells(x, emptyCellY, x, previousRecordCellY);
				}
			}
		}
	}
	
	
	public void shiftColumns(int index, int movementSize) {
		int previousRecordBottom = index + mappingInfo.getStep() - 1;
		for (int x=0; x<=getLastRowNumber(); ++x) {
			Row row = getRow(x);
			if (row==null) {
				row = createRow(x);
			}
			int lastColumnNumber = getLastCellNumber(row);
			
			if (lastColumnNumber < previousRecordBottom) {
				appendEmptyLines(previousRecordBottom - lastColumnNumber);
			} else {
				Integer minimalYOffset = null; 
				Interval yInterval = mappingStats.getMinimalYIntervalForXOffset(x-mappingStats.getFirstRecordXYRange().x1);
				if (yInterval!=null) {
					minimalYOffset = yInterval.min; 
				}
				if (minimalYOffset == null) {
					minimalYOffset = 0;
				}
				int cellY = transformations.maximum(0, previousRecordBottom + minimalYOffset);
				if (movementSize > 0) {
					for (int movementIndex = lastColumnNumber; movementIndex >= cellY; --movementIndex) {
						swapCells(row, movementIndex, movementIndex + movementSize);
					}
				} else {
					for (int movementIndex = cellY; movementIndex <= lastColumnNumber; ++movementIndex) {
						swapCells(row, movementIndex, movementIndex + movementSize);
					}
				}
			}
		}
		shiftMergedRegionsWhileShiftingColumns(index, movementSize);
		setTemplateCopiedRegionY2(getTemplateCopiedRegionY2() + movementSize);
	}

	private void shiftMergedRegionsWhileShiftingColumns(int index, int movementSize) {
		List<Integer> regionsToRemove = new ArrayList<Integer>();
		List<CellRangeAddress> regionsToAdd = new ArrayList<CellRangeAddress>(); 
		for (int i=0; i<getMergedRegionsCount(); ++i) {
			CellRangeAddress regionRangeAddress = getMergedRegion(i);
			if (regionRangeAddress.getLastColumn() >= index) {
				regionsToRemove.add(i);
				CellRangeAddress cellRangeAddress = new CellRangeAddress(regionRangeAddress.getFirstRow(), regionRangeAddress.getLastRow(), regionRangeAddress.getFirstColumn()+movementSize, regionRangeAddress.getLastColumn()+movementSize);
				regionsToAdd.add(cellRangeAddress);
			}
		}
		for (Integer regionToRemove : regionsToRemove) {
			removeMergedRegion(regionToRemove);
		}
		
		for (CellRangeAddress regionToAdd : regionsToAdd) {
			addMergedRegion(regionToAdd);
		}
	}
	
	public void swapCells(Row row, int sourceCellIndex, int targetCellIndex) {
		Cell sourceCell = row.getCell(sourceCellIndex);
		if (sourceCell==null) {
			sourceCell = createCellAndRefreshLastColumnNumber(row, sourceCellIndex);
		}
		Cell targetCell = row.getCell(targetCellIndex);
		if (targetCell == null) {
			targetCell = createCellAndRefreshLastColumnNumber(row, targetCellIndex);
		}
		CellOperations.swapCells(sourceCell, targetCell);
	}


	public void autosizeColumns() {
		XYRange firstRecordXYRange = mappingStats.getFirstRecordXYRange();
		if (firstRecordXYRange != null && mappingInfo.getOrientation() == XLSMapping.HEADER_ON_TOP) { // mapping initialized at least
			for (Integer cloverFieldIndex : mappingStats.getRegisteredCloverFields()) {
				int x = firstRecordXYRange.x1 + mappingStats.getXOffsetForCloverField(cloverFieldIndex);
				if (mappingInfo.getOrientation() == XLSMapping.HEADER_ON_TOP) {
					sheet.autoSizeColumn(x);
				}
			}
		}
	}

}