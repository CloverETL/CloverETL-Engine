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
import java.util.List;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.jetel.data.DataRecord;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;

/**
 * XLS and XLSX in-memory parser.
 * 
 * @author Martin Janik
 * @author tkramolis (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 11 Aug 2011
 */
public class SpreadsheetDOMParser extends AbstractSpreadsheetParser {

	/** the workbook parsed by this parser */
	private Workbook workbook;
	/** currently parsed sheet */
	private Sheet sheet;
	
	private int nextRecordStartRow;
	/** last row or last column in sheet (depends on orientation) */
	private int lastLine;

	private final DataFormatter dataFormatter = new DataFormatter();
	private final static FormulaEval FORMULA_EVAL = new FormulaEval();
	
	public SpreadsheetDOMParser(DataRecordMetadata metadata, XLSMapping mappingInfo) {
		super(metadata, mappingInfo);
	}

	@Override
	protected void prepareInput(InputStream inputStream) throws IOException, ComponentNotReadyException {
		try {
			workbook = WorkbookFactory.create(inputStream);
		} catch (Exception exception) {
			throw new ComponentNotReadyException("Error opening the XLS(X) workbook!", exception);
		}
	}

	@Override
	public List<String> getSheetNames() {
		List<String> toReturn = new ArrayList<String>(workbook.getNumberOfSheets());
		for (int i = 0; i < workbook.getNumberOfSheets(); i++) {
			toReturn.add(workbook.getSheetName(i));
		}
		
		return toReturn;
	}
	
	@Override
	protected int getRecordStartRow() {
		return nextRecordStartRow;
	}

	@Override
	public boolean setCurrentSheet(int sheetNumber) {
		try {
			sheet = workbook.getSheetAt(sheetNumber);
		} catch (IllegalArgumentException e) {
			return false;
		}
		
		if (mappingInfo.getOrientation() == SpreadsheetOrientation.HORIZONTAL) {
			lastLine = sheet.getLastRowNum();
		} else {
			for (Row row : sheet) {
				if (row.getLastCellNum() - 1 > lastLine) { //
					lastLine = row.getLastCellNum() - 1;
				}
			}
		}
		nextRecordStartRow = startLine;
		return true;
	}

	@Override
	public String[][] getHeader(int startRow, int startColumn, int endRow, int endColumn) throws ComponentNotReadyException {
		if (sheet == null) {
			throw new ComponentNotReadyException("No sheet to read from!");
		}
		
		if (sheet.getLastRowNum() < endRow) {
			throw new ComponentNotReadyException("Sheet does not contain header!");
		}

		int rowsToRead = endRow - startRow;
		String[][] result = new String[rowsToRead][];
		
		List<String> rowResult = new ArrayList<String>();
		int lastColumn = startColumn - 1;
		for (int i = 0; i < rowsToRead; i++) {
			Row row = sheet.getRow(startRow + i);
			if (row != null) {
				int finalColumn = Math.min(row.getLastCellNum(), endColumn);
				for (int j = startColumn; j < finalColumn; j++) {
					Cell cell = row.getCell(j);
					if (cell != null && cell.getCellType() != Cell.CELL_TYPE_BLANK) {
						// add missing cells
						for (int k = lastColumn + 1; k < j; k++) {
							rowResult.add(null);
						}
						rowResult.add(dataFormatter.formatCellValue(cell, FORMULA_EVAL));
						lastColumn = j;
					}
				}
				result[i] = rowResult.toArray(new String[rowResult.size()]);
				rowResult.clear();
				lastColumn = startColumn - 1;
				
			} else {
				result[i] = new String[0];
			}
		}
		return result;
	}

	@Override
	protected DataRecord parseNext(DataRecord record) throws JetelException {
		if (nextRecordStartRow > lastLine) {
			return null;
		}
		if (mappingInfo.getOrientation() == SpreadsheetOrientation.HORIZONTAL) {
			return parseHorizontal(record);
		} else {
			return parseVertical(record);
		}
	}

	private DataRecord parseHorizontal(DataRecord record) {
		int cloverFieldIndex;
		for (int mappingRowIndex = 0; mappingRowIndex < mapping.length; mappingRowIndex++) {
			int[] recordRow = mapping[mappingRowIndex];
			Row row = sheet.getRow(nextRecordStartRow + mappingRowIndex);
			
			if (row == null) {
				for (int column = 0; column < recordRow.length; column++) {
					if ((cloverFieldIndex = recordRow[column]) != XLSMapping.UNDEFINED) {
						try {
							record.getField(cloverFieldIndex).setNull(true);
						} catch (BadDataFormatException e) {
							handleException(new BadDataFormatException("There is no data row for field. Moreover, cannot set default value or null", e), record, cloverFieldIndex, null);
						}
					}
				}
				continue;
			}
			
			for (int column = mappingMinColumn; column < recordRow.length + mappingMinColumn; column++) {
				if ((cloverFieldIndex = recordRow[column - mappingMinColumn]) != XLSMapping.UNDEFINED) {
					fillCloverField(row.getCell(column), record, cloverFieldIndex);					
				}
			}
		}

		nextRecordStartRow += mappingInfo.getStep();
		return record;
	}

	private DataRecord parseVertical(DataRecord record) {
		int cloverFieldIndex;
		for (int mappingRowIndex = 0; mappingRowIndex < mapping.length; mappingRowIndex++) {
			int[] recordRow = mapping[mappingRowIndex];
			Row row = sheet.getRow(mappingMinRow + mappingRowIndex);
			
			if (row == null) {
				for (int column = 0; column < recordRow.length; column++) {
					if ((cloverFieldIndex = recordRow[column]) != XLSMapping.UNDEFINED) {
						try {
							record.getField(cloverFieldIndex).setNull(true);
						} catch (BadDataFormatException e) {
							handleException(new BadDataFormatException("There is no data row for field. Moreover, cannot set default value or null", e), record, cloverFieldIndex, null);
						}
					}
				}
				continue;
			}
			
			for (int column = nextRecordStartRow; column < recordRow.length + nextRecordStartRow; column++) {
				if ((cloverFieldIndex = recordRow[column - nextRecordStartRow]) != XLSMapping.UNDEFINED) {
					fillCloverField(row.getCell(column), record, cloverFieldIndex);					
				}
			}
		}

		nextRecordStartRow += mappingInfo.getStep();
		return record;
	}

	private void fillCloverField(Cell cell, DataRecord record, int cloverFieldIndex) {
		if (cell == null || cell.getCellType() == Cell.CELL_TYPE_BLANK) {
			try {
				record.getField(cloverFieldIndex).setNull(true);
				return;
			} catch (BadDataFormatException e) {
				handleException(new BadDataFormatException("There is no data cell for field. Moreover, cannot set default value or null", e), record, cloverFieldIndex, null);
				return;
			}
		}

		char type = metadata.getField(cloverFieldIndex).getType();

		try {
			switch (type) {
			case DataFieldMetadata.DATE_FIELD:
			case DataFieldMetadata.DATETIME_FIELD:
				record.getField(cloverFieldIndex).setValue(cell.getDateCellValue());
				break;
			case DataFieldMetadata.BYTE_FIELD:
			case DataFieldMetadata.STRING_FIELD:
				record.getField(cloverFieldIndex).fromString(dataFormatter.formatCellValue(cell, FORMULA_EVAL));
				break;
			case DataFieldMetadata.DECIMAL_FIELD:
			case DataFieldMetadata.INTEGER_FIELD:
			case DataFieldMetadata.LONG_FIELD:
			case DataFieldMetadata.NUMERIC_FIELD:
				record.getField(cloverFieldIndex).setValue(cell.getNumericCellValue());
				break;
			case DataFieldMetadata.BOOLEAN_FIELD:
				record.getField(cloverFieldIndex).setValue(cell.getBooleanCellValue());
				break;
			}
		} catch (RuntimeException exception) { // exception when trying get date or number from a different cell type
			try {
				record.getField(cloverFieldIndex).fromString(dataFormatter.formatCellValue(cell));
			} catch (Exception ex) {
				handleException(new BadDataFormatException(ex.getMessage()), record, cloverFieldIndex, cell.getStringCellValue());
			}
		}
	}

	@Override
	public int skip(int nRec) throws JetelException {
		int numberOfRows = nRec * mappingInfo.getStep();
		
		if (nextRecordStartRow + numberOfRows <= lastLine) {
			nextRecordStartRow += numberOfRows;
			return nRec;
		} else {
			int retval = 1 + ((lastLine - nextRecordStartRow) / mappingInfo.getStep());
			nextRecordStartRow = lastLine + 1;
			return retval;
		}
	}

	@Override
	public void postExecute() throws ComponentNotReadyException {
		super.postExecute();
		workbook = null;
		sheet = null;
	}

	@Override
	public void close() throws IOException {
		super.close();
		workbook = null;
		sheet = null;
	}

	/**
	 * This class is used for the dataFormater to return cell formula cached result (stored in the cell XML), not
	 * formula itself.
	 */
	private static class FormulaEval implements FormulaEvaluator {

		@Override
		public void clearAllCachedResultValues() {
		}

		@Override
		public void notifySetFormula(Cell cell) {
		}

		@Override
		public void notifyDeleteCell(Cell cell) {
		}

		@Override
		public void notifyUpdateCell(Cell cell) {
		}

		@Override
		public org.apache.poi.ss.usermodel.CellValue evaluate(Cell cell) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void evaluateAll() {
			throw new UnsupportedOperationException();
		}

		@Override
		public int evaluateFormulaCell(Cell cell) {
			return cell.getCachedFormulaResultType();
		}

		@Override
		public Cell evaluateInCell(Cell cell) {
			throw new UnsupportedOperationException();
		}

	}
}
