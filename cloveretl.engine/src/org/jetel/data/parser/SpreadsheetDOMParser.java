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
import java.io.PushbackInputStream;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.poi.hssf.record.crypto.Biff8EncryptionKey;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.parser.XLSMapping.SpreadsheetOrientation;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ExcelUtils;
import org.jetel.util.ExcelUtils.ExcelType;
import org.jetel.util.SpreadsheetUtils;

/**
 * XLS and XLSX in-memory parser.
 * 
 * @author Martin Janik
 * @author tkramolis (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @author sgerguri (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
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
	
	private String password;

	private final DataFormatter dataFormatter = new DataFormatter();
	private final static FormulaEval FORMULA_EVAL = new FormulaEval();
	
	public SpreadsheetDOMParser(DataRecordMetadata metadata, XLSMapping mappingInfo, String password) {
		super(metadata, mappingInfo);
		this.password = password;
		dataFormatter.addFormat("General", new DecimalFormat("#.############"));
	}

	@Override
	protected void prepareInput(InputStream inputStream) throws IOException, ComponentNotReadyException {
		try {
			if (!inputStream.markSupported()) {
				inputStream = new PushbackInputStream(inputStream, 8);
			}
			
			InputStream bufferedStream = null;
			ExcelType documentType = ExcelUtils.getStreamType(inputStream);
			if (ExcelUtils.getStreamType(inputStream) == ExcelType.XLS) {
				bufferedStream = ExcelUtils.getBufferedStream(inputStream);
				inputStream = ExcelUtils.getDecryptedXLSXStream(bufferedStream, password);
				if (inputStream == null) {
					bufferedStream.reset();
					inputStream = bufferedStream;
					Biff8EncryptionKey.setCurrentUserPassword(password);
				}
			} else if (documentType == ExcelType.INVALID) {
				throw new ComponentNotReadyException("Your InputStream was neither an OLE2 stream, nor an OOXML stream");
			}
			
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
		
		if (mappingInfo.getOrientation() == SpreadsheetOrientation.VERTICAL) {
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
		
		if (sheet.getLastRowNum() < endRow - 1) {
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
		record.setToNull();
		if (mappingInfo.getOrientation() == SpreadsheetOrientation.VERTICAL) {
//			if (nextRecordStartRow > lastLine - mapping.length + 1) {
			if (nextRecordStartRow > lastLine) {
				return null;
			}
			return parse(record, nextRecordStartRow, mappingMinColumn, false);
		} else {
//			if (nextRecordStartRow > lastLine - mapping[0].length + 1) {
			if (nextRecordStartRow > lastLine) {
				return null;
			}
			return parse(record, mappingMinRow, nextRecordStartRow, true);
		}
	}

	private DataRecord parse(DataRecord record, int recordStartRow, int startColumn, boolean horizontal) {
		for (int mappingRowIndex = 0; mappingRowIndex < mapping.length; mappingRowIndex++) {
			int[] recordRow = mapping[mappingRowIndex];
			int[] formatRecordRow = formatMapping != null ? formatMapping[mappingRowIndex] : null;
			Row row = sheet.getRow(recordStartRow + mappingRowIndex);
			
			if (row == null) {
				processNullRow(record, recordRow, recordStartRow + mappingRowIndex);
				processNullRow(record, formatRecordRow, recordStartRow + mappingRowIndex);
				continue;
			}
			
			int cloverFieldIndex;
			for (int column = startColumn; column < recordRow.length + startColumn; column++) {
				if ((cloverFieldIndex = recordRow[column - startColumn]) != XLSMapping.UNDEFINED) {
					fillCloverField(row.getCell(column), record, cloverFieldIndex, column, horizontal);					
				}
				if (formatRecordRow != null) {
					if ((cloverFieldIndex = formatRecordRow[column - startColumn]) != XLSMapping.UNDEFINED) {
						fillFormatField(row.getCell(column), record, cloverFieldIndex, column, horizontal);					
					}
				}
			}
		}

		nextRecordStartRow += mappingInfo.getStep();
		return record;
	}

	private void processNullRow(DataRecord record, int[] recordRow, int currentParseRow) {
		if (recordRow != null) {
			for (int column = 0; column < recordRow.length; column++) {
				int cloverFieldIndex;
				if ((cloverFieldIndex = recordRow[column]) != XLSMapping.UNDEFINED) {
					try {
						record.getField(cloverFieldIndex).setNull(true);
						if (currentParseRow > lastLine) {
							handleException(new BadDataFormatException("Unexpected end of sheet - expected another data row for field " + record.getField(cloverFieldIndex).getMetadata().getName() +
									". Occurred"), record, cloverFieldIndex, null, null);
						}
					} catch (BadDataFormatException e) {
						handleException(new BadDataFormatException("Unexpected end of sheet - expected another data row for field " + record.getField(cloverFieldIndex).getMetadata().getName() +
								". Moreover, cannot set default value or null", e), record, cloverFieldIndex, null, null);
					}
					
				}
			}
		}
	}

	private void fillCloverField(Cell cell, DataRecord record, int cloverFieldIndex, int currentParseRow, boolean horizontal) {
		if (cell == null || cell.getCellType() == Cell.CELL_TYPE_BLANK) {
			try {
				record.getField(cloverFieldIndex).setNull(true);
				if (currentParseRow > lastLine && horizontal) {
					handleException(new BadDataFormatException("Unexpected end of sheet - expected another data row for field " + record.getField(cloverFieldIndex).getMetadata().getName() +
							". Occurred"), record, cloverFieldIndex, null, null);
				}
				return;
			} catch (BadDataFormatException e) {
				handleException(new BadDataFormatException("There is no data cell for field. Moreover, cannot set default value or null", e), record, cloverFieldIndex, null, null);
				return;
			}
		}

		char type = metadata.getField(cloverFieldIndex).getType();

		String expectedType = null;
//		try {
			try {
				
				switch (type) {
				case DataFieldMetadata.DATE_FIELD:
				case DataFieldMetadata.DATETIME_FIELD:
					expectedType = "date";
					record.getField(cloverFieldIndex).setValue(cell.getDateCellValue());
					break;
				case DataFieldMetadata.BYTE_FIELD:
					expectedType = "byte";
				case DataFieldMetadata.STRING_FIELD:
					expectedType = "string";
					record.getField(cloverFieldIndex).fromString(dataFormatter.formatCellValue(cell, FORMULA_EVAL));
					break;
				case DataFieldMetadata.DECIMAL_FIELD:
					expectedType = "decimal";
				case DataFieldMetadata.INTEGER_FIELD:
					expectedType = "integer";
				case DataFieldMetadata.LONG_FIELD:
					expectedType = "long";
				case DataFieldMetadata.NUMERIC_FIELD:
					expectedType = "number";
					record.getField(cloverFieldIndex).setValue(cell.getNumericCellValue());
					break;
				case DataFieldMetadata.BOOLEAN_FIELD:
					expectedType = "boolean";
					record.getField(cloverFieldIndex).setValue(cell.getBooleanCellValue());
					break;
				}
				
			} catch (IllegalStateException e) {
				// Thrown by cell.get*CellValue if cell value type expected here in code is different than the actual cell value.
				// If the actual cell value is empty string (after trimming), interpret it as null, otherwise rethrow the exception.
				if (cell.getCellType() == Cell.CELL_TYPE_STRING && "".equals(cell.getStringCellValue().trim())) {
					record.getField(cloverFieldIndex).setNull(true);
				} else {
//					throw e;
//					String errorMessage = "Cannot get " + expectedType + " value from cell of type " + cellTypeToString(cell.getCellType());
					try {
						record.getField(cloverFieldIndex).setNull(true);
					} catch (Exception ex) {
					}
					String cellCoordinates = SpreadsheetUtils.getColumnReference(cell.getColumnIndex()) + String.valueOf(cell.getRowIndex());
					handleException(new BadDataFormatException("Cannot get " + expectedType + " value from cell of type " + 
							cellTypeToString(cell.getCellType()) + " in " + cellCoordinates), record, cloverFieldIndex, cellCoordinates, dataFormatter.formatCellValue(cell));
				}
			}
//		} catch (RuntimeException exception) { // exception when trying get date or number from a different cell type
//			String errorMessage = exception.getMessage();
//			try {
//				record.getField(cloverFieldIndex).setNull(true);
//			} catch (Exception ex) {
//			}
//			String cellCoordinates = SpreadsheetUtils.getColumnReference(cell.getColumnIndex()) + String.valueOf(cell.getRowIndex());
//			handleException(new BadDataFormatException(errorMessage), record, cloverFieldIndex, cellCoordinates, dataFormatter.formatCellValue(cell));
//		}
	}
	
	private String cellTypeToString(int cellType) {
		switch (cellType) {
		case Cell.CELL_TYPE_BOOLEAN:
			return "Boolean";
		case Cell.CELL_TYPE_STRING:
			return "String";
		case Cell.CELL_TYPE_NUMERIC:
			return "Numeric";
		default:
			return "Unknown";
		}
	}

	private void fillFormatField(Cell cell, DataRecord record, int cloverFieldIndex, int currentParseRow, boolean horizontal) {
		String formatString = cell != null ? cell.getCellStyle().getDataFormatString() : null;
		try {
			// formatString may be null, or namely "GENERAL"
			record.getField(cloverFieldIndex).setValue(formatString);
			if (currentParseRow > lastLine && horizontal) {
				handleException(new BadDataFormatException("Unexpected end of sheet - expected another data row for field " + record.getField(cloverFieldIndex).getMetadata().getName() +
						". Occurred"), record, cloverFieldIndex, null, null);
			}
		} catch (RuntimeException exception) {
			String errorMessage = "Failed to set cell format to field; cause: " + exception;
			String cellCoordinates = SpreadsheetUtils.getColumnReference(cell.getColumnIndex()) + String.valueOf(cell.getRowIndex());
			handleException(new BadDataFormatException(errorMessage), record, cloverFieldIndex, cellCoordinates, formatString);
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
		Biff8EncryptionKey.setCurrentUserPassword(null);
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
