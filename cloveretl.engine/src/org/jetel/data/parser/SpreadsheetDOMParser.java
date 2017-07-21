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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

import org.apache.poi.hssf.record.crypto.Biff8EncryptionKey;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.poifs.filesystem.NPOIFSFileSystem;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.jetel.data.DataRecord;
import org.jetel.data.parser.XLSMapping.SpreadsheetOrientation;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.exception.SpreadsheetException;
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
	
	private String fileName;
	private String sheetName;
	
	private int nextRecordStartRow;
	/** last row or last column in sheet (depends on orientation) */
	private int lastLine;
	private int[] columnEnds;
	private int columnOffset;

	private boolean[] overFlow;
	private boolean overLastLine;
	private boolean stopParsing;
	private ExceptionBuffer exceptionBuffer;
	
	boolean horizontal;
	
	private String password;

	private final CellValueFormatter dataFormatter = new CellValueFormatter();
	private final static FormulaEval FORMULA_EVAL = new FormulaEval();
	
	public SpreadsheetDOMParser(DataRecordMetadata metadata, XLSMapping mappingInfo, String password) {
		super(metadata, mappingInfo);
		this.password = password;
		exceptionBuffer = new ExceptionBuffer();
	}

	@Override
	protected void prepareInput(Object inputSource) throws IOException, ComponentNotReadyException {
		try {
			InputStream inputStream;
			if (inputSource instanceof InputStream) {
				inputStream = (InputStream) inputSource;
			} else {
				inputStream = new FileInputStream((File) inputSource);
			}
			
			if (!inputStream.markSupported()) {
				inputStream = new PushbackInputStream(inputStream, 8);
			}
			
			InputStream bufferedStream = null;
			ExcelType documentType = ExcelUtils.getStreamType(inputStream);
			if (documentType == ExcelType.XLS) {
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
			
			if (inputSource instanceof InputStream) {
				workbook = WorkbookFactory.create(inputStream);
			} else {
				inputStream.close();
				File inputFile = (File) inputSource;
				fileName = inputFile.getAbsolutePath();
				if (documentType == ExcelType.XLS) {
					workbook = new HSSFWorkbook(new NPOIFSFileSystem(inputFile).getRoot(), true);
				} else {
					workbook = new XSSFWorkbook(OPCPackage.open(inputFile.getAbsolutePath()));
				}
			}
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
	
	private void calculateColEnds(boolean horizontal) {
		int mappingMax;
		int mappingMin;
		if (horizontal) {
			mappingMax = mappingInfo.getStats().getMappingMaxRow();
			mappingMin = mappingInfo.getStats().getMappingMinRow();
		} else {
			if (mappingInfo.getHeaderGroups().isEmpty()) {
				mappingMax = mapping[0].length - 1;
				mappingMin = 0;
			} else {
				mappingMax = mappingInfo.getStats().getMappingMaxColumn();
				mappingMin = mappingInfo.getStats().getMappingMinColumn();
			}
		}
		
		columnEnds = new int[mappingMax - mappingMin + 1];
		for (int i = 0; i < columnEnds.length; i++) {
			columnEnds[i] = -1;
		}
		
		overFlow = new boolean[metadata.getNumFields()];
		for (int i = 0; i < overFlow.length; i++) {
			overFlow[i] = true;
		}
		
		for (int i = 0; i < mapping.length; i++) {
			for (int j = 0; j < mapping[0].length; j++) {
				if (mapping[i][j] > -1) {
					overFlow[mapping[i][j]] = false;
				}
			}
		}
		
		if (horizontal) {
			for (int i = mappingMin; i < mappingMax + 1; i++) {
				Row row = sheet.getRow(i);
				for (int j=row.getLastCellNum(); j>=0; j--) {
					Cell cell = row.getCell(j);
					if (cell!=null) {						
						break;
					}
				}
//				columnEnds[i - mappingMin] = sheet.getRow(i).getLastCellNum();
			}
		} else {
			for (Row row : sheet) {
				for (int i = mappingMin; i < mappingMax + 1; i++) {
					if (row.getCell(i) != null) {
						columnEnds[i - mappingMin] = row.getRowNum();
					}
				}
			}
		}
		
		columnOffset = -mappingMin;
	}

	@Override
	public boolean setCurrentSheet(int sheetNumber) {
		try {
			sheet = workbook.getSheetAt(sheetNumber);
			sheetName = sheet.getSheetName();
		} catch (IllegalArgumentException e) {
			return false;
		}
		
		if (mappingInfo.getOrientation() == SpreadsheetOrientation.VERTICAL) {
			lastLine = sheet.getLastRowNum();
			if (lastLine == 0 && sheet.getPhysicalNumberOfRows() == 0) {
				lastLine = -1;
			}
		} else {
			for (Row row : sheet) {
				if (row.getLastCellNum() - 1 > lastLine) {
					lastLine = row.getLastCellNum() - 1; 
				}
			}
		}
		
		overLastLine = false;
		stopParsing = false;
		exceptionBuffer.clear();
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
						rowResult.add(dataFormatter.formatCellValue(cell, FORMULA_EVAL, null));
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
		horizontal = mappingInfo.getOrientation() == SpreadsheetOrientation.HORIZONTAL;
		if (overFlow == null) {
			calculateColEnds(horizontal);
		}
		if (horizontal) {
			return parse(record, mappingMinRow, nextRecordStartRow, true);
			
		} else {
			return parse(record, nextRecordStartRow, mappingMinColumn, false);
		}
	}

	private DataRecord parse(DataRecord record, int recordStartRow, int startColumn, boolean horizontal) {
		for (int mappingRowIndex = 0; mappingRowIndex < mapping.length; mappingRowIndex++) {
			int[] recordRow = mapping[mappingRowIndex];
			int[] formatRecordRow = formatMapping != null ? formatMapping[mappingRowIndex] : null;
			Row row = sheet.getRow(recordStartRow + mappingRowIndex);
			
			if (row == null) {
				processNullRow(record, recordRow, recordStartRow, startColumn, mappingRowIndex);
				processNullRow(record, formatRecordRow, recordStartRow, startColumn, mappingRowIndex);
				continue;
			}
			
			int cloverFieldIndex;
			for (int column = startColumn; column < recordRow.length + startColumn; column++) {
				if ((cloverFieldIndex = recordRow[column - startColumn]) != XLSMapping.UNDEFINED) {
					fillCloverField(row.getCell(column), column, recordStartRow, mappingRowIndex, record, cloverFieldIndex, horizontal);					
				}
				if (formatRecordRow != null) {
					if ((cloverFieldIndex = formatRecordRow[column - startColumn]) != XLSMapping.UNDEFINED) {
						fillFormatField(row.getCell(column), record, cloverFieldIndex, column, horizontal);					
					}
				}
			}
		}

		if (overLastLine) {
			stopParsing = true;
			for (int i = 0; i < overFlow.length; i++) {
				stopParsing &= overFlow[i];
			}
		}
		exceptionBuffer.fireExceptions();
		nextRecordStartRow += mappingInfo.getStep();
		return stopParsing ? null : record;
	}

	private void processNullRow(DataRecord record, int[] recordRow, int recordStartRow, int startColumn, int mappingRowIndex) {
		int currentParseRow = recordStartRow + mappingRowIndex;
		SpreadsheetException se;		
		if (recordRow != null) {
			for (int column = 0; column < recordRow.length; column++) {
				int cloverFieldIndex;
				if ((cloverFieldIndex = recordRow[column]) != XLSMapping.UNDEFINED) {
					try {
						record.getField(cloverFieldIndex).setNull(true);
						if (currentParseRow > columnEnds[column]) {
							overFlow[cloverFieldIndex] = true;
						}
						if (currentParseRow > lastLine) {
							overLastLine = true;
							se = new SpreadsheetException("Unexpected end of sheet - expected another data row for field " + 
									record.getField(cloverFieldIndex).getMetadata().getName() + ". Occurred");
							exceptionBuffer.addExceptionInfo(new ExceptionInfo(se, record, cloverFieldIndex, fileName, sheetName,
									null, null, null, null));
						}
					} catch (BadDataFormatException e) {
						se = new SpreadsheetException("Unexpected end of sheet - expected another data row for field " + record.getField(cloverFieldIndex).getMetadata().getName() +
								". Moreover, cannot set default value or null", e);
						exceptionBuffer.addExceptionInfo(new ExceptionInfo(se, record, cloverFieldIndex, fileName, sheetName, 
								null, null, null, null));
					}
					
				}
			}
		}
	}

	private void fillCloverField(Cell cell, int cellColumn, int startRow, int offset, DataRecord record, int cloverFieldIndex, boolean horizontal) {
		SpreadsheetException se;
		int currentParseRow = startRow + offset;
		if (cell == null || cell.getCellType() == Cell.CELL_TYPE_BLANK) {
			try {
				record.getField(cloverFieldIndex).setNull(true);
				if (horizontal) {
					overFlow[cloverFieldIndex] = cellColumn > columnEnds[offset];
					if (cellColumn > lastLine) {
						overLastLine = true;
						se = new SpreadsheetException("Unexpected end of sheet - expected another data row for field " +
								record.getField(cloverFieldIndex).getMetadata().getName() + ". Occurred");
						exceptionBuffer.addExceptionInfo(new ExceptionInfo(se, record, cloverFieldIndex, fileName, sheetName, null, null, null, null));
					}
				} else {
					overFlow[cloverFieldIndex] = currentParseRow > columnEnds[cellColumn + columnOffset];
				}
				return;
			} catch (BadDataFormatException e) {
				se = new SpreadsheetException("There is no data cell for field. Moreover, cannot set default value or null", e);
				exceptionBuffer.addExceptionInfo(new ExceptionInfo(se, record, cloverFieldIndex, fileName, sheetName, null, null, null, null));
				return;
			}
		}

		char type = metadata.getField(cloverFieldIndex).getType();

		String expectedType = null;
		String cellFormat = null;
		try {
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
					String fieldLocale = metadata.getField(cloverFieldIndex).getLocaleStr();
					record.getField(cloverFieldIndex).fromString(dataFormatter.formatCellValue(cell, FORMULA_EVAL, fieldLocale));
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
				if (cell.getCellType() == Cell.CELL_TYPE_STRING || (cell.getCellType() == Cell.CELL_TYPE_FORMULA && cell.getCachedFormulaResultType() == Cell.CELL_TYPE_STRING)) {
					String cellStringValue = cell.getStringCellValue().trim();
					// If the actual cell value is empty string (after trimming), interpret it as null, otherwise try to set the string value to the field.
					if ("".equals(cellStringValue)) {
						record.getField(cloverFieldIndex).setNull(true);
					} else {
						record.getField(cloverFieldIndex).fromString(cellStringValue);
					}
				} else {
					throw e;
				}
			}
		} catch (RuntimeException e) {
			try {
				record.getField(cloverFieldIndex).setNull(true);
			} catch (Exception ex) {
			}
			String cellCoordinates = SpreadsheetUtils.getColumnReference(cell.getColumnIndex()) + String.valueOf(cell.getRowIndex());
			se = new SpreadsheetException("Cannot get " + expectedType + " value from cell of type " +
					cellTypeToString(cell.getCellType()) + " in " + cellCoordinates);
			String fieldLocale = metadata.getField(cloverFieldIndex).getLocaleStr();
			exceptionBuffer.addExceptionInfo(new ExceptionInfo(se, record, cloverFieldIndex, fileName, sheetName, 
					cellCoordinates, dataFormatter.formatCellValue(cell, FORMULA_EVAL, fieldLocale), cellTypeToString(cell.getCellType()), cellFormat));
		}
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
	
	private class ExceptionInfo {
		
		private SpreadsheetException se;
		private DataRecord record;
		private int fieldIndex;
		private String fileName;
		private String sheetName;		
		private String cellCoordinates;
		private String cellValue;
		private String cellType;
		private String cellFormat;
		
		public ExceptionInfo(SpreadsheetException se, DataRecord record, int fieldIndex, String fileName, String sheetName,
				String cellCoordinates, String cellValue, String cellType, String cellFormat) {
			this.se = se;
			this.record = record;
			this.fieldIndex = fieldIndex;
			this.fileName = fileName;
			this.sheetName = sheetName;
			this.cellCoordinates = cellCoordinates;
			this.cellValue = cellValue;
			this.cellType = cellType;
			this.cellFormat = cellFormat;
		}
		
		public SpreadsheetException getException() {
			return se;
		}
		
		public DataRecord getRecord() {
			return record;
		}
		
		public int getFieldIndex() {
			return fieldIndex;
		}
		
		public String getFileName() {
			return fileName;
		}
		
		public String getSheetName() {
			return sheetName;
		}
		
		public String getCellCoordinates() {
			return cellCoordinates;
		}
		
		public String getCellValue() {
			return cellValue;
		}
		
		public String getCellType() {
			return cellType;
		}
		
		public String getCellFormat() {
			return cellFormat;
		}
	}
	
	private class ExceptionBuffer {
		
		private ArrayDeque<ExceptionInfo> infoQueue = new ArrayDeque<ExceptionInfo>();
		
		public void addExceptionInfo(ExceptionInfo info) {
			this.infoQueue.add(info);			
		}
		
		public void fireExceptions() {
			ExceptionInfo ex;
			if (!stopParsing) {
				while (!infoQueue.isEmpty()) {
					ex = infoQueue.remove();
					handleException(ex.getException(), ex.getRecord(), ex.getFieldIndex(), ex.getFileName(), ex.getSheetName(), 
							ex.getCellCoordinates(), ex.getCellValue(), ex.getCellType(), ex.getCellFormat());
				}
			}
		}
		
		public void clear() {
			infoQueue.clear();
		}
		
	}
}
