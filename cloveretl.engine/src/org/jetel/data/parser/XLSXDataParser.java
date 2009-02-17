/*
 * jETeL/Clover.ETL - Java based ETL application framework.
 * Copyright (C) 2002-2008  David Pavlis <david.pavlis@javlin.cz>
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
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package org.jetel.data.parser;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.jetel.data.DataRecord;
import org.jetel.data.formatter.XLSFormatter;
import org.jetel.data.formatter.XLSXDataFormatter;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.NumberIterator;
import org.jetel.util.file.WcardPattern;
import org.jetel.util.string.StringUtils;
import org.openxml4j.exceptions.InvalidFormatException;
import org.openxml4j.opc.Package;

/**
 * Represents a XLSX data parser based on the Apache POI library.
 *
 * @author Martin Janik <martin.janik@javlin.cz>
 *
 * @version 31st January 2009
 * @since 30th January 2009
 */
public final class XLSXDataParser extends XLSParser {

	/** the workbook parsed by this parser */
	private Workbook workbook;
	/** currently parsed sheet */
	private Sheet sheet;

	@Override
	public void setDataSource(Object dataSource) throws ComponentNotReadyException {
		if (dataSource == null) {
			throw new NullPointerException("dataSource");
		}

		InputStream dataInputStream = null;

		if (dataSource instanceof InputStream) {
			dataInputStream = (InputStream) dataSource;
		} else if (dataSource instanceof ReadableByteChannel) {
			dataInputStream = Channels.newInputStream((ReadableByteChannel) dataSource);
		} else {
			throw new IllegalArgumentException(dataSource.getClass() + " not supported as a data source");
		}

		try {
			workbook = new XSSFWorkbook(Package.open(dataInputStream));
		} catch (IOException exception) {
			throw new ComponentNotReadyException("Error opening the XLSX workbook!", exception);
		} catch (InvalidFormatException exception) {
			throw new ComponentNotReadyException("The XLSX workbook has invalid format!", exception);
		} finally {
			if (releaseDataSource) {
				try {
					dataInputStream.close();
				} catch (IOException exception) {
					throw new ComponentNotReadyException("Error releasing the data source!", exception);
				}
			}
		}

		if (sheetName == null && sheetNumber != null) {
			sheetNumberIterator = new NumberIterator(sheetNumber, 0, Integer.MAX_VALUE);
		}

		sheet = null;
		sheetCounter = -1;
		currentRow = firstRow;

		if (!getNextSheet()) {
			throw new ComponentNotReadyException("There is no sheet conforming sheet name nor sheet number pattern");
		}

        if (metadata != null) {
			fieldNumber = new int[metadata.getNumFields()][2];
			mapFields();
		}
	}

	@Override
	protected boolean getNextSheet() {
		if (sheet != null) {
			if (incremental == null) {
				incremental = new Incremental();
			}

			incremental.setRow(workbook.getSheetName(sheetCounter), currentRow);
		}

		if (sheetNumberIterator != null) {
			//
			// get the next sheet corresponding to the value of the sheetNumber attribute
			//

			if (!sheetNumberIterator.hasNext()) {
				return false;
			}

			sheetCounter = sheetNumberIterator.next().shortValue();

			if (sheetCounter >= workbook.getNumberOfSheets()) {
				return false;
			}

			sheet = workbook.getSheetAt(sheetCounter);
		} else {
			//
			// get the next sheet corresponding to the value of the sheetName attribute
			//

			while (++sheetCounter < workbook.getNumberOfSheets()) {
				if (WcardPattern.checkName(sheetName, workbook.getSheetName(sheetCounter))) {
					sheet = workbook.getSheetAt(sheetCounter);
					break;
				}
			}

			if (sheetCounter >= workbook.getNumberOfSheets()) {
				return false;
			}
		}

		currentRow = firstRow;

		// set last row to read on set attribute or to last row in current sheet
		lastRow = (lastRowAttribute == -1 || lastRowAttribute >= sheet.getLastRowNum())
				? sheet.getLastRowNum() + 1 : lastRowAttribute;

		discardBytes(workbook.getSheetName(sheetCounter));
        logger.info("Reading data from sheet " + sheetCounter + " (" + workbook.getSheetName(sheetCounter) + ").");

		return true;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected void cloverfieldsAndXlsNames(Map fieldNames) throws ComponentNotReadyException {
		if (fieldNames == null) {
			throw new NullPointerException("fieldNames");
		}

		if (cloverFields.length != xlsFields.length) {
			throw new ComponentNotReadyException("Number of clover fields and XLSX fields must be the same");
		}

		Row row = sheet.getRow(metadataRow);
		int numberOfFoundFields = 0;

		for (int i = 0; i < row.getLastCellNum(); i++) {
			Cell cell = row.getCell(i);

			if (cell != null) {
				String cellValue = cell.getStringCellValue();
				int xlsNumber = StringUtils.findString(cellValue, xlsFields);
	
				if (xlsNumber > -1) {// string from cell found in xlsFields attribute
					fieldNumber[numberOfFoundFields][XLS_NUMBER] = i;
	
					try {
						fieldNumber[numberOfFoundFields][CLOVER_NUMBER] = (Integer) fieldNames.get(cloverFields[xlsNumber]);
					}catch (NullPointerException ex) {
						throw new ComponentNotReadyException("Clover field \"" + cloverFields[xlsNumber] + "\" not found");
					}
	
					numberOfFoundFields++;
				} else {
					logger.warn("There is no field corresponding to \"" + cellValue + "\" in output metadata");
				}
			}
		}

		if (numberOfFoundFields < cloverFields.length) {
			logger.warn("Not all fields found");
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	protected void mapNames(Map fieldNames) throws ComponentNotReadyException {
		if (fieldNames == null) {
			throw new NullPointerException("fieldNames");
		}

		if (cloverFields.length != xlsFields.length) {
			throw new ComponentNotReadyException("Number of clover fields and XLSX fields must be the same");
		}

		Row row = sheet.getRow(metadataRow);
		int numberOfFoundFields = 0;

		for (int i = 0; i < row.getLastCellNum(); i++) {
			Cell cell = row.getCell(i);

			if (cell != null) {
				String cellValue = cell.getStringCellValue();
	
				if (fieldNames.containsKey(cellValue)) {// corresponding field in metadata found
					fieldNumber[numberOfFoundFields][XLS_NUMBER] = i;
					fieldNumber[numberOfFoundFields][CLOVER_NUMBER] = (Integer) fieldNames.get(cellValue);
					numberOfFoundFields++;
	
					fieldNames.remove(cellValue);
				} else {
					logger.warn("There is no field \"" + cellValue + "\" in output metadata");
				}
			}
		}

		if (numberOfFoundFields < metadata.getNumFields()) {
			logger.warn("Not all fields found:");

			for (Object fieldName : fieldNames.keySet()) {
				logger.warn(fieldName);
			}
		}
	}

	@Override
	public String[] getNames() {
		List<String> names = new ArrayList<String>();
		Row row = (metadataRow > -1) ? sheet.getRow(metadataRow) : sheet.getRow(firstRow);

		for (int i = 0; i < row.getLastCellNum(); i++) {
			Cell cell = row.getCell(i);

			if (cell != null) {
				String cellValue = cell.getStringCellValue();
				names.add(XLSFormatter.getCellCode(i) + " - "
						+ cellValue.substring(0, Math.min(cellValue.length(), MAX_NAME_LENGTH)));
			}
		}

		return names.toArray(new String[names.size()]);
	}

	@Override
	public DataRecordMetadata createMetadata() {
		if (workbook == null) {
			return null;
		}

		String sheetName = workbook.getSheetName(sheetCounter);

		if (!StringUtils.isValidObjectName(sheetName)) {
			sheetName = StringUtils.normalizeName(sheetName);
		}

		DataRecordMetadata xlsMetadata = new DataRecordMetadata(sheetName, DataRecordMetadata.DELIMITED_RECORD);
		xlsMetadata.setFieldDelimiter(DEFAULT_FIELD_DELIMITER);
		xlsMetadata.setRecordDelimiter(DEFAULT_RECORD_DELIMITER);

		Row namesRow = (metadataRow > -1) ? sheet.getRow(metadataRow) : sheet.getRow(firstRow);
		Row dataRow = sheet.getRow(firstRow);
		int maxNumberOfColumns = Math.max(namesRow.getLastCellNum(), dataRow.getLastCellNum());

		for (int i = 0; i < maxNumberOfColumns; i++) {
			Cell nameCell = (i < namesRow.getLastCellNum()) ? namesRow.getCell(i) : null;
			Cell dataCell = (i < dataRow.getLastCellNum()) ? dataRow.getCell(i) : null;

			int cellType = (dataCell != null) ? dataCell.getCellType() : Cell.CELL_TYPE_STRING;

			if (namesRow != dataRow && nameCell.getCellType() == Cell.CELL_TYPE_BLANK && cellType == Cell.CELL_TYPE_BLANK) {
				continue;
			}

			String cellName = (metadataRow > -1 && nameCell != null) ? nameCell.getStringCellValue() : XLSFormatter.getCellCode(i);

			if (!StringUtils.isValidObjectName(cellName)) {
				cellName = StringUtils.normalizeName(cellName);
			}

			DataFieldMetadata dataField = null;

			if (cellType == Cell.CELL_TYPE_BOOLEAN) {
				dataField = new DataFieldMetadata(cellName, DataFieldMetadata.BOOLEAN_FIELD, null);
			} else if (cellType == Cell.CELL_TYPE_NUMERIC) {
				dataField = new DataFieldMetadata(cellName, DateUtil.isCellDateFormatted(dataCell)
						? DataFieldMetadata.DATE_FIELD : DataFieldMetadata.NUMERIC_FIELD, null);
				String formatString = dataCell.getCellStyle().getDataFormatString();

				if (!formatString.equals(XLSXDataFormatter.GENERAL_FORMAT_STRING)) {
					dataField.setFormatStr(formatString);
				}
			} else {
				dataField = new DataFieldMetadata(cellName, DataFieldMetadata.STRING_FIELD, null);
			}

			xlsMetadata.addField(dataField);
		}

		return xlsMetadata;
	}

	@Override
	public String[] getSheets() {
		if (workbook == null) {
			return null;
		}

		String[] sheetNames = new String[workbook.getNumberOfSheets()];

		for (int i = 0; i < sheetNames.length; i++) {
			sheetNames[i] = workbook.getSheetName(i);
		}

		return sheetNames;
	}

	@Override
	public String getSheetName(int index) {
		if (workbook == null) {
			return null;
		}

		return workbook.getSheetName(index);
	}

	@Override
	public boolean getSheet(int sheetNumber) {
		if (sheetNumber >= workbook.getNumberOfSheets()) {
			return false;
		}

		sheet = workbook.getSheetAt(sheetNumber);

		return true;
	}

	@Override
	public boolean getSheet(String sheetName) {
		sheet = workbook.getSheet(sheetName);

		return (sheet != null);
	}

	@Override
	protected DataRecord parseNext(DataRecord record) throws JetelException {
		if (record == null) {
			throw new NullPointerException("record");
		}

		if (currentRow >= lastRow) {
			return null;
		}

		Row row = sheet.getRow(currentRow);

		for (short i = 0; i < fieldNumber.length; i++) {
			// skip fields that are internally filled 
			// skip fields with no metadata attached
			if (isAutoFilling[i] || fieldNumber[i][CLOVER_NUMBER] == -1) {
				continue;
			}

			if (fieldNumber[i][XLS_NUMBER] > row.getLastCellNum()) {
				record.getField(fieldNumber[i][CLOVER_NUMBER]).setNull(true);
				continue;
			}

			Cell cell = row.getCell(fieldNumber[i][CLOVER_NUMBER]);

			if (cell == null) {
				continue;
			}

			char type = metadata.getField(fieldNumber[i][CLOVER_NUMBER]).getType();

			try {
				switch (type) {
					case DataFieldMetadata.DATE_FIELD:
					case DataFieldMetadata.DATETIME_FIELD:
						record.getField(fieldNumber[i][CLOVER_NUMBER]).setValue(cell.getDateCellValue());
						break;
					case DataFieldMetadata.BYTE_FIELD:
					case DataFieldMetadata.STRING_FIELD:
						record.getField(fieldNumber[i][CLOVER_NUMBER]).fromString(cell.getStringCellValue());
						break;
					case DataFieldMetadata.DECIMAL_FIELD:
					case DataFieldMetadata.INTEGER_FIELD:
					case DataFieldMetadata.LONG_FIELD:
					case DataFieldMetadata.NUMERIC_FIELD:
						record.getField(fieldNumber[i][CLOVER_NUMBER]).setValue(cell.getNumericCellValue());
						break;
					case DataFieldMetadata.BOOLEAN_FIELD:
						record.getField(fieldNumber[i][CLOVER_NUMBER]).setValue(cell.getBooleanCellValue());
						break;
				}
			} catch (ClassCastException exception) {// exception when trying get date or number from a different cell type
				try {
					record.getField(fieldNumber[i][CLOVER_NUMBER]).fromString(cell.getStringCellValue());
				} catch (Exception ex) {
					BadDataFormatException bdfe = new BadDataFormatException(exception.getMessage());
					bdfe.setRecordNumber(currentRow + 1);
					bdfe.setFieldNumber(fieldNumber[i][CLOVER_NUMBER]);

					if (exceptionHandler != null) { // use handler only if configured
						exceptionHandler.populateHandler(getErrorMessage(bdfe.getMessage(), currentRow + 1,
								fieldNumber[i][CLOVER_NUMBER]), record, currentRow + 1, fieldNumber[i][CLOVER_NUMBER],
								cell.getStringCellValue(), bdfe);
					} else {
						throw new RuntimeException(getErrorMessage(bdfe.getMessage(), currentRow + 1,
								fieldNumber[i][CLOVER_NUMBER]));
					}
				}
			}
		}

		currentRow++;
		recordCounter++;

		return record;
	}

	@Override
	public void reset() throws ComponentNotReadyException {
		super.reset();

		workbook = null;
        sheet = null;
	}

	@Override
	public void close() {
		workbook = null;
        sheet = null;
	}

}
