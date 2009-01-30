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
package org.jetel.data.formatter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLConnection;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.naming.InvalidNameException;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.DataFormat;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.primitive.Decimal;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.string.StringUtils;
import org.openxml4j.exceptions.InvalidFormatException;
import org.openxml4j.opc.Package;

/**
 * Represents a XLSX data formatter based on the Apache POI library.
 *
 * @author Martin Janik <martin.janik@javlin.cz>
 *
 * @version 30th January 2009
 * @since 30th January 2009
 */
public final class XLSXDataFormatter extends XLSFormatter {

	/**
	 * A structure used to save states when multiple sheet writing is enabled.
	 *
	 * @author Martin Janik <martin.janik@javlin.cz>
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

	/** the value specifying that no data format is used/set */
	public static final String GENERAL_FORMAT_STRING = "General";

	/** the currently open workbook */
	private Workbook workbook;
	/** the sheet that is being used */
	private Sheet sheet;
	/** the cell styles for the written cells */
	private CellStyle[] cellStyles;

	/** the map of sheet data used for multiple sheets writing */
	private Map<String, SheetData> sheetData;
	/** the name of the currently used sheet */
	private String currentSheetName;
	/** the sheet data associated with the currently used sheet */
	private SheetData currentSheetData;
	/** the current row index used for writing */
	private int currentRowIndex;

	/** the output stream used to output the workbook */
	private OutputStream outputStream;

	/**
	 * Creates a XLSX data formatter.
	 *
	 * @param append determines whether the new data should be appended to the old data or replace them
	 */
	public XLSXDataFormatter(boolean append) {
		super(append);
	}

	public void setDataTarget(Object dataTarget) {
		if (dataTarget == null) {
			throw new NullPointerException("dataTarget");
		}

		close();

		//
		// prepare a workbook based on the type of the output data target
		//

		try {
			if (dataTarget instanceof URL) {
				URL url = (URL) dataTarget;

				if (url.getProtocol().equals(FILE_PROTOCOL)) {
					File file = new File(url.getFile());

					workbook = (file.length() > 0) ? new XSSFWorkbook(file.getPath()) : new XSSFWorkbook();
					outputStream = new FileOutputStream(file);
				} else {
					URLConnection urlConnection = url.openConnection();

					InputStream inputStream = urlConnection.getInputStream();
					workbook = (inputStream.available() > 0) ? new XSSFWorkbook(Package.open(inputStream)) : new XSSFWorkbook();
					inputStream.close();

					outputStream = urlConnection.getOutputStream();
				}
			} else if (dataTarget instanceof WritableByteChannel) {
				workbook = new XSSFWorkbook();
				outputStream = Channels.newOutputStream((WritableByteChannel) dataTarget);
			} else {
				throw new IllegalArgumentException(dataTarget.getClass() + " not supported as a data target");
			}
		} catch (IOException exception) {
			throw new IllegalArgumentException("Error opening/writing the XLS(X) workbook!", exception);
		} catch (InvalidFormatException exception) {
			throw new IllegalArgumentException("The XLSX workbook has invalid format!", exception);
		}

		//
		// set up the formatter for writing multiple sheets
		//

		if (!StringUtils.isEmpty(sheetName) && sheetName.startsWith(CLOVER_FIELD_PREFIX)) {
        	String[] fields = sheetName.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);

			for (int i = 0; i < fields.length; i++) {
				fields[i] = fields[i].substring(1);
			}

			setKeyFields(fields);
			sheetData = new HashMap<String, SheetData>();
        }
	}

	@Override
	public void prepareSheet() {
		String tempSheetName = (currentSheetName != null) ? currentSheetName : sheetName;

		//
		// get a sheet if it exists or create a new one
		//

		if (!StringUtils.isEmpty(tempSheetName)){
			sheet = workbook.getSheet(tempSheetName);

			if (sheet == null) {
				sheet = workbook.createSheet(tempSheetName);
			} else if (!append) {
				Iterator<Row> rowIterator = sheet.iterator();

				while (rowIterator.hasNext()) {
					rowIterator.next();
					rowIterator.remove();
				}
			}
		} else if (sheetNumber > -1) {
			if (sheetNumber >= workbook.getNumberOfSheets()) {
				throw new IndexOutOfBoundsException("sheetNumber >= " + workbook.getNumberOfSheets());
			}

			sheet = workbook.getSheetAt(sheetNumber);
		} else {
			sheet = workbook.createSheet();
		}

		//
		// prepare cell styles
		//

		cellStyles = new CellStyle[metadata.getNumFields()];
		DataFormat dataFormat = workbook.createDataFormat();

		for (int i = 0; i < metadata.getNumFields(); i++) {
			DataFieldMetadata fieldMetadata = metadata.getField(i);

			CellStyle cellStyle = workbook.createCellStyle();
			cellStyle.setDataFormat(dataFormat.getFormat((fieldMetadata.getFormatStr() != null)
					? fieldMetadata.getFormatStr() : GENERAL_FORMAT_STRING));

			cellStyles[i] = cellStyle;
		}

		//
		// determine the first column
		//

		try {
			firstColumn = XLSFormatter.getCellNum(firstColumnIndex);
		} catch (InvalidNameException exception) {
			throw new IllegalArgumentException("firstColumnIndex", exception);
		}

		//
		// determine the correct row and write the names row
		//

		currentRowIndex = append ? sheet.getLastRowNum() + 1 : 0;

		if (namesRow > -1) {
			if (!append || sheet.getLastRowNum() < 0) {
				Row row = sheet.getRow(namesRow);

				if (row == null) {
					row = sheet.createRow(namesRow);
				}

				CellStyle cellStyle = workbook.createCellStyle();

				Font boldFont = workbook.createFont();
				boldFont.setBoldweight(Font.BOLDWEIGHT_BOLD);

				cellStyle.setFont(boldFont);

				for (int i = 0; i < metadata.getNumFields(); i++) {
					Cell newCell = row.createCell(firstColumn + i);
					newCell.setCellValue(metadata.getField(i).getName());
					newCell.setCellStyle(cellStyle);
				}

				currentRowIndex = namesRow + 1;
			}
		}

		if (firstRow > currentRowIndex) {
			currentRowIndex = firstRow;
		}

		//
		// save the current sheet
		//

		if (sheetData != null) {
			currentSheetData = new SheetData(sheet, currentRowIndex);
			sheetData.put(tempSheetName, currentSheetData);
		}
	}

	@Override
	public void prepareSheet(DataRecord dataRecord) {
		if (dataRecord == null) {
			throw new NullPointerException("dataRecord");
		}

		currentSheetName = sheetNameKeyRecord.getKeyString(dataRecord);

		if (sheetData.containsKey(currentSheetName)) {
			currentSheetData = sheetData.get(currentSheetName);
			currentRowIndex = currentSheetData.currentRow;
		} else {
			prepareSheet();
		}
	}

	public int write(DataRecord dataRecord) throws IOException {
		if (dataRecord == null) {
			throw new NullPointerException("dataRecord");
		}

		if (sheetData != null) {
			prepareSheet(dataRecord);
			sheet = currentSheetData.sheet;
		} else if (sheet == null) {
			prepareSheet();
		}

		Row newRow = sheet.createRow(currentRowIndex);

		for (int i = 0; i < metadata.getNumFields(); i++) {
			Object fieldValue = dataRecord.getField(i).getValue();

			if (fieldValue == null) {
				continue;
			}

			Cell newCell = newRow.createCell(firstColumn + i);
			newCell.setCellStyle(cellStyles[i]);

			switch (metadata.getField(i).getType()) {
				case DataFieldMetadata.BYTE_FIELD:
				case DataFieldMetadata.BYTE_FIELD_COMPRESSED:
				case DataFieldMetadata.STRING_FIELD:
					newCell.setCellValue(fieldValue.toString());
					break;
				case DataFieldMetadata.DATE_FIELD:
				case DataFieldMetadata.DATETIME_FIELD:
					newCell.setCellValue((Date) fieldValue);
					break;
				case DataFieldMetadata.INTEGER_FIELD:
					newCell.setCellValue((Integer) fieldValue);
					break;
				case DataFieldMetadata.LONG_FIELD:
					newCell.setCellValue((Long) fieldValue);
					break;
				case DataFieldMetadata.DECIMAL_FIELD:
					newCell.setCellValue(((Decimal) fieldValue).getDouble());
					break;
				case DataFieldMetadata.NUMERIC_FIELD:
					newCell.setCellValue((Double) fieldValue);
					break;
				case DataFieldMetadata.BOOLEAN_FIELD:
					newCell.setCellValue((Boolean) fieldValue);
					break;
			}
		}

		currentRowIndex++;

		if (sheetData != null) {
			currentSheetData.currentRow++;
		}
        
		return 0;
	}

	public void flush() throws IOException {
		// do nothing, this functionality is not supported
	}

	public void close() {
		if (workbook != null) {
			if (metadata.getRecType() == DataRecordMetadata.DELIMITED_RECORD) {
				for (SheetData aSheetData : sheetData.values()) {
					for (int i = 0; i < metadata.getNumFields(); i++) {
						aSheetData.sheet.autoSizeColumn(firstColumn + i);
					}
				}
			}

			try {
				workbook.write(outputStream);
			} catch (IOException exception) {
				logger.error("Error closing the output stream!", exception);
			}

			reset();
		}
	}

	public void reset() {
		workbook = null;
		sheet = null;
		cellStyles = null;

		sheetData = null;
		currentSheetName = null;
		currentSheetData = null;
		currentRowIndex = 0;

		outputStream = null;
	}

}
