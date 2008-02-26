
/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2005-06  Javlin Consulting <info@javlinconsulting.cz>
*    
*    This library is free software; you can redistribute it and/or
*    modify it under the terms of the GNU Lesser General Public
*    License as published by the Free Software Foundation; either
*    version 2.1 of the License, or (at your option) any later version.
*    
*    This library is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU    
*    Lesser General Public License for more details.
*    
*    You should have received a copy of the GNU Lesser General Public
*    License along with this library; if not, write to the Free Software
*    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*
*/

package org.jetel.data.parser;

import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

import jxl.BooleanCell;
import jxl.Cell;
import jxl.CellType;
import jxl.DateCell;
import jxl.NumberCell;
import jxl.Sheet;
import jxl.Workbook;
import jxl.WorkbookSettings;

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.formatter.XLSFormatter;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.NumberIterator;
import org.jetel.util.file.WcardPattern;
import org.jetel.util.string.StringUtils;

/**
 * Parsing data from xls file using JExcelAPI.
 * 
 * @author avackova (agata.vackova@javlinconsulting.cz) ; 
 * (c) JavlinConsulting s.r.o.
 *  www.javlinconsulting.cz
 *
 * @since Jan 16, 2007
 *
 */
public class JExcelXLSDataParser extends XLSParser {
	
	private Workbook wb;
	private Sheet sheet;
	private Cell cell;
	private String charset = null;
	private short sheetCounter;
	private boolean releaseInputSource = true;
	
	/**
	 * Default constructor
	 */
	public JExcelXLSDataParser() {
		charset = Defaults.DataParser.DEFAULT_CHARSET_DECODER;
	}
	
	/**
	 * @param charset
	 */
	public JExcelXLSDataParser(String charset) {
		this.charset = charset;
	}

	@Override
	public void close() {
		if (wb != null) {
			wb.close();
		}
	}

	@Override
	protected void cloverfieldsAndXlsNames(Map fieldNames)
			throws ComponentNotReadyException {
		if (cloverFields.length!=xlsFields.length){
			throw new ComponentNotReadyException("Number of clover fields and xls fields must be the same");
		}
		//getting metadata row
		Cell[] row = sheet.getRow(metadataRow);
		int count = 0;
		//go through each not empty cell
		for (int i=0;i<row.length;i++){
			cell = row[i];
			String cellValue = cell.getContents();
			int xlsNumber = StringUtils.findString(cellValue,xlsFields);
			if (xlsNumber > -1){//string from cell found in xlsFields attribute
				fieldNumber[count][XLS_NUMBER] = cell.getColumn();
				try {
					fieldNumber[count][CLOVER_NUMBER] = (Integer)fieldNames.get(cloverFields[xlsNumber]);
				}catch (NullPointerException ex) {
					throw new ComponentNotReadyException("Clover field \""
							+ cloverFields[xlsNumber] + "\" not found");
				}
				count++;
			}else{
				logger.warn("There is no field corresponding to \"" + cellValue + "\" in output metadata");
			}
		}
		if (count<cloverFields.length){
			logger.warn("Not all fields found");
		}
	}

	@Override
	public String[] getNames() {
		ArrayList<String> names = new ArrayList<String>();
		if (metadataRow > -1) {
			Cell[] row = sheet.getRow(metadataRow);
			//go through each not empty cell
			for (int i=0;i<row.length;i++){
				cell = row[i];
				names.add(XLSFormatter.getCellCode(cell.getColumn()) + " - " +
						cell.getContents());
			}
		}else{
			Cell[] row = sheet.getRow(firstRow);
			for (int i=0;i<row.length;i++){
				cell = row[i];
				names.add(XLSFormatter.getCellCode(cell.getColumn()) + " - " +
						cell.getContents().substring(0, Math.min(
								cell.getContents().length(), MAX_NAME_LENGTH)));
			}
		}
		return names.toArray(new String[0]);
	}
	
	public String[][] getPreview(int length){
		if (sheet == null) return null;
		
		String[][] result = new String[length][];
		Cell[] row;
		String cellContents;
		for (int i=0; i<length; i++){
			row = sheet.getRow(i);
			result[i] = new String[row.length];
			for (int j = 0; j < row.length; j++) {
				cellContents = row[j].getContents();
				result[i][j] = cellContents.substring(0, Math.min(cellContents.length(), MAX_NAME_LENGTH));
				if (cellContents.length() > MAX_NAME_LENGTH) {
					result[i][j] += "...";
				}
			}
		}
		return result;
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.parser.XLSParser#createMetadata()
	 */
	@Override
	public DataRecordMetadata createMetadata(){
		if (wb == null) return null;
		String name = sheet.getName();
		if (!StringUtils.isValidObjectName(name)) {
			name = StringUtils.normalizeName(name);
		}
		DataRecordMetadata xlsMetadata = new DataRecordMetadata(name, DataRecordMetadata.DELIMITED_RECORD);
		Cell[] namesRow;
		if (metadataRow > -1) {
			 namesRow = sheet.getRow(metadataRow);
		}else{
			metadataRow = -1;
  			namesRow = sheet.getRow(firstRow);
		}
		Cell[] dataRow = sheet.getRow(firstRow);
		//go through each cell
		CellType type;
		DataFieldMetadata field;
		Cell nameCell, dataCell;
		for (int i=0;i<namesRow.length;i++){
			nameCell = namesRow[i];
			dataCell = dataRow[i];
			type = dataCell.getType();
			name = (metadataRow>-1) ? nameCell.getContents() : XLSFormatter.getCellCode(i);
			if (!StringUtils.isValidObjectName(name)) {
				name = StringUtils.normalizeName(name);
			}
			if (type == CellType.EMPTY && namesRow != dataRow && nameCell.getType() == CellType.EMPTY){
				continue;
			}else if (type == CellType.BOOLEAN) {
				field = new DataFieldMetadata(name, DataFieldMetadata.BOOLEAN_FIELD, 
						i < namesRow.length -1 ? DEFAULT_FIELD_DELIMITER : DEFAULT_LAST_FIELD_DELIMITER);
				xlsMetadata.addField(field);
			}else if (type == CellType.DATE){
				field = new DataFieldMetadata(name, DataFieldMetadata.DATE_FIELD,  
						i < namesRow.length -1 ? DEFAULT_FIELD_DELIMITER : DEFAULT_LAST_FIELD_DELIMITER);
				field.setFormatStr(((SimpleDateFormat)((DateCell)dataCell).getDateFormat()).toPattern());
				xlsMetadata.addField(field);
			}else if (type == CellType.NUMBER) {
				field = new DataFieldMetadata(name, DataFieldMetadata.NUMERIC_FIELD,  
						i < namesRow.length -1 ? DEFAULT_FIELD_DELIMITER : DEFAULT_LAST_FIELD_DELIMITER);
				field.setFormatStr(((DecimalFormat)((NumberCell)dataCell).getNumberFormat()).toPattern());
				xlsMetadata.addField(field);
			}else{
				field = new DataFieldMetadata(name, DataFieldMetadata.STRING_FIELD,  
						i < namesRow.length -1 ? DEFAULT_FIELD_DELIMITER : DEFAULT_LAST_FIELD_DELIMITER);
				xlsMetadata.addField(field);
			}
		}
		return xlsMetadata;
	}
	
	@Override
	protected void mapNames(Map fieldNames) throws ComponentNotReadyException {
		//getting metadata row
		Cell[] row = sheet.getRow(metadataRow);
		int count = 0;
		//go through each not empty cell
		for (int i=0;i<row.length;i++){
			cell = row[i];
			String cellValue = cell.getContents();
			if (fieldNames.containsKey(cellValue)){//corresponding field in metadata found
				fieldNumber[count][XLS_NUMBER] = cell.getColumn();
				fieldNumber[count][CLOVER_NUMBER] = (Integer)fieldNames.get(cellValue);
				fieldNames.remove(cellValue);
				count++;
			}else{
				logger.warn("There is no field \"" + cellValue + "\" in output metadata");
			}
		}
		if (count<metadata.getNumFields()){
			logger.warn("Not all fields found:");
			for (Iterator i=fieldNames.keySet().iterator();i.hasNext();){
				logger.warn(i.next());
			}
		}
	}

	@Override
	protected DataRecord parseNext(DataRecord record) throws JetelException {
		if (currentRow>=lastRow) return null;
		char type;
		for (short i=0;i<fieldNumber.length;i++){
			// skip all fields that are internally filled 
			if (isAutoFilling[i]) {
				continue;
			}
			if (fieldNumber[i][CLOVER_NUMBER] == -1) continue; //in metdata there is not any field corresponding to this column
			try {
				cell = sheet.getCell(fieldNumber[i][XLS_NUMBER], currentRow);
			} catch (ArrayIndexOutOfBoundsException e1) {
				//more fields in metadata then in xls file
				record.getField(fieldNumber[i][CLOVER_NUMBER]).setNull(true);
				continue;
			}
			type = metadata.getField(fieldNumber[i][CLOVER_NUMBER]).getType();
			try{
				switch (type) {
				case DataFieldMetadata.DATE_FIELD:
				case DataFieldMetadata.DATETIME_FIELD:
					record.getField(fieldNumber[i][CLOVER_NUMBER]).setValue(
							((DateCell)cell).getDate());
					break;
				case DataFieldMetadata.BYTE_FIELD:
				case DataFieldMetadata.STRING_FIELD:
					record.getField(fieldNumber[i][CLOVER_NUMBER]).fromString(
							cell.getContents());
					break;
				case DataFieldMetadata.DECIMAL_FIELD:
				case DataFieldMetadata.INTEGER_FIELD:
				case DataFieldMetadata.LONG_FIELD:
				case DataFieldMetadata.NUMERIC_FIELD:
					record.getField(fieldNumber[i][CLOVER_NUMBER]).setValue(
							((NumberCell)cell).getValue());
					break;
				case DataFieldMetadata.BOOLEAN_FIELD:
					record.getField(fieldNumber[i][CLOVER_NUMBER]).setValue(
							((BooleanCell)cell).getValue());
					break;
				}
			} catch (ClassCastException bdne) {//exception when trying get date or number from diffrent cell type
				try {
					record.getField(fieldNumber[i][CLOVER_NUMBER]).fromString(
							cell.getContents());
				} catch (Exception e) {
					BadDataFormatException bdfe = new BadDataFormatException(bdne.getMessage());
					bdfe.setRecordNumber(currentRow+1);
					bdfe.setFieldNumber(fieldNumber[i][CLOVER_NUMBER]);
					if (exceptionHandler != null) { // use handler only if configured
						exceptionHandler.populateHandler(getErrorMessage(bdfe
								.getMessage(), currentRow + 1, fieldNumber[i][CLOVER_NUMBER]),
								record, currentRow + 1, fieldNumber[i][CLOVER_NUMBER],
								cell.getContents(), bdfe);
					} else {
						throw new RuntimeException(getErrorMessage(bdfe
								.getMessage(), currentRow + 1, fieldNumber[i][CLOVER_NUMBER]));
					}
				}
			}
		}
		currentRow++;
		recordCounter++;
		return record;
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#setDataSource(java.lang.Object)
	 */
	public void setReleaseDataSource(boolean releaseInputSource)  {
		this.releaseInputSource = releaseInputSource;
	}

	@Override
	public void setDataSource(Object inputDataSource)
			throws ComponentNotReadyException {
		if (releaseInputSource && wb != null) {
			wb.close();
			sheetNumberIterator = null;
		}
        WorkbookSettings settings = new WorkbookSettings();
		settings.setEncoding(charset);
		InputStream input;
		if (inputDataSource instanceof InputStream) {
			input = (InputStream)inputDataSource;
		}else{
			input = Channels.newInputStream((ReadableByteChannel)inputDataSource);
		}
		//creating workbook from input stream 
        try {
            wb = Workbook.getWorkbook(input,settings);
       }catch(Exception ex){
            throw new ComponentNotReadyException(ex);
        }
        currentRow = firstRow;
         sheetCounter = -1;
         if (sheetNumber != null){
        	 sheetNumberIterator = new NumberIterator(sheetNumber,0,Integer.MAX_VALUE);
         }
        if (!getNextSheet()) {
        	throw new ComponentNotReadyException("There is no sheet conforming sheet name nor sheet number pattern");
        }
        logger.info("Reading data from sheet " + sheetCounter + " (" + 
           		sheet.getName() + ")." );
		if (metadata != null) {
        	fieldNumber = new int[metadata.getNumFields()][2];
        	mapFields();
        }
	}
	
	public void reset() {
		if (wb != null) {
			wb.close();
		}
        currentRow = firstRow;
        recordCounter = 1;
        sheetCounter = -1;
        if (sheetNumber != null){
        	sheetNumberIterator.reset();
        }
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.data.parser.XLSParser#getNextSheet()
	 */
	@Override
	public boolean getNextSheet() {
    	if (sheetNumberIterator != null){//get next sheet conforming sheetNumber attribute
    		if (!sheetNumberIterator.hasNext()){
    			return false;
    		}
    		sheetCounter = sheetNumberIterator.next().shortValue();
    		try{
    			sheet = wb.getSheet(sheetCounter);
    		}catch(IndexOutOfBoundsException e){
    			return false;
    		}
    	}else{//get next sheet conforming sheetName attribute
    		boolean found = false;
    		while (!found){
    			try {
					sheet = wb.getSheet(++sheetCounter);
				} catch (IndexOutOfBoundsException e) {
					return false;
				}
				if (WcardPattern.checkName(sheetName, sheet.getName())) {
					found = true;
				}
    		}
    	}
        currentRow = firstRow;
        //set last row to read on set attribute or to last row in current sheet
		if (lastRowAttribute == -1 || lastRowAttribute > sheet.getRows()) {
			lastRow = sheet.getRows();
		}else{
			lastRow = lastRowAttribute;
		}
		return true;
	}
	
	@Override
	public boolean getSheet(int sheetNumber){
		if (sheetNumber >= wb.getNumberOfSheets()) return false;
		sheet = wb.getSheet(sheetNumber);
		return true;
	}

	@Override
	public boolean getSheet(String sheetName){
		sheet = wb.getSheet(sheetName);
		return sheet != null;
	}

	public String getCharset() {
		return charset;
	}

	public void setCharset(String charset){
		this.charset = charset;
	}

	@Override
	public String[] getSheets() {
		if (wb == null) return null;
		return wb.getSheetNames();
	}

	@Override
	public String getSheetName(int index) {
		if (wb == null) return null;
		return wb.getSheet(index).getName();
	}
	
}
