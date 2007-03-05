
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

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFDataFormat;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.jetel.data.DataRecord;
import org.jetel.data.formatter.XLSDataFormatter;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.util.NumberIterator;
import org.jetel.util.StringUtils;
import org.jetel.util.WcardPattern;

/**
 * Parsing data from xls file using POI library.
 * 
/**
* @author avackova <agata.vackova@javlinconsulting.cz> ; 
* (c) JavlinConsulting s.r.o.
*	www.javlinconsulting.cz
*	@created October 10, 2006
 */
public class XLSDataParser extends XLSParser {
	
	private HSSFWorkbook wb;
	private HSSFSheet sheet;
	private HSSFRow row;
	private HSSFCell cell;
	private HSSFDataFormat format;
	private short sheetCounter;
	
	/**
	 * This method gets string representation of cell value 
	 * 
	 * @param cell
	 * @return
	 */
	private String getStringFromCell(HSSFCell cell) {
		short formatNumber = cell.getCellStyle().getDataFormat();
		String pattern = format.getFormat(formatNumber);
		String cellValue = "";
		switch (cell.getCellType()){
		case HSSFCell.CELL_TYPE_BOOLEAN: cellValue = String.valueOf(cell.getBooleanCellValue());
			break;
		case HSSFCell.CELL_TYPE_STRING: cellValue = cell.getStringCellValue();
			break;
		case HSSFCell.CELL_TYPE_FORMULA: cellValue = cell.getCellFormula();
			break;
		case HSSFCell.CELL_TYPE_ERROR: cellValue = String.valueOf(cell.getErrorCellValue());
			break;
		case HSSFCell.CELL_TYPE_NUMERIC: //in numeric cell can be date ...
			if (pattern.contains("M")||pattern.contains("D")||pattern.contains("Y")){
				cellValue = cell.getDateCellValue().toString();
			}else{//... or number
				cellValue = String.valueOf(cell.getNumericCellValue());
			}
			break;
		}
		return cellValue;
	}

	/**
	 * This method gets the next record from the sheet
	 * 
	 * @param record
	 * @return
	 * @throws JetelException
	 */
	protected DataRecord parseNext(DataRecord record) throws JetelException {
		if (currentRow>lastRow) return null;
		row = sheet.getRow(currentRow);
		char type;
		for (short i=0;i<fieldNumber.length;i++){
			if (fieldNumber[i][CLOVER_NUMBER] == -1) continue; //in metdata there is not any field corresponding to this column
 			cell = row.getCell((short)fieldNumber[i][XLS_NUMBER]);
			type = metadata.getField(fieldNumber[i][CLOVER_NUMBER]).getType();
			try{
				switch (type) {
				case DataFieldMetadata.DATE_FIELD:
				case DataFieldMetadata.DATETIME_FIELD:
					record.getField(fieldNumber[i][CLOVER_NUMBER]).setValue(
							cell.getDateCellValue());
					break;
				case DataFieldMetadata.BYTE_FIELD:
				case DataFieldMetadata.STRING_FIELD:
					record.getField(fieldNumber[i][CLOVER_NUMBER]).fromString(
							getStringFromCell(cell));
					break;
				case DataFieldMetadata.DECIMAL_FIELD:
				case DataFieldMetadata.INTEGER_FIELD:
				case DataFieldMetadata.LONG_FIELD:
				case DataFieldMetadata.NUMERIC_FIELD:
					record.getField(fieldNumber[i][CLOVER_NUMBER]).setValue(
							cell.getNumericCellValue());
					break;
				}
			} catch (NumberFormatException bdne) {//exception when trying get date or number from not numeric cell
				BadDataFormatException bdfe = new BadDataFormatException(bdne.getMessage());
				bdfe.setRecordNumber(currentRow+1);
				bdfe.setFieldNumber(fieldNumber[i][CLOVER_NUMBER]);
				String cellValue = getStringFromCell(cell);
				try {
					record.getField(fieldNumber[i][CLOVER_NUMBER]).fromString(cellValue);
				} catch (Exception e) {
					if (exceptionHandler != null) { // use handler only if configured
						exceptionHandler.populateHandler(getErrorMessage(bdfe
								.getMessage(), currentRow + 1, fieldNumber[i][CLOVER_NUMBER]),
								record, currentRow + 1, fieldNumber[i][CLOVER_NUMBER],
								cellValue, bdfe);
					} else {
						throw new RuntimeException(getErrorMessage(bdfe
								.getMessage(), currentRow + 1, fieldNumber[i][CLOVER_NUMBER]));
					}
				}
			}catch (NullPointerException np){// empty cell
				try {
					record.getField(fieldNumber[i][CLOVER_NUMBER]).setNull(true);
				}catch (BadDataFormatException ex){
					BadDataFormatException bdfe = new BadDataFormatException(np.getMessage());
					bdfe.setRecordNumber(currentRow+1);
					bdfe.setFieldNumber(fieldNumber[i][CLOVER_NUMBER]);
					if(exceptionHandler != null ) {  //use handler only if configured
		                exceptionHandler.populateHandler(
		                		getErrorMessage(bdfe.getMessage(), currentRow+1, fieldNumber[i][CLOVER_NUMBER]), 
		                		record,	currentRow + 1, fieldNumber[i][CLOVER_NUMBER], "null", bdfe);
					} else {
						throw new RuntimeException(getErrorMessage(bdfe.getMessage(), 
								currentRow + 1, fieldNumber[i][CLOVER_NUMBER]));
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
    public void setDataSource(Object in) throws ComponentNotReadyException {
		InputStream input;
		if (in instanceof InputStream) {
			input = (InputStream)in;
		}else{
			input = Channels.newInputStream((ReadableByteChannel)in);
		}
      //creating workbook from input stream 
        try {
            wb = new HSSFWorkbook(input);
        }catch(IOException ex){
            throw new ComponentNotReadyException(ex);
        }
        format = wb.createDataFormat();
         sheetCounter = -1;
         if (sheetNumber != null){
        	 sheetNumberIterator = new NumberIterator(sheetNumber,0,Integer.MAX_VALUE);
         }
        if (!getNextSheet()) {
        	throw new ComponentNotReadyException("There is no sheet conforming sheet name nor sheet number pattern");
        }
        logger.info("Reading data from sheet " + sheetCounter + " (" + 
        		wb.getSheetName(sheetCounter) + ")." );
		if (metadata != null) {
        	fieldNumber = new int[metadata.getNumFields()][2];
        	mapFields();
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
    			sheet = wb.getSheetAt(sheetCounter);
    		}catch(IndexOutOfBoundsException e){
    			return false;
    		}
    	}else{//get next sheet conforming sheetName attribute
    		boolean found = false;
    		while (!found){
    			try {
					sheet = wb.getSheetAt(++sheetCounter);
				} catch (IndexOutOfBoundsException e) {
					return false;
				}
				if (WcardPattern.checkName(sheetName, wb.getSheetName(sheetCounter))) {
					found = true;
				}
    		}
    	}
        currentRow = firstRow;
        //set last row to read on set attribute or to last row in current sheet
		if (lastRowAttribute == -1 || lastRowAttribute > sheet.getLastRowNum()) {
			lastRow = sheet.getLastRowNum();
		}else{
			lastRow = lastRowAttribute;
		}
		return true;
    }
	
	/**
	 * If clover fields and xls colums are set there is made mapping between coresponding fields and cells
	 * 
	 * @param fieldNames
	 * @throws ComponentNotReadyException
	 */
	protected void cloverfieldsAndXlsNames(Map fieldNames)throws ComponentNotReadyException{
		if (cloverFields.length!=xlsFields.length){
			throw new ComponentNotReadyException("Number of clover fields and xls fields must be the same");
		}
		//getting metadata row
		row = sheet.getRow(metadataRow);
		int count = 0;
		//go through each not empty cell
		for (Iterator i=row.cellIterator();i.hasNext();){
			cell = (HSSFCell)i.next();
			String cellValue = getStringFromCell(cell);
			int xlsNumber = StringUtils.findString(cellValue,xlsFields);
			if (xlsNumber > -1){//string from cell found in xlsFields attribute
				fieldNumber[count][XLS_NUMBER] = cell.getCellNum();
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
	
	/**
	 * If there is given metadata row but other metedata attributes are empty we try find
	 * fields in metadata with the same names as names in xls sheet
	 * 
	 * @param fieldNames
	 * @throws ComponentNotReadyException
	 */
	protected void mapNames(Map fieldNames) throws ComponentNotReadyException {
		//getting metadata row
		row = sheet.getRow(metadataRow);
		int count = 0;
		//go through each not empty cell
		for (Iterator i=row.cellIterator();i.hasNext();){
			cell = (HSSFCell)i.next();
			String cellValue = getStringFromCell(cell);
			if (fieldNames.containsKey(cellValue)){//corresponding field in metadata found
				fieldNumber[count][XLS_NUMBER] = cell.getCellNum();
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
	
	public void close() {
		// TODO Auto-generated method stub
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.parser.XLSParser#getNames()
	 */
	public String[] getNames(){
		ArrayList<String> names = new ArrayList<String>();
		if (metadataRow > -1) {
			row = sheet.getRow(metadataRow);
			//go through each not empty cell
			for (Iterator i=row.cellIterator();i.hasNext();){
				cell = (HSSFCell)i.next();
				names.add(XLSDataFormatter.getCellCode(cell.getCellNum()) + " - " +
						getStringFromCell(cell));
			}
		}else{
			row = sheet.getRow(firstRow);
			String cellString; 
			for (Iterator i=row.cellIterator();i.hasNext();){
				cell = (HSSFCell)i.next();
				cellString = getStringFromCell(cell);
				names.add(XLSDataFormatter.getCellCode(cell.getCellNum()) + " - " +
						cellString.substring(0, Math.min(cellString.length(), MAX_NAME_LENGTH)));
			}
		}
		return names.toArray(new String[0]);
	}

}

