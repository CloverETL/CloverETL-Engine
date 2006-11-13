
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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

import javax.naming.InvalidNameException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFDataFormat;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.jetel.data.DataRecord;
import org.jetel.data.formatter.XLSDataFormatter;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.IParserExceptionHandler;
import org.jetel.exception.JetelException;
import org.jetel.exception.PolicyType;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.StringUtils;

/**
 * Parsing data from xls file.
 * 
/**
* @author avackova <agata.vackova@javlinconsulting.cz> ; 
* (c) JavlinConsulting s.r.o.
*	www.javlinconsulting.cz
*	@created October 10, 2006
 */
public class XLSDataParser implements Parser {
	
	public final static int NO_METADATA_INFO = 0;
	public final static int ONLY_CLOVER_FIELDS = 1;
	public final static int CLOVER_FIELDS_AND_XLS_NUMBERS = 2;
	public final static int MAP_NAMES = 3;
	public final static int CLOVER_FIELDS_AND_XLS_NAMES = 4;
	
	static Log logger = LogFactory.getLog(XLSDataParser.class);
	
	private DataRecordMetadata metadata = null;
	private IParserExceptionHandler exceptionHandler;
	private String sheetName = null;
	private int sheetNumber = 0;
	private int recordCounter;
	private int firstRow = 0;
	private int currentRow;
	private int lastRow;
	private HSSFWorkbook wb;
	private HSSFSheet sheet;
	private HSSFRow row;
	private HSSFCell cell;
	private HSSFDataFormat format;
	private int metadataRow = -1;
	private String[] cloverFields = null;
	private String[] xlsFields = null;
	int mappingType = -1;
	private int[][] fieldNumber ; //mapping of xls fields and clover fields
	
	private final int XLS_NUMBER = 0;
	private final int CLOVER_NUMBER = 1;
	
	private final static int MAX_NAME_LENGTH = 10;

	/**
	 * Constructor
	 */
	public XLSDataParser() {
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#getNext()
	 */
	public DataRecord getNext() throws JetelException {
		// create a new data record
		DataRecord record = new DataRecord(metadata);

		record.init();

		record = parseNext(record);
		if(exceptionHandler != null ) {  //use handler only if configured
			while(exceptionHandler.isExceptionThrowed()) {
                exceptionHandler.handleException();
				record = parseNext(record);
			}
		}
		return record;
	}
	
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
	private DataRecord parseNext(DataRecord record) throws JetelException {
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
					record.getField(fieldNumber[i][CLOVER_NUMBER]).setValue(cell.getDateCellValue());
					break;
				case DataFieldMetadata.BYTE_FIELD:
				case DataFieldMetadata.STRING_FIELD:
					record.getField(fieldNumber[i][CLOVER_NUMBER]).fromString(getStringFromCell(cell));
					break;
				case DataFieldMetadata.DECIMAL_FIELD:
				case DataFieldMetadata.INTEGER_FIELD:
				case DataFieldMetadata.LONG_FIELD:
				case DataFieldMetadata.NUMERIC_FIELD:
					record.getField(fieldNumber[i][CLOVER_NUMBER]).setValue(cell.getNumericCellValue());
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

	/**
	 * Assembles error message when exception occures during parsing
	 * 
	 * @param exceptionMessage
	 *            message from exception getMessage() call
	 * @param recNo
	 *            recordNumber
	 * @param fieldNo
	 *            fieldNumber
	 * @return error message
	 */
	private String getErrorMessage(String exceptionMessage, int recNo, int fieldNo) {
		StringBuffer message = new StringBuffer();
		message.append(exceptionMessage);
		message.append(" when parsing record #");
		message.append(recordCounter);
		message.append(" field ");
		message.append(metadata.getField(fieldNo).getName());
		return message.toString();
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#skip(int)
	 */
	public int skip(int nRec) throws JetelException {
		currentRow+=nRec;
		return nRec;
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#init(org.jetel.metadata.DataRecordMetadata)
	 */
	public void init(DataRecordMetadata _metadata)throws ComponentNotReadyException{
		this.metadata = _metadata;
	}
	
    /* (non-Javadoc)
     * @see org.jetel.data.parser.Parser#setDataSource(java.lang.Object)
     */
    public void setDataSource(Object in) throws ComponentNotReadyException {
        recordCounter = 1;
        //creating workbook from input stream 
        try {
            wb = new HSSFWorkbook((InputStream)in);
        }catch(IOException ex){
            throw new ComponentNotReadyException(ex);
        }
        //setting sheet for reading data
        if (sheetName!=null){
            sheet = wb.getSheet(sheetName);
        }else{
            try {
                sheet = wb.getSheetAt(sheetNumber);
            }catch(IndexOutOfBoundsException ex){
                throw new ComponentNotReadyException("There is no sheet with number \"" +   sheetNumber +"\"");
            }
        }
        if (sheet == null) {
            throw new ComponentNotReadyException("There is no sheet with name \"" + sheetName +"\"");
        }
        format = wb.createDataFormat();
        currentRow = firstRow;
        lastRow = sheet.getLastRowNum();
        if (metadata != null) {
        	mapFields();
        }
    }

    /**
     * Method for mapping metadata with columns in xls
     * 
     */
    private void mapFields() throws ComponentNotReadyException{
        for (int i=0;i<fieldNumber.length;i++){
            fieldNumber[i][CLOVER_NUMBER] = -1;
        }
        Map fieldNames = metadata.getFieldNames();
        switch (mappingType) {
        case NO_METADATA_INFO:noMetadataInfo();break;
        case ONLY_CLOVER_FIELDS:onlyCloverFields(fieldNames);break;
        case CLOVER_FIELDS_AND_XLS_NUMBERS:cloverFieldsAndXlsNumbers(fieldNames);break;
        case MAP_NAMES:mapNames(fieldNames);break;
        case CLOVER_FIELDS_AND_XLS_NAMES:cloverfieldsAndXlsNames(fieldNames);break;
        default:noMetadataInfo();break;
        }
    	
    }
    
	/**
	 * If any of the metadata attribute wasn't set cell order coresponds with field order in metadata
	 */
	private void noMetadataInfo(){
		for (short i=0;i<fieldNumber.length;i++){
			fieldNumber[i][XLS_NUMBER] = i;
			fieldNumber[i][CLOVER_NUMBER] = i;
		}
	}
	
	/**
	 * If clover fields are set but xls fields are not set cells are read in order of clover fields
	 * 
	 * @param fieldNames
	 * @throws ComponentNotReadyException
	 */
	private void onlyCloverFields(Map fieldNames) throws ComponentNotReadyException{
		for (int i = 0; i < cloverFields.length; i++) {
			fieldNumber[i][XLS_NUMBER] = i;
			try {
				fieldNumber[i][CLOVER_NUMBER] = (Integer) fieldNames.get(cloverFields[i]);
			} catch (NullPointerException ex) {
				throw new ComponentNotReadyException("Clover field \""
						+ cloverFields[i] + "\" not found");
			}
		}
	}
	
	/**
	 * If clover fields and xls colums are set there is made mapping between coresponding fields and cells
	 * 
	 * @param fieldNames
	 * @throws ComponentNotReadyException
	 */
	private void cloverFieldsAndXlsNumbers(Map fieldNames) throws ComponentNotReadyException {
		if (cloverFields.length!=xlsFields.length){
			throw new ComponentNotReadyException("Number of clover fields and xls fields must be the same");
		}
		for (short i=0;i<cloverFields.length;i++){
			int cellNumber;
			try {
				cellNumber = XLSDataFormatter.getCellNum(xlsFields[i]);
			}catch(InvalidNameException ex){
				throw new ComponentNotReadyException(ex);
			}
			fieldNumber[i][XLS_NUMBER] = cellNumber;
			try {
				fieldNumber[i][CLOVER_NUMBER] = (Integer)fieldNames.get(cloverFields[i]);
			}catch (NullPointerException ex) {
				throw new ComponentNotReadyException("Clover field \""
						+ cloverFields[i] + "\" not found");
			}
		}
	}
	
	/**
	 * If clover fields and xls colums are set there is made mapping between coresponding fields and cells
	 * 
	 * @param fieldNames
	 * @throws ComponentNotReadyException
	 */
	private void cloverfieldsAndXlsNames(Map fieldNames)throws ComponentNotReadyException{
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
	private void mapNames(Map fieldNames) throws ComponentNotReadyException {
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
	 * @see org.jetel.data.parser.Parser#getNext(org.jetel.data.DataRecord)
	 */
	public DataRecord getNext(DataRecord record) throws JetelException {
		record = parseNext(record);
		if(exceptionHandler != null ) {  //use handler only if configured
			while(exceptionHandler.isExceptionThrowed()) {
                exceptionHandler.handleException();
				record = parseNext(record);
			}
		}
		return record;
	}

	public void setExceptionHandler(IParserExceptionHandler handler) {
        this.exceptionHandler = handler;
	}

	public IParserExceptionHandler getExceptionHandler() {
        return exceptionHandler;
	}

	public PolicyType getPolicyType() {
        if(exceptionHandler != null) {
            return exceptionHandler.getType();
        }
        return null;
	}

	public void setSheetName(String sheetName) {
		this.sheetName = sheetName;
	}

	public void setFirstRow(int firstRecord) {
		this.firstRow = firstRecord;
	}

	public int getFirstRow() {
		return firstRow;
	}

	public int getSheetNumber() {
		return sheetNumber;
	}

	public int getRecordCount() {
		return recordCounter;
	}

	public void setCloverFields(String[] cloverFields) {
		this.cloverFields = cloverFields;
	}

	public void setMetadataRow(int metadataRow) throws ComponentNotReadyException{
		if (metadataRow < 0) 
			throw new ComponentNotReadyException("Number of metadata row has to be greter then 0");
		this.metadataRow = metadataRow;
		if (firstRow == 0) {
			firstRow = this.metadataRow +1;
		}
	}

	public void setXlsFields(String[] xlsFields) {
		this.xlsFields = xlsFields;
	}

	public void setSheetNumber(int sheetNumber) {
		this.sheetNumber = sheetNumber;
	}

	public void setMappingType(int mappingType) {
		this.mappingType = mappingType;
		switch (mappingType) {
		case NO_METADATA_INFO:logger.info("Mapping type set to NO_METADATA_INFO");break;
		case ONLY_CLOVER_FIELDS:logger.info("Mapping type set to ONLY_CLOVER_FIELDS");break;
		case CLOVER_FIELDS_AND_XLS_NUMBERS:logger.info("Mapping type set to CLOVER_FIELDS_AND_XLS_NUMBERS");break;
		case MAP_NAMES:logger.info("Mapping type set to MAP_NAMES");break;
		case CLOVER_FIELDS_AND_XLS_NAMES:logger.info("Mapping type set to CLOVER_FIELDS_AND_XLS_NAMES");break;
		}
	}

	public int getMetadataRow() {
		return metadataRow;
	}

	public String getSheetName() {
		return sheetName;
	}
	
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
