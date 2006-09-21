
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
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFDataFormat;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.jetel.data.DataRecord;
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
 * @author avackova
 *
 */
public class XLSDataParser implements Parser {
	
	private final static int CELL_NUMBER_IN_SHEET = 25;//number of "Z" cell in excel sheet
	
	private final static int NO_METADATA_INFO = 0;
	private final static int ONLY_CLOVER_FIELDS = 1;
	private final static int CLOVER_FIELDS_AND_XLS_NUMBERS = 2;
	private final static int MAP_NAMES = 3;
	private final static int CLOVER_FIELDS_AND_XLS_NAMES = 4;
	
	static Log logger = LogFactory.getLog(XLSDataParser.class);
	
	private DataRecordMetadata metadata;
	private IParserExceptionHandler exceptionHandler;
	private String sheetName = null;
	private int recordCounter;
	private int firstRow = 0;
	private int currentRow;
	private HSSFWorkbook wb;
	private HSSFSheet sheet;
	private HSSFRow row;
	private HSSFCell cell;
	private HSSFDataFormat format;
	private int metadataRow = -1;
	private String[] cloverFields = null;
	private String[] xlsFields = null;
	private int[] fieldNumber ;

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
		row = sheet.getRow(currentRow);
		if (row==null) return null; //afterlast row
		char type;
		for (short i=0;i<fieldNumber.length;i++){
			if (fieldNumber[i] == -1) continue; //in metdata there is not any field corresponding to this column
 			cell = row.getCell(i);
			type = metadata.getField(fieldNumber[i]).getType();
			try{
				switch (type) {
				case DataFieldMetadata.DATE_FIELD:
					record.getField(fieldNumber[i]).setValue(cell.getDateCellValue());
					break;
				case DataFieldMetadata.BYTE_FIELD:
				case DataFieldMetadata.STRING_FIELD:
					record.getField(fieldNumber[i]).fromString(getStringFromCell(cell));
					break;
				case DataFieldMetadata.DECIMAL_FIELD:
				case DataFieldMetadata.INTEGER_FIELD:
				case DataFieldMetadata.LONG_FIELD:
				case DataFieldMetadata.NUMERIC_FIELD:
					record.getField(fieldNumber[i]).setValue(cell.getNumericCellValue());
					break;
				}
			} catch (NumberFormatException bdne) {//exception when trying get date or number from not numeric cell
				BadDataFormatException bdfe = new BadDataFormatException(bdne.getMessage());
				bdfe.setRecordNumber(currentRow+1);
				bdfe.setFieldNumber(fieldNumber[i]);
				String cellValue = getStringFromCell(cell);
				try {
					record.getField(fieldNumber[i]).fromString(cellValue);
				} catch (Exception e) {
					if (exceptionHandler != null) { // use handler only if configured
						exceptionHandler.populateHandler(getErrorMessage(bdfe
								.getMessage(), currentRow + 1, fieldNumber[i]),
								record, currentRow + 1, fieldNumber[i],
								cellValue, bdfe);
					} else {
						throw new RuntimeException(getErrorMessage(bdfe
								.getMessage(), currentRow + 1, fieldNumber[i]));
					}
				}
			}catch (NullPointerException np){// empty cell
				try {
					record.getField(fieldNumber[i]).setNull(true);
				}catch (BadDataFormatException ex){
					BadDataFormatException bdfe = new BadDataFormatException(np.getMessage());
					bdfe.setRecordNumber(currentRow+1);
					bdfe.setFieldNumber(fieldNumber[i]);
					if(exceptionHandler != null ) {  //use handler only if configured
		                exceptionHandler.populateHandler(
		                		getErrorMessage(bdfe.getMessage(), currentRow+1, fieldNumber[i]), 
		                		record,	currentRow + 1, fieldNumber[i], "null", bdfe);
					} else {
						throw new RuntimeException(getErrorMessage(bdfe.getMessage(), 
								currentRow + 1, fieldNumber[i]));
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
	 * @see org.jetel.data.parser.Parser#open(java.lang.Object, org.jetel.metadata.DataRecordMetadata)
	 */
	public void open(Object in, DataRecordMetadata _metadata)throws ComponentNotReadyException{
		this.metadata = _metadata;
		recordCounter = 1;
		//creating workbook from input stream 
		try {
			wb = new HSSFWorkbook((InputStream)in);
		}catch(IOException ex){
			throw new ComponentNotReadyException(ex);
		}
		if (sheetName!=null){
			sheet = wb.getSheet(sheetName);
		}else{
			sheet = wb.getSheetAt(0);
		}
		format = wb.createDataFormat();
		currentRow = firstRow;
		fieldNumber = new int[metadata.getNumFields()];
		Arrays.fill(fieldNumber,-1);
		Map fieldNames = metadata.getFieldNames();
		switch (getMappingType(metadataRow,cloverFields!=null,xlsFields!=null)) {
		case NO_METADATA_INFO:
			for (short i=0;i<fieldNumber.length;i++){
				fieldNumber[i] = i;
			}
			break;
		case ONLY_CLOVER_FIELDS:
			for (int i = 0; i < cloverFields.length; i++) {
				try {
					fieldNumber[i] = ((Integer) fieldNames.get(cloverFields[i]));
				} catch (NullPointerException ex) {
					throw new ComponentNotReadyException("Clover field \""
							+ cloverFields[i] + "\" not found");
				}
			}
			break;
		case CLOVER_FIELDS_AND_XLS_NUMBERS:
			if (cloverFields.length!=xlsFields.length){
				throw new ComponentNotReadyException("Number of clover fields and xls fields must be the same");
			}
			for (short i=0;i<cloverFields.length;i++){
				String cellCode = xlsFields[i].toUpperCase(); 
				int cellNumber  = 0;
				for (int j=0;j<cellCode.length();j++){
					cellNumber+=cellCode.charAt(j);
				}
				cellNumber+=CELL_NUMBER_IN_SHEET*(cellCode.length()-1) - 'A'*cellCode.length();
				try {
					fieldNumber[cellNumber] = 
						((Integer)fieldNames.get(cloverFields[i])).shortValue();
				}catch (NullPointerException ex) {
					throw new ComponentNotReadyException("Clover field \""
							+ cloverFields[i] + "\" not found");
				}
			}
			break;
		case MAP_NAMES:
			row = sheet.getRow(metadataRow);
			int count = 0;
			for (Iterator i=row.cellIterator();i.hasNext();){
				cell = (HSSFCell)i.next();
				String cellValue = cell.getStringCellValue();
				if (fieldNames.containsKey(cellValue)){
					fieldNumber[cell.getCellNum()] = ((Integer)fieldNames.get(cellValue)).shortValue();
					fieldNames.remove(cellValue);
				}else{
					logger.warn("There is no field \"" + cellValue + "\" in output metadata");
				}
				count++;
			}
			if (count<metadata.getNumFields()){
				short lastCell = row.getLastCellNum();
				for (short i=0;i<fieldNumber.length;i++){
					if (fieldNumber[i] == -1) {
						fieldNumber[i] = lastCell++;
					}
				}
			}
			break;
		case CLOVER_FIELDS_AND_XLS_NAMES:
			if (cloverFields.length!=xlsFields.length){
				throw new ComponentNotReadyException("Number of clover fields and xls fields must be the same");
			}
			row = sheet.getRow(metadataRow);
			count = 0;
			for (Iterator i=row.cellIterator();i.hasNext();){
				cell = (HSSFCell)i.next();
				String cellValue = cell.getStringCellValue();
				int xlsNumber = StringUtils.findString(cellValue,xlsFields);
				if (xlsNumber > -1){
					try {
						fieldNumber[cell.getCellNum()] = ((Integer)fieldNames.get(cloverFields[xlsNumber])).shortValue();
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
			break;
		}
	}
	
	private int getMappingType(int metadataRow,boolean cloverFields,boolean xlsFields){
		if (metadataRow == -1 && !cloverFields && !xlsFields){
			return NO_METADATA_INFO;
		}
		if (metadataRow == -1 && !xlsFields){
			return ONLY_CLOVER_FIELDS;
		}
		if (metadataRow == -1){
			return CLOVER_FIELDS_AND_XLS_NUMBERS;
		}
		if (!cloverFields && !xlsFields){
			return MAP_NAMES;
		}
		if (cloverFields && xlsFields){
			return CLOVER_FIELDS_AND_XLS_NAMES;
		}
		return -1;
	}
	
	public void close() {
		// TODO Auto-generated method stub
	}

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
		this.firstRow = firstRecord-1;
	}

	public int getRecordCount() {
		return recordCounter;
	}

	public void setCloverFields(String[] cloverFields) {
		this.metadataRow = 0;
		this.cloverFields = cloverFields;
	}

	public void setMetadataRow(int metadataRow) {
		this.metadataRow = metadataRow - 1;
		if (firstRow == 0) {
			firstRow = this.metadataRow +1;
		}
	}

	public void setXlsFields(String[] xlsFields, boolean names) {
		this.xlsFields = xlsFields;
		this.metadataRow = 0;
	}

}
