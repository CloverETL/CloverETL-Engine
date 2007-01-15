
package org.jetel.data.parser;

import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;

import jxl.Cell;
import jxl.DateCell;
import jxl.NumberCell;
import jxl.Sheet;
import jxl.Workbook;
import jxl.WorkbookSettings;

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.util.StringUtils;

public class JExcelXLSDataParser extends XLSParser {
	
	private Workbook wb;
	private Sheet sheet;
	private Cell cell;
	private String charset = null;
	
	public JExcelXLSDataParser() {
		charset = Defaults.DataParser.DEFAULT_CHARSET_DECODER;
	}
	
	public JExcelXLSDataParser(String charset) {
		this.charset = charset;
	}

	@Override
	public void close() {
		wb.close();
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
		// TODO Auto-generated method stub
		return null;
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
//		row = sheet.getRow(currentRow);
		char type;
		for (short i=0;i<fieldNumber.length;i++){
			if (fieldNumber[i][CLOVER_NUMBER] == -1) continue; //in metdata there is not any field corresponding to this column
			cell = sheet.getCell(fieldNumber[i][XLS_NUMBER], currentRow);
// 			cell = row.getCell((short)fieldNumber[i][XLS_NUMBER]);
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
//			catch (NullPointerException np){// empty cell
//				try {
//					record.getField(fieldNumber[i][CLOVER_NUMBER]).setNull(true);
//				}catch (BadDataFormatException ex){
//					BadDataFormatException bdfe = new BadDataFormatException(np.getMessage());
//					bdfe.setRecordNumber(currentRow+1);
//					bdfe.setFieldNumber(fieldNumber[i][CLOVER_NUMBER]);
//					if(exceptionHandler != null ) {  //use handler only if configured
//		                exceptionHandler.populateHandler(
//		                		getErrorMessage(bdfe.getMessage(), currentRow+1, fieldNumber[i][CLOVER_NUMBER]), 
//		                		record,	currentRow + 1, fieldNumber[i][CLOVER_NUMBER], "null", bdfe);
//					} else {
//						throw new RuntimeException(getErrorMessage(bdfe.getMessage(), 
//								currentRow + 1, fieldNumber[i][CLOVER_NUMBER]));
//					}
//				}
//			}
		}
		currentRow++;
		recordCounter++;
		return record;
	}

	@Override
	public void setDataSource(Object inputDataSource)
			throws ComponentNotReadyException {
        recordCounter = 1;
        WorkbookSettings settings = new WorkbookSettings();
		settings.setEncoding(charset);
		//creating workbook from input stream 
        try {
            wb = Workbook.getWorkbook((InputStream)inputDataSource,settings);
       }catch(Exception ex){
            throw new ComponentNotReadyException(ex);
        }
        //setting sheet for reading data
        if (sheetName!=null){
            sheet = wb.getSheet(sheetName);
        }else{
            try {
                sheet = wb.getSheet(sheetNumber);
            }catch(IndexOutOfBoundsException ex){
                throw new ComponentNotReadyException("There is no sheet with number \"" +   sheetNumber +"\"");
            }
        }
        if (sheet == null) {
            throw new ComponentNotReadyException("There is no sheet with name \"" + sheetName +"\"");
        }
        currentRow = firstRow;
        lastRow = sheet.getRows();
        if (metadata != null) {
        	fieldNumber = new int[metadata.getNumFields()][2];
        	mapFields();
        }
	}

	public String getCharset() {
		return charset;
	}

}
