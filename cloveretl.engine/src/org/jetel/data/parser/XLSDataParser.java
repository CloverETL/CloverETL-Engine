
package org.jetel.data.parser;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
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
import org.jetel.data.Defaults;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.IParserExceptionHandler;
import org.jetel.exception.JetelException;
import org.jetel.exception.PolicyType;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.StringUtils;

public class XLSDataParser implements Parser {
	
	private static int CELL_NUMBER_IN_SHEET = 25;

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
	private boolean names;
	private short[] fieldNumber ;

	public XLSDataParser() {
	}

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
		case HSSFCell.CELL_TYPE_NUMERIC: 
			if (pattern.contains("M")||pattern.contains("D")||pattern.contains("Y")){
				cellValue = cell.getDateCellValue().toString();
			}else{
				cellValue = String.valueOf(cell.getNumericCellValue());
			}
			break;
		}
		return cellValue;
	}

	private DataRecord parseNext(DataRecord record) throws JetelException {
		row = sheet.getRow(currentRow);
		if (row==null) return null;
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
			} catch (NumberFormatException bdne) {
				BadDataFormatException bdfe = new BadDataFormatException(bdne.getMessage());
				bdfe.setRecordNumber(currentRow+1);
				bdfe.setFieldNumber(fieldNumber[i]);
				if(exceptionHandler != null ) {  //use handler only if configured
					String cellValue = getStringFromCell(cell);
					try{
						record.getField(fieldNumber[i]).fromString(cellValue);
					}catch (Exception e) {
		                exceptionHandler.populateHandler(
		                		getErrorMessage(bdfe.getMessage(), currentRow+1, fieldNumber[i]), 
		                		record, currentRow + 1, fieldNumber[i], cellValue, bdfe);
					}
				} else {
					throw new RuntimeException(getErrorMessage(bdfe.getMessage(), 
							recordCounter, fieldNumber[i]));
				}
			}catch (NullPointerException np){
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
								recordCounter, fieldNumber[i]));
					}
				}
			}
		}
		currentRow++;
		recordCounter++;
		return record;
	}

	private String getErrorMessage(String exceptionMessage, int recNo, int fieldNo) {
		StringBuffer message = new StringBuffer();
		message.append(exceptionMessage);
		message.append(" when parsing record #");
		message.append(recordCounter);
		message.append(" field ");
		message.append(metadata.getField(fieldNo).getName());
		return message.toString();
	}
	
	public int skip(int nRec) throws JetelException {
		currentRow+=nRec;
		return nRec;
	}

	public void open(Object in, DataRecordMetadata _metadata)throws ComponentNotReadyException{
		this.metadata = _metadata;
		recordCounter = 1;
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
		fieldNumber = new short[metadata.getNumFields()];
		if (metadataRow == -1){
			for (short i=0;i<fieldNumber.length;i++){
				fieldNumber[i] = i;
			}
		}else{
			Arrays.fill(fieldNumber,(short)-1);
			Map fieldNames = metadata.getFieldNames();
			if (cloverFields == null){
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
			}else if (xlsFields == null){
				for (short i = 0; i < cloverFields.length; i++) {
					try {
						fieldNumber[i] = ((Integer) fieldNames
								.get(cloverFields[i])).shortValue();
					} catch (NullPointerException ex) {
						throw new ComponentNotReadyException("Clover field \""
								+ cloverFields[i] + "\" not found");
					}
				}
			}else{
				if (cloverFields.length!=xlsFields.length){
					throw new ComponentNotReadyException("Number of clover fields and xls fields must be the same");
				}
				if (!names){
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
				}else{
					row = sheet.getRow(metadataRow);
					int count = 0;
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
				}
			}
		}
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
		this.names = names;
		this.metadataRow = 0;
	}

}
