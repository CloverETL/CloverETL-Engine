
package org.jetel.data.parser;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
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

public class XLSDataParser implements Parser {

	static Log logger = LogFactory.getLog(XLSDataParser.class);
	
	private CharsetDecoder decoder;
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
	private int metadataRow = -1;
	private String[] metadataNames = null;
	private short[] fieldNumber;

	public XLSDataParser() {
		decoder = Charset.forName(Defaults.DataParser.DEFAULT_CHARSET_DECODER).newDecoder();	
	}

	public XLSDataParser(String charsetDecoder) {
		decoder = Charset.forName(charsetDecoder).newDecoder();
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
		HSSFDataFormat format= wb.createDataFormat();
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
			cell = row.getCell(fieldNumber[i]);
			type = metadata.getField(i).getType();
			try{
				switch (type) {
				case DataFieldMetadata.DATE_FIELD:
					record.getField(i).setValue(cell.getDateCellValue());
					break;
				case DataFieldMetadata.BYTE_FIELD:
				case DataFieldMetadata.STRING_FIELD:
					record.getField(i).fromString(getStringFromCell(cell));
					break;
				case DataFieldMetadata.DECIMAL_FIELD:
				case DataFieldMetadata.INTEGER_FIELD:
				case DataFieldMetadata.LONG_FIELD:
				case DataFieldMetadata.NUMERIC_FIELD:
					record.getField(i).setValue(cell.getNumericCellValue());
					break;
				}
			} catch (NumberFormatException bdne) {
				BadDataFormatException bdfe = new BadDataFormatException(bdne.getMessage());
				bdfe.setRecordNumber(currentRow+1);
				bdfe.setFieldNumber(fieldNumber[i]);
				if(exceptionHandler != null ) {  //use handler only if configured
					String cellValue = getStringFromCell(cell);
					try{
						record.getField(i).fromString(cellValue);
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
	
		decoder.reset();
		// reset CharsetDecoder
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
		currentRow = firstRow;
		fieldNumber = new short[metadata.getNumFields()];
		if (metadataRow == -1){
			for (short i=0;i<fieldNumber.length;i++){
				fieldNumber[i] = i;
			}
		}else{
			row = sheet.getRow(metadataRow);
			Map fieldNames = metadata.getFieldNames();
			if (metadataNames == null){
				for (short i=0;i<fieldNumber.length;i++){
					cell = row.getCell(i);
					fieldNumber[i] = (Short)fieldNames.get(cell.getStringCellValue());
				}
			}else{
				for (short i=0;i<fieldNumber.length;i++){
					fieldNumber[i] = (Short)fieldNames.get(metadataNames[i]);
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

	public void setMetadataNames(String[] metadaNames) {
		this.metadataRow = 0;
		this.metadataNames = metadaNames;
		if (firstRow == 0) {
			firstRow = 1;
		}
	}

	public void setMetadaNames(String[] metadaNames, int metadataRow) {
		this.metadataRow = metadataRow - 1;
		this.metadataNames = metadaNames;
		if (firstRow == 0) {
			firstRow = this.metadataRow +1;
		}
	}

	public void setMetadataRow(int metadataRow) {
		this.metadataRow = metadataRow - 1;
		if (firstRow == 0) {
			firstRow = this.metadataRow +1;
		}
	}

}
