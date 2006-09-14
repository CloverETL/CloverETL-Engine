
package org.jetel.data.parser;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.poi.hssf.usermodel.HSSFCell;
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
	private int firstRecord = 1;
	private int currentRecord;
	HSSFWorkbook wb;
	HSSFSheet sheet;
	HSSFRow row;
	HSSFCell cell;

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

	private DataRecord parseNext(DataRecord record) throws JetelException {
		row = sheet.getRow(currentRecord);
		char type;
		for (short i=0;i<metadata.getNumFields();i++){
			cell = row.getCell(i);
			type = metadata.getField(i).getType();
			try{
				switch (type) {
				case DataFieldMetadata.BYTE_FIELD:record.getField(i).setValue(cell.getErrorCellValue());				
					break;
				case DataFieldMetadata.DATE_FIELD:record.getField(i).setValue(cell.getDateCellValue());
					break;
				case DataFieldMetadata.DECIMAL_FIELD:
				case DataFieldMetadata.INTEGER_FIELD:
				case DataFieldMetadata.LONG_FIELD:
				case DataFieldMetadata.NUMERIC_FIELD:
					record.getField(i).setValue(cell.getNumericCellValue());
					break;
				case DataFieldMetadata.STRING_FIELD:record.getField(i).setValue(cell.getStringCellValue());
					break;
				}
			} catch (BadDataFormatException bdfe) {
				if(exceptionHandler != null ) {  //use handler only if configured
	                exceptionHandler.populateHandler(getErrorMessage(bdfe.getMessage(), recordCounter, i), record, -1, i, cell.getStringCellValue(), bdfe);
				} else {
					throw new RuntimeException(getErrorMessage(bdfe.getMessage(), recordCounter, i));
				}
			}
		}
		currentRecord++;
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
		currentRecord+=nRec;
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
		currentRecord = firstRecord-1;
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

	public void setFirstRecord(int firstRecord) {
		this.firstRecord = firstRecord;
	}


}
