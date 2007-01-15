
package org.jetel.data.parser;

import java.util.Map;

import javax.naming.InvalidNameException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.formatter.XLSDataFormatter;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.IParserExceptionHandler;
import org.jetel.exception.JetelException;
import org.jetel.exception.PolicyType;
import org.jetel.metadata.DataRecordMetadata;

public abstract class XLSParser implements Parser {

	public final static int NO_METADATA_INFO = 0;
	public final static int ONLY_CLOVER_FIELDS = 1;
	public final static int CLOVER_FIELDS_AND_XLS_NUMBERS = 2;
	public final static int MAP_NAMES = 3;
	public final static int CLOVER_FIELDS_AND_XLS_NAMES = 4;

	static Log logger = LogFactory.getLog(XLSParser.class);

	protected DataRecordMetadata metadata = null;
	protected IParserExceptionHandler exceptionHandler;
	protected String sheetName = null;
	protected int sheetNumber = 0;
	protected int recordCounter;
	protected int firstRow = 0;
	protected int currentRow;
	protected int lastRow;
	protected int metadataRow = -1;
	protected String[] cloverFields = null;
	protected String[] xlsFields = null;
	protected int mappingType = -1;
	protected int[][] fieldNumber ; //mapping of xls fields and clover fields
	
	protected final int XLS_NUMBER = 0;
	protected final int CLOVER_NUMBER = 1;
	
	protected final static int MAX_NAME_LENGTH = 10;


	public abstract void close() ;

	public void setExceptionHandler(IParserExceptionHandler handler) {
        this.exceptionHandler = handler;
	}

	public IParserExceptionHandler getExceptionHandler() {
        return exceptionHandler;
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

	protected abstract DataRecord parseNext(DataRecord record) throws JetelException;
	
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

	public PolicyType getPolicyType() {
        if(exceptionHandler != null) {
            return exceptionHandler.getType();
        }
        return null;
	}

	public void init(DataRecordMetadata _metadata)throws ComponentNotReadyException{
		this.metadata = _metadata;
	}

	public abstract void setDataSource(Object inputDataSource) throws ComponentNotReadyException;


	public int skip(int nRec) throws JetelException {
		currentRow+=nRec;
		return nRec;
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
	protected String getErrorMessage(String exceptionMessage, int recNo, int fieldNo) {
		StringBuffer message = new StringBuffer();
		message.append(exceptionMessage);
		message.append(" when parsing record #");
		message.append(recordCounter);
		message.append(" field ");
		message.append(metadata.getField(fieldNo).getName());
		return message.toString();
	}

    /**
     * Method for mapping metadata with columns in xls
     * 
     */
    protected void mapFields() throws ComponentNotReadyException{
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
	 * If there is given metadata row but other metedata attributes are empty we try find
	 * fields in metadata with the same names as names in xls sheet
	 * 
	 * @param fieldNames
	 * @throws ComponentNotReadyException
	 */
	protected abstract void mapNames(Map fieldNames) 
		throws ComponentNotReadyException;
	
	/**
	 * If clover fields and xls colums are set there is made mapping between coresponding fields and cells
	 * 
	 * @param fieldNames
	 * @throws ComponentNotReadyException
	 */
	protected abstract void cloverfieldsAndXlsNames(Map fieldNames)
		throws ComponentNotReadyException;

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

	public abstract String[] getNames();

}
