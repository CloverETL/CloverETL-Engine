
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

/**
 * Parses data from xls file. Order of method calling:
 * <ul>
 * <li>init(DataRecordMetadata)</li>
 * <li><i>setSheetName(String)</i> or <i>setSheetNumber(int)</i> - optional</li>
 * <li><i>setFirstRow(int)</i> - optional</li>
 * <li><i>setMappingType(int)</i> - optional</li>
 * <li><i>setCloverFields(int)</i> - optional (depending on mapping type)</li>
 * <li><i>setXlsFields(int)</i> - optional (depending on mapping type)</li>
 * <li><i>setMetadataRow(int)</i> - optional (depending on mapping type)</li>
 * <li>setDataSource(InputStream) </li>
 * <li><i>getNames()</i> - optional</li>
 * <li>getNext(DataRecord)</li>
 * <li>close()</li></ul>
 * 
 * @author avackova (agata.vackova@javlinconsulting.cz) ; 
 * (c) JavlinConsulting s.r.o.
 *  www.javlinconsulting.cz
 *
 * @since Jan 16, 2007
 *
 */
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


	/* (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#close()
	 */
	public abstract void close() ;

	/* (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#setExceptionHandler(org.jetel.exception.IParserExceptionHandler)
	 */
	public void setExceptionHandler(IParserExceptionHandler handler) {
        this.exceptionHandler = handler;
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#getExceptionHandler()
	 */
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

	/**
	 * An operation that produces next record from Input data or null
	 * 
	 * @param record to fill
	 * @return The Next value
	 * @throws JetelException
	 */
	protected abstract DataRecord parseNext(DataRecord record) throws JetelException;
	
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

	/* (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#getPolicyType()
	 */
	public PolicyType getPolicyType() {
        if(exceptionHandler != null) {
            return exceptionHandler.getType();
        }
        return null;
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
	public abstract void setDataSource(Object inputDataSource) throws ComponentNotReadyException;


	/* (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#skip(int)
	 */
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

	/**
	 * Sets xls sheet for reading data
	 * 
	 * @param sheetName
	 */
	public void setSheetName(String sheetName) {
		this.sheetName = sheetName;
	}

	/**
	 * Sets row, which data will be read from
	 * 
	 * @param firstRecord
	 */
	public void setFirstRow(int firstRecord) {
		this.firstRow = firstRecord;
	}

	/**
	 * Row, wchich was set by setFirstRow method
	 * 
	 * @return
	 */
	public int getFirstRow() {
		return firstRow;
	}

	/**
	 * Gets number, which was set by setSheetNumber method
	 * 
	 * @return
	 */
	public int getSheetNumber() {
		return sheetNumber;
	}

	/**
	 * @return numbers of read records
	 */
	public int getRecordCount() {
		return recordCounter;
	}

	/**
	 * Sets clover fields for mapping metadata with xls columns
	 * 
	 * @param cloverFields
	 */
	public void setCloverFields(String[] cloverFields) {
		this.cloverFields = cloverFields;
	}

	/**
	 * Sets row, names of columns will be read from
	 * 
	 * @param metadataRow
	 * @throws ComponentNotReadyException
	 */
	public void setMetadataRow(int metadataRow) throws ComponentNotReadyException{
		if (metadataRow < 0) 
			throw new ComponentNotReadyException("Number of metadata row has to be greter then 0");
		this.metadataRow = metadataRow;
		if (firstRow == 0) {
			firstRow = this.metadataRow +1;
		}
	}

	/**
	 * Sets columns' names for mapping metadata with xls columns
	 * 
	 * @param xlsFields
	 */
	public void setXlsFields(String[] xlsFields) {
		this.xlsFields = xlsFields;
	}

	/**
	 * Sets number of xls sheet, data will be read from
	 * 
	 * @param sheetNumber
	 */
	public void setSheetNumber(int sheetNumber) {
		this.sheetNumber = sheetNumber;
	}

	/**
	 * Sets mapping type between xls columns and metadata 
	 * 
	 * @param mappingType: <ul>
	 * <li>NO_METADATA_INFO - cell order coresponds with field order in metadata</li>
	 * <li>ONLY_CLOVER_FIELDS  - cells are read in order of clover fields</li>
	 * <li>CLOVER_FIELDS_AND_XLS_NUMBERS - mapping between coresponding fields and cells</li>
	 * <li>CLOVER_FIELDS_AND_XLS_NAMES - mapping between coresponding fields and cells</li>
	 * <li>MAP_NAMES - finds fields in metadata with the same names as names in xls sheet</li>
	 * </ul>
	 */
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

	/**
	 * @return number, which was set by setMetadataRow method
	 */
	public int getMetadataRow() {
		return metadataRow;
	}

	/**
	 * @return String, which was set by setSheetName method
	 */
	public String getSheetName() {
		return sheetName;
	}

	/**
	 * @return array with names from row set by setMetadataRow method 
	 * (or setFirstRow if metadata row was not been set)  
	 */
	public abstract String[] getNames();

}
