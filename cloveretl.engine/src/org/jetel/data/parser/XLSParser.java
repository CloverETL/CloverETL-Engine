/*
 * jETeL/CloverETL - Java based ETL application framework.
 * Copyright (c) Javlin, a.s. (info@cloveretl.com)
 *  
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.jetel.data.parser;

import java.util.HashMap;
import java.util.Map;

import javax.naming.InvalidNameException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.formatter.XLSFormatter;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.IParserExceptionHandler;
import org.jetel.exception.JetelException;
import org.jetel.exception.PolicyType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.AutoFilling;
import org.jetel.util.NumberIterator;

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
public abstract class XLSParser extends AbstractParser {

	public final static int NO_METADATA_INFO = 0;
	public final static int ONLY_CLOVER_FIELDS = 1;
	public final static int CLOVER_FIELDS_AND_XLS_NUMBERS = 2;
	public final static int MAP_NAMES = 3;
	public final static int CLOVER_FIELDS_AND_XLS_NAMES = 4;

	protected final static String DEFAULT_FIELD_DELIMITER = ";";
	protected final static String DEFAULT_LAST_FIELD_DELIMITER = "\n";
	protected final static String DEFAULT_RECORD_DELIMITER = "\n";
	
	protected final static String DEFAULT_SHEET_NUMBER = "0";

	protected Log logger = LogFactory.getLog(this.getClass());

	protected DataRecordMetadata metadata = null;
	protected IParserExceptionHandler exceptionHandler;
	protected String sheetName = null;
	protected String sheetNumber = null;
	protected Incremental incremental;
	protected boolean useIncrementalReading;
	protected NumberIterator sheetNumberIterator = null;
	protected short sheetCounter = -1;
	protected int recordCounter = 1;
	protected int firstRow = 0;
	protected int currentRow;
	protected int lastRow = -1;
	protected int lastRowAttribute = -1;
	protected int metadataRow = -1;
	protected String[] cloverFields = null;
	protected String[] xlsFields = null;
	protected int mappingType = -1;
	protected int[][] fieldNumber ; //mapping of xls fields and clover fields
	protected boolean[] isAutoFilling;
	
	protected final int XLS_NUMBER = 0;
	protected final int CLOVER_NUMBER = 1;
	
	// autofilling for sheet_name
	private boolean noAutofillingSheetName;
	private int[] autofillingFieldPositions;
	protected String autoFillingSheetName = null;

	/** the data formatter used to format cell values as strings */
	protected final DataFormatter dataFormatter = new DataFormatter();

	public final static int MAX_NAME_LENGTH = 15;

	public XLSParser(DataRecordMetadata metadata){
		this.metadata = metadata;
	}
	
	@Override
	public void setExceptionHandler(IParserExceptionHandler handler) {
        this.exceptionHandler = handler;
	}

	@Override
	public IParserExceptionHandler getExceptionHandler() {
        return exceptionHandler;
	}

	@Override
	public DataRecord getNext() throws JetelException {
		// create a new data record
		DataRecord record = DataRecordFactory.newRecord(metadata);
		record.init();

		return getNext(record);
	}

	@Override
	public DataRecord getNext(DataRecord record) throws JetelException {
		record = parseNext(record);
		setAutofillingSheetName(record);
		
		if(exceptionHandler != null ) {  //use handler only if configured
			while(exceptionHandler.isExceptionThrowed()) {
                exceptionHandler.handleException();
				record = parseNext(record);
			}
		}
/* following code was moved to MultiFileReader to allow proper implementation of the skip-per-spreadsheet feature
		if (record == null) {//record from current sheet
			if (getNextSheet()) {
				record = getNext();
			}
		}
*/
		return record;
	}

	private void setAutofillingSheetName(DataRecord record) {
		if (record == null || noAutofillingSheetName) return;
		if (autoFillingSheetName == null) return;
		for (int i: autofillingFieldPositions) {
			record.getField(i).setValue(autoFillingSheetName);
		}
	}
	
	/**
	 * An operation that produces next record from Input data or null
	 * 
	 * @param record to fill
	 * @return The Next value
	 * @throws JetelException
	 */
	protected abstract DataRecord parseNext(DataRecord record) throws JetelException;
	
	/**
	 * This method checks if there is next sheet with name conforming with 
	 * sheetName or sheetNumber pattern
	 * 
	 * @return
	 */
	protected abstract boolean getNextSheet();
	
	@Override
	public PolicyType getPolicyType() {
        if(exceptionHandler != null) {
            return exceptionHandler.getType();
        }
        return null;
	}

	@Override
	public void init()throws ComponentNotReadyException{
		if (metadata == null) {
			throw new ComponentNotReadyException("Metadata are null");
		}
		isAutoFilling = new boolean[metadata.getNumFields()];
		for (int i = 0; i < metadata.getNumFields(); i++) {
			isAutoFilling[i] = metadata.getField(i).getAutoFilling() != null;
		}
		if (sheetName == null && sheetNumber == null) {
			setSheetNumber(DEFAULT_SHEET_NUMBER);
		}
		
		// creates autofilling for sheet_name
		prepareAutofilling();
	}

	/**
	 * Creates autofilling for sheet_name.
	 */
	private void prepareAutofilling() {
        int numFields = metadata.getNumFields();
        int[] sheetNameTmp = new int[numFields];
        int sheetNameLen = 0;
        for (int i=0; i<numFields; i++) {
        	if (metadata.getField(i).getAutoFilling() != null) {
        		if (metadata.getField(i).getAutoFilling().equalsIgnoreCase(AutoFilling.SHEET_NAME)) sheetNameTmp[sheetNameLen++] = i;
        	}
        }
        autofillingFieldPositions = new int[sheetNameLen];
        noAutofillingSheetName = sheetNameLen <= 0;

        // reduce arrays' sizes
        System.arraycopy(sheetNameTmp, 0, autofillingFieldPositions, 0, sheetNameLen);
	}
	
	@Override
	public abstract void setDataSource(Object inputDataSource) throws ComponentNotReadyException;

	@Override
	public int skip(int nRec) {
		if (currentRow + nRec <= lastRow) {
			currentRow += nRec;
			return nRec;			
		} else {
			int retval = 1 + lastRow - currentRow;
			currentRow = lastRow + 1;
			return retval;
		}
	}

	/**
	 * Changes current sheet to requested
	 * 
	 * @param sheetNumber
	 * @return true if requested sheet exists in current workbook
	 */
	public abstract boolean getSheet(int sheetNumber);
	
	/**
	 * Changes current sheet to requested
	 * 
	 * @param sheetName
	 * @return true if requested sheet exists in current workbook
	 */
	public abstract boolean getSheet(String sheetName);
	
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
	protected String getErrorMessage(int recNo, int fieldNo) {
		StringBuffer message = new StringBuffer();
		message.append("Error when parsing record from row ");
		message.append(currentRow);
		message.append(" field ");
		message.append(metadata.getField(fieldNo).getName());
		return message.toString();
	}

    /**
     * Method for mapping metadata with columns in xls
     * 
     */
    protected void mapFields() throws ComponentNotReadyException {
		for (int i = 0; i < fieldNumber.length; i++) {
			fieldNumber[i][CLOVER_NUMBER] = -1;
		}

		Map<String, Integer> fieldNames = metadata.getFieldNamesMap();

		switch (mappingType) {
			case ONLY_CLOVER_FIELDS:
				onlyCloverFields(fieldNames);
				break;
			case CLOVER_FIELDS_AND_XLS_NUMBERS:
				cloverFieldsAndXlsNumbers(fieldNames);
				break;
			case MAP_NAMES:
				mapNames(fieldNames);
				break;
			case CLOVER_FIELDS_AND_XLS_NAMES:
				cloverfieldsAndXlsNames(fieldNames);
				break;
			case NO_METADATA_INFO:
			default:
				noMetadataInfo();
				break;
		}
	}
    
	/**
	 * If any of the metadata attribute wasn't set cell order corresponds with field order in metadata
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
	private void onlyCloverFields(Map<String, Integer> fieldNames) throws ComponentNotReadyException{
		for (int i = 0; i < cloverFields.length; i++) {
			fieldNumber[i][XLS_NUMBER] = i;
			try {
				fieldNumber[i][CLOVER_NUMBER] = fieldNames.get(cloverFields[i]);
			} catch (NullPointerException ex) {
				throw new ComponentNotReadyException("Clover field \"" + cloverFields[i] + "\" not found");
			}
		}
	}
	
	/**
	 * If clover fields and xls colums are set there is made mapping between corresponding fields and cells
	 * 
	 * @param fieldNames
	 * @throws ComponentNotReadyException
	 */
	private void cloverFieldsAndXlsNumbers(Map<String, Integer> fieldNames) throws ComponentNotReadyException {
		if (cloverFields.length!=xlsFields.length){
			throw new ComponentNotReadyException("Number of clover fields and xls fields must be the same");
		}
		for (short i=0;i<cloverFields.length;i++){
			int cellNumber;
			try {
				cellNumber = XLSFormatter.getCellNum(xlsFields[i]);
			}catch(InvalidNameException ex){
				throw new ComponentNotReadyException(ex);
			}
			fieldNumber[i][XLS_NUMBER] = cellNumber;
			try {
				fieldNumber[i][CLOVER_NUMBER] = fieldNames.get(cloverFields[i]);
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
	protected abstract void mapNames(Map<String, Integer> fieldNames) throws ComponentNotReadyException;
	
	/**
	 * If clover fields and xls colums are set there is made mapping between corresponding fields and cells
	 * 
	 * @param fieldNames
	 * @throws ComponentNotReadyException
	 */
	protected abstract void cloverfieldsAndXlsNames(Map<String, Integer> fieldNames) throws ComponentNotReadyException;

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
	public String getSheetNumber() {
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
	 * Sets row, names of columns will be read from. -1 means no line
	 * 
	 * @param metadataRow
	 * @throws ComponentNotReadyException
	 */
	public void setMetadataRow(int metadataRow) throws ComponentNotReadyException{
		if (metadataRow < -1) { 
			throw new ComponentNotReadyException("Number of metadata row has to be greater than -1");
		}

		this.metadataRow = metadataRow;
	}
	
	/**
	 * Sets row, names of columns will be read from. -1 means no metadata line
	 * 
	 * @param metadataRow
	 * @throws ComponentNotReadyException
	 */
	public void setMetadataRowWithAllowedNone(int metadataRow) throws ComponentNotReadyException{
		if (metadataRow < -1) { 
			throw new ComponentNotReadyException("Number of metadata row has to be greater than -1");
		}

		this.metadataRow = metadataRow;
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
	public void setSheetNumber(String sheetNumber) {
		this.sheetNumber = sheetNumber;
	}

	/**
	 * Sets mapping type between xls columns and metadata 
	 * 
	 * @param mappingType: <ul>
	 * <li>NO_METADATA_INFO - cell order corresponds with field order in metadata</li>
	 * <li>ONLY_CLOVER_FIELDS  - cells are read in order of clover fields</li>
	 * <li>CLOVER_FIELDS_AND_XLS_NUMBERS - mapping between corresponding fields and cells</li>
	 * <li>CLOVER_FIELDS_AND_XLS_NAMES - mapping between corresponding fields and cells</li>
	 * <li>MAP_NAMES - finds fields in metadata with the same names as names in xls sheet</li>
	 * </ul>
	 */
	public void setMappingType(int mappingType) {
		this.mappingType = mappingType;
		switch (mappingType) {
		case NO_METADATA_INFO:logger.debug("Mapping type set to NO_METADATA_INFO");break;
		case ONLY_CLOVER_FIELDS:logger.debug("Mapping type set to ONLY_CLOVER_FIELDS");break;
		case CLOVER_FIELDS_AND_XLS_NUMBERS:logger.debug("Mapping type set to CLOVER_FIELDS_AND_XLS_NUMBERS");break;
		case MAP_NAMES:logger.debug("Mapping type set to MAP_NAMES");break;
		case CLOVER_FIELDS_AND_XLS_NAMES:logger.debug("Mapping type set to CLOVER_FIELDS_AND_XLS_NAMES");break;
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
	public abstract String[] getNames() throws ComponentNotReadyException;
	
	/**
	 * @return metadata created from actual file, sheet, metadata row and first data row
	 */
	public abstract DataRecordMetadata createMetadata();
	
	public abstract String[][] getPreview(int startRow, int length);
	public abstract String[][] getPreview(int length);

	public int getLastRow() {
		return lastRow;
	}

	public void setLastRow(int lastRow) {
		this.lastRowAttribute = lastRow;
	}
	
	/**
	 * @return sheets in current workbook
	 */
	public abstract String[] getSheets();
	
	/**
	 * @param index
	 * @return sheet name in current workbook
	 */
	public abstract String getSheetName(int index);

	@Override
	public Object getPosition() {
		return ((incremental != null) ? incremental.getPosition() : null);
	}

	@Override
	public void movePosition(Object position) {
		if (!useIncrementalReading) return;
		incremental = new Incremental(position.toString());
		discardBytes(getSheetName(sheetCounter));
	}

	protected void discardBytes(String sheetName) {
		if (incremental == null) {
			return;
		}

		Integer position = incremental.getRow(sheetName);

		if (position != null && position.intValue() > 0) {
			currentRow = position.intValue();
		}
	}

	@Override
	public void reset() throws ComponentNotReadyException {
        sheetCounter = -1;
        recordCounter = 1;
		currentRow = firstRow;

		if (sheetNumber != null && sheetNumberIterator != null){
        	sheetNumberIterator.reset();
        }

		incremental = null;
	}

	@Override
	public abstract void close();

	public void useIncrementalReading(boolean useIncrementalReading) {
		this.useIncrementalReading = useIncrementalReading;
	}

	/**
	 * For incremental reading.
	 */
	protected static class Incremental {
		private Map<String, Integer> sheetRow;

		public Incremental() {
			this(null);
		}

		public Incremental(String position) {
			sheetRow = new HashMap<String, Integer>();
			parsePosition(position);
		}
		
		private void parsePosition(String position) {
			if (position == null) return;
			String[] all = position.split("#");
			if (all.length != 2) return;
			String[] tabs = all[0].split(",");
			String[] rows = all[1].split(",");
			if (tabs.length != rows.length) return;
			
			try {
				for (int i=0; i<tabs.length; i++) {
					sheetRow.put(tabs[i], Integer.parseInt(rows[i]));
				}
			} catch (NumberFormatException e) {
				sheetRow.clear();
				return;
			}
		}
		
		public Integer getRow(String sheetName) {
			return sheetRow.get(sheetName);
		}
		
		public void setRow(String sheetName, int row) {
			sheetRow.put(sheetName, row);
		}
		
		public void clear() {
			sheetRow.clear();
		}
		
		public String getPosition() {
			StringBuilder sbKey = new StringBuilder();
			StringBuilder sbValue = new StringBuilder();
			if (sheetRow.size() <= 0) return "";
			for (String key: sheetRow.keySet()) {
				sbKey.append(key).append(",");
				sbValue.append(sheetRow.get(key)).append(",");
			}
			sbKey.deleteCharAt(sbKey.length()-1);
			sbValue.deleteCharAt(sbValue.length()-1);
			sbKey.append("#");
			return sbKey.append(sbValue.toString()).toString();
		}
	}
	
	@Override
	public boolean nextL3Source() {
		return getNextSheet();
	}

	@Override
	public DataSourceType getPreferredDataSourceType() {
		return DataSourceType.STREAM;
	}
	
	
	
}
