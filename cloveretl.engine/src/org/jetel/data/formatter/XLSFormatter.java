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
package org.jetel.data.formatter;

import java.io.File;
import java.io.IOException;

import javax.naming.InvalidNameException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.RecordKey;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Formats records for xls file. Order of method calling:
 * <ul>
 * <li>init(DataRecordMetadata)</li>
 * <li>setDataTarget(File) </li>
 * <li><i>setSheetName(String)</i> or <i>setSheetNumber(int)</i> - optional</li>
 * <li><i>setFirstRow(int)</i> - optional</li>
 * <li><i>setFirstColumn(int)</i> - optional</li>
 * <li><i>setNamesRow(int)</i> - optional</li>
 * <li>prepareSheet()</li>
 * <li>write(DataRecord)</li>
 * <li>close()</li></ul>
 * 
 * @author Agata Vackova, Javlin a.s. &lt;agata.vackova@javlin.eu&gt;
 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
 *
 * @version 17th July 2009
 * @since 15th January 2007
 */
public abstract class XLSFormatter extends AbstractFormatter {
	
    /**
     * The type of a XLS(X) that should be used.
     *
     * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
     *
     * @version 31st January 2009
     * @since 31st January 2009
     */
    public static enum XLSType {

        /** the type should be chosen automatically (based on a file extension) */
        AUTO,
        /** the classic XLS parser (JExcel) should be used */
        XLS,
        /** the XLSX parser (Apache POI) should be used */
        XLSX;

        public static XLSType valueOfIgnoreCase(String string) {
            for (XLSType parserType : values()) {
                if (parserType.name().equalsIgnoreCase(string)) {
                    return parserType;
                }
            }

            return AUTO;
        }

    }

    public static final String XLSX_FILE_PATTERN = "^.*\\.[Xx][Ll][Ss][Xx]$";

	protected static final String FILE_PROTOCOL = "file";
	protected static final String CLOVER_FIELD_PREFIX = "$";

	protected static final int CELL_NUMBER_IN_SHEET = 'Z' - 'A' + 1; // number of "AA" cell in excel sheet

	protected static final int FIELD_SIZE_MULTIPLIER = 256;

	protected static Log logger = LogFactory.getLog(XLSFormatter.class);

	protected DataRecordMetadata metadata;
	protected int firstRow = 0;
	protected int namesRow = -1;
	protected boolean removeSheets;
	protected String sheetName = null;
	protected int sheetNumber = -1;
	protected String firstColumnIndex = "A";
	protected int firstColumn;
	protected RecordKey sheetNameKeyRecord;

	private String[] excludedFieldNames;
	protected int[] includedFieldIndices;

	protected Boolean inMemory;
	protected File tmpDir;


	/**
	 * Constructor
	 * 
	 * @param append
	 *         indicates if append data to existing xls sheet or replace them by new data
	 * @param removeSheets
	 *         indicates if all sheets are to be removed from a file 
	 */
	public XLSFormatter(boolean append, boolean removeSheets){
		this.append = append;
		this.removeSheets = removeSheets;
	}

	@Override
	public void init(DataRecordMetadata metadata) throws ComponentNotReadyException{
		this.metadata = metadata;
		this.includedFieldIndices = metadata.fieldsIndicesComplement(excludedFieldNames);
	}

	/**
	 * Prepares sheet in xls file, which was set by method setSheetName(String sheetName)
	 *  or setSheetNumber(int sheetNumber) for saving data. If any of above methods wasn't
	 *  called there is created new sheet with default name.
	 */
	public abstract void prepareSheet();
	
	/**
	 * Prepares sheet in xls file, which name is created from given record. Before calling this method, 
	 * setKeyFields(String[]) method must be called.
	 * 
	 * @param record
	 */
	public abstract void prepareSheet(DataRecord record);
	
	/**
	 * Sets from which fields from input metadata will be created sheet name for different records
	 * 
	 * @param fieldNames
	 */
	public void setKeyFields(String[] fieldNames){
		sheetNameKeyRecord = new RecordKey(fieldNames, metadata);
		sheetNameKeyRecord.init();
	}

	/**
	 * Set name of sheet, data will be written to. It has higher prioryty then
	 * setSheetNumber method
	 * 
	 * @param sheetName
	 */
	public void setSheetName(String sheetName) {
		this.sheetName = sheetName;
	}

	/**
	 * Set number of sheet, data will be written to. If there was called method
	 * setSheetName before calling prepareSheet, this number will be ignored.
	 * 
	 * @param sheetNumber
	 */
	public void setSheetNumber(int sheetNumber) {
		this.sheetNumber = sheetNumber;
	}
	
	/**
	 * Sets the number of row (0-based), for first data record.
	 * 
	 * @param firstRow
	 */
	public void setFirstRow(int firstRow){
		this.firstRow = firstRow;
	}

	/**
	 * Sets the code of first column 
	 * 
	 * @param firstColumn: "A","B",..,"AA",...
	 */
	public void setFirstColumn(String firstColumn){
		this.firstColumnIndex = firstColumn;
	}

	/**
	 * @return code of first column ("A","B",..,"AA",...)
	 */
	public String getFirstColumnName(){
		return firstColumnIndex;
	}

	/**
	 * @return first column number
	 */
	@Deprecated
	public int getFirstColumn() {
		return firstColumn;
	}

	/**
	 * Sets the number of row, that metadata names will be save
	 * 
	 * @param namesRow
	 */
	public void setNamesRow(int namesRow) {
		this.namesRow = namesRow;
	}

	/**
	 * @return if data will be appent to currently set sheet or will be replaced by new data
	 */
	public boolean isAppend() {
		return append;
	}
	
	/**
	 * @return true iff all sheets are removed when a source file is opened 
	 */
	public boolean isRemoveSheets() {
		return removeSheets;
	}	
	

	/**
	 * @return number of first data row
	 */
	public int getFirstRow() {
		return firstRow;
	}

	/**
	 * @return number of row with names of columns
	 */
	public int getNamesRow() {
		return namesRow;
	}

	/**
	 * @return sheet name, which was set by setSheetName method
	 */
	public String getSheetName() {
		return sheetName;
	}

	/**
	 * @return sheet number, which was set by setSheetNumber method
	 */
	public int getSheetNumber() {
		return sheetNumber;
	}

	public void setExcludedFieldNames(String[] excludedFieldNames) {
		this.excludedFieldNames = excludedFieldNames;
	}

	/**
	 * This method calculates xls colum number from its code:
	 * 	A='A'-65=0,B='B'-65,....,Z='Z'-65=CELL_NUMBER_IN_SHEET-1,
	 *  XYZ = ('X'-64)*CELL_NUMBER_IN_SHEET^2+('Y'-64)*CELL_NUMBER_IN_SHEET+('Z'-65)
	 * @param cellCode
	 * @return column's number from its code
	 * @throws InvalidNameException
	 */
	public static short getCellNum(String cellCode)throws InvalidNameException{
		char[] cellCodeU = cellCode.toUpperCase().toCharArray();
		int length = cellCodeU.length;
		int cellNumber = 0;
		for (int j=0;j<length;j++){
			if (cellCodeU[j]<'A' || cellCodeU[j]>'Z')
				throw new InvalidNameException("Wrong column index: " + cellCode);
			cellNumber = cellNumber*CELL_NUMBER_IN_SHEET + (cellCodeU[j]-64);
		}
		cellNumber-=1;
		if (cellNumber<0) 
			throw new InvalidNameException("Wrong column index: " + cellCode);
		return (short)cellNumber;
	}
	
	/**
	 * This method finds xls column code from its number (0 based)
	 * 
	 * @param cellNumber
	 * @return column's code from its number
	 */
	public static String getCellCode(int cellNumber){
		//invert column number to CELL_NUMBER_IN_SHEET based form
		String cellNumberXlsBase = Integer.toString(cellNumber,CELL_NUMBER_IN_SHEET);
		StringBuffer cellCode = new StringBuffer(cellNumberXlsBase.length());
		//each digit except last diminished by 1 ('A'=0) invert back to decimal form
		for (short i=0;i<cellNumberXlsBase.length()-1;i++){
			cellCode.append((char)(Integer.parseInt(cellNumberXlsBase.substring(i,i+1),CELL_NUMBER_IN_SHEET)-1+'A'));
		}
		cellCode.append((char)(Integer.parseInt(cellNumberXlsBase.substring(cellNumberXlsBase.length()-1,cellNumberXlsBase.length()),CELL_NUMBER_IN_SHEET)+'A'));
		return cellCode.toString();
	}
	
	@Override
	public int writeHeader() throws IOException {
		return 0;
	}
	
	@Override
	public int writeFooter() throws IOException {
		return 0;
	}

	@Override
	public void finish() throws IOException {
		flush();
	}

	/**
	 * Sets type of processing for xls files
	 * 
	 * @param inMemory
	 */
	public void setInMemory(boolean inMemory) {
		this.inMemory  = inMemory;
	}
	
	/**
	 * @return true if Formatter is to process formatting in memory <br>
	 * 			false if Formatter is to create temporary files when formating
	 */
	public boolean isInMemory() {
		return inMemory;
	}

	/**
	 * Sets directory for temporary files
	 * 
	 * @param tmpDir
	 */
	public void setTmpDir(File tmpDir) {
		this.tmpDir = tmpDir;
	}

	/**
	 * @return directory for temporary files
	 */
	public File getTmpDir(){
		return tmpDir;
	}
}

