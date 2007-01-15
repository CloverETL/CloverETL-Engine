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

package org.jetel.data.formatter;

import java.io.FileOutputStream;

import javax.naming.InvalidNameException;

import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Formats records for xls file. Order of method calling:
 * <li>init(DataRecordMetadata)</li>
 * <li>setDataTarget(File) </li>
 * <li><i>setSheetName(String)</i> or <i>setSheetNumber(int)</i> - optional</li>
 * <li><i>setFirstRow(int)</i> - optional</li>
 * <li><i>setFirstColumn(int)</i> - optional</li>
 * <li><i>setNamesRow(int)</i> - optional</li>
 * <li>prepareSheet()</li>
 * <li>write(DataRecord)</li>
 * <li>close()</li>
 * 
 * @author avackova (agata.vackova@javlinconsulting.cz) ; 
 * (c) JavlinConsulting s.r.o.
 *  www.javlinconsulting.cz
 *
 * @since Jan 15, 2007
 *
 */
public abstract class XLSFormatter implements Formatter {
	
	protected final static int CELL_NUMBER_IN_SHEET = 'Z'-'A'+1;//number of "AA" cell in excel sheet

	protected DataRecordMetadata metadata;
	protected FileOutputStream out;
	protected int firstRow = 0;
	protected int recCounter;
	protected int namesRow = -1;
	protected boolean append;
	protected String sheetName = null;
	protected int sheetNumber = -1;
	protected String firstColumnIndex = "A";
	protected int firstColumn;
	protected boolean savedNames = false;

	/**
	 * Constructor
	 * 
	 * @param append indicates if append data to existing xls sheet or replace 
	 * them by new data 
	 */
	public XLSFormatter(boolean append){
		this.append = append;
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.Formatter#init(org.jetel.metadata.DataRecordMetadata)
	 */
	public void init(DataRecordMetadata _metadata) throws ComponentNotReadyException{
		this.metadata = _metadata;
	}

	/**
	 * Prepares sheet in xls file, which was set by method setSheetName(String sheetName)
	 *  or setSheetNumber(int sheetNumber) for saving data. If any of above methods wasn't
	 *  called there is created new sheet with default name.
	 */
	public abstract void prepareSheet();

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

	public void setFirstColumn(String firstColumn){
		this.firstColumnIndex = firstColumn;
	}

	public int getFirstColumn() {
		return firstColumn;
	}

	public void setNamesRow(int namesRow) {
		this.namesRow = namesRow;
	}

	public boolean isAppend() {
		return append;
	}

	public int getFirstRow() {
		return firstRow;
	}

	public int getNamesRow() {
		return namesRow;
	}

	public String getSheetName() {
		return sheetName;
	}

	public int getSheetNumber() {
		return sheetNumber;
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
}
