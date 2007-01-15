
package org.jetel.data.formatter;

import java.io.FileOutputStream;

import javax.naming.InvalidNameException;

import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataRecordMetadata;

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

	public XLSFormatter(boolean append){
		this.append = append;
	}

	public void init(DataRecordMetadata _metadata) throws ComponentNotReadyException{
		this.metadata = _metadata;
	}

	public abstract void prepareSheet();

	public void setSheetName(String sheetName) {
		this.sheetName = sheetName;
	}

	public void setSheetNumber(int sheetNumber) {
		this.sheetNumber = sheetNumber;
	}
	
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
