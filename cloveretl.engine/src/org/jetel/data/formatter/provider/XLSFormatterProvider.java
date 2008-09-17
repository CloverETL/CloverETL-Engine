package org.jetel.data.formatter.provider;

import org.jetel.data.Defaults;
import org.jetel.data.formatter.Formatter;
import org.jetel.data.formatter.JExcelXLSDataFormatter;
import org.jetel.data.formatter.XLSFormatter;

/**
 * Provides support for getting the delimited data formatter.
 * 
 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 */
public class XLSFormatterProvider implements FormatterProvider {

	private boolean append;
	private String sheetName;
	private int namesRow = -1;
	private int firstRow;
	private int sheetNumber = -1;
	private String firstColumnIndex = "A";
	private String charset;
	
	/**
	 * Contructors.
	 */
	public XLSFormatterProvider(boolean append) {
		this(Defaults.DataFormatter.DEFAULT_CHARSET_ENCODER, append);
	}

	public XLSFormatterProvider(String charset, boolean append) {
		this.append = append;
		this.charset = charset;
	}

	/**
	 * Creates new data formatter.
	 * 
	 * @return data formatter
	 */
	public Formatter getNewFormatter() {
		XLSFormatter formatter = new JExcelXLSDataFormatter(charset, append);
		formatter.setSheetName(sheetName);
		formatter.setSheetNumber(sheetNumber);
		formatter.setFirstColumn(firstColumnIndex);
		formatter.setFirstRow(firstRow);
		formatter.setNamesRow(namesRow);
		return formatter;
	}

	/**
	 * @return sheet name, which was set by setSheetName method
	 */
	public String getSheetName() {
		return sheetName;
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
	 * @return number of row with names of columns
	 */
	public int getNamesRow() {
		return namesRow;
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
	 * @return number of first data row
	 */
	public int getFirstRow() {
		return firstRow;
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
	 * @return code of first column ("A","B",..,"AA",...)
	 */
	public String getFirstColumn() {
		return firstColumnIndex;
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
	 * @return if data will be appent to currently set sheet or will be replaced by new data
	 */
	public boolean isAppend() {
		return append;
	}

	public String getCharset() {
		return charset;
	}

	public void setCharset(String charset) {
		this.charset = charset;
	}
}
