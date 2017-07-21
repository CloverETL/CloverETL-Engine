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
package org.jetel.data.formatter.provider;

import org.jetel.data.Defaults;
import org.jetel.data.formatter.Formatter;
import org.jetel.data.formatter.JExcelXLSDataFormatter;
import org.jetel.data.formatter.XLSFormatter;
import org.jetel.data.formatter.XLSXDataFormatter;
import org.jetel.exception.TempFileCreationException;
import org.jetel.graph.ContextProvider;
import org.jetel.graph.runtime.IAuthorityProxy;

/**
 * Provides support for getting the delimited data formatter.
 *
 * @author Jan Ausperger, Javlin a.s. &lt;jan.ausperger@javlin.eu&gt;
 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
 *
 * @version 17th July 2009
 */
public class XLSFormatterProvider implements FormatterProvider {

	private boolean useXLSX;
	private boolean append;
	private boolean removeSheets;
	private String sheetName;
	private int namesRow = -1;
	private int firstRow;
	private int sheetNumber = -1;
	private String firstColumnIndex = "A";
	private String charset;

	private String[] excludedFieldNames;
	private boolean inMemory;

	public XLSFormatterProvider(boolean append, boolean removeSheets) {
		this(append, removeSheets, Defaults.DataFormatter.DEFAULT_CHARSET_ENCODER);
	}

	public XLSFormatterProvider(boolean append, boolean removeSheets, String charset) {
		this.append = append;
		this.removeSheets = removeSheets;
		this.charset = charset;
	}

	/**
	 * Creates a new data formatter.
	 * 
	 * @return data formatter
	 */
	@Override
	public Formatter getNewFormatter() {
		XLSFormatter formatter = useXLSX ? new XLSXDataFormatter(append, removeSheets) : new JExcelXLSDataFormatter(charset, append, removeSheets);
		formatter.setSheetName(sheetName);
		formatter.setSheetNumber(sheetNumber);
		formatter.setFirstColumn(firstColumnIndex);
		formatter.setFirstRow(firstRow);
		formatter.setNamesRow(namesRow);
		formatter.setExcludedFieldNames(excludedFieldNames);
		
		if (formatter instanceof JExcelXLSDataFormatter) {
			formatter.setInMemory(inMemory);
		}
		return formatter;
	}

	public void setUseXLSX(boolean useXLSX) {
		this.useXLSX = useXLSX;
	}

	/**
	 * @return sheet name, which was set by setSheetName method
	 */
	public String getSheetName() {
		return sheetName;
	}

	/**
	 * Set name of sheet, data will be written to. It has higher prioryty then setSheetNumber method
	 * 
	 * @param sheetName
	 */
	public void setSheetName(String sheetName) {
		this.sheetName = sheetName;
	}

	/**
	 * Set number of sheet, data will be written to. If there was called method setSheetName before calling
	 * prepareSheet, this number will be ignored.
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
	public void setFirstRow(int firstRow) {
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
	 * @param firstColumn
	 *            : "A","B",..,"AA",...
	 */
	public void setFirstColumn(String firstColumn) {
		this.firstColumnIndex = firstColumn;
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

	public String getCharset() {
		return charset;
	}

	public void setCharset(String charset) {
		this.charset = charset;
	}

	public void setExcludedFieldNames(String[] excludedFieldNames) {
		this.excludedFieldNames = excludedFieldNames;
	}

	/**
	 * @return true if Formatter is to process formatting in memory <br>
	 * 			false if Formatter is to create temporary files when formating
	 */
	public boolean isInMemory() {
		return inMemory;
	}

	/**
	 * Sets type of processing for xls files
	 * 
	 * @param inMemory
	 */
	public void setInMemory(boolean inMemory) {
		this.inMemory = inMemory;
	}

}
