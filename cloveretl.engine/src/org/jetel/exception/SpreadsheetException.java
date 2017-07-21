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
package org.jetel.exception;

/**
 * An extension of BadDataFormatException specifically for the purposes of SpreadsheetReader;
 * contains additional fields relevant for the component's error port.
 * 
 * @author sgerguri (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Jan 30, 2012
 */
public class SpreadsheetException extends BadDataFormatException {
	
	private String fileName;
	private String sheetName;
	private String cellCoordinates;
	private String cellType;
	private String cellFormat;
	
	public SpreadsheetException(String message) {
		super(message);
	}
	
	public SpreadsheetException(String message, Throwable cause) {
		super(message, cause);
	}
	
	public SpreadsheetException(String message, CharSequence offendingValue) {
		super(message, offendingValue);
	}
	
	public SpreadsheetException(String message, CharSequence offendingValue, Throwable cause) {
		super(message, offendingValue, cause);
	}
	
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	
	public String getFileName() {
		return fileName;
	}
	
	public void setSheetName(String sheetName) {
		this.sheetName = sheetName;
	}
	
	public String getSheetName() {
		return sheetName;
	}
	
	public void setCellCoordinates(String cellCoordinates) {
		this.cellCoordinates = cellCoordinates;
	}
	
	public String getCellCoordinates() {
		return cellCoordinates;
	}
	
	public void setCellType(String cellType) {
		this.cellType = cellType;
	}
	
	public String getCellType() {
		return cellType;
	}
	
	public void setCellFormat(String cellFormat) {
		this.cellFormat = cellFormat;
	}
	
	public String getCellFormat() {
		return cellFormat;
	}
	
	@Override
	public String getMessage() {
		StringBuffer ret = new StringBuffer();
		ret.append(super.getMessage());
        
        if (sheetName != null) {
        	ret.append(", sheet: '");
        	ret.append(sheetName);
        	ret.append("'");
        }
        
        if (fileName != null) {
        	ret.append(", file: '");
        	ret.append(fileName);
        	ret.append("'");
        }
        
        return ret.toString();
	}

}
