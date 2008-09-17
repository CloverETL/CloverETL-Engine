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

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.naming.InvalidNameException;

import jxl.CellView;
import jxl.Workbook;
import jxl.WorkbookSettings;
import jxl.write.Boolean;
import jxl.write.DateFormat;
import jxl.write.DateTime;
import jxl.write.Label;
import jxl.write.Number;
import jxl.write.NumberFormat;
import jxl.write.WritableCell;
import jxl.write.WritableCellFormat;
import jxl.write.WritableFont;
import jxl.write.WritableSheet;
import jxl.write.WritableWorkbook;
import jxl.write.biff.RowsExceededException;

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.primitive.Decimal;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.MiscUtils;
import org.jetel.util.string.StringUtils;

/**
 * Writes records to xls sheet using JExcelAPI
 *  
 * @author avackova (agata.vackova@javlinconsulting.cz) ; 
 * (c) JavlinConsulting s.r.o.
 *  www.javlinconsulting.cz
 *
 * @since Jan 16, 2007
 *
 */
public class JExcelXLSDataFormatter extends XLSFormatter {
	
	private static final String CLOVER_FIELD_PREFIX = "$";
	
	private WritableWorkbook wb;
	private WritableSheet sheet;
	private Map<String, SheetData> sheets = null;
	private boolean multiSheetWriting;
	private WritableCellFormat[] cellStyle;
	private boolean open = false;
	private String currentSheetName;
	private int currentRow;
	private SheetData currentSheet;

	private String charset;

	/**
	 * Constructor
	 * 
	 * @param append indicates if append data to existing xls sheet or replace 
	 * them by new data 
	 */
	public JExcelXLSDataFormatter(boolean append){
		this(Defaults.DataFormatter.DEFAULT_CHARSET_ENCODER, append);
	}

	public JExcelXLSDataFormatter(String charset, boolean append){
		super(append);
		this.charset = charset;
	}
	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.Formatter#close()
	 */
	public void close() {
		if (open) {
			try {
				wb.close();
				sheet = null;
				open = false;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}		
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.Formatter#flush()
	 */
	public void flush() throws IOException {
		if (wb.getNumberOfSheets() > 0) {
			wb.write();
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.XLSFormatter#prepareSheet()
	 */
	public void prepareSheet(){
		String tmpName = currentSheetName != null ? currentSheetName : sheetName;
		
		//get or create sheet depending of its existence and append attribute
		if (!StringUtils.isEmpty(tmpName)){
			sheet = wb.getSheet(tmpName);
			if (sheet == null) {
				sheet = wb.createSheet(tmpName, wb.getNumberOfSheets());
			}else if (!append){
				int sheetIndex = StringUtils.findString(tmpName, wb.getSheetNames());
				wb.removeSheet(sheetIndex);
				sheet = wb.createSheet(tmpName, wb.getNumberOfSheets());
			}
		}else if (sheetNumber > -1){
			try {
				sheet = wb.getSheet(sheetNumber);
				tmpName = sheet.getName();
			}catch(IndexOutOfBoundsException ex){
				throw new IllegalArgumentException("There is no sheet with number \"" +	sheetNumber +"\"");
			}
		}else {
			sheet = wb.createSheet("Sheet" + wb.getNumberOfSheets(), wb.getNumberOfSheets());
		}
		try {
			firstColumn = XLSFormatter.getCellNum(firstColumnIndex);
		}catch(InvalidNameException ex){
			throw new IllegalArgumentException(ex);
		}
		//set proper current row and save metadata field's names if required
		currentRow = append ? sheet.getRows() : 0;
		if (namesRow > -1) {
			if (!append || sheet.getRows() == 0) {
				currentRow = namesRow;
				try {
					saveNames();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
				currentRow = namesRow + 1;
			}
		}
		if (firstRow > currentRow) {
			currentRow = firstRow;
		}
		//creating cell formats from metadata formats
		cellStyle = new WritableCellFormat[metadata.getNumFields()];
		CellView view = new CellView();
		view.setHidden(false);
		if (metadata.getRecType() == DataRecordMetadata.DELIMITED_RECORD) {
			view.setAutosize(true);
		}
		for (short i=0;i<metadata.getNumFields();i++){
			if (metadata.getField(i).getSize() > 0){
				view.setSize(metadata.getField(i).getSize() * 256);
			}
			sheet.setColumnView(firstColumn + i, view);
			cellStyle[i] = getCellFormat(metadata.getField(i));
		}
		//Remember current sheet
		if (multiSheetWriting) {
			SheetData sheetData = new SheetData(sheet, currentRow);
			sheets.put(tmpName, sheetData);
			currentSheet = sheetData;
		}
		open = true;
	}
	
	/**
	 * Prepares cell format depending on field format and locale
	 * 
	 * @param field
	 * @return
	 */
	private static WritableCellFormat getCellFormat(DataFieldMetadata field){
		if (!(field.isNumeric() || field.getType() == DataFieldMetadata.DATE_FIELD || field.getType() == DataFieldMetadata.DATETIME_FIELD)){
			return null;
		}
		String format = field.getFormatStr() ;
		if (field.isNumeric()) {
			return format != null ? new WritableCellFormat(new NumberFormat(format)) : null;
		}
		//DateDataField
		if (format != null) {
			return new WritableCellFormat(new DateFormat(format));
		}
		//format is null
		if (field.getLocaleStr() != null) {
			format = ((SimpleDateFormat) java.text.DateFormat.getDateInstance(java.text.DateFormat.DEFAULT,
							MiscUtils.createLocale(field.getLocaleStr()))).toPattern();
		}else{
			format = new SimpleDateFormat().toPattern();
		}
		return new WritableCellFormat(new DateFormat(format));
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.Formatter#setDataTarget(java.lang.Object)
	 */
	public void setDataTarget(Object outputDataTarget) {
		close();
		Workbook oldWb = null;
        try{
            WorkbookSettings settings = new WorkbookSettings();
    		settings.setEncoding(charset);
            if (((File)outputDataTarget).length() > 0) {//if xls file exist add to it new data
                oldWb = Workbook.getWorkbook(((File)outputDataTarget), settings);
            }
            if (oldWb != null){
            	wb = Workbook.createWorkbook((File)outputDataTarget, oldWb, settings);
        		open = true;
           }else{
            	wb = Workbook.createWorkbook((File)outputDataTarget, settings);
            }
        }catch(Exception ex){
            throw new RuntimeException(ex);
        }
        multiSheetWriting = !StringUtils.isEmpty(sheetName) && sheetName.startsWith(CLOVER_FIELD_PREFIX);
        if (multiSheetWriting) {
        	String[] fields = sheetName.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
        	for (int i = 0; i < fields.length; i++) {
				fields[i] = fields[i].substring(1);
			}
        	setKeyFields(fields);
        	sheets = new HashMap<String, SheetData>();
        }
	}

	public void reset() {
		if (open) {
			try {
				wb.close();
				sheet = null;
				open = false;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}		
		if (sheets != null){
			sheets.clear();
		}
	}
	
	public void finish() throws IOException {
		flush();
	}
	
    /**
     * Method for saving names of columns
     */
    protected void saveNames() throws IOException{
//		recCounter = namesRow > -1 ? namesRow : 0;
		WritableFont font = new WritableFont(WritableFont.ARIAL, WritableFont.DEFAULT_POINT_SIZE, WritableFont.BOLD);
		WritableCellFormat format = new WritableCellFormat(font);
		Label name;
		for (short i=0;i<metadata.getNumFields();i++){
			name = new Label(firstColumn + i, currentRow, metadata.getField(i).getName(), format);
			try {
				sheet.addCell(name);
			} catch (Exception e) {
				throw new IOException(e.getMessage());
			}
		}
//		if (firstRow > ++recCounter) {
//			recCounter = firstRow;
//		}
    }
	
	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.Formatter#write(org.jetel.data.DataRecord)
	 */
	public int write(DataRecord record) throws IOException {
		if (multiSheetWriting) {
			prepareSheet(record);
			sheet = currentSheet.sheet;
		}else if (sheet == null) {
			prepareSheet();
		}
		char metaType;//metadata field type
		Object value;//field value
		Object valueXls = null;//value to set
		short colNum;
		for (short i=0;i<metadata.getNumFields();i++){
			metaType = metadata.getField(i).getType();
			colNum = (short)(firstColumn + i);
			value = record.getField(i).getValue();
			if (value == null) continue;
			if (metaType == DataFieldMetadata.BYTE_FIELD || metaType == DataFieldMetadata.BYTE_FIELD_COMPRESSED
					|| metaType == DataFieldMetadata.STRING_FIELD){
				valueXls = new Label(colNum,currentRow,value.toString());
			}else{
				switch (metaType) {
				case DataFieldMetadata.DATE_FIELD:
				case DataFieldMetadata.DATETIME_FIELD:
					if (cellStyle[i] != null) {
						valueXls = new DateTime(colNum, currentRow,
								(Date) value, cellStyle[i]);
					}else{
						valueXls = new DateTime(colNum, currentRow,
								(Date) value);
					}
					break;
				case DataFieldMetadata.INTEGER_FIELD:
					if (cellStyle[i] != null) {
						valueXls = new Number(colNum, currentRow,
								(Integer) value, cellStyle[i]);
					}else{
						valueXls = new Number(colNum, currentRow,
								(Integer) value);
					}
					break;
				case DataFieldMetadata.LONG_FIELD:
					if (cellStyle[i] != null) {
						valueXls = new Number(colNum, currentRow,
								(Long) value, cellStyle[i]);
					}else{
						valueXls = new Number(colNum, currentRow,
								(Long) value);
					}
					break;
				case DataFieldMetadata.DECIMAL_FIELD:
					if (cellStyle[i] != null) {
						valueXls = new Number(colNum, currentRow,
								((Decimal)value).getDouble(), cellStyle[i]);
					}else{
						valueXls = new Number(colNum, currentRow,
								((Decimal)value).getDouble());
					}
					break;
				case DataFieldMetadata.NUMERIC_FIELD:
					if (cellStyle[i] != null) {
						valueXls = new Number(colNum, currentRow,
								(Double) value, cellStyle[i]);
					}else{
						valueXls = new Number(colNum, currentRow,
								(Double) value);
					}
					break;
				case DataFieldMetadata.BOOLEAN_FIELD:
					if (cellStyle[i] != null) {
						valueXls = new Boolean(colNum, currentRow,
								(java.lang.Boolean) value, cellStyle[i]);
					}else{
						valueXls = new Boolean(colNum, currentRow,
								(java.lang.Boolean) value);
					}
					break;
				}
			}
			try {
				sheet.addCell((WritableCell)valueXls);
			}catch (RowsExceededException e) {
				//write data to new sheet
				setSheetNumber(-1);
				prepareSheet();
			} catch (Exception e) {
				throw new IOException(e.getMessage());
			}
		}
		currentRow++;
		if (multiSheetWriting) {
			currentSheet.currentRow++;
		}
        
        return 0;
	}

	public int writeFooter() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void prepareSheet(DataRecord record) {
		currentSheetName = sheetNameKeyRecord.getKeyString(record);
		if (sheets.containsKey(currentSheetName)) {
			currentSheet = sheets.get(currentSheetName);
			currentRow = currentSheet.currentRow;
		}else{
			prepareSheet();
		}
	}

}

class SheetData {
	
	WritableSheet sheet;
	Integer currentRow = 0;
	
	SheetData(WritableSheet sheet, int currentRow) {
		this.sheet = sheet;
		this.currentRow = currentRow;
	}
	
}
