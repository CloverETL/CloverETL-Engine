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
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.naming.InvalidNameException;

import jxl.CellView;
import jxl.Workbook;
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
	
	private WritableWorkbook wb;
	private WritableSheet sheet;
	private WritableCellFormat[] cellStyle;
	private boolean open = false;

	/**
	 * Constructor
	 * 
	 * @param append indicates if append data to existing xls sheet or replace 
	 * them by new data 
	 */
	public JExcelXLSDataFormatter(boolean append){
		super(append);
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.Formatter#close()
	 */
	public void close() {
		if (open) {
			try {
				wb.write();
				wb.close();
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
		wb.write();
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.XLSFormatter#prepareSheet()
	 */
	public void prepareSheet(){
		//get or create sheet depending of its existence and append attribute
		if (sheetName != null){
			sheet = wb.getSheet(sheetName);
			if (sheet == null) {
				sheet = wb.createSheet(sheetName, wb.getNumberOfSheets());
			}else if (!append){
				int sheetIndex = StringUtils.findString(sheetName, wb.getSheetNames());
				wb.removeSheet(sheetIndex);
				sheet = wb.createSheet(sheetName, wb.getNumberOfSheets());
			}
		}else if (sheetNumber > -1){
			try {
				sheet = wb.getSheet(sheetNumber);
				sheetName = sheet.getName();
			}catch(IndexOutOfBoundsException ex){
				throw new IllegalArgumentException("There is no sheet with number \"" +	sheetNumber +"\"");
			}
		}else {
			sheet = wb.createSheet("Sheet" + wb.getNumberOfSheets(), wb.getNumberOfSheets());
		}
		recCounter = 0;
		//set recCounter for proper row
		if (append) {
			if (sheet.getRows() != 0){
				recCounter = sheet.getRows() + 1;
			}
		}
		try {
			firstColumn = XLSFormatter.getCellNum(firstColumnIndex);
		}catch(InvalidNameException ex){
			throw new IllegalArgumentException(ex);
		}
		if (namesRow == -1 || (append && recCounter > 0)){//do not save metadata
			if (firstRow > recCounter) {
				recCounter = firstRow;
			}
		}
		//creating cell formats from metadata formats
		cellStyle = new WritableCellFormat[metadata.getNumFields()];
		String format;
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
			format = metadata.getField(i).getFormatStr();
			if (format!=null){//apply format coherent to metadata format
				cellStyle[i] = metadata.getField(i).getType() == DataFieldMetadata.DATE_FIELD
						|| metadata.getField(i).getType() == DataFieldMetadata.DATETIME_FIELD 
						? new WritableCellFormat(new DateFormat(format))
						: new WritableCellFormat(new NumberFormat(format));
			}else if (metadata.getField(i).getType() == DataFieldMetadata.DATE_FIELD || metadata.getField(i).getType() == DataFieldMetadata.DATETIME_FIELD){
				//for date it has to be set an format (else data are sensless)
				if (metadata.getField(i).getLocaleStr() != null) {
					format = ((SimpleDateFormat) java.text.DateFormat
							.getDateInstance(java.text.DateFormat.DEFAULT,
									MiscUtils.createLocale(metadata.getField(i)
											.getLocaleStr()))).toPattern();
				}else{
					format = new SimpleDateFormat().toPattern();
				}
				cellStyle[i] = new WritableCellFormat(new DateFormat(format));
			}
		}
		open = true;
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.Formatter#setDataTarget(java.lang.Object)
	 */
	public void setDataTarget(Object outputDataTarget) {
		close();
		Workbook oldWb = null;
        try{
            if (((File)outputDataTarget).length() > 0) {//if xls file exist add to it new data
                oldWb = Workbook.getWorkbook(((File)outputDataTarget));
            }
            if (oldWb != null){
            	wb = Workbook.createWorkbook((File)outputDataTarget, oldWb);
        		open = true;
           }else{
            	wb = Workbook.createWorkbook((File)outputDataTarget);
            }
        }catch(Exception ex){
            throw new RuntimeException(ex);
        }
        prepareSheet();
	}

    /**
     * Method for saving names of columns
     */
    protected void saveNames() throws IOException{
		recCounter = namesRow > -1 ? namesRow : 0;
		WritableFont font = new WritableFont(WritableFont.ARIAL, WritableFont.DEFAULT_POINT_SIZE, WritableFont.BOLD);
		WritableCellFormat format = new WritableCellFormat(font);
		Label name;
		for (short i=0;i<metadata.getNumFields();i++){
			name = new Label(firstColumn + i, recCounter, metadata.getField(i).getName(), format);
			try {
				sheet.addCell(name);
			} catch (Exception e) {
				throw new IOException(e.getMessage());
			}
		}
		if (firstRow > ++recCounter) {
			recCounter = firstRow;
		}
    }
	
	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.Formatter#write(org.jetel.data.DataRecord)
	 */
	public int write(DataRecord record) throws IOException {
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
				valueXls = new Label(colNum,recCounter,value.toString());
			}else{
				switch (metaType) {
				case DataFieldMetadata.DATE_FIELD:
				case DataFieldMetadata.DATETIME_FIELD:
					if (cellStyle[i] != null) {
						valueXls = new DateTime(colNum, recCounter,
								(Date) value, cellStyle[i]);
					}else{
						valueXls = new DateTime(colNum, recCounter,
								(Date) value);
					}
					break;
				case DataFieldMetadata.INTEGER_FIELD:
					if (cellStyle[i] != null) {
						valueXls = new Number(colNum, recCounter,
								(Integer) value, cellStyle[i]);
					}else{
						valueXls = new Number(colNum, recCounter,
								(Integer) value);
					}
					break;
				case DataFieldMetadata.LONG_FIELD:
					if (cellStyle[i] != null) {
						valueXls = new Number(colNum, recCounter,
								(Long) value, cellStyle[i]);
					}else{
						valueXls = new Number(colNum, recCounter,
								(Long) value);
					}
					break;
				case DataFieldMetadata.DECIMAL_FIELD:
					if (cellStyle[i] != null) {
						valueXls = new Number(colNum, recCounter,
								((Decimal)value).getDouble(), cellStyle[i]);
					}else{
						valueXls = new Number(colNum, recCounter,
								((Decimal)value).getDouble());
					}
					break;
				case DataFieldMetadata.NUMERIC_FIELD:
					if (cellStyle[i] != null) {
						valueXls = new Number(colNum, recCounter,
								(Double) value, cellStyle[i]);
					}else{
						valueXls = new Number(colNum, recCounter,
								(Double) value);
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
		recCounter++;
        
        return 0;
	}

	public int writeFooter() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

}
