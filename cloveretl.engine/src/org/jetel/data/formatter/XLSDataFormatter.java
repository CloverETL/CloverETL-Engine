
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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Date;

import javax.naming.InvalidNameException;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFDataFormat;
import org.apache.poi.hssf.usermodel.HSSFFont;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.jetel.data.DataRecord;
import org.jetel.data.primitive.Decimal;
import org.jetel.metadata.DataFieldMetadata;

/**
 * Writes records to xls sheet using POI library
 *  
/**
* @author avackova <agata.vackova@javlinconsulting.cz> ; 
* (c) JavlinConsulting s.r.o.
*	www.javlinconsulting.cz
*	@created October 10, 2006
 */
public class XLSDataFormatter extends XLSFormatter {

	private HSSFWorkbook wb;
	private HSSFSheet sheet;
	private HSSFRow row;
	private HSSFCell cell;
	private HSSFCellStyle[] cellStyle;
	private HSSFDataFormat dataFormat;
	
	/**
	 * Constructor
	 * 
	 * @param append indicates if append data to existing xls sheet or replace 
	 * them by new data 
	 */
	public XLSDataFormatter(boolean append) {
		super(append);
	}

    /* (non-Javadoc)
     * @see org.jetel.data.formatter.Formatter#setDataTarget(java.lang.Object)
     */
    public void setDataTarget(Object out) {
        try{
            if (((File)out).length() > 0) {//if xls file exist add to it new data
                wb = new HSSFWorkbook(new FileInputStream((File)out));
            }else{//create new xls file
                wb = new HSSFWorkbook();
            }
            this.out = new FileOutputStream((File)out);
        }catch(IOException ex){
            throw new RuntimeException(ex);
        }
    }
    
    /* (non-Javadoc)
     * @see org.jetel.data.formatter.XLSFormatter#prepareSheet()
     */
    public void prepareSheet(){
		//get or create sheet depending of its existence and append attribute
		if (sheetName != null){
			sheet = wb.getSheet(sheetName);
			if (sheet == null) {
				sheet = wb.createSheet(sheetName);
			}else if (!append){
				wb.removeSheetAt(wb.getSheetIndex(sheetName));
				sheet = wb.createSheet(sheetName);
			}
		}else if (sheetNumber > -1){
			try {
				sheet = wb.getSheetAt(sheetNumber);
				sheetName = wb.getSheetName(sheetNumber);
			}catch(IndexOutOfBoundsException ex){
				throw new IllegalArgumentException("There is no sheet with number \"" +	sheetNumber +"\"");
			}
		}else {
			sheet = wb.createSheet();
		}
		recCounter = 0;
		//set recCounter for proper row
		if (append) {
			if (sheet.getLastRowNum() != 0){
				recCounter = sheet.getLastRowNum() + 1;
			}
		}
		try {
			firstColumn = getCellNum(firstColumnIndex);
		}catch(InvalidNameException ex){
			throw new IllegalArgumentException(ex);
		}
		if (namesRow == -1 || (append && recCounter > 0)){//do not save metadata
			if (firstRow > recCounter) {
				recCounter = firstRow;
			}
			savedNames = true;
		}
		//creating cell formats from metadata formats
		dataFormat = wb.createDataFormat();
		cellStyle = new HSSFCellStyle[metadata.getNumFields()];
		String format;
		for (short i=0;i<metadata.getNumFields();i++){
			cellStyle[i] = wb.createCellStyle();
			format = metadata.getField(i).getFormatStr();
			if (format!=null){
				cellStyle[i].setDataFormat(dataFormat.getFormat(format));
			}
			if (sheet.getColumnWidth((short)(firstColumn + i)) < metadata.getField(i).getSize() * 256) {
				sheet.setColumnWidth((short)(firstColumn + i),(short)( metadata.getField(i).getSize() * 256));
			}
		}
    }
    
    /**
     * Method for saving names of columns
     */
    private void saveNames(){
		recCounter = namesRow > -1 ? namesRow : 0;
		HSSFCellStyle metaStyle = wb.createCellStyle();
		HSSFFont font = wb.createFont();
		font.setBoldweight(HSSFFont.BOLDWEIGHT_BOLD);//save metadata names bold
		metaStyle.setFont(font);
		row = sheet.createRow(recCounter);
		String name;
		//iterate over metadata
		for (short i=0;i<metadata.getNumFields();i++){
			cell = row.createCell((short)(firstColumn + i));
			name = metadata.getField(i).getName();
			if (sheet.getColumnWidth((short)(firstColumn + i)) < name.length() * 256 ) {
				sheet.setColumnWidth((short)(firstColumn + i),(short)(256 * name.length()));
			}
			cell.setCellStyle(metaStyle);
			cell.setCellValue(name);
		}
		if (firstRow > ++recCounter) {
			recCounter = firstRow;
		}
		savedNames = true;
    }
    
	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.Formatter#close()
	 */
	public void close() {
		try {
				if (out.getFD().valid()) {
					wb.write(out);//write workbook to file
					out.close();
				}				
		}catch(IOException ex){
			ex.printStackTrace();
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.Formatter#write(org.jetel.data.DataRecord)
	 */
	public int write(DataRecord record) throws IOException {
		if (!savedNames){
			saveNames();
			return 0;
		}
		row = sheet.createRow(recCounter);
		char metaType;//metadata field type
		Object value;//field value
		short colNum;
		for (short i=0;i<metadata.getNumFields();i++){
			metaType = metadata.getField(i).getType();
			colNum = (short)(firstColumn + i);
			cell = row.createCell(colNum);
			value = record.getField(i).getValue();
			if (value == null) continue;
			cell.setCellStyle(cellStyle[i]);
			if (metaType == DataFieldMetadata.BYTE_FIELD || metaType == DataFieldMetadata.BYTE_FIELD_COMPRESSED
					|| metaType == DataFieldMetadata.STRING_FIELD){
				cell.setCellType(HSSFCell.CELL_TYPE_STRING);
				cell.setEncoding(HSSFCell.ENCODING_UTF_16);
				cell.setCellValue(value.toString());
			}else{
				cell.setCellType(HSSFCell.CELL_TYPE_NUMERIC);
				switch (metaType) {
				case DataFieldMetadata.DATE_FIELD:
				case DataFieldMetadata.DATETIME_FIELD:
					cell.setCellValue((Date)value);
					break;
				case DataFieldMetadata.INTEGER_FIELD:
					cell.setCellValue((Integer)value);
					break;
				case DataFieldMetadata.LONG_FIELD:
					cell.setCellValue((Long)value);
					break;
				case DataFieldMetadata.DECIMAL_FIELD:
					cell.setCellValue(((Decimal)value).getDouble());
					break;
				case DataFieldMetadata.NUMERIC_FIELD:
					cell.setCellValue((Double)value);
					break;
				}
			}
		}
		recCounter++;
        
        return 0;
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.Formatter#flush()
	 */
	public void flush() throws IOException {
		// TODO Auto-generated method stub

	}

}
