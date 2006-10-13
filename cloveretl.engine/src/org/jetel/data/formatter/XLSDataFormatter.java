
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
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.XLSUtils;

/**
 * Writes records to xls sheet
 *  
/**
* @author avackova <agata.vackova@javlinconsulting.cz> ; 
* (c) JavlinConsulting s.r.o.
*	www.javlinconsulting.cz
*	@created October 10, 2006
 */
public class XLSDataFormatter implements Formatter {

	private HSSFWorkbook wb;
	private HSSFSheet sheet;
	private HSSFRow row;
	private HSSFCell cell;
	private HSSFCellStyle[] cellStyle;
	private HSSFDataFormat dataFormat;
	private DataRecordMetadata metadata;
	private FileOutputStream out;
	private int firstRow = 0;
	private int recCounter;
	private boolean saveNames;
	private int namesRow = -1;
	private boolean append;
	private String sheetName = null;
	private int sheetNumber = -1;
	private String firstColumnIndex = "A";
	private int firstColumn;
	
	public XLSDataFormatter(boolean saveNames, boolean append){
		this.saveNames = saveNames;
		this.append = append;
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.Formatter#open(java.lang.Object, org.jetel.metadata.DataRecordMetadata)
	 */
	public void open(Object out, DataRecordMetadata _metadata) throws ComponentNotReadyException{
		this.metadata = _metadata;
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
				throw new ComponentNotReadyException("There is no sheet with number \"" +	sheetNumber +"\"");
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
			firstColumn = XLSUtils.getCellNum(firstColumnIndex);
		}catch(InvalidNameException ex){
			throw new ComponentNotReadyException(ex);
		}
		//save metadata  names
		if (saveNames && (!append || recCounter == 0)){//saveNames=true, but if append=true save names only if there are no records on this sheet
			recCounter = namesRow > -1 ? namesRow : 0;
			HSSFCellStyle metaStyle = wb.createCellStyle();
			HSSFFont font = wb.createFont();
			font.setBoldweight(HSSFFont.BOLDWEIGHT_BOLD);//save metadata names bold
			metaStyle.setFont(font);
			row = sheet.createRow(recCounter);
			String name;
			for (short i=0;i<metadata.getNumFields();i++){
				cell = row.createCell((short)(firstColumn + i));
				name = metadata.getField(i).getName();
				if (sheet.getColumnWidth((short)(firstColumn + i)) < name.length() * 256 ) {
					sheet.setColumnWidth((short)(firstColumn + i),(short)(256 * name.length()));
				}
				cell.setCellStyle(metaStyle);
				cell.setCellValue(name);
			}
			recCounter++;
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
		if (firstRow > recCounter) {
			recCounter = firstRow;
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.Formatter#close()
	 */
	public void close() {
		try {
			wb.write(out);//write workbook to file
			out.close();	
		}catch(IOException ex){
			ex.printStackTrace();
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.Formatter#write(org.jetel.data.DataRecord)
	 */
	public void write(DataRecord record) throws IOException {
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
			if (metaType == DataFieldMetadata.BYTE_FIELD || metaType == DataFieldMetadata.STRING_FIELD){
				cell.setCellType(HSSFCell.CELL_TYPE_STRING);
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
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.Formatter#flush()
	 */
	public void flush() throws IOException {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.Formatter#setOneRecordPerLinePolicy(boolean)
	 */
	public void setOneRecordPerLinePolicy(boolean b) {
		// TODO Auto-generated method stub

	}

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

	public boolean isSaveNames() {
		return saveNames;
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
}
