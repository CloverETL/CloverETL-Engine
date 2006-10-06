
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
import java.io.IOException;
import java.util.Date;

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
import org.jetel.metadata.DataRecordMetadata;

/**
 * @author avackova
 *
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
	private int recCounter;
	private boolean saveNames;
	
	public XLSDataFormatter(boolean saveNames){
		this.saveNames = saveNames;
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.Formatter#open(java.lang.Object, org.jetel.metadata.DataRecordMetadata)
	 */
	public void open(Object out, DataRecordMetadata _metadata) {
		this.metadata = _metadata;
		this.out = (FileOutputStream)out; 
		recCounter = 0;
		wb = new HSSFWorkbook();
		sheet = wb.createSheet();
		if (saveNames){
			HSSFCellStyle metaStyle = wb.createCellStyle();
			HSSFFont font = wb.createFont();
			font.setBoldweight(HSSFFont.BOLDWEIGHT_BOLD);
			metaStyle.setFont(font);
			row = sheet.createRow(recCounter);
			String name;
			for (short i=0;i<metadata.getNumFields();i++){
				cell = row.createCell(i);
				name = metadata.getField(i).getName();
				if (sheet.getColumnWidth(i) < name.length() * 256 ) {
					sheet.setColumnWidth(i,(short)(256 * name.length()));
				}
				cell.setCellStyle(metaStyle);
				cell.setCellValue(name);
			}
			recCounter++;
		}
		dataFormat = wb.createDataFormat();
		cellStyle = new HSSFCellStyle[metadata.getNumFields()];
		String format;
		for (short i=0;i<metadata.getNumFields();i++){
			cellStyle[i] = wb.createCellStyle();
			format = metadata.getField(i).getFormatStr();
			if (format!=null){
				cellStyle[i].setDataFormat(dataFormat.getFormat(format));
			}
			if (sheet.getColumnWidth(i) < metadata.getField(i).getSize() * 256) {
				sheet.setColumnWidth(i,(short)( metadata.getField(i).getSize() * 256));
			}
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.Formatter#close()
	 */
	public void close() {
		try {
			wb.write(out);
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
		char metaType;
		Object value;
		for (short i=0;i<metadata.getNumFields();i++){
			metaType = metadata.getField(i).getType();
			cell = row.createCell(i);
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

}
