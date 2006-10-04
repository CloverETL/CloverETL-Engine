
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
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.jetel.data.DataRecord;
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

	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.Formatter#open(java.lang.Object, org.jetel.metadata.DataRecordMetadata)
	 */
	public void open(Object out, DataRecordMetadata _metadata) {
		this.metadata = _metadata;
		this.out = (FileOutputStream)out; 
		wb = new HSSFWorkbook();
		sheet = wb.createSheet();
		dataFormat = wb.createDataFormat();
		cellStyle = new HSSFCellStyle[metadata.getNumFields()];
		String format;
		for (int i=0;i<metadata.getNumFields();i++){
			cellStyle[i] = wb.createCellStyle();
			format = metadata.getField(i).getFormatStr();
			if (format!=null){
				cellStyle[i].setDataFormat(dataFormat.getFormat(format));
			}
		}
		recCounter = 0;
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
				if (metaType == DataFieldMetadata.DATE_FIELD || metaType == DataFieldMetadata.DATETIME_FIELD){
					cell.setCellValue((Date)value);
				}else{
					cell.setCellValue((Double)value);
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
