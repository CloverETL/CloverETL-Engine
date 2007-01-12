
package org.jetel.data.formatter;

import java.io.File;
import java.io.IOException;
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
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.StringUtils;
import org.jetel.util.Utils;

public class JExcelXLSDataFormatter implements XLSFormatter {
	
	private DataRecordMetadata metadata;
	private WritableWorkbook wb;
	private String sheetName = null;
	private WritableSheet sheet;
	private boolean append;
	private int sheetNumber = -1;
	private int recCounter;
	private int firstColumn;
	private String firstColumnIndex = "A";
	private int namesRow;
	private int firstRow;
	private boolean savedNames;
	private WritableCellFormat[] cellStyle;
	private boolean closed = true;

	public JExcelXLSDataFormatter(boolean append){
		this.append = append;
	}

	public void close() {
		if (!closed) {
			try {
				wb.write();
				wb.close();
				closed = true;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}		
	}

	public void flush() throws IOException {
		wb.write();
	}

	public void init(DataRecordMetadata _metadata)
			throws ComponentNotReadyException {
		this.metadata = _metadata;

	}
	
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
			firstColumn = XLSDataFormatter.getCellNum(firstColumnIndex);
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
			if (format!=null){
				cellStyle[i] = metadata.getField(i).getType() == DataFieldMetadata.DATE_FIELD
						|| metadata.getField(i).getType() == DataFieldMetadata.DATETIME_FIELD 
						? new WritableCellFormat(new DateFormat(format))
						: new WritableCellFormat(new NumberFormat(format));
			}else if (metadata.getField(i).getType() == DataFieldMetadata.DATE_FIELD || metadata.getField(i).getType() == DataFieldMetadata.DATETIME_FIELD){
				if (metadata.getField(i).getLocaleStr() != null) {
					format = ((SimpleDateFormat) java.text.DateFormat
							.getDateInstance(java.text.DateFormat.DEFAULT,
									Utils.createLocale(metadata.getField(i)
											.getLocaleStr()))).toPattern();
				}else{
					format = new SimpleDateFormat().toPattern();
				}
				cellStyle[i] = new WritableCellFormat(new DateFormat(format));
			}
		}
		closed = false;
	}

	public void setDataTarget(Object outputDataTarget) {
		Workbook oldWb = null;
        try{
            if (((File)outputDataTarget).length() > 0) {//if xls file exist add to it new data
                oldWb = Workbook.getWorkbook(((File)outputDataTarget));
            }
            if (oldWb != null){
            	wb = Workbook.createWorkbook((File)outputDataTarget, oldWb);
        		closed = false;
           }else{
            	wb = Workbook.createWorkbook((File)outputDataTarget);
            }
        }catch(Exception ex){
            throw new RuntimeException(ex);
        }
	}

    private void saveNames() throws IOException{
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
		savedNames = true;
    }
	
	public int write(DataRecord record) throws IOException {
		if (!savedNames){
			saveNames();
			return 0;
		}
		char metaType;//metadata field type
		Object value;//field value
		Object valueXls = null;
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
				savedNames = namesRow == -1;
				setSheetNumber(-1);
				prepareSheet();
			} catch (Exception e) {
				throw new IOException(e.getMessage());
			}
		}
		recCounter++;
        
        return 0;
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
