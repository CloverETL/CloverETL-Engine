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
package org.jetel.data.parser;

import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

import jxl.BooleanCell;
import jxl.Cell;
import jxl.CellType;
import jxl.DateCell;
import jxl.NumberCell;
import jxl.Sheet;
import jxl.Workbook;
import jxl.WorkbookSettings;

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.formatter.XLSFormatter;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.DataRecordParsingType;
import org.jetel.util.NumberIterator;
import org.jetel.util.file.WcardPattern;
import org.jetel.util.string.StringUtils;

/**
 * Parsing data from a XLS file using JExcelAPI.
 * 
 * @author Agata Vackova, Javlin a.s. &lt;agata.vackova@javlin.eu&gt;
 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
 *
 * @version 26th November 2009
 * @since 16th January 2007
 */
public class JExcelXLSDataParser extends XLSParser {
	
	private Workbook wb;
	private Sheet sheet;
	private String charset = null;
	
	//calendars used by a method changeTimeShiftToLocal
	//declared and initiated here because of speed (the method is called for each
	//field containing date)
	private Calendar utcCalendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));    
	private Calendar localCalendar = Calendar.getInstance(TimeZone.getDefault());
	
	/**
	 * Default constructor
	 */
	public JExcelXLSDataParser(DataRecordMetadata metadata) {
		super(metadata);
		charset = Defaults.DataParser.DEFAULT_CHARSET_DECODER;
	}
	
	/**
	 * @param charset
	 */
	public JExcelXLSDataParser(DataRecordMetadata metadata, String charset) {
		super(metadata);
		this.charset = charset;
	}

	@Override
	public void close() {
		if (wb != null) {
			wb.close();
		}
	}

	@Override
	protected void cloverfieldsAndXlsNames(Map<String, Integer> fieldNames) throws ComponentNotReadyException {
		if (cloverFields.length!=xlsFields.length){
			throw new ComponentNotReadyException("Number of clover fields and XLS fields must be the same");
		}
		//getting metadata row
		Cell[] row = sheet.getRow(metadataRow);
		int count = 0;
		//go through each not empty cell
		for (int i=0;i<row.length;i++){
			Cell cell = row[i];
			String cellValue = cell.getContents();
			int xlsNumber = StringUtils.findString(cellValue,xlsFields);
			if (xlsNumber > -1){//string from cell found in xlsFields attribute
				fieldNumber[count][XLS_NUMBER] = cell.getColumn();
				try {
					fieldNumber[count][CLOVER_NUMBER] = fieldNames.get(cloverFields[xlsNumber]);
				}catch (NullPointerException ex) {
					throw new ComponentNotReadyException("Clover field \""
							+ cloverFields[xlsNumber] + "\" not found");
				}
				count++;
			}else{
				logger.warn("There is no field corresponding to \"" + cellValue + "\" in output metadata");
			}
		}
		if (count<cloverFields.length){
			logger.warn("Not all fields found");
		}
	}

	@Override
	public String[] getNames() throws ComponentNotReadyException{
		ArrayList<String> names = new ArrayList<String>();
		if (metadataRow > -1) {
		Cell[] row = sheet.getRow(metadataRow);

		if (row.length == 0) {
			throw new ComponentNotReadyException("Metadata row (" + (metadataRow > -1 ? metadataRow : firstRow) + 
					") doesn't exist in sheet " + StringUtils.quote(sheet.getName()) + "!"); 
		}
		
		//go through each not empty cell
		for (int i=0;i<row.length;i++) {
			Cell cell = row[i];
				names.add(XLSFormatter.getCellCode(cell.getColumn()) + " - " +
						cell.getContents());
		}
		}else{
			Cell[] row = sheet.getRow(firstRow);
			for (int i=0;i<row.length;i++){
				Cell cell = row[i];
				names.add(XLSFormatter.getCellCode(cell.getColumn()) + " - " +
						cell.getContents().substring(0, Math.min(
								cell.getContents().length(), MAX_NAME_LENGTH)));
			}
		}
		return names.toArray(new String[names.size()]);
	}
	
	/**
	 * Returns preview of current sheet
	 * 
	 * @param startRow starting row
	 * @param length number of rows
	 * @return preview of current sheet
	 */
	@Override
	public String[][] getPreview(int startRow, int length){
		if (sheet == null) return null;
		
		String[][] result = new String[length][];
		Cell[] row;
		String cellContents;
		for (int i=startRow; i<startRow + length; i++){
			if (i > sheet.getRows() - 1) {
				String[][] newResult = new String[i - startRow][];
				for (int j = 0; j < newResult.length; j++) {
					newResult[j] = result[j];
				}
				return newResult;
			}
			row = sheet.getRow(i);
			result[i - startRow] = new String[row.length];
			for (int j = 0; j < row.length; j++) {
				cellContents = row[j].getContents();
				result[i - startRow][j] = cellContents.substring(0, Math.min(cellContents.length(), MAX_NAME_LENGTH));
				if (cellContents.length() > MAX_NAME_LENGTH) {
					result[i - startRow][j] += "...";
				}
			}
		}
		return result;
	}
	
	@Override
	public String[][] getPreview(int length){
		return getPreview(0, length);
	}

	@Override
	public DataRecordMetadata createMetadata(){
		if (wb == null) return null;
		String name = sheet.getName();
		DataRecordMetadata xlsMetadata = new DataRecordMetadata(DataRecordMetadata.EMPTY_NAME, DataRecordParsingType.DELIMITED);
		xlsMetadata.setLabel(name);
		xlsMetadata.setFieldDelimiter(DEFAULT_FIELD_DELIMITER);
		xlsMetadata.setRecordDelimiter(DEFAULT_RECORD_DELIMITER);
		Cell[] namesRow;
		if (metadataRow > -1) {
		namesRow = sheet.getRow(metadataRow);
		}else{
			metadataRow = -1;
  			namesRow = sheet.getRow(firstRow);
		}
		Cell[] dataRow = sheet.getRow(firstRow);
		//go through each cell
		CellType type;
		DataFieldMetadata field;
		Cell nameCell, dataCell;
		for (int i=0;i<Math.max(namesRow.length, dataRow.length);i++){
			if (i < namesRow.length) {
				nameCell = namesRow[i];
			}else{
				nameCell = null;
			}
			if (i < dataRow.length) {
				dataCell = dataRow[i];
			}else{
				dataCell = null;
			}
			type = dataCell != null ? dataCell.getType() : CellType.LABEL;
			name = (metadataRow>-1) && nameCell != null ? nameCell.getContents() : XLSFormatter.getCellCode(i);
			if (type == CellType.EMPTY && namesRow != dataRow && (nameCell == null || nameCell.getType() == CellType.EMPTY)) {
				continue;
			}else if (type == CellType.BOOLEAN) {
				field = new DataFieldMetadata(DataFieldMetadata.EMPTY_NAME, DataFieldType.BOOLEAN, null);
			}else if (type == CellType.DATE){
				field = new DataFieldMetadata(DataFieldMetadata.EMPTY_NAME, DataFieldType.DATE, null);
				field.setFormatStr(((SimpleDateFormat)((DateCell)dataCell).getDateFormat()).toPattern());
			}else if (type == CellType.NUMBER) {
				field = new DataFieldMetadata(DataFieldMetadata.EMPTY_NAME, DataFieldType.NUMBER, null);
				field.setFormatStr(((DecimalFormat)((NumberCell)dataCell).getNumberFormat()).toPattern());
			}else{
				field = new DataFieldMetadata(DataFieldMetadata.EMPTY_NAME, DataFieldType.STRING, null);
			}
			field.setLabel(name);
			xlsMetadata.addField(field);
		}
		
		xlsMetadata.normalize();
		
		return xlsMetadata;
	}
	
	@Override
	protected void mapNames(Map<String, Integer> fieldNames) throws ComponentNotReadyException {
		//getting metadata row
		Cell[] row = sheet.getRow(metadataRow);
		int count = 0;
		//go through each not empty cell
		for (int i=0;i<row.length;i++){
			Cell cell = row[i];
			String cellValue = cell.getContents();
			if (fieldNames.containsKey(cellValue)){//corresponding field in metadata found
				fieldNumber[count][XLS_NUMBER] = cell.getColumn();
				fieldNumber[count][CLOVER_NUMBER] = fieldNames.get(cellValue);
				fieldNames.remove(cellValue);
				count++;
			}else{
				logger.warn("There is no field \"" + cellValue + "\" in output metadata");
			}
		}
		if (count < metadata.getNumFields()) {
			logger.warn("Not all fields found:");
			for (String fieldName : fieldNames.keySet()) {
				logger.warn(fieldName);
			}
		}
	}

	
	/** 
	 * A method that shifts the date to look under the current time zone settings the same
	 * as it looks under settings of UTC. The method is used to shift the time returned by Excel parser,
	 * because it is wrongly assumed to be in UTC, while the user expects it to be interpreted according to
	 * his or her current time settings.
	 *  
	 * @param date
	 * 		an original date
	 * @return
	 * 		a date which is in the current time zone (according to system settings)
	 *      with the current daylight-saving settings (according to the current time),
	 *      transcribed by the same string as the original date in UTC.
	 * 
	 *    	Example: If the current time zone CET (with daylight saving off),
	 *      and the date is 03-03-2010 10:00:00.234 UTC, then
	 *    	the returned value is 03-03-2010 10:00:00.234 CET (i.e. 03-03-2010 09:00:00.234 UTC) 
	 */
	private Date changeTimeShiftToLocal(Date date) {
		utcCalendar.setTime(date);
		localCalendar.set(Calendar.ERA, utcCalendar.get(Calendar.ERA));
		localCalendar.set(utcCalendar.get(Calendar.YEAR),
				          utcCalendar.get(Calendar.MONTH),
				          utcCalendar.get(Calendar.DAY_OF_MONTH),
				          utcCalendar.get(Calendar.HOUR_OF_DAY),
				          utcCalendar.get(Calendar.MINUTE),
				          utcCalendar.get(Calendar.SECOND));
		localCalendar.set(Calendar.MILLISECOND, utcCalendar.get(Calendar.MILLISECOND));
		
		return localCalendar.getTime();
	}


	
	@SuppressWarnings("deprecation")
	@Override
	protected DataRecord parseNext(DataRecord record) throws JetelException {
		if (currentRow>=lastRow) return null;
		DataFieldType type;
		for (short i=0;i<fieldNumber.length;i++){
			int cloverFieldIndex = fieldNumber[i][CLOVER_NUMBER];
			int xlsFieldIndex = fieldNumber[i][XLS_NUMBER];
			if (cloverFieldIndex == -1) continue; //in metdata there is not any field corresponding to this column
			// skip all fields that are internally filled
			if (isAutoFilling[cloverFieldIndex]) {
				continue;
			}

			Cell cell = null;

			try {
				cell = sheet.getCell(xlsFieldIndex, currentRow);
			} catch (ArrayIndexOutOfBoundsException e1) {
				//more fields in metadata then in xls file
				record.getField(cloverFieldIndex).setNull(true);
				continue;
			}
			type = metadata.getField(cloverFieldIndex).getDataType();
			try{
				switch (type) {
				case DATE:
				case DATETIME:
					if (cell instanceof DateCell) {
						Date dateWronglyInUTC = ((DateCell) cell).getDate();
						Date dateInLocalTimeZone = this.changeTimeShiftToLocal(dateWronglyInUTC);
						record.getField(cloverFieldIndex).setValue(dateInLocalTimeZone);
					} else {
						throw new BadDataFormatException("Incompatible data types, xls type '" + cell.getType() + "' cannot be used to populate a clover 'date' data field.");
					}
					break;
				case BYTE:
				case STRING:
					record.getField(cloverFieldIndex).fromString(parseString(cell));
					break;
				case DECIMAL:
				case INTEGER:
				case LONG:
				case NUMBER:
					if (cell instanceof NumberCell) {
						record.getField(cloverFieldIndex).setValue(((NumberCell) cell).getValue());
					} else {
						throw new BadDataFormatException("Incompatible data types, xls type '" + cell.getType() + "' cannot be used to populate a clover 'numeric' data field.");
					}
					break;
				case BOOLEAN:
					if (cell instanceof BooleanCell) {
						record.getField(cloverFieldIndex).setValue(((BooleanCell) cell).getValue());
					} else {
						throw new BadDataFormatException("Incompatible data types, xls type '" + cell.getType() + "' cannot be used to populate a clover 'boolean' data field.");
					}
					break;
				default:
					break;
				}
			} catch (RuntimeException bdne) { // exception when trying get date or number from different cell type, or there's incorrect format
				// Fix of issue #5470: caught exception changed from ClassCastException to RuntimeException
				try {
					record.getField(cloverFieldIndex).fromString(parseString(cell));
				} catch (Exception e) {
					BadDataFormatException bdfe;
					if (bdne instanceof BadDataFormatException) {
						bdfe = (BadDataFormatException) bdne;
					} else {
						bdfe = new BadDataFormatException(bdne);
					}
					bdfe.setRecordNumber(currentRow+1);
					bdfe.setFieldNumber(cloverFieldIndex);
					if (exceptionHandler != null) { // use handler only if configured
						exceptionHandler.populateHandler(getErrorMessage(currentRow + 1, cloverFieldIndex),
								record, currentRow + 1, cloverFieldIndex,
								cell.getContents(), bdfe);
					} else {
						throw new RuntimeException(getErrorMessage(currentRow + 1, cloverFieldIndex), bdfe);
					}
				}
			}
		}
		currentRow++;
		recordCounter++;
		return record;
	}

	private String parseString(Cell cell) {
		if (cell.getType() == CellType.NUMBER) {
			double cellValue = ((NumberCell) cell).getValue();
			String cellFormat = cell.getCellFormat().getFormat().getFormatString();

			return dataFormatter.formatRawCellContents(cellValue, -1, cellFormat);
		}

		return cell.getContents();
	}

	@Override
	public void setDataSource(Object inputDataSource)
			throws ComponentNotReadyException {
		if (releaseDataSource && wb != null) {
			wb.close();
			sheetNumberIterator = null;
		}
        WorkbookSettings settings = new WorkbookSettings();
		settings.setEncoding(charset);
		InputStream input;
		if (inputDataSource instanceof InputStream) {
			input = (InputStream)inputDataSource;
		}else{
			input = Channels.newInputStream((ReadableByteChannel)inputDataSource);
		}
		// creating workbook from input stream
		try {
			wb = Workbook.getWorkbook(input, settings);
		} catch (Exception ex) {
			throw new ComponentNotReadyException(ex);
		}
		sheet = null;
		currentRow = firstRow;
		sheetCounter = -1;
		if (sheetName == null && sheetNumber != null) {
			sheetNumberIterator = new NumberIterator(sheetNumber, 0, Integer.MAX_VALUE);
		}
		if (!getNextSheet()) {
			throw new ComponentNotReadyException("There is no sheet conforming sheet name nor sheet number pattern");
		}
		logger.info("Reading data from sheet " + sheetCounter + " (" + sheet.getName() + ").");
		if (metadata != null) {
			fieldNumber = new int[metadata.getNumFields()][2];
			mapFields();
		}
	}
	
	@Override
	public void reset() throws ComponentNotReadyException {
		super.reset();

		if (wb != null) {
			wb.close();
		}

        sheet = null;
	}

	@Override
	protected boolean getNextSheet() {
		if (useIncrementalReading && sheet != null) {
			if (incremental == null) incremental = new Incremental();
			incremental.setRow(sheet.getName(), currentRow);
		}
    	if (sheetNumberIterator != null){//get next sheet conforming sheetNumber attribute
    		if (!sheetNumberIterator.hasNext()){
    			return false;
    		}
    		sheetCounter = sheetNumberIterator.next().shortValue();
    		try{
    			sheet = wb.getSheet(sheetCounter);
    		}catch(IndexOutOfBoundsException e){
    			return false;
    		}
    	}else{//get next sheet conforming sheetName attribute
    		boolean found = false;
    		while (!found){
    			try {
					sheet = wb.getSheet(++sheetCounter);
				} catch (IndexOutOfBoundsException e) {
					return false;
				}
				if (WcardPattern.checkName(sheetName, sheet.getName())) {
					found = true;
				}
    		}
    	}
        currentRow = firstRow;
        //set last row to read on set attribute or to last row in current sheet
		if (lastRowAttribute == -1 || lastRowAttribute > sheet.getRows()) {
			lastRow = sheet.getRows();
		}else{
			lastRow = lastRowAttribute;
		}
		
		discardBytes(autoFillingSheetName = sheet.getName());

		return true;
	}

	@Override
	public boolean getSheet(int sheetNumber){
		if (sheetNumber >= wb.getNumberOfSheets()) return false;
		sheet = wb.getSheet(sheetNumber);
		return true;
	}

	@Override
	public boolean getSheet(String sheetName){
		sheet = wb.getSheet(sheetName);
		return sheet != null;
	}

	public String getCharset() {
		return charset;
	}

	public void setCharset(String charset){
		this.charset = charset;
	}

	@Override
	public String[] getSheets() {
		if (wb == null) return null;
		return wb.getSheetNames();
	}

	@Override
	public String getSheetName(int index) {
		if (wb == null) return null;
		return wb.getSheet(index).getName();
	}

	@Override
    public void preExecute() throws ComponentNotReadyException {
    	reset();
    }
    
	@Override
    public void postExecute() throws ComponentNotReadyException {    	
    }
    
	@Override
    public void free() {
    	close();
    }

}
