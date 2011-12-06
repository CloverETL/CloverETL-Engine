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

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.parser.XLSMapping.HeaderGroup;
import org.jetel.data.parser.XLSMapping.HeaderRange;
import org.jetel.data.parser.XLSMapping.SpreadsheetMappingMode;
import org.jetel.data.parser.XLSMapping.SpreadsheetOrientation;
import org.jetel.data.parser.XLSMapping.Stats;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.IParserExceptionHandler;
import org.jetel.exception.JetelException;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.exception.PolicyType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.AutoFilling;
import org.jetel.util.SpreadsheetIndexIterator;

/**
 * Common superclass of spreadsheet parsers.
 * 
 * Handles setting of number of sheet which should be parsed according to sheetName ({@link #setSheet(String)})
 * attribute, "spread_sheet" clover field autofilling, incremental reading and exception handling.
 * 
 * @author tkramolis (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 11 Aug 2011
 */
public abstract class AbstractSpreadsheetParser implements Parser {
	final static Log LOGGER = LogFactory.getLog(AbstractSpreadsheetParser.class);

	protected static final String DEFAULT_SHEET_NUMBER = "0";
	protected static final boolean USE_DATE1904 = false; // TODO date system
	
	private int[] autofillingFieldPositions;

	private boolean releaseInputSource;
	private IParserExceptionHandler exceptionHandler;

	private boolean useIncrementalReading;
	private Incremental incremental;

	private String sheet;
	private SpreadsheetIndexIterator sheetIndexIterator;
	private List<String> sheetNames;

	private List<Integer> unusedFields;
	
	protected int mappingMinRow; // 0-based
	protected int mappingMinColumn;// 0-based
	
	private int currentSheetIndex = -1;

	protected DataRecordMetadata metadata;
	protected final XLSMapping mappingInfo;
	protected int[][] mapping;
	protected int startLine;
	
	private static final Comparator<CellMappedByOrder> CELL_ORDER_COMPARATOR =
			new Comparator<CellMappedByOrder>() {
				@Override
				public int compare(CellMappedByOrder c1, CellMappedByOrder c2) {
					int rowDiff = c1.row - c2.row;
					if (rowDiff != 0) return rowDiff;
					return c1.column - c2.column;
				}
			};

	public AbstractSpreadsheetParser(DataRecordMetadata metadata, XLSMapping mappingInfo) {
		this.metadata = metadata;
		if (mappingInfo == null) {
			this.mappingInfo = XLSMapping.getDefault();
		} else {
			this.mappingInfo = mappingInfo;
		}
	}

	@Override
	public void setDataSource(Object dataSource) throws IOException, ComponentNotReadyException {
		if (dataSource == null) {
			throw new NullPointerException("dataSource");
		}

		InputStream dataInputStream = null;
		if (dataSource instanceof InputStream) {
			dataInputStream = (InputStream) dataSource;
		} else if (dataSource instanceof ReadableByteChannel) {
			dataInputStream = Channels.newInputStream((ReadableByteChannel) dataSource);
		} else {
			throw new IllegalArgumentException(dataSource.getClass() + " not supported as a data source");
		}

		try {
			prepareInput(dataInputStream);
		} finally {
			if (releaseInputSource) {
				try {
					dataInputStream.close();
				} catch (IOException exception) {
					throw new ComponentNotReadyException("Error releasing the data source!", exception);
				}
			}
		}

		this.sheetNames = getSheetNames();
		sheetIndexIterator = new SpreadsheetIndexIterator(sheetNames, sheet, 0, sheetNames.size());

		if (!nextL3Source()) {
			throw new ComponentNotReadyException("There is no sheet conforming sheet pattern");
		}
	}

	@Override
	public DataRecord getNext() throws JetelException {
		DataRecord record = new DataRecord(metadata);
		record.init();
		return getNext(record);
	}

	@Override
	public DataRecord getNext(DataRecord record) throws JetelException {
		if (record == null) {
			throw new NullPointerException("record");
		}

		record = parseNext(record);
		setAutofillingSheetName(record);

		if (exceptionHandler != null) {
			while (exceptionHandler.isExceptionThrowed()) {
				exceptionHandler.handleException();
				record = parseNext(record);
			}
		}
		return record;
	}

	/**
	 * get names of all sheets
	 */
	protected abstract List<String> getSheetNames() throws IOException;

	/**
	 * Set index of sheet which is about to be parsed
	 * 
	 * @param sheetNumber
	 *            0-based sheet index
	 * @return false iff there's no sheet with specified number
	 */
	protected abstract boolean setCurrentSheet(int sheetNumber);

	/**
	 * Get content of header
	 * 
	 * @param firstRow - first row read (inclusive)
	 * @param firstColumn - first column to read (inclusive)
	 * @param lastRow - last row read (exclusive)
	 * @param lastColumn - last row to read (exclusive)
	 * @return
	 * @throws ComponentNotReadyException
	 */
	protected abstract String[][] getHeader(int firstRow, int firstColumn, int lastRow, int lastColumn) throws ComponentNotReadyException;

	/**
	 * Prepares input file to be read
	 * 
	 * @param inputStream
	 * @throws IOException
	 * @throws ComponentNotReadyException
	 */
	protected abstract void prepareInput(InputStream inputStream) throws IOException, ComponentNotReadyException;

	/**
	 * Produces next record from input source or returns null if there are no more data.
	 */
	protected abstract DataRecord parseNext(DataRecord record) throws JetelException;

	/**
	 * Returns record start line   
	 * 
	 * @return
	 */
	protected abstract int getRecordStartRow();

	protected void handleException(BadDataFormatException bdfe, DataRecord record, int cloverFieldIndex, String cellValue) {
		int recordNumber = (getRecordStartRow() - startLine) / mappingInfo.getStep(); //TODO: check record numbering
		bdfe.setRecordNumber(recordNumber);
		bdfe.setFieldNumber(cloverFieldIndex);

		if (exceptionHandler != null) { // use handler only if configured
			exceptionHandler.populateHandler(getErrorMessage(bdfe.getMessage(), recordNumber, cloverFieldIndex), record, recordNumber, cloverFieldIndex, cellValue, bdfe);
		} else {
			throw new RuntimeException(getErrorMessage(bdfe.getMessage(), recordNumber, cloverFieldIndex));
		}
	}

	private String getErrorMessage(String exceptionMessage, int recNo, int fieldNo) {
		StringBuffer message = new StringBuffer();
		message.append(exceptionMessage);
		message.append(" when parsing record starting at row ");
		message.append(getRecordStartRow());
		message.append(" field '");
		message.append(metadata.getField(fieldNo).getName() + '\'');
		return message.toString();
	}

	private void setAutofillingSheetName(DataRecord record) {
		if (record == null || autofillingFieldPositions == null) {
			return;
		}

		for (int i = 0; i < autofillingFieldPositions.length; i++) {
			record.getField(autofillingFieldPositions[i]).setValue(sheetNames.get(currentSheetIndex));
		}
	}

	/**
	 * Creates autofilling for sheet_name.
	 */
	private void prepareAutofilling() {
		List<Integer> autofillingFields = new ArrayList<Integer>(metadata.getNumFields());
		for (int i = 0; i < metadata.getNumFields(); i++) {
			String autofilling = metadata.getField(i).getAutoFilling();
			if (autofilling != null && autofilling.equalsIgnoreCase(AutoFilling.SHEET_NAME)) {
				autofillingFields.add(i);
			}
		}
		if (autofillingFields.size() > 0) {
			autofillingFieldPositions = new int[autofillingFields.size()];
			for (int i = 0; i < autofillingFieldPositions.length; i++) {
				autofillingFieldPositions[i] = autofillingFields.get(i);
			}
		}
	}

	private void initMapping() throws ComponentNotReadyException {
		if (mappingInfo.getHeaderGroups().isEmpty()) {
			// create default mapping
			mapping = new int[1][metadata.getNumFields()];
			for (int i = 0; i < metadata.getNumFields(); i++) {
				mapping[0][i] = i;
			}
		} else {
			Stats stats = mappingInfo.getStats();
			
			/** extract some frequently used stats*/
			mappingMinRow = stats.getMappingMinRow();
			mappingMinColumn = stats.getMappingMinColumn();
			startLine = stats.getStartLine();
			
			mapping = new int[stats.getRowCount()][stats.getColumnCount()];
			clearMapping();
			
			if (!stats.useAutoNameMapping() && !stats.useNameMapping()) {
				resolveDirectMapping();
				resolveOrderMapping();
			}
		}
	}

	protected void clearMapping() {
		for (int i = 0; i < mapping.length; i++) {
			Arrays.fill(mapping[i], -1);
		}
		fillUnusedFields();
	}

	protected void resolveDirectMapping() throws ComponentNotReadyException {
		Set<Integer> toRemove = new HashSet<Integer>();
		for (HeaderGroup group : mappingInfo.getHeaderGroups()) {
			if (group.getCloverField() != XLSMapping.UNDEFINED) {
				HeaderRange range = group.getRanges().get(0);
				
				setMappingFieldIndex(range.getRowStart(), range.getColumnStart(), group, group.getCloverField());
				
				if (unusedFields.isEmpty()) {
					throw new ComponentNotReadyException("Number of clover fields and cells must be the same");
				}
				if (!toRemove.add(group.getCloverField())) {
					throw new ComponentNotReadyException("Field '" + metadata.getField(group.getCloverField()).getName() + "' already used!");
				}
			}
		}
		unusedFields.removeAll(toRemove);
	}

	/*
	 * @Override protected void mapNames(Map<String, Integer> fieldNames) throws ComponentNotReadyException { if
	 * (fieldNames == null) { throw new NullPointerException("fieldNames"); }
	 * 
	 * List<CellValue> cellValues = readRow(metadataRow); if (cellValues == null) { throw new
	 * ComponentNotReadyException("Metedata row set to " + metadataRow + ", but sheet " + currentSheetNum +
	 * " has less rows"); }
	 * 
	 * int numberOfFoundFields = 0;
	 * 
	 * for (CellValue cellValue : cellValues) { if (fieldNames.containsKey(cellValue.value)) {// corresponding field in
	 * metadata found fieldNumber[numberOfFoundFields][XLS_NUMBER] = cellValue.columnIndex;
	 * fieldNumber[numberOfFoundFields][CLOVER_NUMBER] = fieldNames.get(cellValue.value); numberOfFoundFields++;
	 * 
	 * fieldNames.remove(cellValue); } else { logger.warn("There is no field \"" + cellValue + "\" in output metadata");
	 * } }
	 * 
	 * if (numberOfFoundFields < metadata.getNumFields()) { logger.warn("Not all fields found:");
	 * 
	 * for (String fieldName : fieldNames.keySet()) { logger.warn(fieldName); } } }

	/*
	 * @Override protected void cloverfieldsAndXlsNames(Map<String, Integer> fieldNames) throws
	 * ComponentNotReadyException { if (fieldNames == null) { throw new NullPointerException("fieldNames"); }
	 * 
	 * if (cloverFields.length != xlsFields.length) { throw new
	 * ComponentNotReadyException("Number of clover fields and XLSX fields must be the same"); }
	 * 
	 * List<CellValue> cellValues = readRow(metadataRow); if (cellValues == null) { throw new
	 * ComponentNotReadyException("Metedata row set to " + metadataRow + ", but sheet " + currentSheetNum +
	 * " has less rows"); } int numberOfFoundFields = 0;
	 * 
	 * for (CellValue cellValue : cellValues) { int xlsNumber = StringUtils.findString(cellValue.value, xlsFields);
	 * 
	 * if (xlsNumber > -1) {// string from cell found in xlsFields attribute
	 * fieldNumber[numberOfFoundFields][XLS_NUMBER] = cellValue.columnIndex; try {
	 * fieldNumber[numberOfFoundFields][CLOVER_NUMBER] = fieldNames.get(cloverFields[xlsNumber]); }catch
	 * (NullPointerException ex) { throw new ComponentNotReadyException("Clover field \"" + cloverFields[xlsNumber] +
	 * "\" not found"); } numberOfFoundFields++; } else { logger.warn("There is no field corresponding to \"" +
	 * cellValue.value + "\" in output metadata"); } }
	 * 
	 * if (numberOfFoundFields < cloverFields.length) { logger.warn("Not all fields found"); } }
	 */// TODO: double check if behaviour is the same!

	protected void resolveNameMapping() throws ComponentNotReadyException {
		Map<String, Integer> nameMap = metadata.getFieldNamesMap();
		Stats stats = mappingInfo.getStats();
		
		String[][] headerCells = getHeader(stats.getMappingMinRow(), stats.getMappingMinColumn(), stats.getMappingMaxRow() + 1, stats.getMappingMaxColumn() + 1);
		for (HeaderGroup group : mappingInfo.getHeaderGroups()) {
			for (HeaderRange range : group.getRanges()) {
				if (group.getCloverField() != XLSMapping.UNDEFINED) {
					continue;
				}
				if ((group.getMappingMode() != SpreadsheetMappingMode.NAME || (stats.useAutoNameMapping() && group.getMappingMode() != SpreadsheetMappingMode.AUTO))) {
					continue;
				}
				for (int row = range.getRowStart(); row <= range.getRowEnd() && row - stats.getMappingMinRow() < headerCells.length; row++) {
					String[] headerRow = headerCells[row - stats.getMappingMinRow()];
					for (int column = range.getColumnStart(); column <= range.getColumnEnd() && column - stats.getMappingMinColumn() < headerRow.length; column++) {
						String header = headerRow[column - stats.getMappingMinColumn()]; // TODO: perform "name mangling"?

						Integer cloverIndex = nameMap.get(header);
						if (cloverIndex == null) {
							LOGGER.warn("There is no field \"" + header + "\" in output metadata");
							continue;
						}

						if (!unusedFields.remove(cloverIndex)) {
							throw new ComponentNotReadyException("Ambiguous mapping!"); // TODO: improve!
						}

						setMappingFieldIndex(row, column, group, cloverIndex);
					}
				}
			}
		}
	}
	
	protected void resolveOrderMapping() throws ComponentNotReadyException {
		Stats stats = mappingInfo.getStats();
		
		List<CellMappedByOrder> mappedCells = new ArrayList<CellMappedByOrder>();
		for (HeaderGroup group : mappingInfo.getHeaderGroups()) {
			if (group.getCloverField() != XLSMapping.UNDEFINED) {
				continue;
			}
			
			if (group.getMappingMode() != SpreadsheetMappingMode.ORDER 
					|| (stats.useAutoNameMapping() && group.getMappingMode() == SpreadsheetMappingMode.AUTO)) {
				continue;
			}

			for (HeaderRange range : group.getRanges()) {
				for (int row = range.getRowStart(); row <= range.getRowEnd(); row++) {
					for (int column = range.getColumnStart(); column <= range.getColumnEnd(); column++) {
						mappedCells.add(new CellMappedByOrder(row, column, group));
					}
				}
			}
		}

		if (mappedCells.size() > unusedFields.size()) {
			// TODO: check in checkConfig
			throw new ComponentNotReadyException("Invalid cells mapping by order! There are " + mappedCells.size() +
					" cells mapped by order, but only " + unusedFields.size() + " available metadata fields.");
		}
		
		Collections.sort(mappedCells, CELL_ORDER_COMPARATOR);
		
		for (CellMappedByOrder cell : mappedCells) {
			setMappingFieldIndex(cell.row, cell.column, cell.group, unusedFields.remove(0));
		}
	}

	/**
	 * Inserts Clover field index into mapping array.
	 * @param rangeCellRow row of a cell C in some header range R
	 * @param rangeCellColumn column of the cell C in header range R
	 * @param group header group of header range R
	 * @param cloverFieldIndex index of Clover metadata field to which cell C is mapped
	 */
	private void setMappingFieldIndex(int rangeCellRow, int rangeCellColumn, HeaderGroup group, int cloverFieldIndex) {
		int mappingRow = rangeCellRow;
		int mappingColumn = rangeCellColumn;
		if (mappingInfo.getOrientation() == SpreadsheetOrientation.VERTICAL) {
			mappingRow += group.getSkip() - mappingInfo.getStats().getStartLine();
			mappingColumn -= mappingInfo.getStats().getMappingMinColumn();
		} else {
			mappingColumn += group.getSkip() - mappingInfo.getStats().getStartLine();
			mappingRow -= mappingInfo.getStats().getMappingMinRow();
		}
		mapping[mappingRow][mappingColumn] = cloverFieldIndex;
	}

	
	private void fillUnusedFields() {
		unusedFields = new ArrayList<Integer>();
		for (int i = 0; i < metadata.getNumFields(); i++) {
			unusedFields.add(i);
		}
	}

	@Override
	public void init() throws ComponentNotReadyException {
		if (metadata == null) {
			throw new ComponentNotReadyException("Metadata must not be null");
		}

		prepareAutofilling();
		initMapping();
	}

	@Override
	public void preExecute() throws ComponentNotReadyException {
		// Do nothing
	}

	@Override
	public void postExecute() throws ComponentNotReadyException {
		// Do nothing
	}

	@Override
	public void reset() throws ComponentNotReadyException {
		// Do nothing
	}

	@Override
	public void free() throws ComponentNotReadyException, IOException {
		close();
	}

	@Override
	public void close() throws IOException {
		// Do nothing
	}

	@Override
	public void setReleaseDataSource(boolean releaseInputSource) {
		this.releaseInputSource = releaseInputSource;
	}

	@Override
	public void setExceptionHandler(IParserExceptionHandler handler) {
		this.exceptionHandler = handler;
	}

	@Override
	public IParserExceptionHandler getExceptionHandler() {
		return exceptionHandler;
	}

	@Override
	public PolicyType getPolicyType() {
		if (exceptionHandler != null) {
			return exceptionHandler.getType();
		}
		return null;
	}

	@Override
	public Object getPosition() {
		return ((incremental != null) ? incremental.getPosition() : null);
	}

	@Override
	public void movePosition(Object position) throws IOException {
		if (!useIncrementalReading) {
			return;
		}

		incremental = new Incremental(position.toString());
		skipAlreadyRead(currentSheetIndex);
	}

	/**
	 * Skips already read rows during incremental reading
	 * 
	 * @param sheetIndex
	 */
	private void skipAlreadyRead(int sheetIndex) {
		if (incremental == null) {
			return;
		}

		Integer rowPosition = incremental.getRow(sheetNames.get(sheetIndex));
		if (rowPosition != null && rowPosition > 0) {
			try {
				skip((rowPosition - startLine) / mappingInfo.getStep());
			} catch (JetelException e) {
				throw new JetelRuntimeException(e);
			}
		}
	}

	/**
	 * @param sheetName
	 *            name(s) of sheet(s) which will be read.
	 */
	public void setSheet(String sheet) {
		this.sheet = sheet;
	}

	/**
	 * Moves to next sheet which should be parsed
	 */
	@Override
	public boolean nextL3Source() {
		if (useIncrementalReading && currentSheetIndex > -1) {
			if (incremental == null) {
				incremental = new Incremental();
			}
			incremental.setRow(sheetNames.get(currentSheetIndex), getRecordStartRow());
		}

		if (!sheetIndexIterator.hasNext()) {
			return false;
		}

		int nextSheet = sheetIndexIterator.next();

		if (setCurrentSheet(nextSheet)) {
			currentSheetIndex = nextSheet;
			Stats stats = mappingInfo.getStats();

			if (stats.useAutoNameMapping() || stats.useNameMapping()) {
				clearMapping();
				try {
					resolveDirectMapping();
					resolveNameMapping();
					resolveOrderMapping();
				} catch (ComponentNotReadyException e) {
					throw new JetelRuntimeException(e.getMessage(), e);
				}
			}
			skipAlreadyRead(nextSheet);
			return true;
		}

		return false;
	}	

	/**
	 * For incremental reading.
	 */
	protected static class Incremental {
		private Map<String, Integer> sheetRow;

		public Incremental() {
			this(null);
		}

		public Incremental(String position) {
			sheetRow = new HashMap<String, Integer>();
			parsePosition(position);
		}

		private void parsePosition(String position) {
			if (position == null) {
				return;
			}

			String[] entries = position.split("#");
			if (entries.length != 2) {
				return;
			}

			String[] sheets = entries[0].split(",");
			String[] rows = entries[1].split(",");
			if (sheets.length != rows.length) {
				return;
			}

			try {
				for (int i = 0; i < sheets.length; i++) {
					sheetRow.put(sheets[i], Integer.parseInt(rows[i]));
				}
			} catch (NumberFormatException e) {
				sheetRow.clear();
				return;
			}
		}

		public Integer getRow(String sheetName) {
			return sheetRow.get(sheetName);
		}

		public void setRow(String sheet, int row) {
			sheetRow.put(sheet, row);
		}

		public String getPosition() {
			StringBuilder sbKey = new StringBuilder();
			StringBuilder sbValue = new StringBuilder();
			if (sheetRow.isEmpty()) {
				return "";
			}

			for (Entry<String, Integer> entry : sheetRow.entrySet()) {
				sbKey.append(entry.getKey()).append(",");
				sbValue.append(entry.getValue()).append(",");
			}
			sbKey.deleteCharAt(sbKey.length() - 1);
			sbValue.deleteCharAt(sbValue.length() - 1);
			sbKey.append("#");

			return sbKey.append(sbValue.toString()).toString();
		}
	}
	
	private static class CellMappedByOrder {
		public final int row;
		public final int column;
		public final HeaderGroup group;

		public CellMappedByOrder(int row, int column, HeaderGroup group) {
			this.row = row;
			this.column = column;
			this.group = group;
		}
	}
}
