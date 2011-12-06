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
import java.io.PushbackInputStream;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;

import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ExcelUtils;
import org.jetel.util.ExcelUtils.ExcelType;
import org.jetel.util.SpreadsheetUtils.SpreadsheetFormat;

/**
 * @author lkrejci (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 * @author sgerguri (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 26 Aug 2011
 */
public class SpreadsheetStreamParser extends AbstractSpreadsheetParser {
	
	private SpreadsheetFormat currentFormat = null;
	private SpreadsheetStreamHandler xlsHandler;
	private SpreadsheetStreamHandler xlsxHandler;
	private SpreadsheetStreamHandler currentHandler;
	
	private String password;

	public SpreadsheetStreamParser(DataRecordMetadata metadata, XLSMapping mappingInfo, String password) {
		super(metadata, mappingInfo);
		this.password = password;
	}

	@Override
	public int skip(int nRec) throws JetelException {
		return currentHandler.skip(nRec);
	}

	@Override
	protected List<String> getSheetNames() throws IOException {
		return currentHandler.getSheetNames();
	}

	@Override
	protected boolean setCurrentSheet(int sheetNumber) {
		return currentHandler.setCurrentSheet(sheetNumber);
	}

	@Override
	protected String[][] getHeader(int startRow, int startColumn, int endRow, int endColumn) throws ComponentNotReadyException {
		return currentHandler.getHeader(startRow, startColumn, endRow, endColumn);
	}

	@Override
	protected int getRecordStartRow() {
		return currentHandler.getCurrentRecordStartRow();
	}

	@Override
	protected void prepareInput(InputStream inputStream) throws IOException, ComponentNotReadyException {
		SpreadsheetFormat format;
		
		if (!inputStream.markSupported()) {
			inputStream = new PushbackInputStream(inputStream, 8);
		}
		
		InputStream bufferedStream = null;
		ExcelType documentType = ExcelUtils.getStreamType(inputStream);
		if (documentType == ExcelType.XLS) {
			bufferedStream = ExcelUtils.getBufferedStream(inputStream);
			inputStream = ExcelUtils.getDecryptedXLSXStream(bufferedStream, password);
			format = SpreadsheetFormat.XLSX;
			if (inputStream == null) {
				bufferedStream.reset();
				inputStream = bufferedStream;
				format = SpreadsheetFormat.XLS;
			}
		} else if (documentType == ExcelType.XLSX) {
			format = SpreadsheetFormat.XLSX;
		} else {
			throw new ComponentNotReadyException("Your InputStream was neither an OLE2 stream, nor an OOXML stream");
		}
		
		if (currentFormat != format) {
			switch (format) {
			case XLS:
				if (xlsHandler == null) {
					xlsHandler = new XLSStreamParser(this, password);
					xlsHandler.init();
				}
				currentHandler = xlsHandler;
				break;
			case XLSX:
				if (xlsxHandler == null) {
					xlsxHandler = new XLSXStreamParser(this);
					xlsxHandler.init();
				}
				currentHandler = xlsxHandler;
				break;
			}
		}
		currentFormat = format;
		currentHandler.prepareInput(inputStream);
	}

	@Override
	protected DataRecord parseNext(DataRecord record) throws JetelException {
		record.setToNull();
		return currentHandler.parseNext(record);
	}

	@Override
	public void close() throws IOException {
		super.close();
		currentHandler.close();
	}
	
	
	/**
	 * Intended to be used as a buffer of field values of records which parser will send to output after current parser record.
	 * Result of refactoring of duplicated code in {@link XLSXStreamParser.RecordFillingContentHandler} and {@link XLSStreamParser.RecordFillingHSSFListener}.
	 * 
	 * @author tkramolis (info@cloveretl.com)
	 *         (c) Javlin, a.s. (www.cloveretl.com)
	 *
	 * @param <T> type of row field value read from input file
	 * @created Dec 6, 2011
	 */
	class CellBuffers<T> {

		private T[][] cellBuffers;
		private int nextPartial;
		private RecordFieldValueSetter<T> fieldValueSetter;
		
		@SuppressWarnings("unchecked")
		public void init(Class<T> clazz, RecordFieldValueSetter<T> fieldValueSetter) {
			this.fieldValueSetter = fieldValueSetter;
			int numberOfBuffers = (mapping.length / mappingInfo.getStep()) - (mapping.length % mappingInfo.getStep() == 0 ? 1 : 0);
			cellBuffers = (T[][]) Array.newInstance(clazz, numberOfBuffers, metadata.getNumFields());
		}
		
		public int getCount() {
			return cellBuffers.length;
		}
		
		/**
		 * Returns index of cell buffer of future records.
		 * @param recordOffset 0 means buffer of next record, 1 means buffer of record after next record, etc.
		 */
		private int getCellBufferIndex(int recordOffset) {
			return (nextPartial + recordOffset) % cellBuffers.length;
		}
		
		private T[] getBuffer(int recordOffset) {
			return cellBuffers[getCellBufferIndex(recordOffset)];
		}

		private void moveToNextCellBuffer() {
			Arrays.fill(cellBuffers[getCellBufferIndex(0)], null);
			nextPartial = ((nextPartial + 1) % cellBuffers.length);
		}

		public void clear() {
			for (int i = 0; i < cellBuffers.length; i++) {
				Arrays.fill(cellBuffers[i], null);
			}
		}
		
		public void fillRecordFromBuffer(DataRecord record) {
			if (getCount() > 0) {
				T[] cellBuffer = getBuffer(0);
				for (int i = 0; i < cellBuffer.length; i++) {
					if (cellBuffer[i] != null) {
						fieldValueSetter.setFieldValue(i, cellBuffer[i]);
					}
				}
				moveToNextCellBuffer();
			}
		}
		
		public void setCellBufferValue(int mappingRow, int mappingColumn, T bufferCellValue) {
			for (int i = 0; i < getCount(); i++) {
				if ((mappingRow -= (mappingInfo.getStep())) >= 0) {
					if (mapping[mappingRow][mappingColumn] != XLSMapping.UNDEFINED) {
						getBuffer(i)[mapping[mappingRow][mappingColumn]] = bufferCellValue;
						break;
					}
				}
			}
		}
		
	}

	interface RecordFieldValueSetter<T> {
		public void setFieldValue(int fieldIndex, T value);
	}

}
