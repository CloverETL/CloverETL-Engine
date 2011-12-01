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
import java.util.List;

import org.apache.poi.POIXMLDocument;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.SpreadsheetUtils.SpreadsheetFormat;

/**
 * @author lkrejci (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 26 Aug 2011
 */
public class SpreadsheetStreamParser extends AbstractSpreadsheetParser {
	
	private SpreadsheetFormat currentFormat = null;
	private SpreadsheetStreamHandler xlsHandler;
	private SpreadsheetStreamHandler xlsxHandler;
	private SpreadsheetStreamHandler currentHandler;

	public SpreadsheetStreamParser(DataRecordMetadata metadata, XLSMapping mappingInfo) {
		super(metadata, mappingInfo);
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
		
		if (POIFSFileSystem.hasPOIFSHeader(inputStream)) {
			format = SpreadsheetFormat.XLS;
		} else if (POIXMLDocument.hasOOXMLHeader(inputStream)) {
			format = SpreadsheetFormat.XLSX;
		} else {
			throw new ComponentNotReadyException("Your InputStream was neither an OLE2 stream, nor an OOXML stream");
		}
		
		if (currentFormat != format) {
			switch (format) {
			case XLS:
				if (xlsHandler == null) {
					xlsHandler = new XLSStreamParser(this, metadata);
					xlsHandler.init();
				}
				currentHandler = xlsHandler;
				break;
			case XLSX:
				if (xlsxHandler == null) {
					xlsxHandler = new XLSXStreamParser(this, metadata);
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
}
