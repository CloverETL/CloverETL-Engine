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
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.IParserExceptionHandler;
import org.jetel.exception.JetelException;
import org.jetel.exception.PolicyType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.DataRecordParsingType;
import org.jetel.util.bytes.CloverBuffer;

/**
 * Superclass for fix-length data parsers.
 * @author Jan Hadrava (jan.hadrava@javlinconsulting.cz), Javlin Consulting (www.javlinconsulting.cz)
 * @since 09/05/06  
 */
public abstract class FixLenDataParser extends AbstractParser implements TextParser {

	protected IParserExceptionHandler exceptionHandler = null;

	/**
	 * Record description.
	 */
	protected DataRecordMetadata metadata = null;
	
	/**
	 * Used for conversion to character data.
	 */
	protected CharsetDecoder decoder = null;
	
	protected ReadableByteChannel inChannel;

	protected CloverBuffer byteBuffer;

	/**
	 * Indicates whether end of input data was already reached.
	 */
	protected boolean eof;

	static Log logger = LogFactory.getLog(FixLenDataParser.class);
	
	protected int fieldCnt;
	protected int[] fieldStart;
	protected int[] fieldEnd;
	protected boolean[] isAutoFilling;
	protected int recordLength;
	protected int fieldIdx;
	protected int recordIdx;
	protected int bytesProcessed;

	private boolean releaseInputSource = true;
	
	FixLenDataParser(DataRecordMetadata metadata, String charset) {
		this.metadata = metadata;
		// initialize charset decoder
		if (charset == null) {  
			decoder = Charset.forName(Defaults.DataParser.DEFAULT_CHARSET_DECODER).newDecoder();
		} else {
			decoder = Charset.forName(charset).newDecoder();
		}		
		byteBuffer = CloverBuffer.allocateDirect(Defaults.Record.RECORDS_BUFFER_SIZE);
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#init(org.jetel.metadata.DataRecordMetadata)
	 */
	@Override
	public void init() throws ComponentNotReadyException {
		if (metadata == null) {
			throw new ComponentNotReadyException("Metadata are null");
		}
		if (metadata.getParsingType() != DataRecordParsingType.FIXEDLEN) {
			throw new ComponentNotReadyException("Fixed length data format expected but not encountered");
		}

		fieldCnt = metadata.getNumFields();
		recordIdx = 0;
		fieldIdx = 0;

		recordLength = metadata.getRecordSizeStripAutoFilling();
		fieldStart = new int[fieldCnt];
		fieldEnd = new int[fieldCnt];
		isAutoFilling = new boolean[fieldCnt];
		int prevEnd = 0;
		for (int fieldIdx = 0; fieldIdx < metadata.getNumFields(); fieldIdx++) {
			if (isAutoFilling[fieldIdx] = metadata.getField(fieldIdx).getAutoFilling() != null) {
				fieldStart[fieldIdx] = prevEnd;
				fieldEnd[fieldIdx] = prevEnd;
			} else {
				fieldStart[fieldIdx] = prevEnd + metadata.getField(fieldIdx).getShift();
				fieldEnd[fieldIdx] = fieldStart[fieldIdx] + metadata.getField(fieldIdx).getSize();
				prevEnd = fieldEnd[fieldIdx];
				if (fieldStart[fieldIdx] < 0 || fieldEnd[fieldIdx] > recordLength) {
					throw new ComponentNotReadyException("field boundaries cannot be outside record boundaries");
				}
			}
		}
	}

	@Override
	public void reset() {
		if (releaseInputSource) releaseDataSource();
		byteBuffer.clear();
		byteBuffer.flip();
		decoder.reset();
		eof = true;
		bytesProcessed = 0;
	}

	@Override
	public Object getPosition() {
		return bytesProcessed;
	}

	@Override
	public void movePosition(Object position) throws IOException {
		int pos = 0;
		if (position instanceof Integer) {
			pos = ((Integer) position).intValue();
		} else if (position != null) {
			pos = Integer.parseInt(position.toString());
		}
		if (pos > 0) {
			discardBytes(pos);
			bytesProcessed = pos;
		}
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#setDataSource(java.lang.Object)
	 */
	@Override
	public void setReleaseDataSource(boolean releaseInputSource)  {
		this.releaseInputSource = releaseInputSource;
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#setDataSource(java.lang.Object)
	 */
	@Override
	public void setDataSource(Object inputDataSource) {
		if (releaseInputSource) releaseDataSource();
		byteBuffer.clear();
		byteBuffer.flip();
		decoder.reset();
		bytesProcessed = 0;

		if (inputDataSource == null) {
			eof = true;
		} else {
			eof = false;
			if (inputDataSource instanceof ReadableByteChannel) {
				inChannel = ((ReadableByteChannel)inputDataSource);
			} else {
				inChannel = Channels.newChannel((InputStream)inputDataSource);
			}
		}
	}

	/**
	 * Discard bytes for incremental reading.
	 * 
	 * @param bytes
	 */
	protected abstract void discardBytes(int bytes) throws IOException;
	
	/**
	 * Release data source.  
	 */
	private void releaseDataSource() {
		if (inChannel == null || !inChannel.isOpen()) {
			return;
		}
		try {
			inChannel.close();
		} catch (IOException e) {
			logger.error(e.getStackTrace());
		}
		inChannel = null;
	}
	
	/**
	 * Release resources.
	 */
	@Override
	public void close() {
		releaseDataSource();
	}

	@Override
	public DataRecord getNext() throws JetelException {
		DataRecord rec = DataRecordFactory.newRecord(metadata);
		rec.init();
		return getNext(rec);
	}

	@Override
	public DataRecord getNext(DataRecord record) throws JetelException {
		DataRecord retval; 
		while (true) {
			retval = parseNext(record);
			if (exceptionHandler == null || !exceptionHandler.isExceptionThrowed()) {
				return retval;
			}
			exceptionHandler.handleException();				
		}
	}
	
	/**
	 * Obtains raw data and tries to fill record fields with them.
	 * @param record Output record, cannot be null.
	 * @return null when no more data are available, output record otherwise.
	 * @throws JetelException
	 */
	protected abstract DataRecord parseNext(DataRecord record)
	throws JetelException;

	@Override
	public abstract int skip(int nRec)
	throws JetelException;

	/**
	 * Fill bad-format exception handler with relevant data.
	 * @param errorMessage
	 * @param record
	 * @param recordNumber
	 * @param fieldNumber
	 * @param offendingValue
	 * @param exception
	 */
	protected void fillXHandler(DataRecord record, CharSequence offendingValue,
        BadDataFormatException exception) {
		
		exception.setFieldNumber(fieldIdx);
		exception.setRecordNumber(recordIdx);
		
		if (exceptionHandler == null) { // no handler available
			throw exception;			
		}
		// set handler
		exceptionHandler.populateHandler(exception.getMessage(), record, recordIdx - 1,
				fieldIdx, offendingValue.toString(), exception);
	}
		
	@Override
	public IParserExceptionHandler getExceptionHandler() {
		return exceptionHandler;
	}

	@Override
	public void setExceptionHandler(IParserExceptionHandler handler) {
		exceptionHandler = handler;
	}

	@Override
	public PolicyType getPolicyType() {
		return exceptionHandler != null ? exceptionHandler.getType() : null;
	}

	public String getCharsetName() {
		if (decoder == null) {
			return null;
		}
		return decoder.charset().name();
	}


	public boolean isEnableIncomplete() {
		return false;
	}

	public void setEnableIncomplete(boolean enableIncomplete) {
		// quietly ignore it
		return;
	}

	public boolean isSkipEmpty() {
		return false;
	}

	public void setSkipEmpty(boolean skipEmpty) {
		// quietly ignore it
		return;
	}

	public boolean isSkipLeadingBlanks() {
		return false;
	}

	public void setSkipLeadingBlanks(boolean skipLeadingBlanks) {
		// quietly ignore it
		return;
	}

	public boolean isSkipTrailingBlanks() {
		return false;
	}

	public void setSkipTrailingBlanks(boolean skipTrailingBlanks) {
		// quietly ignore it
		return;
	}
	
	public static FixLenDataParser createParser(DataRecordMetadata metadata, boolean byteMode) {
		if (byteMode) {
			return new FixLenByteDataParser(metadata);
		} else {
			return new FixLenCharDataParser(metadata);			
		}			
	}

	@Override
	public TextParserConfiguration getConfiguration() {
		return null;
	}
	
}
