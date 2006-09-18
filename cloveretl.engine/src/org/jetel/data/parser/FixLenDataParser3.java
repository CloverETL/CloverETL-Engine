/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2006 Javlin Consulting <info@javlinconsulting>
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
package org.jetel.data.parser;

import java.io.FileInputStream;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.IParserExceptionHandler;
import org.jetel.exception.JetelException;
import org.jetel.exception.PolicyType;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Wrapper for fix-length data parsers working in byte and char mode.
 * @author Jan Hadrava, Javlin Consulting (www.javlinconsulting.cz)
 *
 */
public abstract class FixLenDataParser3 implements Parser {

	protected IParserExceptionHandler exceptionHandler = null;

	/**
	 * Record description.
	 */
	protected DataRecordMetadata metadata = null;
	
	/**
	 * Used for conversion to character data.
	 */
	protected CharsetDecoder decoder = null;

	protected int fieldCnt;
	protected int[] fieldLengths;

	FixLenDataParser3(String charset) {
		// initialize charset decoder
		if (charset == null) {  
			decoder = Charset.forName(Defaults.DataParser.DEFAULT_CHARSET_DECODER).newDecoder();
		} else {
			decoder = Charset.forName(charset).newDecoder();
		}		
	}
	
	/**
	 * Performs basic initialization tasks.
	 * @param inputDataSource 
	 * @param metadata
	 * @return Open input channel.
	 * @throws ComponentNotReadyException
	 */
	protected ReadableByteChannel init(Object inputDataSource, DataRecordMetadata metadata) throws ComponentNotReadyException {
		if (metadata.getRecType() != DataRecordMetadata.FIXEDLEN_RECORD) {
			throw new RuntimeException("Fixed length data format expected but not encountered");
		}
		this.metadata = metadata;

		fieldCnt = metadata.getNumFields();
		fieldLengths = new int[fieldCnt];
		for (int fieldIdx = 0; fieldIdx < metadata.getNumFields(); fieldIdx++) {
			fieldLengths[fieldIdx] = metadata.getField(fieldIdx).getSize();
		}

		return ((FileInputStream)inputDataSource).getChannel(); 
	}
	
	public DataRecord getNext() throws JetelException {
		DataRecord rec = new DataRecord(metadata);
		rec.init();
		return getNext(rec);
	}

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

	public abstract void close();

	public abstract void open(Object inputDataSource, DataRecordMetadata _metadata)
	throws ComponentNotReadyException;

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
	protected void fillXHandler(DataRecord record,
        int recordNumber, int fieldNumber, String offendingValue,
        BadDataFormatException exception) {
		
		exception.setFieldNumber(fieldNumber);
		exception.setRecordNumber(recordNumber);
		
		if (exceptionHandler == null) { // no handler available
			throw new RuntimeException(exception.getMessage());			
		}
		// set handler
		exceptionHandler.populateHandler(exception.getMessage(), record, recordNumber,
				fieldNumber, offendingValue, exception);
	}
		
	public IParserExceptionHandler getExceptionHandler() {
		return exceptionHandler;
	}

	public void setExceptionHandler(IParserExceptionHandler handler) {
		exceptionHandler = handler;
	}

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

}
