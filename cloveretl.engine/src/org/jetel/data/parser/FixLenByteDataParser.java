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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.IParserExceptionHandler;
import org.jetel.exception.JetelException;
import org.jetel.exception.PolicyType;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Parser for sequence of records represented by fixed count of bytes
 * 
 * @author Jan Hadrava, Javlin Consulting (www.javlinconsulting.cz)
 */
public class FixLenByteDataParser implements Parser {

	private IParserExceptionHandler exceptionHandler;

	/**
	 * Record description.
	 */
	private DataRecordMetadata metadata;
	
	/**
	 * Used for conversion to character data.
	 */
	private CharsetDecoder decoder;

	/**
	 * Input record channel.
	 */
	private RecordChannel recChannel;

	/**
	 * Create instance for specified charset.
	 * @param charset
	 */
	public FixLenByteDataParser(String charset) {
		init(charset);
	}

	/**
	 * Create instance for default charset. 
	 */
	public FixLenByteDataParser() {
		init(null);
	}

	/**
	 * Common code for ctors.
	 */
	private void init(String charset) {
		// initialize charset decoder
		if (charset == null) {	// byte data expected  
			decoder = Charset.forName(Defaults.DataParser.DEFAULT_CHARSET_DECODER).newDecoder();
		} else {				// char data expected
			decoder = Charset.forName(charset).newDecoder();
		} 
	}

	/**
	 * Parser iface.
	 */
	public void open(Object inputDataSource, DataRecordMetadata _metadata)
			throws ComponentNotReadyException {
		if (metadata.getRecType() != DataRecordMetadata.FIXEDLEN_RECORD) {
			throw new RuntimeException("Fixed length data format expected but not encountered");
		}
		metadata = _metadata;
		ReadableByteChannel byteChannel =((FileInputStream)inputDataSource).getChannel(); 
		recChannel = new RecordChannel(metadata, byteChannel);
	}

	/**
	 * Parser iface.
	 */
	public void close() {
		if (recChannel != null) {
			recChannel.close();
		}
	}
	
	/**
	 * Parser iface.
	 */
	public DataRecord getNext() throws JetelException {
		DataRecord rec = new DataRecord(metadata);
		rec.init();
		return getNext(rec);
	}

	/**
	 * Parser iface.
	 */
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
	private DataRecord parseNext(DataRecord record) throws JetelException {
		record.init();
		ByteBuffer rawRec = null;
		try {
			rawRec = recChannel.getNext();
		} catch (BadDataFormatException e) {
			fillXHandler(record, recChannel.getRecordCounter(), -1,
					rawRec != null ? rawRec.toString() : null,
					e);
			return record;
		}
	
		if (rawRec == null) {
			return null;	// end of input
		}
		ByteBuffer fieldBuffer = ByteBuffer.allocateDirect(Defaults.DataParser.FIELD_BUFFER_LENGTH);
		for (int fieldIdx = 0; fieldIdx < metadata.getNumFields(); fieldIdx++) {
			fieldBuffer.clear();
			rawRec.limit(rawRec.position() + metadata.getField(fieldIdx).getSize());	// dangerous
			fieldBuffer.put(rawRec);
			fieldBuffer.flip();
			try {
				try {
					record.getField(fieldIdx).fromByteBuffer(fieldBuffer, decoder);
				} catch (CharacterCodingException e) { // convert it to bad-format exception
					throw new BadDataFormatException("Invalid characters in data field");
				}
			} catch (BadDataFormatException e) {
				fillXHandler(record, recChannel.getRecordCounter(), fieldIdx,
					rawRec != null ? rawRec.toString() : null,
					e);
				return record;
			}
		}
		return record;
	}

	/**
	 * Fill bad-format exception handler with relevant data.
	 * @param errorMessage
	 * @param record
	 * @param recordNumber
	 * @param fieldNumber
	 * @param offendingValue
	 * @param exception
	 */
	private void fillXHandler(DataRecord record,
        int recordNumber, int fieldNumber, String offendingValue,
        BadDataFormatException exception) {
		
		// compose error message
		StringBuffer xmsg = new StringBuffer(); 
		xmsg.append(exception.getMessage() + " when parsing record number #");
		xmsg.append(recordNumber);
		if (fieldNumber >= 0) {
			xmsg.append(" field ");
			xmsg.append(metadata.getField(fieldNumber).getName());
		}
		
		if (exceptionHandler == null) { // no handler available
			throw new RuntimeException(xmsg.toString());			
		}
		// set handler
		exceptionHandler.populateHandler(xmsg.toString(), record, recordNumber,
				fieldNumber, offendingValue, exception);
	}

	/**
	 * Skips records without obtaining respective data.
	 */
	public int skip(int nRec) throws JetelException {
		return recChannel.skip(nRec);
	}

	public void setExceptionHandler(IParserExceptionHandler handler) {
		exceptionHandler = handler;
	}

	public IParserExceptionHandler getExceptionHandler() {
		return exceptionHandler;
	}

	public PolicyType getPolicyType() {
		return exceptionHandler != null ? exceptionHandler.getType() : null;
	}
	
	/**
	 * Class directly accessing input data. 
	 * 
	 * @author Jan Hadrava, Javlin Consulting (www.javlinconsulting.cz)
	 */
	private static class RecordChannel {

		/**
		 * Input byte channel.
		 */
		private ReadableByteChannel inChannel;
		
		private ByteBuffer byteBuffer;

		/**
		 * Size of one record.
		 */
		private int recLen;

		/**
		 * Indicates whether end of input data was already reached.
		 */
		private boolean eof;

		/**
		 * Represents number of processed records.
		 */
		private int recCounter;

		/**
		 * Sole ctor.
		 * @param metadata Record specification.
		 * @param inChannel Input channel.
		 */
		public RecordChannel(DataRecordMetadata metadata, ReadableByteChannel inChannel) {
			this.inChannel = inChannel;
			eof = false;
			recLen = 0;
			recCounter = 0;
			// compute total length of the record
			for (int i = 0; i < metadata.getNumFields(); i++) {
				recLen += metadata.getFields()[i].getSize();
			}
			byteBuffer = ByteBuffer.allocateDirect(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
			byteBuffer.flip();
		}

		/**
		 * Release resources.  
		 */
		public void close() {
			try {
				inChannel.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		/**
		 * Reads raw data for one record from input and fills specified
		 * buffer with them. For outBuff==null raw data in input. 
		 * @param outBuf Output buffer to be filled with raw data.
		 * @return false when no more data are available, true otherwise.
		 * @throws JetelException
		 */
		public boolean getNext(ByteBuffer outBuf) throws JetelException {
			if (eof) {	// no more data in input channel
				return false;
			}
			if (byteBuffer.remaining() < recLen) {	// need to get more data from channel
				byteBuffer.compact();
				try {
					int size = inChannel.read(byteBuffer);	// write to buffer
					byteBuffer.flip();						// prepare buffer for reading
					if (size == -1 || byteBuffer.remaining() < recLen) {	// not enough data available
						eof = true;
						return false;	// no more data available 
					}
				} catch (IOException e) {
					throw new JetelException(e.getMessage());
				}				
			}
			if (outBuf == null) {	// skip input data
				byteBuffer.position(byteBuffer.position() + recLen);	
			} else {					// read input data
				outBuf.clear();
				int savedLimit = byteBuffer.limit();
				byteBuffer.limit(byteBuffer.position() + recLen);
				outBuf.put(byteBuffer);
				byteBuffer.limit(savedLimit);
				outBuf.flip();
			}
			recCounter++;
			return true;
		}
		
		/**
		 * Get buffer filled with raw data.
		 * @return null when no more data are available, true otherwise
		 * @throws JetelException
		 */
		public ByteBuffer getNext() throws JetelException {
			ByteBuffer outBuf = createOutBuf(); 
			return getNext(outBuf) ? outBuf : null;
		}
		
		/**
		 * Skip records.
		 * @param nRec Number of records to be skipped
		 * @return Number of successfully skipped records.
		 * @throws JetelException
		 */
		public int skip(int nRec) throws JetelException {
			int skipped;
			for (skipped = 0; skipped < nRec; skipped++) {
				try {
					if (!getNext(null)) {	// end of file reached
						break;
					}
				}
				catch (BadDataFormatException x) {
					// ignore it
				}
			}
			return skipped;
		}
		
		/**
		 * Creates buffer which may be filled by raw data by respective functions
		 * @return
		 */
		private ByteBuffer createOutBuf() {
			return ByteBuffer.allocateDirect(recLen);
		}

		/**
		 * Nomen omen.
		 * @return Number of records read so far.
		 */
		public int getRecordCounter() {
			return recCounter;
		}
	} // class RecordChannel
}
