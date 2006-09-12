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
import org.jetel.util.StringUtils;

/**
 * Parser for sequence of records represented by fixed count of bytes
 * 
 * @author Jan Hadrava, Javlin Consulting (www.javlinconsulting.cz)
 */
public class FixLenCharDataParser implements Parser {

	private IParserExceptionHandler exceptionHandler;

	/**
	 * Record description.
	 */
	private DataRecordMetadata metadata;
	
	/**
	 * Lengths of record fields
	 */
	private int[] fieldLengths;

	/**
	 * Used for conversion to character data.
	 */
	private CharsetDecoder decoder;
	
	/**
	 * Input record channel.
	 */
	private RecordChannel recChannel;

	/**
	 * Indicates whether leading blanks in string fields are to be skipped
	 */
	private boolean skipLeadingBlanks;

	/**
	 * Indicates whether trailing blanks in string fields are to be skipped
	 */
	private boolean skipTrailingBlanks;

	/**
	 * Create instance for specified charset.
	 * @param charset
	 */
	public FixLenCharDataParser(String charset) {
		init(charset);
	}

	/**
	 * Create instance for default charset. 
	 */
	public FixLenCharDataParser() {
		init(null);
	}

	/**
	 * Common code for ctors.
	 */
	private void init(String charset) {
		// initialize charset decoder
		if (charset == null) {  
			decoder = Charset.forName(Defaults.DataParser.DEFAULT_CHARSET_DECODER).newDecoder();
		} else {
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
		recChannel = new RecordChannel(metadata, byteChannel, decoder, metadata.getRecordDelimiters(), true);
		for (int fieldIdx = 0; fieldIdx < metadata.getNumFields(); fieldIdx++) {
			fieldLengths[fieldIdx] = metadata.getField(fieldIdx).getSize();
		}
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
		CharBuffer rawRec = null;
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
		int savedLimit = rawRec.limit();
		for (int fieldIdx = 0; fieldIdx < metadata.getNumFields(); fieldIdx++) {
			try {
				if (rawRec.remaining() == 0) {
					record.getField(fieldIdx).setToDefaultValue();
					continue;
				}
				if (rawRec.remaining() > fieldLengths[fieldIdx]) {
					rawRec.limit(rawRec.position() + fieldLengths[fieldIdx]);
				}
				if (record.getField(fieldIdx).getType()
						== org.jetel.metadata.DataFieldMetadata.STRING_FIELD) // string value expected
				{
					if (skipLeadingBlanks) {
						StringUtils.trimLeading(rawRec);
					}
					if (skipTrailingBlanks) {
						StringUtils.trimTrailing(rawRec);
					}
					StringUtils.unquote(rawRec);
				}
				record.getField(fieldIdx).fromString(rawRec.toString());
				rawRec.limit(savedLimit);
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

	public String[] getRecordDelimiters() {
		return recChannel.getRecordDelimiters();
	}

	public void setRecordDelimiters(String[] recordDelimiters) {
		recChannel.setRecordDelimiters(recordDelimiters);
	}

	/**
	 * Set delimiters to values specified by metadata 
	 */
	public void setRecordDelimiters() {
		setRecordDelimiters(metadata.getRecordDelimiters());
	}
	
	public boolean isEnableIncomplete() {
		return recChannel.isEnableIncomplete();
	}

	public void setEnableIncomplete(boolean enableIncomplete) {
		recChannel.setEnableIncomplete(enableIncomplete);
	}

	public boolean isSkipLeadingBlanks() {
		return skipLeadingBlanks;
	}

	public void setSkipLeadingBlanks(boolean skipLeadingBlanks) {
		this.skipLeadingBlanks = skipLeadingBlanks;
	}

	public boolean isSkipTrailingBlanks() {
		return skipTrailingBlanks;
	}

	public void setSkipTrailingBlanks(boolean skipTrailingBlanks) {
		this.skipTrailingBlanks = skipTrailingBlanks;
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
		private CharBuffer charBuffer;

		/**
		 * Standard size of one record.
		 */
		private int recLen;

		/**
		 * Indicates whether end of the input data was already reached.
		 */
		private boolean eof;

		/**
		 * Represents number of processed records.
		 */
		private int recCounter;

		/**
		 * Record delimiters.
		 */
		private String[] recordDelimiters;

		/**
		 * Indicates whether incomplete records could be valid. 
		 */
		private boolean enableIncomplete;

		/**
		 * Max delimiter length.
		 */
		int maxDelim;

		/**
		 * Charset decoder.
		 */
		private CharsetDecoder decoder;

		/**
		 * Sole ctor.
		 * @param metadata Record specification.
		 * @param inChannel Input channel.
		 * @param decoder Charset decoder
		 * @param recordDelimiters Nomen omen.
		 * @param enableIncomplete Enables/disables incomplete records.
		 * The value is ignored in case that set of delimiters is empty.
		 */
		public RecordChannel(DataRecordMetadata metadata, ReadableByteChannel inChannel,
				CharsetDecoder decoder, String[] recordDelimiters, boolean enableIncomplete) {

			setRecordDelimiters(recordDelimiters);
			setEnableIncomplete(enableIncomplete);

			this.decoder = decoder;
			this.inChannel = inChannel;

			eof = false;
			recLen = 0;
			recCounter = 0;

			// compute total length of the record
			for (int i = 0; i < metadata.getNumFields(); i++) {
				recLen += metadata.getFields()[i].getSize();
			}
			charBuffer = CharBuffer.allocate(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
			charBuffer.flip();
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
		 * buffer with them. For outBuff==null raw data in input are simply skipped. 
		 * @param outBuf Output buffer to be filled with raw data.
		 * @return false when no more data available, true otherwise
		 * @throws JetelException, BadDataFormatException
		 */
		public boolean getNext(CharBuffer outBuf) throws JetelException, BadDataFormatException {
			// both charBuffer and byteBuffer are ready for reading at this point
			// outBuf is ready for writing
			if (eof) {	// no more data in input channel
				return false;
			}
			if (charBuffer.remaining() < recLen + maxDelim) {	// need to get more data from channel
				byteBuffer.compact();	// ready for writing
				try {
					inChannel.read(byteBuffer);	// write to buffer
					byteBuffer.flip();	// ready reading
				} catch (IOException e) {
					throw new JetelException(e.getMessage());
				}
				charBuffer.compact();	// ready for writing
				decoder.decode(byteBuffer, charBuffer, byteBuffer.limit() < byteBuffer.capacity());
				charBuffer.flip();		// ready for reading
			}
			// from now on both buffers will stay ready for reading

			if (charBuffer.remaining() == 0) {
				eof = true;
				return false;	// no more data
			}
			int delimPos;	// delimiter position (relative to the current position in the buffer)
			int nextPos;	// position of next record (relative to the current position in the buffer)

			// find out delimiter position and position of next record
			if (recordDelimiters.length == 0) {	// don't expect delimiter
				nextPos = delimPos = recLen;
			} else {
				int savedLimit = charBuffer.limit();
				// restrict delimiter lookup to the relevant part of buffer
				if (charBuffer.remaining() > recLen + maxDelim) {
					charBuffer.limit(charBuffer.position() + recLen + maxDelim);
				}
				// look up delimiter
				int[] delimMatch = AhoCorasick.firstMatch(recordDelimiters, charBuffer.toString());	// {position, patternIdx}
				charBuffer.limit(savedLimit);	// restore original limit
				if (delimMatch[0] < 0 || delimMatch[0] > recLen) {		// no delimiter
					if (charBuffer.remaining() < recLen) {	// not enough data
						nextPos = delimPos = charBuffer.remaining();	// use all remaining data
					} else {
						nextPos = delimPos = recLen;	// use fixed amount of data
					}
				} else {	// found delimiter
					delimPos = delimMatch[0];
					nextPos = delimPos + recordDelimiters[delimMatch[1]].length();
				}
			}

			// check record data against policies
			if (delimPos < recLen && !enableIncomplete) {
				charBuffer.position(charBuffer.position() + nextPos);	// skip data
				throw new BadDataFormatException("Incomplete record encountered but not expected");
			}

			// sort it out
			if (outBuf == null) {	// skip input data
				charBuffer.position(charBuffer.position() + nextPos);	
			} else {				// read input data
				outBuf.clear();
				int savedLimit = charBuffer.limit();
				charBuffer.limit(charBuffer.position() + delimPos);	// restrict next reading
				outBuf.put(charBuffer);
				charBuffer.limit(savedLimit);	// restore limit
				charBuffer.position(charBuffer.position() + nextPos - delimPos);	// consume delimiter	
				outBuf.flip();	// ready for reading
			}
			recCounter++;
			return true;
		}
		
		/**
		 * Get buffer filled with raw data.
		 * @return null when no more data are available or current record is invalid, true otherwise
		 * @throws JetelException, BadDataFormatException
		 */
		public CharBuffer getNext() throws JetelException, BadDataFormatException {
			CharBuffer outBuf = createOutBuf(); 
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
		 * @return The buffer.
		 */
		private CharBuffer createOutBuf() {
			return CharBuffer.allocate(recLen);
		}

		/**
		 * Nomen omen.
		 * @return Number of records read so far.
		 */
		public int getRecordCounter() {
			return recCounter;
		}
		
		public String[] getRecordDelimiters() {
			return recordDelimiters;
		}

		public void setRecordDelimiters(String[] recordDelimiters) {
			this.recordDelimiters = recordDelimiters;
			if (recordDelimiters.length == 0) {	// no delimiter, requires special handling
				enableIncomplete = false;
			}
			maxDelim = 0;
			for (int i = 0; i < recordDelimiters.length; i++) {
				if (recordDelimiters[i].length() > maxDelim) {
					maxDelim = recordDelimiters[i].length();
				}
			}
		}

		public boolean isEnableIncomplete() {
			return enableIncomplete;
		}

		public void setEnableIncomplete(boolean enableIncomplete) {
			if (recordDelimiters.length == 0) {	// no delimiter, no changes possible
				return; // incomplete records must remain disabled 
			}
			this.enableIncomplete = enableIncomplete;
		}

	} // class RecordChannel

}
