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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.CharsetDecoder;

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.StringUtils;

/**
 * Parser for sequence of records represented by fixed count of bytes
 * 
 * @author Jan Hadrava, Javlin Consulting (www.javlinconsulting.cz)
 */
public class FixLenCharDataParser extends FixLenDataParser3 {

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
	 * Specifies whether incomplete records are allowed.
	 */
	private boolean enableIncomplete;

	/**
	 * Specifies what to do when empty record is encountered.
	 */
	private boolean skipEmpty;

	/**
	 * Create instance for specified charset.
	 * @param charset
	 */
	public FixLenCharDataParser(String charset) {		
		super(charset);
	}

	/**
	 * Create instance for default charset. 
	 */
	public FixLenCharDataParser() {
		super(null);
	}

	/**
	 * Parser iface.
	 */
	public void open(Object inputDataSource, DataRecordMetadata metadata)
			throws ComponentNotReadyException {
		ReadableByteChannel byteChannel = init(inputDataSource, metadata);
		recChannel = new RecordChannel(metadata, byteChannel,
				decoder, metadata.getRecordDelimiters(),
				enableIncomplete, skipEmpty);
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
	protected DataRecord parseNext(DataRecord record) throws JetelException {
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
				rawRec.position(rawRec.limit());	// consume field
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
	 * Skips records without obtaining respective data.
	 */
	public int skip(int nRec) throws JetelException {
		return recChannel.skip(nRec);
	}

	public boolean isEnableIncomplete() {
		if (recChannel == null) {
			return enableIncomplete;
		}

		return recChannel.isEnableIncomplete();
	}

	public boolean isSkipEmpty() {
		if (recChannel == null) {
			return skipEmpty;
		}

		return recChannel.isSkipEmpty();
	}

	public void setEnableIncomplete(boolean enableIncomplete) {
		this.enableIncomplete = enableIncomplete;
		if (recChannel != null) {
			recChannel.setEnableIncomplete(enableIncomplete);
		}
	}

	public void setSkipEmpty(boolean skipEmpty) {
		this.skipEmpty = skipEmpty;
		if (recChannel != null) {
			recChannel.setSkipEmpty(skipEmpty);
		}
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
		
		private AhoCorasick acEngine;

		/**
		 * Indicates whether incomplete records could be valid. 
		 */
		private boolean enableIncomplete;

		/**
		 * Specifies what to do when empty record is encountered.
		 */
		private boolean skipEmpty;

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
		 * @param skipEmpty Turns on/off skipping of empty records.
		 * The value is ignored in case that set of delimiters is empty.
		 */
		public RecordChannel(DataRecordMetadata metadata, ReadableByteChannel inChannel,
				CharsetDecoder decoder, String[] recordDelimiters,
				boolean enableIncomplete, boolean skipEmpty) {

			setRecordDelimiters(recordDelimiters);
			setEnableIncomplete(enableIncomplete);
			setSkipEmpty(skipEmpty);

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
			_outBuf = createOutBuf();
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
		 * Finds position of first delimiter
		 * @return null on end of input,
		 * relative positions of delimiter {delimPos, delimEnd} otherwise. 
		 */
		private int[] findDelim() throws JetelException {
			// both charBuffer and byteBuffer are ready for reading at this point
			// outBuf is ready for writing
			if (eof) {	// no more data in input channel
				return null;
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
				return null;	// no more data
			}

			// find out delimiter position and position of next record
			int delimPos;	// delimiter position (relative to the current position in the buffer)
			int nextPos;	// position of next record (relative to the current position in the buffer)			
			if (recordDelimiters.length == 0) {	// don't expect delimiter
				nextPos = delimPos = recLen;
			} else {
				int savedLimit = charBuffer.limit();
				// restrict delimiter lookup to the relevant part of buffer
				if (charBuffer.remaining() > recLen + maxDelim) {
					charBuffer.limit(charBuffer.position() + recLen + maxDelim);
				}
				// look up delimiter
				int[] delimMatch = acEngine.firstMatch(recordDelimiters, charBuffer.toString());	// {position, patternIdx}
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
			return new int[]{delimPos, nextPos};
		}
		
		/**
		 * Finds position of first delimiter preceded by record
		 * which is not supposed to be ignored. Set buffer position
		 * to the beginning of the record.
		 * @return null on end of input,
		 * relative positions of delimiter {delimPos, delimEnd} otherwise. 
		 */
		private int[] findUsefulDelim() throws JetelException {
			int[] delimStartEnd = null;
			do {
				if (delimStartEnd != null) { 
					charBuffer.position(charBuffer.position() + delimStartEnd[1]);	// consume useless delimiter
				}
				delimStartEnd = findDelim();	// find delimiter for current record
				if (delimStartEnd == null) {	// no more records
					return null;
				}
			} while (skipEmpty && delimStartEnd[0] == 0);	// until interesting data are encountered
			return delimStartEnd;
		}

		/**
		 * Reads raw data for one record from input and fills specified
		 * buffer with them. For outBuff==null raw data in input are simply skipped. 
		 * @param outBuf Output buffer to be filled with raw data.
		 * @return false when no more data available, true otherwise
		 * @throws JetelException, BadDataFormatException
		 */
		public boolean getNext(CharBuffer outBuf) throws JetelException, BadDataFormatException {
			
			// move buffer position to the beginning of next interesting record
			// and retrieve position of following delimiter.
			int[] delimStartEnd = findUsefulDelim();
			if (delimStartEnd == null) {	// end of input data
				return false;
			}
			int delimPos = delimStartEnd[0];// delimiter position (relative to the current position in the buffer)
			int nextPos = delimStartEnd[1];	// position of next record (relative to the current position in the buffer)

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

		private CharBuffer _outBuf = null;
		/**
		 * Get buffer filled with raw data.
		 * @return null when no more data are available or current record is invalid, true otherwise
		 * @throws JetelException, BadDataFormatException
		 */
		public CharBuffer getNext() throws JetelException, BadDataFormatException {
			return getNext(_outBuf) ? _outBuf : null;
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
				if (findUsefulDelim() == null) {	// end of input data
					break;
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
			this.recordDelimiters = recordDelimiters == null ? new String[]{} : recordDelimiters;
			if (this.recordDelimiters.length == 0) {	// no delimiter, requires special handling
				enableIncomplete = false;
				skipEmpty = false;
			}
			maxDelim = 0;
			for (int i = 0; i < this.recordDelimiters.length; i++) {
				if (this.recordDelimiters[i].length() > maxDelim) {
					maxDelim = this.recordDelimiters[i].length();
				}
			}
			acEngine = new AhoCorasick(this.recordDelimiters);
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

		public boolean isSkipEmpty() {
			return enableIncomplete;
		}

		public void setSkipEmpty(boolean skipEmpty) {
			if (recordDelimiters.length == 0) {	// no delimiter, no changes possible
				return; // empty records must remain disabled 
			}
			this.skipEmpty = skipEmpty;
		}

	} // class RecordChannel

}
