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
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.string.StringUtils;

/**
 * Parser for sequence of records represented by fixed count of chars.
 * 
 * @author Jan Hadrava (jan.hadrava@javlinconsulting.cz), Javlin Consulting (www.javlinconsulting.cz)
 * @since 09/15/06  
 */
public class FixLenCharDataParser extends FixLenDataParser {

	private CharBuffer charBuffer;

	/**
	 * Record delimiters.
	 */
	private String[] recordDelimiters;
	
	/**
	 * Max delimiter length.
	 */
	int maxDelim;

	private AhoCorasick acEngine;

	/**
	 * Indicates whether leading blanks in string fields are to be skipped
	 */
	private Boolean skipLeadingBlanks = null;

	/**
	 * Indicates whether trailing blanks in string fields are to be skipped
	 */
	private Boolean skipTrailingBlanks = null;
	
	private Boolean trim = null;
	
	/**
	 * Specifies whether incomplete records are allowed.
	 */
	private boolean enableIncomplete = true;

	/**
	 * Specifies what to do when empty record is encountered.
	 */
	private boolean skipEmpty = true;

	/**
	 * Create instance for specified charset.
	 * @param charset
	 */
	public FixLenCharDataParser(DataRecordMetadata metadata, String charset) {		
		super(metadata, charset);
		this.metadata = metadata;
		charBuffer = CharBuffer.allocate(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		setRecordDelimiters(null);
	}

	/**
	 * Create instance for default charset. 
	 */
	public FixLenCharDataParser(DataRecordMetadata metadata) {
		super(metadata, null);
		this.metadata = metadata;
		charBuffer = CharBuffer.allocate(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		setRecordDelimiters(null);
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.parser.FixLenDataParser3#init(org.jetel.metadata.DataRecordMetadata)
	 */
	@Override
	public void init()
			throws ComponentNotReadyException {
		super.init();

		setRecordDelimiters(metadata.getRecordDelimiters());
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.parser.FixLenDataParser3#setDataSource(java.lang.Object)
	 */
	@Override
	public void setDataSource(Object inputDataSource) {
		super.setDataSource(inputDataSource);
		charBuffer.clear();
		charBuffer.flip();
		_savedLim = _savedPos = 0;
		_delimStartEnd[0] = -1;
		_delimStartEnd[1] = 0;
	}

	/**
	 * Discard bytes for incremental reading.
	 * 
	 * @param bytes
	 * @throws IOException 
	 */
	@Override
	protected void discardBytes(int bytes) throws IOException {
		while (bytes > 0) {
			if (inChannel instanceof FileChannel) {
				((FileChannel)inChannel).position(bytes);
				return;
			}
			byteBuffer.clear();
			if (bytes < Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE) byteBuffer.limit(bytes);
			try {
				inChannel.read(byteBuffer.buf());
			} catch (IOException e) {
				break;
			}
			bytes =- Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE;
		}
		byteBuffer.clear();
		byteBuffer.flip();
	}

	/**
	 * Obtains raw data and tries to fill record fields with them.
	 * @param record Output record, cannot be null.
	 * @return null when no more data are available, output record otherwise.
	 * @throws JetelException
	 */
	@Override
	protected DataRecord parseNext(DataRecord record) throws JetelException {
		CharBuffer rawRec = null;
		fieldIdx = -1;
		try {
			rawRec = getNextRecord();
		} catch (BadDataFormatException e) {
			fillXHandler(record, null, e);
//			return record;
		}
	
		if (rawRec == null) {
			return null;	// end of input
		}
		boolean skipLBlanks = false;
		boolean skipTBlanks;
		int recStart = rawRec.position();
		int recEnd = rawRec.limit();
		for (fieldIdx = 0; fieldIdx < fieldCnt; fieldIdx++) {
			// skip all fields that are internally filled 
			if (isAutoFilling[fieldIdx]) {
				continue;
			}
			skipLBlanks = skipLeadingBlanks != null ? skipLeadingBlanks : trim != null ? trim : true;
			skipTBlanks = skipTrailingBlanks != null ? skipTrailingBlanks : trim != null ? trim : true;
			try {
				if (recStart + fieldStart[fieldIdx] >= recEnd) {	// there are no data available for this field
					record.getField(fieldIdx).setToDefaultValue();
					continue;
				}
				rawRec.position(recStart);	// to avoid exceptions while setting position&limit of the field 
				rawRec.limit(Math.min(recStart + fieldEnd[fieldIdx], recEnd));
				rawRec.position(recStart + fieldStart[fieldIdx]);
				
				if (skipLBlanks) {
					StringUtils.trimLeading(rawRec);
				}
				if (skipTBlanks) {
					StringUtils.trimTrailing(rawRec);
				}
                // shall we remove quotes ??
                switch(record.getField(fieldIdx).getMetadata().getDataType()){
                case BYTE:
                case CBYTE:
                     break;
                default:
   					StringUtils.unquote(rawRec);
                	break;
				}
                
				record.getField(fieldIdx).fromString(rawRec);
			} catch (BadDataFormatException e) {
					fillXHandler(record,  rawRec, e);
//					return record;
			}
		}
		return record;
	}

	@Override
	public boolean isEnableIncomplete() {
		return enableIncomplete;
	}

	@Override
	public void setEnableIncomplete(boolean enableIncomplete) {
		this.enableIncomplete = enableIncomplete;
	}

	@Override
	public boolean isSkipEmpty() {
		return enableIncomplete;
	}

	@Override
	public void setSkipEmpty(boolean skipEmpty) {
		this.skipEmpty = skipEmpty;
	}

	@Override
	public boolean isSkipLeadingBlanks() {
		return skipLeadingBlanks;
	}

	@Override
	public void setSkipLeadingBlanks(boolean skipLeadingBlanks) {
		this.skipLeadingBlanks = skipLeadingBlanks;
	}

	@Override
	public boolean isSkipTrailingBlanks() {
		return skipTrailingBlanks;
	}

	@Override
	public void setSkipTrailingBlanks(boolean skipTrailingBlanks) {
		this.skipTrailingBlanks = skipTrailingBlanks;
	}
	
	public String[] getRecordDelimiters() {
		return recordDelimiters;
	}

	public void setRecordDelimiters(String[] recordDelimiters) {
		this.recordDelimiters = recordDelimiters == null ? new String[]{} : recordDelimiters;
		if (this.recordDelimiters.length == 0) {	// no delimiter, requires special handling
			acEngine = null;
		} else {
			acEngine = new AhoCorasick(this.recordDelimiters);
		}
		maxDelim = 0;
		for (int i = 0; i < this.recordDelimiters.length; i++) {
			if (this.recordDelimiters[i].length() > maxDelim) {
				maxDelim = this.recordDelimiters[i].length();
			}
		}
	}

	private int[] _delimStartEnd = new int[]{-1, 0};
 
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
		if (charBuffer.remaining() < recordLength + maxDelim) {	// need to get more data from channel
			byteBuffer.compact();	// ready for writing
			try {
				inChannel.read(byteBuffer.buf());	// write to buffer
				byteBuffer.flip();	// ready reading
			} catch (IOException e) {
				throw new JetelException(e);
			}
			charBuffer.compact();	// ready for writing
			decoder.decode(byteBuffer.buf(), charBuffer, false);
			charBuffer.flip();		// ready for reading
            _savedPos = 0;
			_savedLim = charBuffer.limit();
		}
		// from now on both buffers will stay ready for reading

		if (charBuffer.remaining() == 0) {
			eof = true;
			return null;	// no more data
		}

		// find out delimiter position and position of next record
		int delimPos;	// delimiter position (relative to the current position in the buffer)
		int nextPos;	// position of next record (relative to the current position in the buffer)			
		if (acEngine == null) {	// don't expect delimiter
			nextPos = delimPos = Math.min(recordLength, charBuffer.remaining());
		} else {
			int savedLimit = charBuffer.limit();
			// restrict delimiter lookup to the relevant part of buffer
			if (charBuffer.remaining() > recordLength + maxDelim) {
				charBuffer.limit(charBuffer.position() + recordLength + maxDelim);
			}
			// look up delimiter
			int[] delimMatch = acEngine.firstMatch(recordDelimiters, charBuffer);	// {position, patternIdx}
			charBuffer.limit(savedLimit);	// restore original limit
			if (delimMatch[0] < 0 || delimMatch[0] > recordLength) {		// no delimiter
				if (charBuffer.remaining() < recordLength) {	// not enough data
					nextPos = delimPos = charBuffer.remaining();	// use all remaining data
				} else {
					nextPos = delimPos = recordLength;	// use fixed amount of data
				}
			} else {	// found delimiter
				acEngine.reset();
				delimPos = delimMatch[0];
				nextPos = delimPos + recordDelimiters[delimMatch[1]].length();
			}
		}
		_delimStartEnd[0] = delimPos;
		_delimStartEnd[1] = nextPos;
		return _delimStartEnd;
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

	private int _savedLim = 0;
	private int _savedPos = 0;

	/**
	 * Reads raw data for one record from input and fills specified
	 * buffer with them. For outBuff==null raw data in input are simply skipped. 
	 * @param outBuf Output buffer to be filled with raw data.
	 * @return false when no more data available, true otherwise
	 * @throws JetelException, BadDataFormatException
	 */
	private CharBuffer getNextRecord() throws JetelException, BadDataFormatException {
		// restore saved scope, so that it will cover all unprocessed data in the buffer
		charBuffer.limit(_savedLim);
		charBuffer.position(_savedPos);
		// move buffer position to the beginning of next interesting record
		// and retrieve position of following delimiter.
		int[] delimStartEnd = findUsefulDelim();
		if (delimStartEnd == null) {	// end of input data
			return null;
		}
		int delimPos = delimStartEnd[0];// delimiter position (relative to the current position in the buffer)
		int nextPos = delimStartEnd[1];	// position of next record (relative to the current position in the buffer)

		// check record data against policies
		if (delimPos < recordLength && !enableIncomplete) {
			charBuffer.position(charBuffer.position() + nextPos);	// skip data
			_savedPos = charBuffer.position() + nextPos;	// prepare for processing of next record
			throw new BadDataFormatException("Incomplete record encountered but not expected");
		}

		// reduce scope covering unprocessed data
		_savedPos = charBuffer.position() + nextPos;

		// set scope to current record
		charBuffer.limit(charBuffer.position() + delimPos);
		recordIdx++;
    	bytesProcessed += delimPos;
		return charBuffer;
	}

	/**
	 * Skip records.
	 * @param nRec Number of records to be skipped
	 * @return Number of successfully skipped records.
	 * @throws JetelException
	 */
	@Override
	public int skip(int nRec) throws JetelException {
		int skipped;
		for (skipped = 0; skipped < nRec; skipped++) {
            charBuffer.limit(_savedLim);
            charBuffer.position(_savedPos);
            int[] delimStartEnd = findUsefulDelim();
			if (delimStartEnd == null) {	// end of input data
				break;
			}
            _savedPos += delimStartEnd[1];
		}
		return skipped;
	}

	public Boolean getTrim() {
		return trim;
	}

	public void setTrim(Boolean trim) {
		this.trim = trim;
	}
		
	/*
	 * (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#reset()
	 */
	@Override
	public void reset() {
		super.reset();
		charBuffer.clear();
		charBuffer.flip();
		_savedLim = _savedPos = 0;
		_delimStartEnd[0] = -1;
		_delimStartEnd[1] = 0;
	}

	@Override
    public void preExecute() throws ComponentNotReadyException {
    }
    
	@Override
    public void postExecute() throws ComponentNotReadyException {    	
    	reset();
    }
    
	@Override
    public void free() {
    	close();
    }

	@Override
	public boolean nextL3Source() {
		return false;
	}
	
}
