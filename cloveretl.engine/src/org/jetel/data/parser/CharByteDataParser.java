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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.InvalidMarkException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

import javax.naming.OperationNotSupportedException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.IParserExceptionHandler;
import org.jetel.exception.JetelException;
import org.jetel.exception.PolicyType;
import org.jetel.exception.UnexpectedEndOfRecordDataFormatException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.string.QuotingDecoder;
import org.jetel.util.string.StringUtils;

/**
 * A parser able to deal with mixed records containing byte-based fields (BYTE/CBYTE) 
 * @author jhadrava (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Nov 30, 2010
 */
public class CharByteDataParser extends AbstractTextParser {
	private static final int RECORD_DELIMITER_IDENTIFIER = -1;
	private static final int DEFAULT_FIELD_DELIMITER_IDENTIFIER = -2;
	

	private final static Log logger = LogFactory.getLog(SimpleDataParser.class);

	ReadableByteChannel inputSource;
	private CharByteInputReader inputReader;
	private CharByteInputReader.DoubleMarkCharByteInputReader verboseInputReader;
	private InputConsumer[] fieldConsumers;
	private RecordSkipper recordSkipper;
	private boolean releaseInputSource;
	private int numConsumers;
	private Charset charset;
	private IParserExceptionHandler exceptionHandler;
	private PolicyType policyType;
	private AhoCorasick byteSearcher;
	private AhoCorasick charSearcher;
	private int lastNonAutoFilledField;
	private boolean isInitialized;
	private int recordCounter;

	public CharByteDataParser(TextParserConfiguration cfg) {
		super(cfg);
		String charsetName = cfg.getCharset(); 
		if (charsetName == null) {
			charsetName = Defaults.DataParser.DEFAULT_CHARSET_DECODER;
		}
		charset = Charset.forName(charsetName);
		policyType = cfg.getPolicyType();
		releaseInputSource = false;
		recordSkipper = null;
		isInitialized = false;
		exceptionHandler = cfg.getExceptionHandler();
	}

	/**
	 * Returns parser speed for specified configuration. See {@link TextParserFactory#getParser(TextParserConfiguration)}.
	 */
	public static Integer getParserSpeed(TextParserConfiguration cfg){
		boolean hasByteFields = false;
		boolean hasCharFields = false;
		for (DataFieldMetadata field : cfg.getMetadata().getFields()) {
			if (field.isAutoFilled()) {
				continue;
			}
			if (field.isByteBased()) {
				hasByteFields = true;				
			} else {
				hasCharFields = true;
			}
		}
		if (!hasByteFields || !hasCharFields) {
			return 8;
		} else if (cfg.isSingleByteCharset()) {
			return 7;
		} else {
			return 6;
		}
	}

	private String getLastRawRecord() {
		if (verboseInputReader != null) {
			Object seq;
			try {
				seq = verboseInputReader.getOuterSequence(0);
			} catch (InvalidMarkException e) {
				return "<Raw record data is not available, please turn on verbose mode.>";
			} catch (OperationNotSupportedException e) {
				return "<Raw record data is not available, please turn on verbose mode.>";
			}
			if (seq instanceof CharSequence) {
				return (new StringBuilder((CharSequence)seq)).toString();
			} else if (seq instanceof ByteBuffer) {
				return Charset.forName("ISO-8859-1").decode((ByteBuffer)seq).toString();
			}
		}
	return "<Raw record data is not available, please turn on verbose mode.>";
	}
	
	private DataRecord parsingErrorFound(String exceptionMessage, DataRecord record, int fieldNum, CharSequence offendingValue) {
        if(exceptionHandler != null) {
            exceptionHandler.populateHandler(exceptionMessage, record, recordCounter, fieldNum , 
            		offendingValue == null ? getLastRawRecord() : (new StringBuilder(offendingValue)).toString(),
            		new BadDataFormatException(exceptionMessage));
            return record;
        } else {
			throw new RuntimeException("Parsing error: " + exceptionMessage + " (" + getLastRawRecord() + ")");
		}
	}
	
	/**
	 * @see org.jetel.data.parser.Parser#getNext()
	 */
	public DataRecord getNext() throws JetelException {
		DataRecord record = new DataRecord(cfg.getMetadata());
		record.init();

		record = parseNext(record);
        if(exceptionHandler != null ) {  //use handler only if configured
            while(exceptionHandler.isExceptionThrowed()) {
            	exceptionHandler.setRawRecord(getLastRawRecord());
                exceptionHandler.handleException();
                record = parseNext(record);
            }
        }
		return record;
	}

	/**
	 * @see org.jetel.data.parser.Parser#getNext(org.jetel.data.DataRecord)
	 */
	public DataRecord getNext(DataRecord record) throws JetelException {
		record = parseNext(record);
        if(exceptionHandler != null ) {  //use handler only if configured
            while(exceptionHandler.isExceptionThrowed()) {
            	exceptionHandler.setRawRecord(getLastRawRecord());
                exceptionHandler.handleException();
                record = parseNext(record);
            }
        }
		return record;
	}

	public DataRecord parseNext(DataRecord record) throws JetelException {
		recordCounter++;
		try {
			if (verboseInputReader != null) {
				verboseInputReader.setOuterMark();
			}
			for (int idx = 0; idx < numConsumers; idx++) {
				try {
					if (!fieldConsumers[idx].consumeInput(record)) {
						if (idx == 0) {
							return null;
						} else {
							if (idx != numConsumers - 1) {
								return parsingErrorFound("Incomplete record at the end of input", record, idx, null);
							} else {
								break;
							}
						}
					}
				} catch (UnexpectedEndOfRecordDataFormatException e) {
					return parsingErrorFound(e.getSimpleMessage(), record, idx, null);					
				} catch (BadDataFormatException e) {
					if (recordSkipper != null) {
						recordSkipper.skipInput(idx);
					}
					return parsingErrorFound(e.getSimpleMessage(), record, idx, e.getOffendingValue());
				}
			}
		} catch (OperationNotSupportedException e) {
			throw new JetelException("Fatal problem occured during input processing", e);
		} catch (IOException e) {
			throw new JetelException("Fatal problem occured during input processing", e);
		}
		return record;
	}
	
	@Override
	public int skip(int nRec) throws JetelException {
		if (recordSkipper == null) {
			return 0;
		}
		int counter;
		for (counter = 0; counter < nRec; counter++) {
			try {
				if (!recordSkipper.skipInput(0)) {
					break;
				}
			} catch (OperationNotSupportedException e) {
				break;
			} catch (IOException e) {
				break;
			}
		}
		return counter; 
	}

	/**
	 * Sets delimiters for each field
	 * @param byteMode
	 * @return
	 */
	private AhoCorasick getByteDelimSearcher() {
		if (lastNonAutoFilledField == -1) {
			return null;
		}
		if (byteSearcher != null) { // use existing searcher
			return byteSearcher;
		}
		// create new searcher
		AhoCorasick searcher = new AhoCorasick();
		DataRecordMetadata metadata = cfg.getMetadata();
		int numFields = metadata.getNumFields();
		String[] recordDelimiters = metadata.getRecordDelimiters(); 
		String[] defaultFieldDelimiters = metadata.getFieldDelimiters();
		if (recordDelimiters == null) {
			recordDelimiters = new String[0];
		}
		if (defaultFieldDelimiters == null) {
			defaultFieldDelimiters = new String[0];
		}

		for (int idx = 0; idx < numFields; idx++) {
			DataFieldMetadata field = metadata.getField(idx);
			if (!field.isDelimited() || field.isAutoFilled() || !field.isByteBased()) {
				continue;
			}
			if (field.getDelimiters() != null) {
				for (String delim : field.getDelimiters()) {
					searcher.addBytePattern(charset.encode(delim), idx);
				}
			}
			for (String recordDelim : recordDelimiters) {
				searcher.addBytePattern(charset.encode(recordDelim), idx);								
			}
			if (idx != lastNonAutoFilledField) { // last field doesn't use default field delimiters 
				for (String defaultFieldDelim : defaultFieldDelimiters) {
					searcher.addBytePattern(charset.encode(defaultFieldDelim), idx);								
				}
			}
		}
		for (String recordDelim : recordDelimiters) {
			searcher.addBytePattern(charset.encode(recordDelim), RECORD_DELIMITER_IDENTIFIER);
		}
		if (recordDelimiters.length == 0) {
			searcher.addBytePattern(null, RECORD_DELIMITER_IDENTIFIER);			
		}
		for (String defaultFieldDelim : defaultFieldDelimiters) {
			searcher.addBytePattern(charset.encode(defaultFieldDelim), DEFAULT_FIELD_DELIMITER_IDENTIFIER);
		}
		if (defaultFieldDelimiters.length == 0) {
			searcher.addBytePattern(null, DEFAULT_FIELD_DELIMITER_IDENTIFIER);
		}
		searcher.compile();
		return byteSearcher = searcher;
	}
	
	/**
	 * Sets delimiters for each field
	 * @param byteMode
	 * @return
	 */
	private AhoCorasick getCharDelimSearcher() {
		if (lastNonAutoFilledField == -1) {
			return null;
		}
		if (charSearcher != null) { // use existing searcher
			return charSearcher;
		}
		
		// create new searcher
		AhoCorasick searcher = new AhoCorasick();
		DataRecordMetadata metadata = cfg.getMetadata();
		int numFields = metadata.getNumFields();
		String[] recordDelimiters = metadata.getRecordDelimiters(); 
		String[] defaultFieldDelimiters = metadata.getFieldDelimiters();
		if (recordDelimiters == null) {
			recordDelimiters = new String[0];
		}
		if (defaultFieldDelimiters == null) {
			defaultFieldDelimiters = new String[0];
		}

		for (int idx = 0; idx < numFields; idx++) {
			DataFieldMetadata field = metadata.getField(idx);
			if (!field.isDelimited() || field.isAutoFilled() || field.isByteBased()) {
				continue;
			}
			if (field.getDelimiters() != null) {
				for (String delim : field.getDelimiters()) {
					searcher.addPattern(delim, idx);
				}
			}
			for (String recordDelim : recordDelimiters) {
				searcher.addPattern(recordDelim, idx);								
			}
			if (idx != lastNonAutoFilledField) { // last field doesn't use default field delimiters 
				for (String defaultFieldDelim : defaultFieldDelimiters) {
					searcher.addPattern(defaultFieldDelim, idx);								
				}
			}
		}
		for (String recordDelim : recordDelimiters) {
			searcher.addPattern(recordDelim, RECORD_DELIMITER_IDENTIFIER);
		}
		if (recordDelimiters.length == 0) {
			searcher.addBytePattern(null, RECORD_DELIMITER_IDENTIFIER);			
		}
		for (String defaultFieldDelim : defaultFieldDelimiters) {
			searcher.addPattern(defaultFieldDelim, DEFAULT_FIELD_DELIMITER_IDENTIFIER);
		}
		if (defaultFieldDelimiters.length == 0) {
			searcher.addBytePattern(null, DEFAULT_FIELD_DELIMITER_IDENTIFIER);
		}
		searcher.compile();
		return charSearcher = searcher;
	}
	
	@Override
	public void init() throws ComponentNotReadyException {
		if (isInitialized) {
			return;
		}
		isInitialized = true;

		DataRecordMetadata metadata = cfg.getMetadata();
		if (metadata == null) {
			throw new ComponentNotReadyException("Metadata are null");
		}
		
		int numFields = metadata.getNumFields();

		for (lastNonAutoFilledField = numFields - 1; lastNonAutoFilledField >= 0; lastNonAutoFilledField--) {
			if (!metadata.getField(lastNonAutoFilledField).isAutoFilled()) {
				break;
			}
		}
		if (lastNonAutoFilledField == -1) {
			numConsumers = 0;
			return;
		}

		// let's find out what kind of input reader we need
		boolean needByteInput = false;
		boolean needCharInput = false;

		for (DataFieldMetadata field : metadata.getFields()) {
			if (field.isAutoFilled()) {
				continue;
			}
			if (field.isByteBased()) {
				needByteInput = true;
			} else {
				needCharInput = true;
			}
		}
		// create input reader according to data record requirements
		if (!needCharInput) {
			inputReader = new CharByteInputReader.ByteInputReader();
		} else if (!needByteInput) {
			inputReader = new CharByteInputReader.CharInputReader(charset);
		} else if (cfg.isSingleByteCharset()) {
			inputReader = new CharByteInputReader.SingleByteCharsetInputReader(charset);
		} else {
			inputReader = new CharByteInputReader.RobustInputReader(charset);
		}
		if (cfg.isVerbose()) {
			inputReader = verboseInputReader = new CharByteInputReader.DoubleMarkCharByteInputReader(inputReader);
		}
		boolean[] isDelimited = new boolean[numFields];
		for (int idx = 0; idx < numFields; idx++) {
			DataFieldMetadata field = metadata.getField(idx);
			isDelimited[idx] = !field.isAutoFilled() && field.isDelimited();
		}

		numConsumers = 0;
		fieldConsumers = new InputConsumer[numFields + 1];
		for (int idx = 0; idx < numFields; numConsumers++) {
			// skip auto-filling fields
			for (; idx < numFields; idx++) {
				if (!metadata.getField(idx).isAutoFilled()) {
					break;
				}
			}
			if (idx == numFields) {
				break;
			}
			int byteFieldCount;
			for (byteFieldCount = 0; idx + byteFieldCount < numFields; byteFieldCount++) {
				DataFieldMetadata field = metadata.getField(idx + byteFieldCount);
				if (field.isAutoFilled()) { // skip auto-filled field
					continue;
				}
				if (!field.isByteBased() || field.isDelimited()) {
					break;
				}
			}
			if (byteFieldCount > 0) { // fixlen byte field consumer
				fieldConsumers[numConsumers] = new FixlenByteFieldConsumer(metadata, inputReader, idx, byteFieldCount);
				idx += byteFieldCount;
			} else {
				DataFieldMetadata field = metadata.getField(idx);
				if (!field.isDelimited()) { // fixlen char field consumer
					assert !field.isByteBased() : "Unexpected execution flow";
					fieldConsumers[numConsumers] = new FixlenCharFieldConsumer(inputReader, idx, field.getSize(),
							isSkipFieldLeadingBlanks(idx), isSkipFieldTrailingBlanks(idx));
				} else if (field.isByteBased()) { // delimited byte field consumer
					fieldConsumers[numConsumers] = new DelimByteFieldConsumer(inputReader, idx, getByteDelimSearcher(),
							cfg.isTreatMultipleDelimitersAsOne(), field.isEofAsDelimiter(), lastNonAutoFilledField == idx ? true : false,
							isSkipFieldLeadingBlanks(idx), isSkipFieldTrailingBlanks(idx));
				} else { // delimited char field consumer
					fieldConsumers[numConsumers] = new DelimCharFieldConsumer(inputReader, idx, getCharDelimSearcher(),
							cfg.isTreatMultipleDelimitersAsOne(), field.isEofAsDelimiter(), lastNonAutoFilledField == idx ? true : false,
									cfg.isQuotedStrings(), isSkipFieldLeadingBlanks(idx), isSkipFieldTrailingBlanks(idx));
				}
				idx++;
			}
		} // loop
		if (needByteInput) {
			recordSkipper = new ByteRecordSkipper(inputReader, getCharDelimSearcher(), isDelimited);			
		} else {
			recordSkipper = new CharRecordSkipper(inputReader, getCharDelimSearcher(), isDelimited);
		}
		if (metadata.isSpecifiedRecordDelimiter() && !metadata.getField(lastNonAutoFilledField).isDelimited()) {
			// last field without autofilling doesn't have delimiter - special consumer needed for record delimiter
			fieldConsumers[numConsumers++] = new CharDelimConsumer(inputReader,
					needCharInput ? getCharDelimSearcher() : getByteDelimSearcher(), 
					RECORD_DELIMITER_IDENTIFIER, metadata.getField(lastNonAutoFilledField).isEofAsDelimiter());
		}
	}
	

	@Override
	public void setDataSource(Object inputDataSource) throws IOException, ComponentNotReadyException {
		recordCounter = 0;
		if (inputDataSource instanceof ReadableByteChannel) {
			inputSource = (ReadableByteChannel)inputDataSource;
			inputReader.setInputSource((ReadableByteChannel)inputDataSource);
		} else if (inputDataSource instanceof FileInputStream) {
			inputReader.setInputSource(((FileInputStream)inputDataSource).getChannel());
		} else { 
			inputReader.setInputSource(Channels.newChannel((InputStream)inputDataSource));
		}
	}

	@Override
	public TextParserConfiguration getConfiguration() {
		return cfg;
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
		return policyType;
	}

	@Override
	public void reset() throws ComponentNotReadyException {
		inputReader.setInputSource(inputSource);
	}

	@Override
	public Object getPosition() {
		return null;
	}

	@Override
	public void movePosition(Object position) throws IOException {
	}

	@Override
	public void setReleaseDataSource(boolean releaseInputSource)  {
		this.releaseInputSource = releaseInputSource;
	}

	@Override
	public void close() throws IOException {
		try {
			free();
		} catch (ComponentNotReadyException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void preExecute() throws ComponentNotReadyException {
	}

	@Override
	public void postExecute() throws ComponentNotReadyException {
		inputReader.setInputSource(null);
	}

	@Override
	public void free() throws ComponentNotReadyException, IOException {
		if (releaseInputSource) {
			inputSource.close();
		}
	}

	public abstract static class InputConsumer {
		public static final int VALUE_WITHOUT_END_OF_FILE = 0;
		public static final int END_OF_FILE_AFTER_VALUE = -1;
		public static final int END_OF_FILE_WITHOUT_VALUE = 2;

		protected CharByteInputReader inputReader;
		
		protected InputConsumer(CharByteInputReader inputReader) {
			this.inputReader = inputReader;
		}
		
		/**
		 * Reads part of input and fill it in in the record
		 * @param record
		 * @return false if no single byte/char of input is available, true otherwise
		 * @throws OperationNotSupportedException
		 * @throws IOException
		 */
		public abstract boolean consumeInput(DataRecord record) throws OperationNotSupportedException, IOException;

	}

	
	public static class FixlenByteFieldConsumer extends InputConsumer {

		private final CharsetDecoder decoder = Charset.forName(Defaults.DataParser.DEFAULT_CHARSET_DECODER).newDecoder();
		private int startFieldIdx;
		private int fieldCount;

		private int dataLength;
		private int[] fieldStart;
		private int[] fieldEnd;
		private boolean[] isAutoFilling;

		FixlenByteFieldConsumer(DataRecordMetadata metadata, CharByteInputReader inputReader, int startFieldIdx, int fieldCount) throws ComponentNotReadyException {
			super(inputReader);
			this.startFieldIdx = startFieldIdx;
			this.fieldCount = fieldCount;
			this.inputReader = inputReader;
			
			fieldStart = new int[fieldCount];
			fieldEnd = new int[fieldCount];
			isAutoFilling = new boolean[fieldCount];
			int prevEnd = 0;
			dataLength = 0;
			for (int fieldIdx = 0; fieldIdx < fieldCount; fieldIdx++) {
				if (isAutoFilling[fieldIdx] = metadata.getField(startFieldIdx + fieldIdx).getAutoFilling() != null) {
					fieldStart[fieldIdx] = prevEnd;
					fieldEnd[fieldIdx] = prevEnd;
				} else {
					fieldStart[fieldIdx] = prevEnd + metadata.getField(startFieldIdx + fieldIdx).getShift();
					fieldEnd[fieldIdx] = fieldStart[fieldIdx] + metadata.getField(startFieldIdx + fieldIdx).getSize();
					prevEnd = fieldEnd[fieldIdx];
					if (fieldStart[fieldIdx] < 0) {
						throw new ComponentNotReadyException("field boundaries cannot be outside record boundaries");
					}
					if (fieldEnd[fieldIdx] > dataLength) {
						dataLength = fieldEnd[fieldIdx];
					}
				}
			}
		}
		

		@Override
		public boolean consumeInput(DataRecord record) throws OperationNotSupportedException, IOException {
			int ibt;
			inputReader.mark();
			
			for (int i = 0; i < dataLength; i++) {
				ibt = inputReader.readByte();
				if (ibt == CharByteInputReader.BLOCKED_BY_MARK) {
					throw new BadDataFormatException("End quote not found, try to increase MAX_RECORD_SIZE");
				}
				if (ibt == CharByteInputReader.END_OF_INPUT) {
					if (i == 0) {
						return false;
					} else {
						throw new BadDataFormatException("End of input encountered instead of the closing quote");
					}
				}
			}
			ByteBuffer seq = inputReader.getByteSequence(0);
			int startPos = seq.position();
			for (int idx = 0; idx < fieldCount; idx++) {
				if (isAutoFilling[idx]) {
					continue;
				}
				seq.position(startPos + fieldStart[idx]);
				seq.limit(startPos + fieldEnd[idx]);
				record.getField(startFieldIdx + idx).fromByteBuffer(seq, decoder);
			}
			return true;
		}
		
	}

	public static class FixlenCharFieldConsumer extends InputConsumer {

		protected int fieldNumber;
		private int fieldLength;
		private boolean lTrim;
		private boolean rTrim;


		FixlenCharFieldConsumer(CharByteInputReader inputReader, int fieldNumber, int fieldLength,
				boolean lTrim, boolean rTrim) {
			super(inputReader);
			this.fieldNumber = fieldNumber;
			this.inputReader = inputReader;
			this.fieldLength = fieldLength;
			this.lTrim = lTrim;
			this.rTrim = rTrim;
		}
		
		@Override
		public boolean consumeInput(DataRecord record) throws OperationNotSupportedException, IOException {
			int ichr;

			inputReader.mark();
			for (int i = 0; i < fieldLength; i++) {
				ichr = inputReader.readChar();
				if (ichr == CharByteInputReader.BLOCKED_BY_MARK) {
					throw new BadDataFormatException("End quote not found, try to increase MAX_RECORD_SIZE");
				}
				if (ichr == CharByteInputReader.DECODING_FAILED) {
					throw new BadDataFormatException("Decoding of input into char data failed");
				}
				if (ichr == CharByteInputReader.END_OF_INPUT) {
					if (i == 0) {
						return false;
					} else {
						throw new BadDataFormatException("End of input encountered while reading fixed-length field");
					}
				}
			}
			CharSequence seq = inputReader.getCharSequence(0);
			if (lTrim || rTrim) {
				StringBuilder sb = new StringBuilder(seq);
				if (lTrim) {
					StringUtils.trimLeading(sb);
				}
				if (rTrim) {
					StringUtils.trimTrailing(sb);
				}
				record.getField(fieldNumber).fromString(sb);
			} else {
				record.getField(fieldNumber).fromString(seq);
			}
			return true;
		}
		
	}
	
	public static class DelimCharFieldConsumer extends InputConsumer {
		private int fieldNumber;
		private QuotingDecoder qDecoder;
		private AhoCorasick delimPatterns;
		private boolean multipleDelimiters;
		private boolean acceptEofAsDelim;
		private boolean acceptEndOfRecord;
		private boolean isQuoted;
		private boolean lTrim;
		private boolean rTrim;

		public DelimCharFieldConsumer(CharByteInputReader inputReader, int fieldNumber, AhoCorasick delimPatterns,
				boolean multipleDelimiters, boolean acceptEofAsDelim, boolean acceptEndOfRecord, boolean isQuoted,
				boolean lTrim, boolean rTrim) {
			super(inputReader);
			this.fieldNumber = fieldNumber;
			this.delimPatterns = delimPatterns;
			this.multipleDelimiters = multipleDelimiters;
			this.acceptEofAsDelim = acceptEofAsDelim;
			this.acceptEndOfRecord = acceptEndOfRecord;
			this.isQuoted = isQuoted;
			if (isQuoted) {
				qDecoder = new QuotingDecoder();
			} else {
				qDecoder = null;
			}
			this.lTrim = lTrim;
			this.rTrim = rTrim;
		}
		
		private boolean consumeQuotedField(DataField field)  throws OperationNotSupportedException, IOException {
			int ichr;
			char chr;

			inputReader.mark();

			StringBuilder fieldValue = new StringBuilder();
			while (true) {
				ichr = inputReader.readChar();
				if (ichr == CharByteInputReader.BLOCKED_BY_MARK) {
					throw new BadDataFormatException("End quote not found, try to increase MAX_RECORD_SIZE");
				}
				if (ichr == CharByteInputReader.DECODING_FAILED) {
					throw new BadDataFormatException("Decoding of input into char data failed");
				}
				if (ichr == CharByteInputReader.END_OF_INPUT) {
					throw new BadDataFormatException("End of input encountered instead of the closing quote");						
				}
				chr = (char)ichr;
				if (qDecoder.isEndQuote(chr)) { // first closing quote
					ichr = inputReader.readChar();
					if (ichr == CharByteInputReader.BLOCKED_BY_MARK) {
						throw new BadDataFormatException("Field delimiter not found, try to increase MAX_RECORD_SIZE");
					}
					if (ichr == CharByteInputReader.DECODING_FAILED) {
						throw new BadDataFormatException("Decoding of input into char data failed - closing quote not followed by a delimiter");
					}
					if (ichr == CharByteInputReader.END_OF_INPUT) {
						if (acceptEofAsDelim) {
							fieldValue.append(inputReader.getCharSequence(-1));
							field.fromString(fieldValue);
							return false; // return value indicates success and end of input
						} else {
							throw new BadDataFormatException("Closing quote not followed by a delimiter");						
						}
					}
					chr = (char)ichr;
					if (qDecoder.isEndQuote(chr)) { // two quotes - they will be interpreted as one quote inside the quoted field
						// append part of the field including first quote, set the mark after the second quote 
						fieldValue.append(inputReader.getCharSequence(-1));
						inputReader.mark();
					} else { // one quote - end of the field
						// append part of the field including the quote. no need for the mark
						// look for a delimiter following immediately the end quote
						// be careful not to lose last input value in ichr
						delimPatterns.reset();
						delimPatterns.update(chr);
						fieldValue.append(inputReader.getCharSequence(-2)); // append without quote
						field.fromString(fieldValue);
						break;
					} // end quote followed by non-quote char
				} // first end quote found
			} // value reading loop

			// consume obligatory delimiter after the closing quote
			while (!delimPatterns.isPattern(fieldNumber)) {
				if (delimPatterns.getMatchLength() == 0) {
					throw new BadDataFormatException("Field value closing quote not followed by delimiter");					
				}
				ichr = inputReader.readChar();
				if (ichr == CharByteInputReader.BLOCKED_BY_MARK) {
					throw new BadDataFormatException("Field delimiter not found, try to increase MAX_RECORD_SIZE");
				}
				if (ichr == CharByteInputReader.DECODING_FAILED) {
					throw new BadDataFormatException("Decoding of input into char data failed");
				}
				if (ichr == CharByteInputReader.END_OF_INPUT) {
					throw new BadDataFormatException("End of input encountered instead of the field delimiter");						
				}
				chr = (char)ichr;
				delimPatterns.update(chr);
			}
			inputReader.mark(); // just to avoid unnecessary BLOCKED_BY_MARK failures
			return true; // return value indicates success without encountering end of input
		}
		
		public boolean consumeInput(DataRecord record) throws OperationNotSupportedException, IOException {
			int ichr;
			char chr;

			inputReader.mark();

			ichr = inputReader.readChar();
			if (ichr == CharByteInputReader.END_OF_INPUT) {
				return false;
			}
			if (isQuoted && qDecoder.isStartQuote(((char)ichr))) { // let's suppose that special values like END_OF_INPUT will not be type-cast to a quote
				if (!consumeQuotedField(record.getField(fieldNumber))) {
					return true;
				}
			} else {
				// we already have a value in ichr
				delimPatterns.reset();
				while (true) {
					if (ichr == CharByteInputReader.BLOCKED_BY_MARK) {
						throw new BadDataFormatException("Field delimiter not found, try to increase MAX_RECORD_SIZE");
					}
					if (ichr == CharByteInputReader.DECODING_FAILED) {
						throw new BadDataFormatException("Decoding of input into char data failed");
					}
					if (ichr == CharByteInputReader.END_OF_INPUT) {
						if (acceptEofAsDelim) {
							record.getField(fieldNumber).fromString(inputReader.getCharSequence(0));
							return true; // indicates success and end of input
						} else {
							throw new BadDataFormatException("End of input encountered instead of the field delimiter");						
						}
					}
					chr = (char)ichr;
					delimPatterns.update(chr);
					if (delimPatterns.isPattern(fieldNumber)) { // first delimiter found
						CharSequence seq = inputReader.getCharSequence(-delimPatterns.getMatchLength());
						if (lTrim || rTrim) {
							StringBuilder sb = new StringBuilder(seq);
							if (lTrim) {
								StringUtils.trimLeading(sb);
							}
							if (rTrim) {
								StringUtils.trimTrailing(sb);
							}
							record.getField(fieldNumber).fromString(sb);
						} else {
							record.getField(fieldNumber).fromString(seq);
						}
						if (!acceptEndOfRecord && delimPatterns.isPattern(RECORD_DELIMITER_IDENTIFIER)) {
							throw new UnexpectedEndOfRecordDataFormatException("Unexpected record delimiter found - missing fields in the record");											
						}
						delimPatterns.reset();
						inputReader.mark();
						break;
					} else if (delimPatterns.isPattern(DEFAULT_FIELD_DELIMITER_IDENTIFIER)) {
						throw new BadDataFormatException("Unexpected field delimiter found - record probably contains too many fields");
					} else if (delimPatterns.isPattern(RECORD_DELIMITER_IDENTIFIER)) {
						throw new UnexpectedEndOfRecordDataFormatException("Unexpected record delimiter found - missing fields in the record");					
					}
					ichr = inputReader.readChar();
				} // unquoted field reading loop
			} // quoted/unquoted if statement

			if (!multipleDelimiters) {
				return true;
			}
			// consume optional delimiters
			delimPatterns.reset();
			inputReader.mark();
			while (true) {
				ichr = inputReader.readChar();
				if (ichr == CharByteInputReader.BLOCKED_BY_MARK) {
					throw new BadDataFormatException("Field delimiter not found, try to increase MAX_RECORD_SIZE");
				}
				if (ichr == CharByteInputReader.DECODING_FAILED) {
					inputReader.revert(); // revert to the position after last delimiter					
					return true;
				}
				if (ichr == CharByteInputReader.END_OF_INPUT) {
					inputReader.revert(); // revert to the position after last delimiter
					return true;
				}
				chr = (char)ichr;
				delimPatterns.update(chr);
				if (delimPatterns.getMatchLength() == 0) {
					inputReader.revert(); // revert to the position after last delimiter
					return true;	// indicates success without end of input
				}
				if (delimPatterns.isPattern(fieldNumber)) {
					if (!acceptEndOfRecord && delimPatterns.isPattern(RECORD_DELIMITER_IDENTIFIER)) {
						throw new UnexpectedEndOfRecordDataFormatException("Unexpected record delimiter found - missing fields in the record");					
					}
					inputReader.mark(); // one more delimiter consumed
				} else if (delimPatterns.isPattern(DEFAULT_FIELD_DELIMITER_IDENTIFIER)) {
					throw new BadDataFormatException("Unexpected field delimiter found - record probably contains too many fields");
				} else if (delimPatterns.isPattern(RECORD_DELIMITER_IDENTIFIER)) {
					throw new UnexpectedEndOfRecordDataFormatException("Unexpected record delimiter found - missing fields in the record");					
				}
			}
		}
		
	} // DelimCharFieldConsumer

	public static class DelimByteFieldConsumer extends InputConsumer {
		private final CharsetDecoder decoder = Charset.forName(Defaults.DataParser.DEFAULT_CHARSET_DECODER).newDecoder();
		private int fieldNumber;
		private AhoCorasick delimPatterns;
		private boolean multipleDelimiters;
		private boolean acceptEofAsDelim;
		private boolean acceptEndOfRecord;
		private boolean lTrim;
		private boolean rTrim;

		public DelimByteFieldConsumer(CharByteInputReader inputReader, int fieldNumber, AhoCorasick delimPatterns,
				boolean multipleDelimiters, boolean acceptEofAsDelim, boolean acceptEndOfRecord, 
				boolean lTrim, boolean rTrim) {
			super(inputReader);
			this.fieldNumber = fieldNumber;
			this.delimPatterns = delimPatterns;
			this.multipleDelimiters = multipleDelimiters;
			this.acceptEofAsDelim = acceptEofAsDelim;
			this.acceptEndOfRecord = acceptEndOfRecord;
			this.lTrim = lTrim;
			this.rTrim = rTrim;
		}
		
		public boolean consumeInput(DataRecord record) throws OperationNotSupportedException, IOException {
			int ibt;
			char bt;

			inputReader.mark();

			ibt = inputReader.readByte();
			if (ibt == CharByteInputReader.END_OF_INPUT) {
				return false;
			}
			// we already have a value in ibt
			delimPatterns.reset();

			while (true) {
				if (ibt == CharByteInputReader.BLOCKED_BY_MARK) {
					throw new BadDataFormatException("Field delimiter not found, try to increase MAX_RECORD_SIZE");
				}
				if (ibt == CharByteInputReader.END_OF_INPUT) {
					if (acceptEofAsDelim) {
						record.getField(fieldNumber).fromByteBuffer(inputReader.getByteSequence(0), decoder);
						return true;
					} else {
						throw new BadDataFormatException("End of input encountered instead of the field delimiter");						
					}
				}
				bt = (char)ibt;
				delimPatterns.update(bt);
				if (delimPatterns.isPattern(fieldNumber)) { // first delimiter found
					ByteBuffer seq = inputReader.getByteSequence(-delimPatterns.getMatchLength());
					record.getField(fieldNumber).fromByteBuffer(seq, decoder);
					if (!acceptEndOfRecord && delimPatterns.isPattern(RECORD_DELIMITER_IDENTIFIER)) {
						throw new UnexpectedEndOfRecordDataFormatException("Unexpected record delimiter found - missing fields in the record");											
					}
					delimPatterns.reset();
					inputReader.mark();
					break;
				} else if (delimPatterns.isPattern(DEFAULT_FIELD_DELIMITER_IDENTIFIER)) {
					throw new BadDataFormatException("Unexpected field delimiter found - record probably contains too many fields");
				} else if (delimPatterns.isPattern(RECORD_DELIMITER_IDENTIFIER)) {
					throw new UnexpectedEndOfRecordDataFormatException("Unexpected record delimiter found - missing fields in the record");					
				}
				ibt = inputReader.readByte();
			} // unquoted field reading loop

			if (!multipleDelimiters) {
				return true;
			}
			// consume optional delimiters
			delimPatterns.reset();
			inputReader.mark();
			while (true) {
				ibt = inputReader.readByte();
				if (ibt == CharByteInputReader.BLOCKED_BY_MARK) {
					throw new BadDataFormatException("Field delimiter not found, try to increase MAX_RECORD_SIZE");
				}
				if (ibt == CharByteInputReader.DECODING_FAILED) {
					inputReader.revert(); // revert to the position after last delimiter					
					return true;
				}
				if (ibt == CharByteInputReader.END_OF_INPUT) {
					inputReader.revert(); // revert to the position after last delimiter
					return true;	// indicates success and end of input
				}
				bt = (char)ibt;
				delimPatterns.update(bt);
				if (delimPatterns.getMatchLength() == 0) {
					inputReader.revert(); // revert to the position after last delimiter
					return true;	// indicates success without end of input
				}
				if (delimPatterns.isPattern(fieldNumber)) {
					if (!acceptEndOfRecord && delimPatterns.isPattern(RECORD_DELIMITER_IDENTIFIER)) {
						throw new UnexpectedEndOfRecordDataFormatException("Unexpected record delimiter found - missing fields in the record");											
					}
					inputReader.mark(); // one more delimiter consumed
				} else if (delimPatterns.isPattern(DEFAULT_FIELD_DELIMITER_IDENTIFIER)) {
					throw new BadDataFormatException("Unexpected field delimiter found - record probably contains too many fields");
				} else if (delimPatterns.isPattern(RECORD_DELIMITER_IDENTIFIER)) {
					throw new UnexpectedEndOfRecordDataFormatException("Unexpected record delimiter found - missing fields in the record");					
				}
			}
		}
		
	} // DelimByteFieldConsumer
	
	public static class CharDelimConsumer extends InputConsumer {
		private AhoCorasick delimPatterns;
		private int delimId;
		private boolean acceptEofAsDelim;
		
		CharDelimConsumer(CharByteInputReader inputReader, AhoCorasick delimPatterns, int delimId, boolean acceptEofAsDelim) {
			super(inputReader);
			this.delimPatterns = delimPatterns;
			this.delimId = delimId;
			this.acceptEofAsDelim = acceptEofAsDelim;
		}
		
		public boolean consumeInput(DataRecord record) throws OperationNotSupportedException, IOException {
			delimPatterns.reset();
			inputReader.mark();
			int ichr;
			char chr;
			while (true) {
				ichr = inputReader.readChar();
				if (ichr == CharByteInputReader.BLOCKED_BY_MARK) {
					throw new BadDataFormatException("Field delimiter not found, try to increase MAX_RECORD_SIZE");
				}
				if (ichr == CharByteInputReader.DECODING_FAILED) {
					throw new BadDataFormatException("Decoding of input into char data failed while looking for obligatory delimiter");
				}
				if (ichr == CharByteInputReader.END_OF_INPUT) {
					if (acceptEofAsDelim && delimPatterns.getMatchLength() == 0) {
						return false; // indicates end of input before one single character was read
					} else {
						throw new BadDataFormatException("End of input encountered instead of the field delimiter");						
					}
				}
				chr = (char)ichr;
				delimPatterns.update(chr);
				if (delimPatterns.isPattern(delimId)) {
					return true;
				} else if (delimPatterns.isPattern(DEFAULT_FIELD_DELIMITER_IDENTIFIER)) {
					throw new BadDataFormatException("Unexpected field delimiter found - record probably contains too many fields");
				} else if (delimPatterns.isPattern(RECORD_DELIMITER_IDENTIFIER)) {
					throw new BadDataFormatException("Unexpected record delimiter found - missing fields in the record");					
				}
				if (delimPatterns.getMatchLength() == 0) {
					throw new BadDataFormatException("Obligatory delimiter not found after field");						
				}
			}
		}
		
	} // DelimCharConsumer
	
	public static class ByteDelimConsumer extends InputConsumer {
		private AhoCorasick delimPatterns;
		private int delimId;
		private boolean acceptEofAsDelim;
		
		ByteDelimConsumer(CharByteInputReader inputReader, AhoCorasick delimPatterns, int delimId, boolean acceptEofAsDelim) {
			super(inputReader);
			this.delimPatterns = delimPatterns;
			this.delimId = delimId;
			this.acceptEofAsDelim = acceptEofAsDelim;
		}
		
		public boolean consumeInput(DataRecord record) throws OperationNotSupportedException, IOException {
			delimPatterns.reset();
			inputReader.mark();
			int ibt;
			char bt;
			while (true) {
				ibt = inputReader.readByte();
				if (ibt == CharByteInputReader.BLOCKED_BY_MARK) {
					throw new BadDataFormatException("Field delimiter not found, try to increase MAX_RECORD_SIZE");
				}
				if (ibt == CharByteInputReader.END_OF_INPUT) {
					if (acceptEofAsDelim && delimPatterns.getMatchLength() == 0) {
						return false; // indicates end of input before one single character was read
					} else {
						throw new BadDataFormatException("End of input encountered instead of the field delimiter");						
					}
				}
				bt = (char)ibt;
				delimPatterns.update(bt);
				if (delimPatterns.isPattern(delimId)) {
					return true;
				} else if (delimPatterns.isPattern(DEFAULT_FIELD_DELIMITER_IDENTIFIER)) {
					throw new BadDataFormatException("Unexpected field delimiter found - record probably contains too many fields");
				} else if (delimPatterns.isPattern(RECORD_DELIMITER_IDENTIFIER)) {
					throw new BadDataFormatException("Unexpected record delimiter found - missing fields in the record");					
				}
				if (delimPatterns.getMatchLength() == 0) {
					throw new BadDataFormatException("Obligatory delimiter not found after field");						
				}
			}
		}
		
		public static class RecordSkipper extends InputConsumer {

			/**
			 * @param inputReader
			 */
			protected RecordSkipper(CharByteInputReader inputReader) {
				super(inputReader);
				// TODO Auto-generated constructor stub
			}

			@Override
			public boolean consumeInput(DataRecord record) throws OperationNotSupportedException, IOException {
				// TODO Auto-generated method stub
				return false;
			}
			
		}
				
	}
	
	public abstract static class RecordSkipper {
		protected CharByteInputReader inputReader; 
		protected AhoCorasick delimPatterns;
		protected boolean[] isDelimited;
		protected int numFields;

		public RecordSkipper(CharByteInputReader inputReader, AhoCorasick delimPatterns, boolean[] isDelimited) {
			this.inputReader = inputReader;
			this.delimPatterns = delimPatterns;
			this.isDelimited = isDelimited;
			this.numFields = isDelimited.length;
		}

		public abstract boolean skipInput(int nextField) throws OperationNotSupportedException, IOException;
	}
	
	public static class CharRecordSkipper extends RecordSkipper {

		/**
		 * @param inputReader
		 * @param delimPatterns
		 * @param isDelimited
		 */
		public CharRecordSkipper(CharByteInputReader inputReader, AhoCorasick delimPatterns, boolean[] isDelimited) {
			super(inputReader, delimPatterns, isDelimited);
		}

		public boolean skipInput(int nextField) throws OperationNotSupportedException, IOException {
			delimPatterns.reset();
			inputReader.mark();
			int ichr;
			char chr;
			int currentField;
			
			for (currentField = nextField; currentField < numFields && !isDelimited[currentField]; currentField++);

			ichr = inputReader.readChar();
			if (ichr == CharByteInputReader.END_OF_INPUT) {
				return false;
			}

			while (true) {
				if (ichr == CharByteInputReader.BLOCKED_BY_MARK) {
					// move mark and continue
					inputReader.mark();
				} else if (ichr == CharByteInputReader.END_OF_INPUT) {
					return true;
				} else if (ichr == CharByteInputReader.DECODING_FAILED) {
					throw new BadDataFormatException("Invalid byte data encountered in char file");						
				} else {
					chr = (char)ichr;
					delimPatterns.update(chr);
					if (delimPatterns.isPattern(RECORD_DELIMITER_IDENTIFIER)) {
						return true;
					}
					if (delimPatterns.isPattern(currentField)) {
						for (currentField++; currentField < numFields && !isDelimited[currentField]; currentField++);
						if (currentField == numFields) {
							return true;
						}
					}
				}
				ichr = inputReader.readChar();
			}
			// unreachable
		}
	}

	public static class ByteRecordSkipper extends RecordSkipper {
		
		/**
		 * @param inputReader
		 * @param delimPatterns
		 * @param isDelimited
		 */
		public ByteRecordSkipper(CharByteInputReader inputReader, AhoCorasick delimPatterns, boolean[] isDelimited) {
			super(inputReader, delimPatterns, isDelimited);
		}

		public boolean skipInput(int nextField) throws OperationNotSupportedException, IOException {
			delimPatterns.reset();
			inputReader.mark();
			int ibt;
			char bt;
			int currentField;

			for (currentField = nextField; currentField < numFields && !isDelimited[currentField]; currentField++);

			ibt = inputReader.readByte();
			if (ibt == CharByteInputReader.END_OF_INPUT) {
				return false;
			}

			while (true) {
				if (ibt == CharByteInputReader.BLOCKED_BY_MARK) {
					// move mark and continue
					inputReader.mark();
				} else if (ibt == CharByteInputReader.END_OF_INPUT) {
					return true;
				} else if (ibt == CharByteInputReader.DECODING_FAILED) {
					throw new BadDataFormatException("Invalid byte data encountered in char file");						
				} else {
					bt = (char)ibt;
					delimPatterns.update(bt);
					if (delimPatterns.isPattern(RECORD_DELIMITER_IDENTIFIER)) {
						return true;
					}
					if (delimPatterns.isPattern(currentField)) {
						for (currentField++; currentField < numFields && !isDelimited[currentField]; currentField++);
						if (currentField == numFields) {
							return true;
						}
					}
				}
				ibt = inputReader.readByte();
			}
			// unreachable
		}

	}
		
		
} 

