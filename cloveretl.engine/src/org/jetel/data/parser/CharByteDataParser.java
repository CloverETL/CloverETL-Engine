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
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

import javax.naming.OperationNotSupportedException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.IParserExceptionHandler;
import org.jetel.exception.JetelException;
import org.jetel.exception.PolicyType;
import org.jetel.exception.UnexpectedEndOfRecordDataFormatException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.bytes.CloverBuffer;
import org.jetel.util.string.QuotingDecoder;
import org.jetel.util.string.StringUtils;

/**
 * A parser able to deal with mixed records containing both char-based and byte-based fields (BYTE/CBYTE)
 * 
 * @author jhadrava (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created Nov 30, 2010
 */
public class CharByteDataParser extends AbstractTextParser {
	private static final int RECORD_DELIMITER_IDENTIFIER = -1;
	private static final int DEFAULT_FIELD_DELIMITER_IDENTIFIER = -2;

	ReadableByteChannel inputSource;
	private ICharByteInputReader inputReader;
	private CharByteInputReader.DoubleMarkCharByteInputReader verboseInputReader;
	private InputConsumer[] fieldConsumers;
	private RecordSkipper recordSkipper;
	private int numConsumers;
	private Charset charset;
	private IParserExceptionHandler exceptionHandler;
	private PolicyType policyType;
	private AhoCorasick byteSearcher;
	private AhoCorasick charSearcher;
	private int lastNonAutoFilledField;
	private boolean isInitialized;
	private int recordCounter;
	private String lastRawRecord;
	private int numFields;
	
	static Log logger = LogFactory.getLog(CharByteDataParser.class);

	/**
	 * Sole constructor
	 * @param cfg
	 */
	public CharByteDataParser(TextParserConfiguration cfg) {
		super(cfg);
		String charsetName = cfg.getCharset();
		if (charsetName == null) {
			charsetName = Defaults.DataParser.DEFAULT_CHARSET_DECODER;
		}
		charset = Charset.forName(charsetName);
		policyType = cfg.getPolicyType();
		releaseDataSource = false;
		recordSkipper = null;
		isInitialized = false;
		exceptionHandler = cfg.getExceptionHandler();
	}

	/**
	 * Returns parser speed for specified configuration. See
	 * {@link TextParserFactory#getParser(TextParserConfiguration)}.
	 */
	public static Integer getParserSpeed(TextParserConfiguration cfg) {
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

	/**
	 * Nomen omen
	 * @return
	 */
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
				return (new StringBuilder((CharSequence) seq)).toString();
			} else if (seq instanceof CloverBuffer) {
				return Charset.forName("ISO-8859-1").decode(((CloverBuffer) seq).buf()).toString();
			} else if (seq instanceof ByteBuffer) {
				return Charset.forName("ISO-8859-1").decode((ByteBuffer) seq).toString();
			}
		}
		return "<Raw record data is not available, please turn on verbose mode.>";
	}

	private DataRecord parsingErrorFound(String exceptionMessage, DataRecord record, int fieldNum,
			String offendingValue) {
		if (exceptionHandler != null) {
			exceptionHandler.populateHandler(exceptionMessage, record, recordCounter, fieldNum,
					offendingValue, new BadDataFormatException(exceptionMessage));
			return record;
		} else {
			throw new RuntimeException("Parsing error: " + exceptionMessage + 
					(offendingValue != null ? " (" + offendingValue + ")" : ""));
		}
	}

	/**
	 * @see org.jetel.data.parser.Parser#getNext()
	 */
	@Override
	public DataRecord getNext() throws JetelException {
		DataRecord record = DataRecordFactory.newRecord(cfg.getMetadata());
		record.init();

		record = parseNext(record);
		if (exceptionHandler != null) { // use handler only if configured
			while (exceptionHandler.isExceptionThrowed()) {
				exceptionHandler.setRawRecord(lastRawRecord);
				exceptionHandler.handleException();
				record = parseNext(record);
			}
		}
		return record;
	}

	/**
	 * @see org.jetel.data.parser.Parser#getNext(org.jetel.data.DataRecord)
	 */
	@Override
	public DataRecord getNext(DataRecord record) throws JetelException {
		record = parseNext(record);
		if (exceptionHandler != null) { // use handler only if configured
			while (exceptionHandler.isExceptionThrowed()) {
				exceptionHandler.setRawRecord(lastRawRecord);
				exceptionHandler.handleException();
				record = parseNext(record);
			}
		}
		return record;
	}

	/**
	 * Parses next record from input reader
	 * @param record
	 * @return parsed record
	 * @throws JetelException
	 */
	public DataRecord parseNext(DataRecord record) throws JetelException {
		recordCounter++;
		try {
			if (verboseInputReader != null) {
				verboseInputReader.setOuterMark();
				lastRawRecord = null;
			}
			int consumerIdx = 0;
			try {
				for (consumerIdx = 0; consumerIdx < numConsumers; consumerIdx++) {
					if (!fieldConsumers[consumerIdx].consumeInput(record)) {
						if (consumerIdx == 0) {
							return null;
						} else {
							if (consumerIdx != numConsumers - 1) {
								return parsingErrorFound("Incomplete record at the end of input", record, consumerIdx, null);
							} else {
								break;
							}
						}
					}
				}
				boolean doSkip = (consumerIdx == numConsumers) && 
					(policyType == PolicyType.LENIENT) && 
					(fieldConsumers != null && numConsumers > 0) &&
					(Boolean.TRUE.equals(fieldConsumers[numConsumers - 1].hasMoreFields())); 
				if(doSkip) {
					if (recordSkipper != null) {
						recordSkipper.skipInput(consumerIdx);
					}
				}
			} catch (UnexpectedEndOfRecordDataFormatException e) {
				return parsingErrorFound(e.getSimpleMessage(), record, consumerIdx, null);
			} catch (BadDataFormatException e) {
				if (recordSkipper != null) {
					recordSkipper.skipInput(consumerIdx);
				}
				if (cfg.isVerbose()) {
					lastRawRecord = getLastRawRecord(); 
				}
				return parsingErrorFound(e.getSimpleMessage(), record, Math.min(consumerIdx, numFields - 1), //in case extra delimiter consumer is used - index of consumer does not need to match index of field 
						e.getOffendingValue() != null ? e.getOffendingValue().toString() : null);
			} finally {
				if (verboseInputReader != null) {
					verboseInputReader.releaseOuterMark();
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
	 * Returns byte searcher of the delimiters. In case it doesn't exist creates it 
	 * 
	 * @return byte searcher
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
	 * Returns char searcher of the delimiters. In case it doesn't exist creates it 
	 * 
	 * @return char searcher
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

	/**
	 * Sets an input reader to be used by the parser. In typical case this is not necessary
	 * as the reader is created during parser initialization. However, it
	 * may be useful in special cases, eg when we want more parsers to share the same reader. 
	 * 
	 * @param inputReader
	 */
	public void setInputReader(ICharByteInputReader inputReader) {
		this.inputReader = inputReader;
		this.verboseInputReader = null;
	}

	/**
	 * Sets verbose input reader to be used by the parser. In typical case this is not necessary
	 * as the reader is created during parser initialization. However, it
	 * may be useful in special cases, eg when we want more parsers to share the same reader. 
	 * 
	 * @param inputReader
	 */
	public void setVerboseInputReader(CharByteInputReader.DoubleMarkCharByteInputReader verboseInputReader) {
		this.inputReader = this.verboseInputReader = verboseInputReader;
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

		numFields = metadata.getNumFields();

		for (lastNonAutoFilledField = numFields - 1; lastNonAutoFilledField >= 0; lastNonAutoFilledField--) {
			if (!metadata.getField(lastNonAutoFilledField).isAutoFilled()) {
				break;
			}
		}
		if (lastNonAutoFilledField == -1) {
			numConsumers = 0;
			return;
		}

		boolean needByteInput = false;
		boolean needCharInput = false;
		
		// let's find out what kind of input reader we need
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
		if (inputReader == null) {
			CharByteInputReader singleMarkInputReader = CharByteInputReader.createInputReader(metadata, charset, false, false);
			if (cfg.isVerbose()) {
				inputReader = verboseInputReader = new CharByteInputReader.DoubleMarkCharByteInputReader(singleMarkInputReader);
			} else {
				verboseInputReader = null;
				inputReader = singleMarkInputReader;
			}
		} else {
		} // preserve inputReader set externally - this is useful for creating a set of parsers sharing the same
			// inputReader
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
			DataFieldMetadata field = metadata.getField(idx);
			if (byteFieldCount > 0) { // fixlen byte field consumer
				fieldConsumers[numConsumers] = new FixlenByteFieldConsumer(metadata, inputReader, idx, byteFieldCount, field.getShift());
				idx += byteFieldCount;
			} else {
				if (!field.isDelimited()) { // fixlen char field consumer
					assert !field.isByteBased() : "Unexpected execution flow";
					fieldConsumers[numConsumers] = new FixlenCharFieldConsumer(inputReader, idx, field.getSize(), isSkipFieldLeadingBlanks(idx), field.getShift());
				} else { // delimited
					boolean acceptDefaultFieldDelimiter = (policyType == PolicyType.LENIENT && idx == lastNonAutoFilledField); 
					if (field.isByteBased()) { // delimited byte field consumer
						fieldConsumers[numConsumers] = new DelimByteFieldConsumer(inputReader, idx, getByteDelimSearcher(), cfg.isTreatMultipleDelimitersAsOne(), field.isEofAsDelimiter(), lastNonAutoFilledField == idx ? true : false, isSkipFieldLeadingBlanks(idx), isSkipFieldTrailingBlanks(idx), field.getShift(), acceptDefaultFieldDelimiter, cfg.isTryToMatchLongerDelimiter());
					} else { // delimited char field consumer
						fieldConsumers[numConsumers] = new DelimCharFieldConsumer(inputReader, idx, getCharDelimSearcher(), cfg.isTreatMultipleDelimitersAsOne(), field.isEofAsDelimiter(), lastNonAutoFilledField == idx ? true : false, cfg.isQuotedStrings(), cfg.getQuoteChar(), isSkipFieldLeadingBlanks(idx), isSkipFieldTrailingBlanks(idx), field.getShift(), acceptDefaultFieldDelimiter, cfg.isTryToMatchLongerDelimiter());
					}
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
			fieldConsumers[numConsumers++] = new CharDelimConsumer(inputReader, needCharInput ? getCharDelimSearcher() : getByteDelimSearcher(), RECORD_DELIMITER_IDENTIFIER, metadata.getField(lastNonAutoFilledField).isEofAsDelimiter(), cfg.isTryToMatchLongerDelimiter());
		}
	}
	
	private void closeInputSource() throws IOException {
		if (inputSource != null) {
			inputSource.close();
		}
	}

	@Override
	protected void releaseDataSource() {
		try {
			closeInputSource(); 
		} catch (IOException ioe) {
			logger.warn("Failed to close data source", ioe);
		}
	}

	@Override
	public void setDataSource(Object inputDataSource) throws IOException, ComponentNotReadyException {
		super.setDataSource(inputDataSource);
		recordCounter = 0;
		if (inputDataSource instanceof ReadableByteChannel) {
			inputSource = (ReadableByteChannel) inputDataSource;
		} else if (inputDataSource instanceof FileInputStream) {
			inputSource = ((FileInputStream) inputDataSource).getChannel();
		} else if (inputDataSource instanceof InputStream) {
			inputSource = Channels.newChannel((InputStream) inputDataSource);
		} else {
			inputSource = null;
		}
		inputReader.setInputSource(inputSource);
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
	public void close() throws IOException {
		free();
	}

	@Override
	public void preExecute() throws ComponentNotReadyException {
	}

	@Override
	public void postExecute() throws ComponentNotReadyException {
		inputReader.setInputSource(null);
		releaseDataSource();
	}

	@Override
	public void free() throws IOException {
		if (releaseDataSource) {
			closeInputSource();
		}
	}

	/**
	 * 
	 * @author jhadrava (info@cloveretl.com)
	 *         (c) Javlin, a.s. (www.cloveretl.com)
	 *
	 * @created Mar 31, 2011
	 */
	public abstract static class InputConsumer {
		
		protected ICharByteInputReader inputReader;

		protected InputConsumer(ICharByteInputReader inputReader) {
			this.inputReader = inputReader;
		}

		/**
		 * Reads part of the input and save it into the record
		 * 
		 * @param record
		 * @return false if no single byte/char of input is available, true otherwise
		 * @throws OperationNotSupportedException
		 * @throws IOException
		 */
		public abstract boolean consumeInput(DataRecord record) throws OperationNotSupportedException, IOException;
		
		/**
		 * A flag that indicates that the last consumed field should have completed a record and it did not,
		 * typically when encountered an unexpected delimiter (a field delimiter instead of a record delimiter).
		 * 
		 * The method is supposed to return <code>null</code> if unknown.
		 * 
		 * @return <code>true</code> if the record probably contains more fields, <code>null</code> if not known
		 */
		public Boolean hasMoreFields() {
			return null;
		}

	}

	/**
	 * Consumes several adjacent byte fields of fixed length.
	 * 
	 * @author jhadrava (info@cloveretl.com)
	 *         (c) Javlin, a.s. (www.cloveretl.com)
	 *
	 * @created Mar 31, 2011
	 */
	public static class FixlenByteFieldConsumer extends InputConsumer {

		private final CharsetDecoder decoder = Charset.forName(Defaults.DataParser.DEFAULT_CHARSET_DECODER).newDecoder();
		private int startFieldIdx;
		private int fieldCount;
		private int shift;

		private int dataLength;
		private int[] fieldStart;
		private int[] fieldEnd;
		private boolean[] isAutoFilling;

		/**
		 * @param metadata Metadata, used to retrieve lenght and position of the fields being consumed 
		 * @param inputReader
		 * @param startFieldIdx index of the first field to be consumed
		 * @param fieldCount number of field to be consumed
		 * @param shift 
		 * @throws ComponentNotReadyException
		 */
		FixlenByteFieldConsumer(DataRecordMetadata metadata, ICharByteInputReader inputReader, int startFieldIdx,
				int fieldCount, int shift) throws ComponentNotReadyException {
			super(inputReader);
			this.startFieldIdx = startFieldIdx;
			this.fieldCount = fieldCount;
			this.inputReader = inputReader;
			this.shift = shift;

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
			inputReader.skip(shift);
			inputReader.mark();

			for (int i = 0; i < dataLength; i++) {
				ibt = inputReader.readByte();
				if (ibt == CharByteInputReader.BLOCKED_BY_MARK) {
					throw new BadDataFormatException("Insufficient buffer capacity, try to increase Record.RECORD_LIMIT_SIZE");
				}
				if (ibt == CharByteInputReader.END_OF_INPUT) {
					//end of file reached
					if (i == 0) {
						//don't worry, no bytes have been read yet, EOF was on correct place (at the end of a record)
						return false;
					} else {
						//what about isEofAsDelimiter() switch? all fields at this position needs to have this switch turned on
						for (int idx = 0; idx < fieldCount; idx++) {
							if (!isAutoFilling[idx] && i >= fieldStart[idx] && i < fieldEnd[idx]) {
								if (!record.getMetadata().getField(startFieldIdx + idx).isEofAsDelimiter()) {
									throw new BadDataFormatException("End of input encountered instead of the closing quote");
								}
							}
						}
						//little bit unexpected EOF, but still correct, populate fields which we have data for
						break;
					}
				}
			}
			CloverBuffer seq = inputReader.getByteSequence(0);
			int startPos = seq.position();
			int endPos = seq.limit();
			for (int idx = 0; idx < fieldCount; idx++) {
				if (isAutoFilling[idx]) {
					continue;
				}
				if (startPos + fieldStart[idx] > endPos) {
					//range of data for the field is out of read data (in case eofAsDelimiter)
					continue;
				}
				seq.position(startPos + fieldStart[idx]);
				seq.limit(Math.min(startPos + fieldEnd[idx], endPos)); //range of data for the field may be wider than read data (in case eofAsDelimiter)
				record.getField(startFieldIdx + idx).fromByteBuffer(seq, decoder);
			}
			return true;
		}

	}

	/**
	 * Consumes a char-based field of fixed length
	 * 
	 * @author jhadrava (info@cloveretl.com)
	 *         (c) Javlin, a.s. (www.cloveretl.com)
	 *
	 * @created Mar 31, 2011
	 */
	public static class FixlenCharFieldConsumer extends InputConsumer {

		protected int fieldNumber;
		private int fieldLength;
		private boolean lTrim;
		private int shift;

		/** 
		 * Sole constructor
		 * @param inputReader
		 * @param fieldNumber
		 * @param fieldLength
		 * @param lTrim
		 * @param rTrim
		 * @param shift
		 */
		FixlenCharFieldConsumer(ICharByteInputReader inputReader, int fieldNumber, int fieldLength, boolean lTrim,
				int shift) {
			super(inputReader);
			this.fieldNumber = fieldNumber;
			this.inputReader = inputReader;
			this.fieldLength = fieldLength;
			this.lTrim = lTrim;
			this.shift = shift;
		}

		@Override
		public boolean consumeInput(DataRecord record) throws OperationNotSupportedException, IOException {
			int ichr;

			inputReader.skip(shift);
			inputReader.mark();
			// DP-465 - DataParser always trims tailing whitespace for fixed-length string fields (search
			// for "removes tailing blanks" comment). CharByteDataParser should behave the same way.
			int tailingWhitespaces = 0;
			for (int i = 0; i < fieldLength; i++) {
				ichr = inputReader.readChar();
				if (ichr == CharByteInputReader.BLOCKED_BY_MARK) {
					throw new BadDataFormatException("End quote not found, try to increase Record.RECORD_LIMIT_SIZE");
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
				if (Character.isWhitespace(ichr)) {
					tailingWhitespaces++;
				} else {
					tailingWhitespaces = 0;
				}
			}
			CharSequence seq = inputReader.getCharSequence(-tailingWhitespaces);
			if (lTrim) {
				StringBuilder sb = new StringBuilder(seq);
				StringUtils.trimLeading(sb);
				record.getField(fieldNumber).fromString(sb);
			} else {
				record.getField(fieldNumber).fromString(seq);
			}
			return true;
		}

	}

	/**
	 * Consumes delimited char-based field
	 * 
	 * @author jhadrava (info@cloveretl.com)
	 *         (c) Javlin, a.s. (www.cloveretl.com)
	 *
	 * @created Mar 31, 2011
	 */
	public static class DelimCharFieldConsumer extends InputConsumer {
		private int fieldNumber;
		private QuotingDecoder qDecoder;
		private AhoCorasick delimPatterns;
		private boolean multipleDelimiters;
		private boolean matchLongestDelimiter;
		private boolean acceptEofAsDelim;
		private boolean acceptEndOfRecord;
		private boolean isQuoted;
		private boolean lTrim;
		private boolean rTrim;
		private int shift;
		private boolean acceptDefaultFieldDelim;
		
		private Boolean hasMoreFields = null;

		/**
		 * Sole constructor
		 * @param inputReader
		 * @param fieldNumber
		 * @param delimPatterns Searcher used to identify end of field
		 * @param multipleDelimiters
		 * @param acceptEofAsDelim
		 * @param acceptEndOfRecord
		 * @param isQuoted
		 * @param lTrim
		 * @param rTrim
		 * @param shift
		 */
		public DelimCharFieldConsumer(ICharByteInputReader inputReader, int fieldNumber, AhoCorasick delimPatterns,
				boolean multipleDelimiters, boolean acceptEofAsDelim, boolean acceptEndOfRecord, boolean isQuoted,
				Character quoteCharacter, boolean lTrim, boolean rTrim, int shift, boolean acceptDefaultFieldDelim, boolean matchLongestDelimiter) {
			super(inputReader);
			this.fieldNumber = fieldNumber;
			this.delimPatterns = delimPatterns;
			this.multipleDelimiters = multipleDelimiters;
			this.acceptEofAsDelim = acceptEofAsDelim;
			this.acceptEndOfRecord = acceptEndOfRecord;
			this.isQuoted = isQuoted;
			if (isQuoted) {
				qDecoder = new QuotingDecoder();
				qDecoder.setQuoteChar(quoteCharacter);
			} else {
				qDecoder = null;
			}
			this.lTrim = lTrim;
			this.rTrim = rTrim;
			this.shift = shift;
			this.acceptDefaultFieldDelim = acceptDefaultFieldDelim;
			this.matchLongestDelimiter = matchLongestDelimiter;
		}

		private boolean consumeQuotedField(DataField field) throws OperationNotSupportedException, IOException {
			int ichr;
			char chr;

			inputReader.skip(shift);
			inputReader.mark();

			StringBuilder fieldValue = new StringBuilder();
			while (true) {
				ichr = inputReader.readChar();
				if (ichr == CharByteInputReader.BLOCKED_BY_MARK) {
					throw new BadDataFormatException("End quote not found, try to increase Record.RECORD_LIMIT_SIZE");
				}
				if (ichr == CharByteInputReader.DECODING_FAILED) {
					throw new BadDataFormatException("Decoding of input into char data failed");
				}
				if (ichr == CharByteInputReader.END_OF_INPUT) {
					throw new BadDataFormatException("End of input encountered instead of the closing quote");
				}
				chr = (char) ichr;
				if (qDecoder.isEndQuote(chr)) { // first closing quote
					ichr = inputReader.readChar();
					if (ichr == CharByteInputReader.BLOCKED_BY_MARK) {
						throw new BadDataFormatException("Field delimiter not found, try to increase Record.RECORD_LIMIT_SIZE");
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
					chr = (char) ichr;
					if (qDecoder.isEndQuote(chr)) { // two quotes - they will be interpreted as one quote inside the
													// quoted field
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
				chr = (char) ichr;
				delimPatterns.update(chr);
			}
			inputReader.mark(); // just to avoid unnecessary BLOCKED_BY_MARK failures
			return true; // return value indicates success without encountering end of input
		}
		
		private void produceOutput(DataRecord record) throws OperationNotSupportedException {
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
		}

		@Override
		public boolean consumeInput(DataRecord record) throws OperationNotSupportedException, IOException {
			
			this.hasMoreFields = null; //reset the incomplete record flag
			
			int ichr;
			char chr;

			inputReader.skip(shift);
			inputReader.mark();

			ichr = inputReader.readChar();
			if (ichr == CharByteInputReader.END_OF_INPUT) {				
				return false;
			}
			if (isQuoted && qDecoder.isStartQuote(((char) ichr))) { // let's suppose that special values like
																	// END_OF_INPUT will not be type-cast to a quote
				if (!consumeQuotedField(record.getField(fieldNumber))) {
					return true;
				}
			} else {
				// we already have a value in ichr
				delimPatterns.reset();
				while (true) {
					if (ichr == CharByteInputReader.BLOCKED_BY_MARK) {
						throw new BadDataFormatException("Field delimiter not found, try to increase Record.RECORD_LIMIT_SIZE");
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
					chr = (char) ichr;
					delimPatterns.update(chr);
					if (delimPatterns.isPattern(fieldNumber)) { // first delimiter found
						produceOutput(record);
						break;
					} else if (delimPatterns.isPattern(DEFAULT_FIELD_DELIMITER_IDENTIFIER)) {
						// lenient data policy - allow default field delimiters for the last field 
						if(acceptDefaultFieldDelim) {
							hasMoreFields = true;
							produceOutput(record);
							break;
						}
						throw new BadDataFormatException("Unexpected field delimiter found - record probably contains too many fields");
					} else if (delimPatterns.isPattern(RECORD_DELIMITER_IDENTIFIER)) {
						throw new UnexpectedEndOfRecordDataFormatException("Unexpected record delimiter found - missing fields in the record");
					}
					ichr = inputReader.readChar();
				} // unquoted field reading loop
			} // quoted/unquoted if statement

			// tries to find longer delimiter - typically for a set of delimiters \n, \r, \r\n, after \r is found, this will look for possible match of
			// \r\n. NOTE: this is different behavior than matching multiple delimiters
			if (matchLongestDelimiter) {
				// consume longest possible delimiter
				inputReader.mark();
				while (true) {
					ichr = inputReader.readChar();
					if (ichr == CharByteInputReader.BLOCKED_BY_MARK) {
						throw new BadDataFormatException("Field delimiter not found, try to increase MAX_RECORD_SIZE");
					}
					if (ichr == CharByteInputReader.DECODING_FAILED) {
						inputReader.revert(); // revert to the position after last delimiter
						break;
					}
					if (ichr == CharByteInputReader.END_OF_INPUT) {
						inputReader.revert(); // revert to the position after last delimiter
						break;
					}
					chr = (char) ichr;
					boolean withoutFail = delimPatterns.update(chr);
					if (!withoutFail || delimPatterns.getMatchLength() == 0) {
						inputReader.revert(); // revert to the position after last delimiter
						break;
					}
					if (delimPatterns.isPattern(fieldNumber)) {
						if (!acceptEndOfRecord && delimPatterns.isPattern(RECORD_DELIMITER_IDENTIFIER)) {
							throw new UnexpectedEndOfRecordDataFormatException("Unexpected record delimiter found - missing fields in the record");
						}
						inputReader.mark(); // longer delimiter consumed
					} else if (delimPatterns.isPattern(DEFAULT_FIELD_DELIMITER_IDENTIFIER)) {
						throw new BadDataFormatException("Unexpected field delimiter found - record probably contains too many fields");
					} else if (delimPatterns.isPattern(RECORD_DELIMITER_IDENTIFIER)) {
						throw new UnexpectedEndOfRecordDataFormatException("Unexpected record delimiter found - missing fields in the record");
					}
				}
			}			
			
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
				chr = (char) ichr;
				delimPatterns.update(chr);
				if (delimPatterns.getMatchLength() == 0) {
					inputReader.revert(); // revert to the position after last delimiter
					return true; // indicates success without end of input
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

		@Override
		public Boolean hasMoreFields() {
			return hasMoreFields;
		}

	} // DelimCharFieldConsumer

	/**
	 * Consumes delimited byte field. (Can such field even exist?)
	 * 
	 * @author jhadrava (info@cloveretl.com)
	 *         (c) Javlin, a.s. (www.cloveretl.com)
	 *
	 * @created Mar 31, 2011
	 */
	public static class DelimByteFieldConsumer extends InputConsumer {
		private final CharsetDecoder decoder = Charset.forName(Defaults.DataParser.DEFAULT_CHARSET_DECODER).newDecoder();
		private int fieldNumber;
		private AhoCorasick delimPatterns;
		private boolean multipleDelimiters;
		private boolean matchLongestDelimiter;
		private boolean acceptEofAsDelim;
		private boolean acceptEndOfRecord;
		private int shift;
		private boolean acceptDefaultFieldDelim = false;

		private Boolean hasMoreFields = null;

		/**
		 * Sole constructor
		 * 
		 * @param inputReader
		 * @param fieldNumber
		 * @param delimPatterns searcher used to identify delimiter
		 * @param multipleDelimiters
		 * @param acceptEofAsDelim
		 * @param acceptEndOfRecord
		 * @param lTrim
		 * @param rTrim
		 * @param shift
		 */
		public DelimByteFieldConsumer(ICharByteInputReader inputReader, int fieldNumber, AhoCorasick delimPatterns,
				boolean multipleDelimiters, boolean acceptEofAsDelim, boolean acceptEndOfRecord, boolean lTrim,
				boolean rTrim, int shift, boolean acceptDefaultFieldDelim, boolean matchLongestDelimiter) {
			super(inputReader);
			this.fieldNumber = fieldNumber;
			this.delimPatterns = delimPatterns;
			this.multipleDelimiters = multipleDelimiters;
			this.acceptEofAsDelim = acceptEofAsDelim;
			this.acceptEndOfRecord = acceptEndOfRecord;
			this.shift = shift;
			this.acceptDefaultFieldDelim = acceptDefaultFieldDelim;
			this.matchLongestDelimiter = matchLongestDelimiter;
		}
		
		private void produceOutput(DataRecord record) throws CharacterCodingException, OperationNotSupportedException {
			CloverBuffer seq = inputReader.getByteSequence(-delimPatterns.getMatchLength());
			record.getField(fieldNumber).fromByteBuffer(seq, decoder);
			if (!acceptEndOfRecord && delimPatterns.isPattern(RECORD_DELIMITER_IDENTIFIER)) {
				throw new UnexpectedEndOfRecordDataFormatException("Unexpected record delimiter found - missing fields in the record");
			}
			delimPatterns.reset();
			inputReader.mark();
		}

		@Override
		public boolean consumeInput(DataRecord record) throws OperationNotSupportedException, IOException {
			
			hasMoreFields = null; // reset the incomplete record flag
			
			int ibt;
			char bt;

			inputReader.skip(shift);
			inputReader.mark();

			ibt = inputReader.readByte();
			if (ibt == CharByteInputReader.END_OF_INPUT) {
				return false;
			}
			// we already have a value in ibt
			delimPatterns.reset();

			while (true) {
				if (ibt == CharByteInputReader.BLOCKED_BY_MARK) {
					throw new BadDataFormatException("Field delimiter not found, try to increase Record.RECORD_LIMIT_SIZE");
				}
				if (ibt == CharByteInputReader.END_OF_INPUT) {
					if (acceptEofAsDelim) {
						record.getField(fieldNumber).fromByteBuffer(inputReader.getByteSequence(0), decoder);
						return true;
					} else {
						throw new BadDataFormatException("End of input encountered instead of the field delimiter");
					}
				}
				bt = (char) ibt;
				delimPatterns.update(bt);
				if (delimPatterns.isPattern(fieldNumber)) { // first delimiter found
					produceOutput(record);
					break;
				} else if (delimPatterns.isPattern(DEFAULT_FIELD_DELIMITER_IDENTIFIER)) {
					// lenient data policy - allow default field delimiters for the last field 
					if(acceptDefaultFieldDelim) {
						hasMoreFields = true;
						produceOutput(record);
						break;
					}
					throw new BadDataFormatException("Unexpected field delimiter found - record probably contains too many fields");
				} else if (delimPatterns.isPattern(RECORD_DELIMITER_IDENTIFIER)) {
					throw new UnexpectedEndOfRecordDataFormatException("Unexpected record delimiter found - missing fields in the record");
				}
				ibt = inputReader.readByte();
			} // unquoted field reading loop

			if (matchLongestDelimiter) {
				// consume longest possible delimiter
				inputReader.mark();
				while (true) {
					ibt = inputReader.readByte();
					if (ibt == CharByteInputReader.BLOCKED_BY_MARK) {
						throw new BadDataFormatException("Field delimiter not found, try to increase MAX_RECORD_SIZE");
					}
					if (ibt == CharByteInputReader.DECODING_FAILED) {
						inputReader.revert(); // revert to the position after last delimiter
						break;
					}
					if (ibt == CharByteInputReader.END_OF_INPUT) {
						inputReader.revert(); // revert to the position after last delimiter
						break;
					}
					bt = (char) ibt;
					boolean withoutFail = delimPatterns.update(bt);
					if (!withoutFail) {
						inputReader.revert();
						break;
					}
					if (delimPatterns.getMatchLength() == 0) {
						inputReader.revert(); // revert to the position after last delimiter
						break;
					}
					if (delimPatterns.isPattern(fieldNumber)) {
						if (!acceptEndOfRecord && delimPatterns.isPattern(RECORD_DELIMITER_IDENTIFIER)) {
							throw new UnexpectedEndOfRecordDataFormatException("Unexpected record delimiter found - missing fields in the record");
						}
						inputReader.mark(); // longer delimiter found
					} else if (delimPatterns.isPattern(DEFAULT_FIELD_DELIMITER_IDENTIFIER)) {
						throw new BadDataFormatException("Unexpected field delimiter found - record probably contains too many fields");
					} else if (delimPatterns.isPattern(RECORD_DELIMITER_IDENTIFIER)) {
						throw new UnexpectedEndOfRecordDataFormatException("Unexpected record delimiter found - missing fields in the record");
					}
				}
			}
			
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
					return true; // indicates success and end of input
				}
				bt = (char) ibt;
				delimPatterns.update(bt);
				if (delimPatterns.getMatchLength() == 0) {
					inputReader.revert(); // revert to the position after last delimiter
					return true; // indicates success without end of input
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

		@Override
		public Boolean hasMoreFields() {
			return hasMoreFields;
		}

	} // DelimByteFieldConsumer

	/**
	 * Consumes a char-based delimiter that cannot be preceded by any other data
	 * 
	 * @author jhadrava (info@cloveretl.com)
	 *         (c) Javlin, a.s. (www.cloveretl.com)
	 *
	 * @created Mar 31, 2011
	 */
	public static class CharDelimConsumer extends InputConsumer {
		private AhoCorasick delimPatterns;
		private int delimId;
		private boolean acceptEofAsDelim;
		private boolean matchLongerDelimiter;

		CharDelimConsumer(ICharByteInputReader inputReader, AhoCorasick delimPatterns, int delimId,
				boolean acceptEofAsDelim, boolean matchLongerDelimiter) {
			super(inputReader);
			this.delimPatterns = delimPatterns;
			this.delimId = delimId;
			this.acceptEofAsDelim = acceptEofAsDelim;
			this.matchLongerDelimiter = matchLongerDelimiter;
		}

		@Override
		public boolean consumeInput(DataRecord record) throws OperationNotSupportedException, IOException {
			delimPatterns.reset();
			inputReader.mark();
			int ichr;
			char chr;
			while (true) {
				ichr = inputReader.readChar();
				if (ichr == CharByteInputReader.BLOCKED_BY_MARK) {
					throw new BadDataFormatException("Field delimiter not found, try to increase Record.RECORD_LIMIT_SIZE");
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
				chr = (char) ichr;
				boolean withoutFail = delimPatterns.update(chr);
				if (delimPatterns.isPattern(delimId)) {
					// we are trying to match longest possible delimiter
					if (matchLongerDelimiter && withoutFail) {
						inputReader.mark();
						continue;
					} else {
						return true;
					}
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

	/**
	 * Consumes a char-based delimiter that cannot be preceded by any other data
	 * 
	 * @author jhadrava (info@cloveretl.com)
	 *         (c) Javlin, a.s. (www.cloveretl.com)
	 *
	 * @created Mar 31, 2011
	 */
	public static class ByteDelimConsumer extends InputConsumer {
		private AhoCorasick delimPatterns;
		private int delimId;
		private boolean acceptEofAsDelim;
		private int shift;

		ByteDelimConsumer(ICharByteInputReader inputReader, AhoCorasick delimPatterns, int delimId,
				boolean acceptEofAsDelim, int shift) {
			super(inputReader);
			this.delimPatterns = delimPatterns;
			this.delimId = delimId;
			this.acceptEofAsDelim = acceptEofAsDelim;
			this.shift = shift;
		}

		@Override
		public boolean consumeInput(DataRecord record) throws OperationNotSupportedException, IOException {
			delimPatterns.reset();
			inputReader.skip(shift);
			inputReader.mark();
			int ibt;
			char bt;
			while (true) {
				ibt = inputReader.readByte();
				if (ibt == CharByteInputReader.BLOCKED_BY_MARK) {
					throw new BadDataFormatException("Field delimiter not found, try to increase Record.RECORD_LIMIT_SIZE");
				}
				if (ibt == CharByteInputReader.END_OF_INPUT) {
					if (acceptEofAsDelim && delimPatterns.getMatchLength() == 0) {
						return false; // indicates end of input before one single character was read
					} else {
						throw new BadDataFormatException("End of input encountered instead of the field delimiter");
					}
				}
				bt = (char) ibt;
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

	}

	/**
	 * Attempts to skip a record.
	 * 
	 * @author jhadrava (info@cloveretl.com)
	 *         (c) Javlin, a.s. (www.cloveretl.com)
	 *
	 * @created Mar 31, 2011
	 */
	public abstract static class RecordSkipper {
		protected ICharByteInputReader inputReader;
		protected AhoCorasick delimPatterns;
		protected boolean[] isDelimited;
		protected int numFields;

		public RecordSkipper(ICharByteInputReader inputReader, AhoCorasick delimPatterns, boolean[] isDelimited) {
			this.inputReader = inputReader;
			this.delimPatterns = delimPatterns;
			this.isDelimited = isDelimited;
			this.numFields = isDelimited.length;
		}

		/**
		 * Skip the rest of current record
		 * @param nextField index of the field at cursor's position
		 * @return
		 * @throws OperationNotSupportedException
		 * @throws IOException
		 */
		public abstract boolean skipInput(int nextField) throws OperationNotSupportedException, IOException;
	}

	/**
	 * Record skipper for input reader that may not support byte operations 
	 * @author jhadrava (info@cloveretl.com)
	 *         (c) Javlin, a.s. (www.cloveretl.com)
	 *
	 * @created Mar 31, 2011
	 */
	public static class CharRecordSkipper extends RecordSkipper {

		/**
		 * @param inputReader
		 * @param delimPatterns
		 * @param isDelimited
		 */
		public CharRecordSkipper(ICharByteInputReader inputReader, AhoCorasick delimPatterns, boolean[] isDelimited) {
			super(inputReader, delimPatterns, isDelimited);
		}

		@Override
		public boolean skipInput(int nextField) throws OperationNotSupportedException, IOException {
			delimPatterns.reset();
			inputReader.mark();
			int ichr;
			char chr;
			int currentField;

			for (currentField = nextField; currentField < numFields && !isDelimited[currentField]; currentField++)
				;

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
					chr = (char) ichr;
					delimPatterns.update(chr);
					if (delimPatterns.isPattern(RECORD_DELIMITER_IDENTIFIER)) {
						return true;
					}
					if (currentField < numFields && delimPatterns.isPattern(currentField)) {
						currentField++;
						if (currentField == numFields) {
							return true;
						}
						for (; currentField < numFields && !isDelimited[currentField]; currentField++)
							;
					}
				}
				ichr = inputReader.readChar();
			}
			// unreachable
		}
	}

	/**
	 * Record skipper for input reader that may not support char operations 
	 * @author jhadrava (info@cloveretl.com)
	 *         (c) Javlin, a.s. (www.cloveretl.com)
	 *
	 * @created Mar 31, 2011
	 */
	public static class ByteRecordSkipper extends RecordSkipper {

		/**
		 * @param inputReader
		 * @param delimPatterns
		 * @param isDelimited
		 */
		public ByteRecordSkipper(ICharByteInputReader inputReader, AhoCorasick delimPatterns, boolean[] isDelimited) {
			super(inputReader, delimPatterns, isDelimited);
		}

		@Override
		public boolean skipInput(int nextField) throws OperationNotSupportedException, IOException {
			delimPatterns.reset();
			inputReader.mark();
			int ibt;
			char bt;
			int currentField;

			for (currentField = nextField; currentField < numFields && !isDelimited[currentField]; currentField++)
				;

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
					bt = (char) ibt;
					delimPatterns.update(bt);
					if (delimPatterns.isPattern(RECORD_DELIMITER_IDENTIFIER)) {
						return true;
					}
					if (currentField < numFields && delimPatterns.isPattern(currentField)) {
						currentField++;
						if (currentField == numFields) {
							return true;
						}
						for (; currentField < numFields && !isDelimited[currentField]; currentField++)
							;
					}
				}
				ibt = inputReader.readByte();
			}
			// unreachable
		}

	}

}
