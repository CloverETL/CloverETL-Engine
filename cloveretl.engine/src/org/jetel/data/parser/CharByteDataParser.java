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
import java.nio.CharBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;

import javax.naming.OperationNotSupportedException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.reader.InputReader;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.IParserExceptionHandler;
import org.jetel.exception.JetelException;
import org.jetel.exception.PolicyType;
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
public class CharByteDataParser implements TextParser {
	TextParserConfiguration cfg;
	private final static Log logger = LogFactory.getLog(SimpleDataParser.class);

	ReadableByteChannel inputSource;
	private CharByteInputReader inputReader;
	private InputConsumer[] fieldConsumers;
	private boolean releaseInputSource;
	private int numFields;
	int numConsumers;
	Charset charset;
	IParserExceptionHandler exceptionHandler;
	PolicyType policyType;
	boolean skipLeftBlanks;
	boolean skipRightBlanks;

	public CharByteDataParser(TextParserConfiguration cfg) {
		this.cfg = cfg;
		String charsetName = cfg.getCharset(); 
		if (charsetName == null) {
			charsetName = Defaults.DataParser.DEFAULT_CHARSET_DECODER;
		}
		charset = Charset.forName(charsetName);
		policyType = cfg.getPolicyType();
		releaseInputSource = false;
		skipLeftBlanks = cfg.getSkipLeadingBlanks() != null && cfg.getSkipLeadingBlanks().booleanValue();
		skipRightBlanks = cfg.getSkipTrailingBlanks() != null && cfg.getSkipTrailingBlanks().booleanValue();
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
			return 7;
		} else if (cfg.isSingleByteCharset()) {
			return 6;
		} else {
			return 5;
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
//            	exceptionHandler.setRawRecord(getLastRawRecord());
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
//            	exceptionHandler.setRawRecord(getLastRawRecord());
                exceptionHandler.handleException();
                record = parseNext(record);
            }
        }
		return record;
	}

	public DataRecord parseNext(DataRecord record) throws JetelException {
		for (int idx = 0; idx < numConsumers; idx++) {
			try {
				if (!fieldConsumers[idx].consumeInput(record)) {
					if (idx == 0) {
						return null;
					}
					break;
				}
			} catch (OperationNotSupportedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return record;
	}

	@Override
	public int skip(int nRec) throws JetelException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void init() throws ComponentNotReadyException {

		DataRecordMetadata metadata = cfg.getMetadata();
		if (metadata == null) {
			throw new ComponentNotReadyException("Metadata are null");
		}
		
		boolean needByteInput = false;
		boolean needCharInput = false;

		numFields = metadata.getNumFields();
		
		// first we determine what kind of input reader we need
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
		boolean singleByteCharset = Math.round(charset.newEncoder().maxBytesPerChar()) == 1;

		// create input reader according to data record requirements
		if (!needCharInput) {
			inputReader = new CharByteInputReader.ByteInputReader();
		} else if (!needByteInput) {
			inputReader = new CharByteInputReader.CharInputReader(charset);
//			inputReader = new CharByteInputReader.RobustInputReader(charset);
//			inputReader = new CharByteInputReader.SingleByteCharsetInputReader(charset);
		} else if (singleByteCharset) {
			inputReader = new CharByteInputReader.SingleByteCharsetInputReader(charset);
		} else {
			inputReader = new CharByteInputReader.RobustInputReader(charset);
		}

		numConsumers = 0;
		fieldConsumers = new InputConsumer[numFields];
		for (int idx = 0; idx < numFields; numConsumers++) {
			// skip autofilling fields
			for (; idx < numFields; idx++) {
				if (!metadata.getField(idx).isAutoFilled()) {
					break;
				}
			}
			int byteFieldCount = 0;
			for (; idx + byteFieldCount < numFields; byteFieldCount++) {
				DataFieldMetadata field = metadata.getField(idx + byteFieldCount);
				if (field.isAutoFilled()) { // skip auto-filled field
					continue;
				}
				if (!field.isByteBased()) {
					break;
				}
				if (!field.isFixed()) {
					throw new ComponentNotReadyException("Byte-based values must have fixed length");
				}
			}
			if (byteFieldCount > 0) { // use FixlenFieldConsumer
				fieldConsumers[numConsumers] = new FixlenByteFieldConsumer(metadata, inputReader, idx, byteFieldCount);
				idx += byteFieldCount;
			} else { // char field consumer
				DataFieldMetadata field = metadata.getField(idx);
				if (field.isFixed()) { // fixlen char field consumer
					fieldConsumers[numConsumers] = new FixlenCharFieldConsumer(inputReader, idx, field.getSize());
				} else { // delimited char field consumer
					AhoCorasick delimPatterns = new AhoCorasick(field.getDelimiters());
					fieldConsumers[numConsumers] = new DelimCharFieldConsumer(inputReader, idx, delimPatterns,
							cfg.isTreatMultipleDelimitersAsOne(), field.isEofAsDelimiter(), cfg.isQuotedStrings(),
							field.isTrim() || skipLeftBlanks, field.isTrim() || skipRightBlanks);
				}
				idx++;
			}
		} // loop
		
	}
	

	@Override
	public void setDataSource(Object inputDataSource) throws IOException, ComponentNotReadyException {
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
		public final int ANY_PATTERN = 0;
		public final int QUOTE_PATTERN = 0;

		protected CharByteInputReader inputReader;
		
		protected InputConsumer(CharByteInputReader inputReader) {
			this.inputReader = inputReader;
		}
		
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

		private int fieldNumber;
		private int fieldLength;

		FixlenCharFieldConsumer(CharByteInputReader inputReader, int fieldNumber, int fieldLength) {
			super(inputReader);
			this.fieldNumber = fieldNumber;
			this.inputReader = inputReader;
			this.fieldLength = fieldLength;
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
						throw new BadDataFormatException("End of input encountered instead of the closing quote");
					}
				}
			}
			record.getField(fieldNumber).fromString(inputReader.getCharSequence(0));
			return true;
		}
		
	}
	
	public static class DelimCharFieldConsumer extends InputConsumer {
		private int fieldNumber;
		private QuotingDecoder qDecoder;
		private AhoCorasick delimPatterns;
		private boolean multipleDelimiters;
		private boolean acceptEofAsDelim;
		private boolean isQuoted;
		private boolean lTrim;
		private boolean rTrim;

		public DelimCharFieldConsumer(CharByteInputReader inputReader, int fieldNumber, AhoCorasick delimPatterns,
				boolean multipleDelimiters, boolean acceptEofAsDelim, boolean isQuoted,
				boolean lTrim, boolean rTrim) {
			super(inputReader);
			this.fieldNumber = fieldNumber;
			this.delimPatterns = delimPatterns;
			this.multipleDelimiters = multipleDelimiters;
			this.acceptEofAsDelim = acceptEofAsDelim;
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
						fieldValue.append(inputReader.getCharSequence(-1)); // append without quote
						field.fromString(fieldValue);
						break;
					} // end quote followed by non-quote char
				} // first end quote found
			} // value reading loop

			// consume obligatory delimiter after the closing quote
			while (!delimPatterns.isPattern(ANY_PATTERN)) {
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
					return false; // indicates success and end of input
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
							return false; // indicates success and end of input
						} else {
							throw new BadDataFormatException("End of input encountered instead of the field delimiter");						
						}
					}
					chr = (char)ichr;
					delimPatterns.update(chr);
					if (delimPatterns.isPattern(ANY_PATTERN)) { // first delimiter found
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
						}
						record.getField(fieldNumber).fromString(seq);
						delimPatterns.reset();
						inputReader.mark();
						break;
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
					return false;	// indicates success and end of input
				}
				chr = (char)ichr;
				delimPatterns.update(chr);
				if (delimPatterns.getMatchLength() == 0) {
					inputReader.revert(); // revert to the position after last delimiter
					return true;	// indicates success without end of input
				}
				if (delimPatterns.isPattern(ANY_PATTERN)) {
					inputReader.mark(); // one more delimiter consumed
				}
			}
		}
		
	} // DelimCharFieldConsumer

}
