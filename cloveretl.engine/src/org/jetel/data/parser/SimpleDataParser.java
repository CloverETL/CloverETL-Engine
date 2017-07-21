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
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.CharBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.CharacterCodingException;
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
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.string.StringUtils;

/**
 * This parser is as simple as posible - limited validation, error handling, functionality - but very fast. List of
 * limitation:
 * <ul>
 * <li>trim, skipLeadingBlanks and skipTraillingBlanks is omitted</li>
 * <li>only delimited output metadata are accepted</li>
 * <li>skip method is not supported</li>
 * <li>all output metadata fields has to have first character occurence ony ones</li>
 * <li>movePosition() method for incremental reading is not supported</li>
 * </ul>
 * 
 * @author Cyril Sochor (info@javlin.eu)
 * @author Martin Zatopek (info@javlin.eu)
 * 
 * @created 19.8.2009
 */
public class SimpleDataParser extends AbstractTextParser {

	private IParserExceptionHandler exceptionHandler;
	private int numFields;
	private char[][] delimiters;
	private Reader reader;

	private int recordCounter;

	private StringBuilder fieldBuffer = null;
	private char cb[];
	private int nChars;
	private int nextChar;
	private boolean isEOF;

	private boolean[] isSkipLeadingBlanks;
	private boolean[] isSkipTrailingBlanks;
	
	private final static Log logger = LogFactory.getLog(SimpleDataParser.class);

	public SimpleDataParser(TextParserConfiguration cfg) {
		super(cfg);
		exceptionHandler = cfg.getExceptionHandler();
	}

	/**
	 * Returns parser speed for specified configuration. See {@link TextParserFactory#getParser(TextParserConfiguration)}.
	 */
	public static Integer getParserSpeed(TextParserConfiguration cfg) {
		if (cfg.isVerbose()) {
			logger.debug("This parser can't be used because 'verbose' feature");
		} else if (cfg.isQuotedStringsOverride() ? cfg.isQuotedStrings() : cfg.getMetadata().isQuotedStrings()) {
			logger.debug("This parser can't be used because of the 'quotedStrings' feature");
		} else if (cfg.isTreatMultipleDelimitersAsOne()) {
			logger.debug("This parser can't be used because of the 'treatMultipleDelimitersAsOne' feature");
		} else if (cfg.isSkipRows()) {
			logger.debug("This parser can't be used because of the 'skipRows' feature");
		} else if (!isSimpleData(cfg.getMetadata())) {
			logger.debug("This parser can't be used because of the data record complexity");
		} else if (cfg.isTryToMatchLongerDelimiter()) {
			logger.debug("This parser can't be used because of the delimiter complexity");
		} else {
			logger.debug("This parser may be used");
			return 80;
		}
		return null;
	}

	static private boolean isSimpleData(DataRecordMetadata metadata) {
		for (DataFieldMetadata field : metadata.getFields()) {
			if (field.getShift() != 0) {
				logger.debug("Field " + field + " has non-zero shift");
				return false;
			}
			if (!field.isDelimited()) {
				logger.debug("Field " + field + " is not delimited");
				return false;
			}
			final String[] fieldDelimiters = field.getDelimiters();
			if( fieldDelimiters == null ){
				logger.debug("Field " + field + " has no delimiter");
				return false;
			}
			if (fieldDelimiters.length > 1) {
				logger.debug("Field " + field + " has multiple delimiters");
				return false;
			}
			final char[] delimiter = fieldDelimiters[0].toCharArray();
			// first char of delimiter cannot be in another location in
			if (delimiter.length > 1) {
				for (int y = 1; y < delimiter.length; y++) {
					if (delimiter[y] == delimiter[0]) {
						logger.debug("SimpleDataParser found invalid delimiter '"
								+ new String(delimiter) + "'. First char '"
								+ delimiter[0] + "' is found on position " + y);
						return false;
					}
				}
			}
			if (field.isAutoFilled()) {
				logger.debug("Field " + field + " has 'autoFill' feature");
				return false;
			}
			if (field.isByteBased()) {
				logger.debug("Field " + field + " is byte-based");
				return false;
			}
		}

		return true;
	}

	@Override
	public DataRecord getNext() throws JetelException {
		DataRecord record = DataRecordFactory.newRecord(cfg.getMetadata());

		return getNext(record);
	}

	/**
	 * @see org.jetel.data.parser.Parser#getNext(org.jetel.data.DataRecord)
	 */
	@Override
	public DataRecord getNext(DataRecord record) throws JetelException {
		record = parseNext(record);
		if (exceptionHandler != null) { // use handler only if configured
			while (exceptionHandler.isExceptionThrowed()) {
				if (exceptionHandler.getRecordNumber() > -1) {
					exceptionHandler.setRawRecord("SimpleDataParser does not provide raw record.");
				}
				exceptionHandler.handleException();
				record = parseNext(record);
			}
		}
		return record;
	}

	@Override
	public void init() throws ComponentNotReadyException {
		if (cfg.getMetadata() == null) {
			throw new ComponentNotReadyException("Metadata are null");
		}
		for (DataFieldMetadata field : cfg.getMetadata()) {
			if (!field.isDelimited()) {
				throw new ComponentNotReadyException("Fixed length fields not supported");
			}
		}
		numFields = cfg.getMetadata().getNumFields();
		cb = new char[8192];
		isEOF = false;
		fieldBuffer = new StringBuilder(Defaults.Record.FIELD_INITIAL_SIZE);

		// create array of delimiters & initialize them
		delimiters = new char[numFields][];
		isSkipLeadingBlanks = new boolean[numFields];
		isSkipTrailingBlanks = new boolean[numFields];
		for (int i =0; i< numFields; i++) {
			final DataFieldMetadata field = cfg.getMetadata().getField(i);
			delimiters[i] = field.getDelimiters()[0].toCharArray();
			isSkipLeadingBlanks[i] = isSkipFieldLeadingBlanks(i);
			isSkipTrailingBlanks[i] = isSkipFieldTrailingBlanks(i);
		}
		
		
	}

	@Override
	public void setDataSource(Object inputDataSource) {
		if (releaseDataSource)
			releaseDataSource();

		recordCounter = 0;// reset record counter
		// bytesCounter = 0;
		isEOF = false;

		CharsetDecoder decoder = createCharsetDecoder();
		
		if (inputDataSource == null) {
			reader = null;
		} else if (inputDataSource instanceof CharBuffer) {
			throw new UnsupportedOperationException("NOT IMPLEMENTED");
		} else if (inputDataSource instanceof ReadableByteChannel) {
			reader = Channels.newReader((ReadableByteChannel) inputDataSource, decoder, -1);
		} else {
			reader = new InputStreamReader((InputStream) inputDataSource, decoder);
		}
	}

	/**
	 * Release data source
	 * 
	 */
	@Override
	protected void releaseDataSource() {
		if (reader == null) {
			return;
		}
		try {
			reader.close();
		} catch (IOException e) {
			logger.warn("Failed to release data source", e);
		}
		reader = null;
	}

	@Override
	public void close() {
		if (reader != null) {
			try {
				reader.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
	}

	private DataRecord parseNext(DataRecord record) {
		if (isEOF) {
			return null;
		}
		recordCounter++;

		for (int fieldIndex = 0; fieldIndex < numFields; fieldIndex++) {
			CharSequence fieldCharacters = readField(record, fieldIndex);
			if (fieldCharacters != null) {
				populateField(record, fieldIndex, fieldCharacters);
				if (isEOF) {
					resetMissingFields(fieldIndex + 1, record);
					return record;
				}
			} else {
				if (isEOF) {
					return null;
				} else {
					// parsing error
					resetMissingFields(fieldIndex + 1, record);
					return record;
				}
			}
		}

		return record;
	}

	private void resetMissingFields(int fieldIndex, DataRecord record) {
		for (int i = fieldIndex; i < numFields; i++) {
			record.getField(i).reset();
		}
	}

	/**
	 * Readed string is stored in the fieldBuffer.
	 * 
	 * @param record
	 * @param fieldIndex
	 * @return
	 */
	private CharSequence readField(DataRecord record, int fieldIndex) {
		fieldBuffer.setLength(0);
		final char[] delimiter = delimiters[fieldIndex];

		try {

			int startChar;
			int delimiterIndex = 0;
			boolean delimiterSplit = false;

			for (;;) {

				if (nextChar >= nChars) {
					fill();
				}
				if (nextChar >= nChars) { /* EOF */
					isEOF = true;
					if (fieldBuffer.length() == 0 && fieldIndex == 0) {
						return null;
					} else if (cfg.getMetadata().getField(fieldIndex).isEofAsDelimiter()) {
						finalFieldDecoration(fieldIndex);
						return fieldBuffer;
					} else {
						parsingErrorFound("Unexpected end of file", record, fieldIndex);
						return null;
					}
				}

				int i;
				charLoop: for (i = nextChar; i < nChars; i++) {
					char c = cb[i];
					if (c == delimiter[delimiterIndex]) {
						delimiterIndex++;
						if (delimiterIndex == delimiter.length) {
							break charLoop;
						}
					} else {
						if (delimiterSplit) {
							delimiterSplit = false;
							fieldBuffer.append(delimiter, 0, delimiterIndex);
							nextChar = i;
						}
						if (c == delimiter[0]) {
							delimiterIndex = 1;
						} else {
							delimiterIndex = 0;
						}
					}
				}

				startChar = nextChar;
				nextChar = i;
				if (delimiterIndex == delimiter.length) {
					nextChar++;
					int len = nextChar - startChar - delimiterIndex;
					if (len > 0) {
						fieldBuffer.append(cb, startChar, len);
					}
					finalFieldDecoration(fieldIndex);
					return fieldBuffer;
				} else {
					int len = nextChar - startChar - delimiterIndex;
					if (len > 0) {
						fieldBuffer.append(cb, startChar, len);
					}
					if (delimiterIndex > 0) {
						delimiterSplit = true;
					}
				}

				if (fieldBuffer.length() > Defaults.Record.RECORD_LIMIT_SIZE) {
					parsingErrorFound("Field delimiter was not found", record, fieldIndex);
					return null;
				}
			}
		} catch (Exception ex) {
			throw new RuntimeException(getErrorMessage(fieldBuffer, fieldIndex), ex);
		}
	}

	private void finalFieldDecoration(int fieldIndex) {
		if( isSkipLeadingBlanks[fieldIndex] ){
			StringUtils.trimLeading(fieldBuffer);
		}
		if( isSkipTrailingBlanks[fieldIndex] ){
			StringUtils.trimTrailing(fieldBuffer);
		}
	}

	private void fill() throws IOException {
		try {
			int dst = 0;
			int n;
			do {
				n = reader.read(cb, dst, cb.length - dst);
			} while (n == 0);
			if (n > 0) {
				nChars = dst + n;
				nextChar = dst;
			}
		} catch (CharacterCodingException e) {
			isEOF = true;
			throw new CharsetDecoderException("Character decoding error occurred. Set correct charset." + (!StringUtils.isEmpty(cfg.getCharset()) ? " Current charset is " + cfg.getCharset() : ""));
		}
	}

	private void parsingErrorFound(String exceptionMessage, DataRecord record, int fieldNum) {
		if (exceptionHandler != null) {
			exceptionHandler.populateHandler("Parsing error: " + exceptionMessage, record, recordCounter, fieldNum,
					null, new BadDataFormatException("Parsing error: " + exceptionMessage));
		} else {
			throw new RuntimeException("Parsing error: " + exceptionMessage);
		}
	}

	/**
	 * Assembles error message when exception occures during parsing
	 * 
	 * @param exceptionMessage
	 *            message from exception getMessage() call
	 * @param recNo
	 *            recordNumber
	 * @param fieldNo
	 *            fieldNumber
	 * @return error message
	 * @since September 19, 2002
	 */
	private String getErrorMessage(CharSequence value, int field) {
		StringBuffer message = new StringBuffer();
		message.append("Error when parsing record #");
		message.append(recordCounter);
		if (field != -1) {
			message.append(" field ");
			message.append(cfg.getMetadata().getField(field).getName());
		}
		if (value != null) {
			message.append(" value \"").append(value).append("\"");
		}
		return message.toString();
	}

	/**
	 * Populate field.
	 * 
	 * @param record
	 * @param fieldIndex
	 * @param data
	 */
	private final void populateField(DataRecord record, int fieldIndex, CharSequence data) {
		try {
			record.getField(fieldIndex).fromString(data);
		} catch (BadDataFormatException bdfe) {
			if (exceptionHandler != null) {
				exceptionHandler.populateHandler(null, record,
						recordCounter, fieldIndex, data.toString(), bdfe);
			} else {
				bdfe.setRecordNumber(recordCounter);
				bdfe.setFieldNumber(fieldIndex);
				bdfe.setOffendingValue(data);
				throw bdfe;
			}
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	public int getRecordCount() {
		return recordCounter;
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
		if (exceptionHandler != null) {
			return exceptionHandler.getType();
		}
		return null;
	}

	@Override
	public void reset() {
		if (releaseDataSource)
			releaseDataSource();
		recordCounter = 0;// reset record counter
	}

	@Override
	public Object getPosition() {
		return 0;
	}

	@Override
	public void movePosition(Object position) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public int skip(int rec) throws JetelException {
		throw new UnsupportedOperationException();
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

	/**
	 * @return the cfg
	 */
	@Override
	public TextParserConfiguration getConfiguration() {
		return cfg;
	}

	
	
}
