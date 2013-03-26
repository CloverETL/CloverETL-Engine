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
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;

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
import org.jetel.metadata.DataFieldType;
import org.jetel.util.bytes.ByteCharBuffer;
import org.jetel.util.bytes.CloverBuffer;
import org.jetel.util.string.QuotingDecoder;
import org.jetel.util.string.StringUtils;

/**
 * Parsing plain text data.
 * 
 * Known bugs: 
 * - Method skip() doesn't recognize records without final delimiter/recordDelimiter,
 *   for example last record in file without termination enter.
 *   That's why skip() doesn't count unfinished records.
 *   
 * @author Martin Zatopek, David Pavlis
 * @since September 29, 2005
 * @see Parser
 * @see org.jetel.data.Defaults
 */
public class DataParser extends AbstractTextParser {
	
	private static final int RECORD_DELIMITER_IDENTIFIER = -1;
	private static final int DEFAULT_FIELD_DELIMITER_IDENTIFIER = -2;
	
	private final static Log logger = LogFactory.getLog(DataParser.class);

	private IParserExceptionHandler exceptionHandler;

	private ReadableByteChannel reader;

	private CharBuffer charBuffer;

	private ByteBuffer byteBuffer;

	//data source (setDataSource()) can be specified by an instance of ByteCharBuffer
	//- in this case the instance is persisted in this variable
	//generally data source is specified either by a ReadableByteChannel (reader) or by this ByteCharBuffer
	private ByteCharBuffer byteCharBuffer;

	private StringBuilder fieldBuffer;

	private CloverBuffer recordBuffer;

	private CharsetDecoder decoder;

	private int fieldLengths[];

	private boolean[] quotedFields;
	
	private int recordCounter;
	
	private int numFields;
	
	private AhoCorasick delimiterSearcher;
	
	private StringBuilder tempReadBuffer;
	
	private boolean[] isAutoFilling;
	
//	private boolean[] isSkipBlanks;
	private boolean[] isSkipLeadingBlanks;
	private boolean[] isSkipTrailingBlanks;

	private boolean[] eofAsDelimiters;

	private boolean hasRecordDelimiter = false;
	
	private boolean hasDefaultFieldDelimiter = false;
	
	private QuotingDecoder qDecoder = new QuotingDecoder();
	
	private boolean isEof;

	private int bytesProcessed;
	
	private DataFieldMetadata[] metadataFields;
	
	/**
	 * We are in the middle of process of record parsing.
	 */
	private boolean recordIsParsed;

	/** Indicates, whether the parser should try to find longer delimiter when a match is found. This
	 *  applies for e.g. delimiter set \r | \r\n. When this flag is false and a \r is found, parser
	 *  should take \r as a delimiter. If the flag is true, parser should look if the next char is \n and 
	 *  if so, take \r\n as delimiter. 
	 */	
	private boolean tryToFindLongerDelimiter = false;  
	
	
	
	public DataParser(TextParserConfiguration cfg){
		super(cfg);
		decoder = Charset.forName(cfg.getCharset()).newDecoder();
		reader = null;
		exceptionHandler = cfg.getExceptionHandler();
		qDecoder.setQuoteChar(cfg.getQuoteChar());
	}
	
	/**
	 * Returns parser speed for specified configuration. See {@link TextParserFactory#getParser(TextParserConfiguration)}.
	 */
	public static Integer getParserSpeed(TextParserConfiguration cfg){
		for (DataFieldMetadata field : cfg.getMetadata().getFields()) {
			if (field.isByteBased() && !field.isAutoFilled()) {
				logger.debug("Parser cannot be used for the specified data as they contain byte-based field '" + field + "'");
				return null;
			}
			if (field.getShift() != 0) {
				logger.debug("Parser cannot be used for the specified data as they contain field '" + field +  "' with non-zero shift");
				return null;
			}
		}
		return 10;
	}

	/**
	 * @see org.jetel.data.parser.Parser#getNext()
	 */
    @Override
	public DataRecord getNext() throws JetelException {
		DataRecord record = DataRecordFactory.newRecord(cfg.getMetadata());
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
	@Override
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

    @Override
	public void init() throws ComponentNotReadyException {
		//init private variables
		if (cfg.getMetadata() == null) {
			throw new ComponentNotReadyException("Metadata are null");
		}
		byteBuffer = ByteBuffer.allocateDirect(Defaults.Record.RECORD_INITIAL_SIZE);
		charBuffer = CharBuffer.allocate(Defaults.Record.RECORD_INITIAL_SIZE);
		charBuffer.flip(); // initially empty 
		fieldBuffer = new StringBuilder(Defaults.Record.FIELD_INITIAL_SIZE);
		recordBuffer = CloverBuffer.allocate(Defaults.Record.RECORD_INITIAL_SIZE, Defaults.Record.RECORD_LIMIT_SIZE);
		tempReadBuffer = new StringBuilder(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		numFields = cfg.getMetadata().getNumFields();
		isAutoFilling = new boolean[numFields];
//		isSkipBlanks = new boolean[numFields];
		isSkipLeadingBlanks = new boolean[numFields];
		isSkipTrailingBlanks = new boolean[numFields];
		eofAsDelimiters = new boolean[numFields];

		//aho-corasick initialize
		delimiterSearcher = new AhoCorasick();

		// create array of delimiters & initialize them
		String[] delimiters;
		for (int i = 0; i < numFields; i++) {
			if(cfg.getMetadata().getField(i).isDelimited()) {
				delimiters = cfg.getMetadata().getField(i).getDelimiters();
				if(delimiters != null && delimiters.length > 0) { //it is possible in case eofAsDelimiter tag is set
					for(int j = 0; j < delimiters.length; j++) {
						delimiterSearcher.addPattern(delimiters[j], i);
					}
				} else {
					delimiterSearcher.addPattern(null, i);
				}
			}
			isAutoFilling[i] = cfg.getMetadata().getField(i).getAutoFilling() != null;
			isSkipLeadingBlanks[i] = isSkipFieldLeadingBlanks(i);
//			isSkipBlanks[i] = skipLeadingBlanks
//					|| trim == Boolean.TRUE
//					|| (trim == null && metadata.getField(i).isTrim());
			isSkipTrailingBlanks[i] = isSkipFieldTrailingBlanks(i);
			eofAsDelimiters[i] = cfg.getMetadata().getField(i).isEofAsDelimiter();
		}

		//aho-corasick initialize
		if(cfg.getMetadata().isSpecifiedRecordDelimiter()) {
			hasRecordDelimiter = true;
			delimiters = cfg.getMetadata().getRecordDelimiters();
			for(int j = 0; j < delimiters.length; j++) {
				delimiterSearcher.addPattern(delimiters[j], RECORD_DELIMITER_IDENTIFIER);
			}
		}
		if(cfg.getMetadata().isSpecifiedFieldDelimiter()) {
			hasDefaultFieldDelimiter = true;
			delimiters = cfg.getMetadata().getFieldDelimiters();
			for(int j = 0; j < delimiters.length; j++) {
				delimiterSearcher.addPattern(delimiters[j], DEFAULT_FIELD_DELIMITER_IDENTIFIER);
			}
		}
		delimiterSearcher.compile();
	
		// create array of field sizes and quoting & initialize them
		fieldLengths = new int[numFields];
		quotedFields = new boolean[numFields];
		for (int i = 0; i < numFields; i++) {
			if(cfg.getMetadata().getField(i).isFixed()) {
				fieldLengths[i] = cfg.getMetadata().getField(i).getSize();
			}
			DataFieldType type = cfg.getMetadata().getDataFieldType(i);
			quotedFields[i] = cfg.isQuotedStrings() 
					&& type != DataFieldType.BYTE
					&& type != DataFieldType.CBYTE;
		}
		
		metadataFields = cfg.getMetadata().getFields();
		tryToFindLongerDelimiter = cfg.isTryToMatchLongerDelimiter();
	}

    @Override
	public void setDataSource(Object inputDataSource) throws IOException {
		if (releaseDataSource) releaseDataSource();

		decoder.reset();// reset CharsetDecoder
		byteBuffer.clear();
		byteBuffer.flip();
		charBuffer.clear();
		charBuffer.flip();
		fieldBuffer.setLength(0);
		recordBuffer.clear();
		tempReadBuffer.setLength(0);
		
		recordCounter = 0;// reset record counter
		bytesProcessed = 0;

		if (inputDataSource == null) {
			reader = null;
			isEof = true;
		} else {
			isEof = false;
			if (inputDataSource instanceof CharBuffer) {
				reader = null;
				charBuffer = (CharBuffer) inputDataSource;
			} if (inputDataSource instanceof ByteCharBuffer) {
				//data source is specified by our custom buffer implementation
				//which provides CharBuffer and a way how to re-load this buffer (byteCharBuffer.readChar())
				byteCharBuffer = (ByteCharBuffer) inputDataSource;
				charBuffer = byteCharBuffer.getCharBuffer();
			} else if (inputDataSource instanceof ReadableByteChannel) {
				reader = ((ReadableByteChannel)inputDataSource);
			} else {
				reader = Channels.newChannel((InputStream)inputDataSource);
			}
		}
	}

	/**
	 * Discard bytes for incremental reading.
	 * 
	 * @param bytes
	 * @throws IOException 
	 */
	private void discardBytes(int bytes) throws IOException {
		while (bytes > 0) {
			if (reader instanceof FileChannel) {
				((FileChannel)reader).position(bytes);
				return;
			}
			byteBuffer.clear();
			if (bytes < Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE) byteBuffer.limit(bytes);
			try {
				reader.read(byteBuffer);
			} catch (IOException e) {
				break;
			}
			bytes =- Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE;
		}
		byteBuffer.clear();
		byteBuffer.flip();
	}
	
	/**
	 * Release data source
	 * @throws IOException 
	 *
	 */
	@Override
	protected void releaseDataSource() {
		if (reader == null) {
			return;
		}
		
		try {
			reader.close();
		} catch (IOException ioe) {
			logger.warn("Failed to release data source", ioe);
		}
		
		reader = null;		
	}

	/**
	 * @see org.jetel.data.parser.Parser#close()
	 */
	@Override
	public void close() throws IOException {
		if (reader != null && reader.isOpen()) {
			reader.close();
		}
	}

	private DataRecord parseNext(DataRecord record) {
		int fieldCounter;
		int character = -1;
		int mark;
		boolean inQuote, quoteFound;
		boolean skipLBlanks, skipTBlanks;
		boolean quotedField;
		
		recordCounter++;
		if (cfg.isVerbose()) {
			recordBuffer.clear();
			
			// FIX: add the content of tempReadBuffer (holding characters, that has been read already, but should be read again) to recordBuffer (used
			// for printing the raw data in case of parse error. In case of an error, this caused that some characters were missing at the beginning 
			// of the raw data in the error message, but they were actually read by the parser
			for (int i=0; i < tempReadBuffer.length(); i++) {
				recordBuffer.putChar(tempReadBuffer.charAt(i));
			}
		}
		recordIsParsed = false;
		for (fieldCounter = 0; fieldCounter < numFields; fieldCounter++) {
			// skip all fields that are internally filled 
			if (isAutoFilling[fieldCounter]) {
				continue;
			}
			skipLBlanks = isSkipLeadingBlanks[fieldCounter];
			skipTBlanks = isSkipTrailingBlanks[fieldCounter];
			quotedField = quotedFields[fieldCounter];
			fieldBuffer.setLength(0);
			if (fieldLengths[fieldCounter] == 0) { //delimited data field
				inQuote = false;
				quoteFound = false;
				try {
					while ((character = readChar()) != -1) {
						recordIsParsed = true;
						//delimiter update
						delimiterSearcher.update((char) character);
						
						//skip leading blanks
						if (skipLBlanks && !Character.isWhitespace(character)) {
							skipLBlanks = false;
                        }

						//quotedStrings
						if (quotedField) {
							if (fieldBuffer.length() == 0 && !inQuote) { //first quote character
								if (qDecoder.isStartQuote((char) character)) {
									inQuote = true;
									skipLBlanks = isSkipLeadingBlanks[fieldCounter];
									continue;
								}
							} else {
								if (inQuote && qDecoder.isEndQuote((char) character)) { //quote character found in quoted field
									if (!quoteFound) { // do nothing, we will see if we get one more quoting character next time
										quoteFound = true;
										continue;
									} else { //we found double quotes "" - will be handled as a escape sequence for single quote
										quoteFound = false;
									}
								} else {
									if (quoteFound) {
										//final quote character for field found (no double quote) so we return the last read character back to reading stream
										//and check whether the field delimiter follows
										tempReadBuffer.append((char) character);
										if (!followFieldDelimiter(fieldCounter)) { //after ending quote can i find delimiter
											findFirstRecordDelimiter();
											return parsingErrorFound("Bad quote format", record, fieldCounter);
										}
										if (skipTBlanks) {
											StringUtils.trimTrailing(fieldBuffer);
										}
										break;
									}
								}
							}
						}

						//fieldDelimiter update
						if(!skipLBlanks) {
						    fieldBuffer.append((char) character);
						    if (fieldBuffer.length() > Defaults.Record.FIELD_LIMIT_SIZE) {
								return parsingErrorFound("Field delimiter was not found (this could be caused by insufficient field buffer size - Record.FIELD_LIMIT_SIZE=" + Defaults.Record.FIELD_LIMIT_SIZE + " - increase the constant if necessary)", record, fieldCounter);
						    }
                        }

						//test field delimiter
						if (!inQuote) {
							if (delimiterSearcher.isPattern(fieldCounter)) {
//							    fieldBuffer.setLength(fieldBuffer.length() - delimiterSearcher.getMatchLength());
								if (!skipLBlanks) {
								    fieldBuffer.setLength(Math.max(0, fieldBuffer.length() - delimiterSearcher.getMatchLength()));
                                }
								if (skipTBlanks) {
									StringUtils.trimTrailing(fieldBuffer);
								}
								
								// if we should consume the longest possible delimiter, try to stretch the match
								stretchDelimiter(fieldCounter);
								
								if(cfg.isTreatMultipleDelimitersAsOne())
									while(followFieldDelimiter(fieldCounter));

								
								delimiterSearcher.reset(); // CL-1859 Fix: We don't want prefix of some other delimiter to be already matched
								break;
							}
							//test default field delimiter 
							if(defaultFieldDelimiterFound()) {
								findFirstRecordDelimiter();
								return parsingErrorFound("Unexpected default field delimiter, probably record has too many fields.", record, fieldCounter);
							}
							//test record delimiter
							if(recordDelimiterFound()) {
								return parsingErrorFound("Unexpected record delimiter, probably record has too few fields.", record, fieldCounter);
							}

						}
					}
				} catch (Exception ex) {
					throw new RuntimeException(getErrorMessage(null, metadataFields[fieldCounter]), ex);
				}
			} else { //fixlen data field
				mark = 0;
				fieldBuffer.setLength(0);
				try {
					for(int i = 0; i < fieldLengths[fieldCounter]; i++) {
						//end of file
						if ((character = readChar()) == -1) {
							break;
						} else {
							recordIsParsed = true;
						}

						//delimiter update
						delimiterSearcher.update((char) character);

						//test record delimiter
						if(recordDelimiterFound()) {
							return parsingErrorFound("Unexpected record delimiter, probably record is too short.", record, fieldCounter);
						}

						//skip leading blanks
						if (skipLBlanks) 
							if(Character.isWhitespace(character)) continue; 
							else skipLBlanks = false;

						//keep track of trailing blanks
						if(!Character.isWhitespace(character)) {
							mark = i;
						} 
						fieldBuffer.append((char) character);
					}
					//removes tailing blanks
					if(/*skipTBlanks && */character != -1 && fieldBuffer.length() > 0) {
						fieldBuffer.setLength(fieldBuffer.length() - 
								(fieldLengths[fieldCounter] - mark - 1));
					}
					//check record delimiter presence for last field
					if(hasRecordDelimiter && fieldCounter + 1 == numFields && character != -1) {
						int followRecord = followRecordDelimiter(); 
						if(followRecord>0) { //record delimiter is not found
							return parsingErrorFound("Too many characters found", record, fieldCounter);
						}
						if(followRecord<0) { //record delimiter is not found
							return parsingErrorFound("Unexpected record delimiter, probably record is too short.", record, fieldCounter);
						}
					}

				} catch (Exception ex) {
					throw new RuntimeException(getErrorMessage(null, metadataFields[fieldCounter]), ex);
				}
			}

			// did we have EOF situation ?
			if (character == -1) {
				try {
    				if (!recordIsParsed) {
                        reader.close();
    				    return null;
                    } else {
                        //maybe the field has EOF delimiter
                        if(eofAsDelimiters[fieldCounter]) {
                            populateField(record, fieldCounter, fieldBuffer);
                            return record;
                        }
                        return parsingErrorFound("Unexpected end of file", record, fieldCounter);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (Exception e1) {
					throw new RuntimeException(getErrorMessage(null, metadataFields[fieldCounter]), e1);
                }
			}

			//populate field
			populateField(record, fieldCounter, fieldBuffer);
		}

		return record;
	}

	public int getOverStepChars() {
		return tempReadBuffer.length();
	}
	
	private DataRecord parsingErrorFound(String exceptionMessage, DataRecord record, int fieldNum) {
        if(exceptionHandler != null) {
            exceptionHandler.populateHandler("Parsing error: " + exceptionMessage, record, recordCounter, fieldNum , getLastRawRecord(), new BadDataFormatException("Parsing error: " + exceptionMessage));
            return record;
        } else {
			throw new RuntimeException("Parsing error: " + exceptionMessage + " (" + getLastRawRecord() + ")");
		}
	}
	
	private int readChar() throws IOException {
		final char character;
		final int size;
        CoderResult result;

		if(tempReadBuffer.length() > 0) { // the tempReadBuffer is used as a cache of already read characters which should be read again
			character = tempReadBuffer.charAt(0);
			tempReadBuffer.deleteCharAt(0);
			return character;
		}

        if (charBuffer.hasRemaining()) {
    		character = charBuffer.get();
    		if (cfg.isVerbose()) {
	    		try {
	    			recordBuffer.putChar(character);
	    		} catch (BufferOverflowException e) {
	    			throw new RuntimeException("Parse error: The size of data buffer for data record is only " + recordBuffer.limit() + ". Set appropriate parameter in defaultProperties file.", e);
	    		}
    		}
    		return character;
        }

        if (isEof || (reader == null && byteCharBuffer == null)) {
            return -1;
        }

        //charBuffer is populated either by ReadableByteChannel (reader) or byteCharBuffer
        if (byteCharBuffer != null) {
        	//let's byteCharBuffer loads data to charBuffer
        	byteCharBuffer.readChars();
        } else { //(reader != null) let's decode bytes from reader to charBuffer
        
	        charBuffer.clear();
	        if (byteBuffer.hasRemaining())
	        	byteBuffer.compact();
	        else
	        	byteBuffer.clear();
	
	        if ((size = reader.read(byteBuffer)) == -1) {
	            isEof = true;
	        } else {
	        	bytesProcessed += size;
	        }
	        byteBuffer.flip();
	
	        result = decoder.decode(byteBuffer, charBuffer, isEof);
	//        if (result == CoderResult.UNDERFLOW) {
	//            // try to load additional data
	//        	byteBuffer.compact();
	//
	//            if (reader.read(byteBuffer) == -1) {
	//                isEof = true;
	//            }
	//            byteBuffer.flip();
	//            decoder.decode(byteBuffer, charBuffer, isEof);
	//        } else 
	        if (result.isError()) {
	            throw new IOException(result.toString()+" when converting from "+decoder.charset());
	        }
	        if (isEof) {
	            result = decoder.flush(charBuffer);
	            if (result.isError()) {
	                throw new IOException(result.toString()+" when converting from "+decoder.charset());
	            }
	        }
	        charBuffer.flip();
        }
		
		if (charBuffer.hasRemaining()) {
			final int ret = charBuffer.get();
			if (cfg.isVerbose()) {
				try {
					recordBuffer.putChar((char) ret);
				} catch (BufferOverflowException e) {
					throw new RuntimeException("Parse error: The size of data buffer for data record is only " + recordBuffer.limit() + ". Set appropriate parameter in defaultProperties file.", e);
				}
			}
			return ret;
		} else {
			return -1;
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
	private String getErrorMessage(CharSequence value, DataFieldMetadata metadataField) {
		StringBuffer message = new StringBuffer();
		message.append("Error when parsing record #");
		message.append(recordCounter);
		if (metadataField != null) {
			message.append(" field ");
			message.append(metadataField.getName());
		}
		if (value != null) {
			message.append(" value \"").append(value).append("\"");
		}
		return message.toString();
	}

	/**
	 * Finish incomplete fields <fieldNumber, metadata.getNumFields()>.
	 * 
	 * @param record
	 *            incomplete record
	 * @param fieldNumber
	 *            first incomlete field in record
	 */
//	private void finishRecord(DataRecord record, int fieldNumber) {
//		for(int i = fieldNumber; i < metadata.getNumFields(); i++) {
//			record.getField(i).setToDefaultValue();
//		}
//	}

	/**
	 * Populate field.
	 * 
	 * @param record
	 * @param fieldNum
	 * @param data
	 */
	private final void populateField(DataRecord record, int fieldNum, StringBuilder data) {
		try {
			record.getField(fieldNum).fromString(data);
		} catch(BadDataFormatException bdfe) {
            if(exceptionHandler != null) {
                exceptionHandler.populateHandler(null, record,
						recordCounter, fieldNum , data.toString(), bdfe);
            } else {
                bdfe.setRecordNumber(recordCounter);
                bdfe.setFieldNumber(fieldNum);
                bdfe.setOffendingValue(data);
                throw bdfe;
            }
		} catch(Exception ex) {
			throw new RuntimeException(getErrorMessage(null, metadataFields[fieldNum]), ex);
		}
	}

	/**
	 * Find first record delimiter in input channel.
	 */
	private boolean findFirstRecordDelimiter() throws JetelException {
        if(!cfg.getMetadata().isSpecifiedRecordDelimiter()) {
            return false;
        }
		int character;
		try {
			while ((character = readChar()) != -1) {
				delimiterSearcher.update((char) character);
				//test record delimiter
				if (recordDelimiterFound()) {
					return true;
				}
			}
		} catch (IOException e) {
			throw new JetelException("Can not find a record delimiter.", e);
		}
		//end of file
		return false;
	}
	
	/**
	 * Find end of record for metadata without record delimiter specified.
	 * @throws JetelException 
	 */
	private boolean findEndOfRecord(int fieldNum) throws JetelException {
		int character = 0;
		try {
			for(int i = fieldNum; i < numFields; i++) {
				if (isAutoFilling[i]) continue;
				if (metadataFields[i].isDelimited()) {
					while((character = readChar()) != -1) {
						delimiterSearcher.update((char) character);
						if(delimiterSearcher.isPattern(i)) {
							stretchDelimiter(i);
							break;
						}
					}
				} else { //data field is fixlen
					for (int j = 0; j < fieldLengths[i]; j++) {
						//end of file
						if ((character = readChar()) == -1) {
							break;
						}
					}
				}
			}
		} catch (IOException e) {
			throw new JetelException("Can not find end of record.", e);
		}
		
		return (character != -1);
	}

//    private void shiftToNextRecord(int fieldNum) {
//        if(metadata.isSpecifiedRecordDelimiter()) {
//            findFirstRecordDelimiter();
//        } else {
//            findEndOfRecord(fieldNum);
//        }
//    }
	
	/**
	 * Is record delimiter in the input channel?
	 * @return
	 */
	private boolean recordDelimiterFound() {
		if (hasRecordDelimiter) {
			if (delimiterSearcher.isPattern(RECORD_DELIMITER_IDENTIFIER)) {
				//in case a record delimiter was found, lets take the longest one
				stretchDelimiter(RECORD_DELIMITER_IDENTIFIER);
				return true;
			} else {
				return false;
			}
		} else {
			return false;
		}
	}

	/**
	 * Is default field delimiter in the input channel?
	 * @return
	 */
	private boolean defaultFieldDelimiterFound() {
		if(hasDefaultFieldDelimiter) {
			return delimiterSearcher.isPattern(DEFAULT_FIELD_DELIMITER_IDENTIFIER);
		} else {
			return false;
		}
	}

	/**
	 * Follow field delimiter in the input channel?
	 * @param fieldNum field delimiter identifier
	 * @return
	 */
	StringBuffer temp = new StringBuffer();
	private boolean followFieldDelimiter(int fieldNum) {
		int character;
		
		temp.setLength(0);
		try {
			while ((character = readChar()) != -1) {
				temp.append((char) character);
				delimiterSearcher.update((char) character);
				if(delimiterSearcher.isPattern(fieldNum)) {
					//in case a record delimiter was found, lets take the longest one
					stretchDelimiter(fieldNum);
					return true;
				}
				if(delimiterSearcher.getMatchLength() == 0) {
					tempReadBuffer.append(temp);
					return false;
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(getErrorMessage(null, null), e);
		}
		//end of file
		return false;
	}
	
	/**
	 * Follow record delimiter in the input channel?
	 * @return 0 if record delimiter follows. -1 if record is too short, 1 if record is longer
	 */
	private int followRecordDelimiter() {
		int count = 1;
		int character;
		try {
			while ((character = readChar()) != -1) {
				delimiterSearcher.update((char) character);
				if (delimiterSearcher.isPattern(RECORD_DELIMITER_IDENTIFIER)) {
					int result = (count - delimiterSearcher.getMatchLength());
					//in case a record delimiter was found, lets take the longest one
					stretchDelimiter(RECORD_DELIMITER_IDENTIFIER);
					return result;
				}
				count++;
			}
		} catch (IOException e) {
			throw new RuntimeException(getErrorMessage(null, null), e);
		}
		//end of file
		return -1;
	}

	/**
	 * Tries to greedy eat the longest delimiter. Useful for alternative delimiters, specially for "\r\n\\|\n\\|\r\\|\n\r" 
	 */
	private void stretchDelimiter(int patternID) {
		if (tryToFindLongerDelimiter && delimiterSearcher.canUpdateWithoutFail()) {
			StringBuffer temp = new StringBuffer();
			try {
				int character;
				while (delimiterSearcher.canUpdateWithoutFail() && (character = readChar()) != -1) {
					// always append a character to a temp buffer
					temp.append((char)character);
					
					// if fail occurs - the string can no more be stretched to a pattern:
					// just finish stretching and update temp read buffer
					if (!delimiterSearcher.update((char) character)) {
						break;
					}
					
					// we have moved in the tree - do we have a node, that is final?
					// if yes, clear the buffer - this is the stretched delimiter. 
					if (delimiterSearcher.isPattern(patternID)) {
						temp.setLength(0);
					}
				}
			} catch (IOException e) {
				throw new RuntimeException(getErrorMessage(null, null), e);
			} finally {
				//end of file or longest delimiter found
				tempReadBuffer.append(temp);
			}
		}
	}
	
	
	public boolean endOfInputChannel() {
		return reader == null || !reader.isOpen();
	}
	
	public int getRecordCount() {
		return recordCounter;
	}
	
	public String getLastRawRecord() {
		if (cfg.isVerbose()) {
			recordBuffer.flip();
			String lastRawRecord = recordBuffer.asCharBuffer().toString();
			recordBuffer.position(recordBuffer.limit()); //it is necessary for repeated invocation of this method for the same record
			return lastRawRecord;
		} else {
			return "<Raw record data is not available, please turn on verbose mode.>";
		}
	}
	
	
//	/**
//	 * Skip first line/record in input channel.
//	 */
//	public void skipFirstLine() {
//		int character;
//		
//		try {
//			while ((character = readChar()) != -1) {
//				delimiterSearcher.update((char) character);
//				if(delimiterSearcher.isPattern(-2)) {
//					break;
//				}
//			}
//			if(character == -1) {
//				throw new RuntimeException("Skipping first line: record delimiter not found.");
//			}
//		} catch (IOException e) {
//			throw new RuntimeException(getErrorMessage(e.getMessage(),	null, -1));
//		}		
//	}
	
    @Override
	public int skip(int count) throws JetelException {
        int skipped;

        if(cfg.getMetadata().isSpecifiedRecordDelimiter()) {
			for(skipped = 0; skipped < count; skipped++) {
				if(!findFirstRecordDelimiter()) {
				    break;
                }
				recordBuffer.clear();
			}
		} else {
			for(skipped = 0; skipped < count; skipped++) {
				if(!findEndOfRecord(0)) {
				    break;
                }
				recordBuffer.clear();
			}
		}
        
		return skipped;
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
        if(exceptionHandler != null) {
            return exceptionHandler.getType();
        }
        return null;
    }

    @Override
	public void reset() {
		if (releaseDataSource) {	
			releaseDataSource();
		}
		decoder.reset();// reset CharsetDecoder
		recordCounter = 0;// reset record counter
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

	@Override
    public void preExecute() throws ComponentNotReadyException {
    }
    
	@Override
    public void postExecute() throws ComponentNotReadyException {    	
       	reset();
    }
    
	@Override
    public void free() throws IOException {
		close();
    }

}
