/*
 *    jETeL/Clover - Java based ETL application framework.
 *    Copyright (C) 2002-04  David Pavlis <david_pavlis@hotmail.com>
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

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
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
 * @revision $Revision: 1.9 $
 */
public class DataParser implements Parser {
	
	private static final int RECORD_DELIMITER_IDENTIFIER = -1;
	private static final int DEFAULT_FIELD_DELIMITER_IDENTIFIER = -2;
	
	private IParserExceptionHandler exceptionHandler;

	private DataRecordMetadata metadata;

	private ReadableByteChannel reader;

	private CharBuffer charBuffer;

	private ByteBuffer byteBuffer;

	private StringBuilder fieldBuffer;

	private CharBuffer recordBuffer;

	private CharsetDecoder decoder;

	private int fieldLengths[];

	private int recordCounter;
	
	private int numFields;
	
	private AhoCorasick delimiterSearcher;
	
	private boolean quotedStrings = false;

	private StringBuilder tempReadBuffer;
	
	private boolean treatMultipleDelimitersAsOne = false;
	
	private Boolean trim = null; 
	private Boolean skipLeadingBlanks = null;
	private Boolean skipTrailingBlanks = null;
	
	private boolean[] isAutoFilling;
	
//	private boolean[] isSkipBlanks;
	private boolean[] isSkipLeadingBlanks;
	private boolean[] isSkipTrailingBlanks;

	private boolean[] eofAsDelimiters;

	private boolean releaseInputSource = true;
	
	private boolean hasRecordDelimiter = false;
	
	private boolean hasDefaultFieldDelimiter = false;
	
	private QuotingDecoder qDecoder = new QuotingDecoder();
	
	private boolean isEof;

	private int bytesProcessed;
	
	public DataParser() {
		decoder = Charset.forName(Defaults.DataParser.DEFAULT_CHARSET_DECODER).newDecoder();
		reader = null;
	}
	
	public DataParser(String charset) {
		decoder = Charset.forName(charset).newDecoder();
		reader = null;
	}
	
	/**
	 * @see org.jetel.data.parser.Parser#getNext()
	 */
	public DataRecord getNext() throws JetelException {
		DataRecord record = new DataRecord(metadata);
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

	/* (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#init(org.jetel.metadata.DataRecordMetadata)
	 */
	public void init(DataRecordMetadata metadata) throws ComponentNotReadyException {
		//init private variables
		if (metadata == null) {
			throw new ComponentNotReadyException("Metadata are null");
		}
		byteBuffer = ByteBuffer.allocateDirect(Defaults.Record.MAX_RECORD_SIZE);
		charBuffer = CharBuffer.allocate(Defaults.Record.MAX_RECORD_SIZE);
		charBuffer.flip(); // initially empty 
		fieldBuffer = new StringBuilder(Defaults.DataParser.FIELD_BUFFER_LENGTH);
		recordBuffer = CharBuffer.allocate(Defaults.Record.MAX_RECORD_SIZE);
		tempReadBuffer = new StringBuilder(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		numFields = metadata.getNumFields();
		isAutoFilling = new boolean[numFields];
//		isSkipBlanks = new boolean[numFields];
		isSkipLeadingBlanks = new boolean[numFields];
		isSkipTrailingBlanks = new boolean[numFields];
		eofAsDelimiters = new boolean[numFields];

		//save metadata
		this.metadata = metadata;

		//aho-corasick initialize
		delimiterSearcher = new AhoCorasick();

		// create array of delimiters & initialize them
		String[] delimiters;
		for (int i = 0; i < numFields; i++) {
			if(metadata.getField(i).isDelimited()) {
				delimiters = metadata.getField(i).getDelimiters();
				if(delimiters != null && delimiters.length > 0) { //it is possible in case eofAsDelimiter tag is set
					for(int j = 0; j < delimiters.length; j++) {
						delimiterSearcher.addPattern(delimiters[j], i);
					}
				} else {
					delimiterSearcher.addPattern(null, i);
				}
			}
			isAutoFilling[i] = metadata.getField(i).getAutoFilling() != null;
			isSkipLeadingBlanks[i] = skipLeadingBlanks != null ? skipLeadingBlanks : trim != null ? trim : metadata.getField(i).isTrim();
//			isSkipBlanks[i] = skipLeadingBlanks
//					|| trim == Boolean.TRUE
//					|| (trim == null && metadata.getField(i).isTrim());
			isSkipTrailingBlanks[i] = skipTrailingBlanks != null ? skipTrailingBlanks : trim != null ? trim : metadata.getField(i).isTrim();
			eofAsDelimiters[i] = metadata.getField(i).isEofAsDelimiter();
		}

		//aho-corasick initialize
		if(metadata.isSpecifiedRecordDelimiter()) {
			hasRecordDelimiter = true;
			delimiters = metadata.getRecordDelimiters();
			for(int j = 0; j < delimiters.length; j++) {
				delimiterSearcher.addPattern(delimiters[j], RECORD_DELIMITER_IDENTIFIER);
			}
		}
		if(metadata.isSpecifiedFieldDelimiter()) {
			hasDefaultFieldDelimiter = true;
			delimiters = metadata.getFieldDelimiters();
			for(int j = 0; j < delimiters.length; j++) {
				delimiterSearcher.addPattern(delimiters[j], DEFAULT_FIELD_DELIMITER_IDENTIFIER);
			}
		}
		delimiterSearcher.compile();
	
		// create array of field sizes & initialize them
		fieldLengths = new int[numFields];
		for (int i = 0; i < numFields; i++) {
			if(metadata.getField(i).isFixed()) {
				fieldLengths[i] = metadata.getField(i).getSize();
			}
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#setDataSource(java.lang.Object)
	 */
	public void setReleaseDataSource(boolean releaseInputSource)  {
		this.releaseInputSource = releaseInputSource;
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#setDataSource(java.lang.Object)
	 */
	public void setDataSource(Object inputDataSource) {
		if (releaseInputSource) releaseDataSource();

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
	 *
	 */
	private void releaseDataSource() {
		if (reader == null) {
			return;
		}
		try {
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		reader = null;		
	}

	/**
	 * @see org.jetel.data.parser.Parser#close()
	 */
	public void close() {
		if(reader != null && reader.isOpen()) {
			try {
				reader.close();
			} catch(IOException ex) {
				ex.printStackTrace();
			}
		}
	}

	private DataRecord parseNext(DataRecord record) {
		int fieldCounter;
		int character = -1;
		int mark;
		boolean inQuote;
		boolean skipLBlanks, skipTBlanks;
		char type;
		
		recordCounter++;
		recordBuffer.clear();
		for (fieldCounter = 0; fieldCounter < numFields; fieldCounter++) {
			// skip all fields that are internally filled 
			if (isAutoFilling[fieldCounter]) {
				continue;
			}
			skipLBlanks = isSkipLeadingBlanks[fieldCounter];
			skipTBlanks = isSkipTrailingBlanks[fieldCounter];
			if (metadata.getField(fieldCounter).isDelimited()) { //delimited data field
				// field
				// read data till we reach field delimiter, record delimiter,
				// end of file or exceed buffer size
				// exceeded buffer is indicated by BufferOverflowException
				fieldBuffer.setLength(0);
				inQuote = false;
				try {
					while ((character = readChar()) != -1) {
						//end of file
						if (character == -1) {
							break;
						}
						//delimiter update
						delimiterSearcher.update((char) character);
						
						//skip leading blanks
						if (skipLBlanks && !Character.isWhitespace(character)) {
							skipLBlanks = false;
                        }

						//quotedStrings
						type = metadata.getField(fieldCounter).getType();
						if (quotedStrings && type != DataFieldMetadata.BYTE_FIELD
								&& type != DataFieldMetadata.BYTE_FIELD_COMPRESSED){
							if (fieldBuffer.length() == 0 && !inQuote) {
								if (qDecoder.isStartQuote((char) character)) {
									inQuote = true;
									continue;
								}
							} else {
								if (inQuote && qDecoder.isEndQuote((char) character)) {
									if (!followFieldDelimiter(fieldCounter)) { //after ending quote can i find delimiter
										findFirstRecordDelimiter();
										return parsingErrorFound("Bad quote format", record, fieldCounter);
									}
									break;
								}
							}
						}

						//fieldDelimiter update
						if(!skipLBlanks) {
						    fieldBuffer.append((char) character);
                        }

						//test field delimiter
						if (!inQuote) {
							if(delimiterSearcher.isPattern(fieldCounter)) {
//							    fieldBuffer.setLength(fieldBuffer.length() - delimiterSearcher.getMatchLength());
								if(!skipLBlanks) {
								    fieldBuffer.setLength(Math.max(0, fieldBuffer.length() - delimiterSearcher.getMatchLength()));
                                }
								if (skipTBlanks) {
									StringUtils.trimTrailing(fieldBuffer);
								}
								if(treatMultipleDelimitersAsOne)
									while(followFieldDelimiter(fieldCounter));
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
					throw new RuntimeException(getErrorMessage(ex.getMessage(),	null, fieldCounter), ex);
				}
			} else { //fixlen data field
				mark = 0;
				fieldBuffer.setLength(0);
				try {
					for(int i = 0; i < fieldLengths[fieldCounter]; i++) {
						//end of file
						if ((character = readChar()) == -1) {
							break;
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
					if(hasRecordDelimiter && fieldCounter + 1 == numFields) {
						if(!followRecordDelimiter()) { //record delimiter is not found
							return parsingErrorFound("Too many characters found", record, fieldCounter);
						}
					}

				} catch (Exception ex) {
//					throw new RuntimeException(getErrorMessage(ex.getMessage(),	null, fieldCounter));
					throw new BadDataFormatException(ex.toString());
				}
			}

			// did we have EOF situation ?
			if (character == -1) {
				try {
    				if(recordBuffer.position() == 0) {
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
                }
			}

			//populate field
			populateField(record, fieldCounter, fieldBuffer);
		}

		return record;
	}

	private DataRecord parsingErrorFound(String exceptionMessage, DataRecord record, int fieldNum) {
        if(exceptionHandler != null) {
            exceptionHandler.populateHandler("Parsing error: " + exceptionMessage, record, recordCounter, fieldNum , null, new BadDataFormatException("Parsing error: " + exceptionMessage));
            return record;
        } else {
			throw new RuntimeException("Parsing error: " + exceptionMessage + " when parsing record #" + recordCounter + " and " + (fieldNum + 1) + ". field (" + recordBuffer.toString() + ")");
		}
	}
	
	private int readChar() throws IOException {
		char character;
		int size;
        CoderResult result;

		if(tempReadBuffer.length() > 0) {
			character = tempReadBuffer.charAt(0);
			tempReadBuffer.deleteCharAt(0);
			return character;
		}

        if (charBuffer.hasRemaining()) {
    		character = charBuffer.get();
    		try {
    			recordBuffer.put(character);
    		} catch (BufferOverflowException e) {
    			throw new RuntimeException("Parse error: The size of data buffer for data record is only " + recordBuffer.limit() + ". Set appropriate parameter in defautProperties file.", e);
    		}
    
    		return character;
        }

        if (isEof || reader == null)
            return -1;

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
        int ret = charBuffer.hasRemaining() ? charBuffer.get() : -1;
        
        if (ret > -1) {
			try {
				recordBuffer.put((char) ret);
			} catch (BufferOverflowException e) {
				throw new RuntimeException("Parse error: The size of data buffer for data record is only " + recordBuffer.limit() + ". Set appropriate parameter in defautProperties file.", e);
			}
        }
		return ret;
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
	private String getErrorMessage(String exceptionMessage, CharSequence value, int fieldNo) {
		StringBuffer message = new StringBuffer();
		message.append(exceptionMessage);
		message.append(" when parsing record #");
		message.append(recordCounter);
		message.append(" field ");
		message.append(metadata.getField(fieldNo).getName());
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
	private void populateField(DataRecord record, int fieldNum,	StringBuilder data) {
		try {
			record.getField(fieldNum).fromString(data);
		} catch(BadDataFormatException bdfe) {
            if(exceptionHandler != null) {
                exceptionHandler.populateHandler(bdfe.getMessage(), record,
						recordCounter, fieldNum , data.toString(), bdfe);
            } else {
                bdfe.setRecordNumber(recordCounter);
                bdfe.setFieldNumber(fieldNum);
                bdfe.setOffendingValue(data);
                throw bdfe;
            }
		} catch(Exception ex) {
			throw new RuntimeException(getErrorMessage(ex.getMessage(), null, fieldNum));
		}
	}

	/**
	 * Find first record delimiter in input channel.
	 */
	private boolean findFirstRecordDelimiter() throws JetelException {
        if(!metadata.isSpecifiedRecordDelimiter()) {
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
			for(int i = fieldNum + 1; i < numFields; i++) {
				if (isAutoFilling[i]) continue;
				while((character = readChar()) != -1) {
					delimiterSearcher.update((char) character);
					if(delimiterSearcher.isPattern(i)) {
						break;
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
		if(hasRecordDelimiter) {
			return delimiterSearcher.isPattern(RECORD_DELIMITER_IDENTIFIER);
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
					return true;
				}
				if(delimiterSearcher.getMatchLength() == 0) {
					tempReadBuffer.append(temp);
					return false;
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(getErrorMessage(e.getMessage(), null, -1));
		}
		//end of file
		return false;
	}
	
	/**
	 * Follow record delimiter in the input channel?
	 * @return
	 */
	private boolean followRecordDelimiter() {
		int count = 1;
		int character;
		try {
			while ((character = readChar()) != -1) {
				delimiterSearcher.update((char) character);
				if(recordDelimiterFound()) {
					return (count == delimiterSearcher.getMatchLength());
				}
				count++;
			}
		} catch (IOException e) {
			throw new RuntimeException(getErrorMessage(e.getMessage(), null, -1));
		}
		//end of file
		return false;
	}

	/**
	 * Specifies whether leading blanks at each field should be skipped
	 * @param skippingLeadingBlanks The skippingLeadingBlanks to set.
	 */
	public void setSkipLeadingBlanks(Boolean skipLeadingBlanks) {
		this.skipLeadingBlanks = skipLeadingBlanks;
	}

	public void setSkipTrailingBlanks(Boolean skipTrailingBlanks) {
		this.skipTrailingBlanks = skipTrailingBlanks;
	}

	public String getCharsetName() {
		return decoder.charset().name();
	}
	
	public boolean endOfInputChannel() {
		return reader == null || !reader.isOpen();
	}
	
	public int getRecordCount() {
		return recordCounter;
	}
	
	public String getLastRawRecord() {
        recordBuffer.flip();
		return recordBuffer.toString();
	}
	
	public void setQuotedStrings(boolean quotedStrings) {
		this.quotedStrings = quotedStrings;
	}
	
	public void setTreatMultipleDelimitersAsOne(boolean treatMultipleDelimitersAsOne) {
		this.treatMultipleDelimitersAsOne = treatMultipleDelimitersAsOne;
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
	
	/* (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#skip(int)
	 */
	public int skip(int count) throws JetelException {
        int skipped;

        if(metadata.isSpecifiedRecordDelimiter()) {
			for(skipped = 0; skipped < count; skipped++) {
				if(!findFirstRecordDelimiter()) {
				    break;
                }
			}
		} else {
			for(skipped = 0; skipped < count; skipped++) {
				if(!findEndOfRecord(0)) {
				    break;
                }
			}
		}
        
		return skipped;
	}

    public void setExceptionHandler(IParserExceptionHandler handler) {
        this.exceptionHandler = handler;
    }

    public IParserExceptionHandler getExceptionHandler() {
        return exceptionHandler;
    }

    public PolicyType getPolicyType() {
        if(exceptionHandler != null) {
            return exceptionHandler.getType();
        }
        return null;
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
	public void reset() {
		if (releaseInputSource)	
			releaseDataSource();
		decoder.reset();// reset CharsetDecoder
		recordCounter = 0;// reset record counter
		bytesProcessed = 0;
	}

	public Object getPosition() {
		return bytesProcessed;
	}

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

	public Boolean getSkipLeadingBlanks() {
		return skipLeadingBlanks;
	}

	public Boolean getSkipTrailingBlanks() {
		return skipTrailingBlanks;
	}

}
