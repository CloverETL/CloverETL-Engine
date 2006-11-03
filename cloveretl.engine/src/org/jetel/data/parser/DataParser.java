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
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.Channels;
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
	
	private StringBuffer logString;
	
	private AhoCorasick delimiterSearcher;

	private boolean skipLeadingBlanks = true;
	
	private boolean quotedStrings = false;

	private StringBuilder tempReadBuffer;
	
	private boolean treatMultipleDelimitersAsOne = false;
	
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
		byteBuffer = ByteBuffer.allocateDirect(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		charBuffer = CharBuffer.allocate(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		charBuffer.flip(); // initially empty 
		fieldBuffer = new StringBuilder(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		recordBuffer = CharBuffer.allocate(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		tempReadBuffer = new StringBuilder(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		logString = new StringBuffer(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);

		//save metadata
		this.metadata = metadata;

		//aho-corasick initialize
		delimiterSearcher = new AhoCorasick();

		// create array of delimiters & initialize them
		String[] delimiters;
		for (int i = 0; i < metadata.getNumFields(); i++) {
			if(metadata.getField(i).isDelimited()) {
				delimiters = metadata.getField(i).getDelimiters();
				for(int j = 0; j < delimiters.length; j++) {
					delimiterSearcher.addPattern(delimiters[j], i);
				}
			}
		}

		//aho-corasick initialize
		if(metadata.isSpecifiedRecordDelimiter()) {
			delimiters = metadata.getRecordDelimiters();
			for(int j = 0; j < delimiters.length; j++) {
				delimiterSearcher.addPattern(delimiters[j], -1);
				delimiterSearcher.addPattern(delimiters[j], -2); //separator for skipping first line
			}
		} else {
            if(metadata.getField(metadata.getFields().length - 1).isDelimited()) {
    			delimiters = metadata.getField(metadata.getFields().length - 1).getDelimiters();
    			for(int j = 0; j < delimiters.length; j++) {
    			    delimiterSearcher.addPattern(delimiters[j], -2); //separator for skipping first line
    			}
            } else {
                delimiterSearcher.addPattern(System.getProperty("line.separator"), -2); //separator for skipping first line
            }
		}
		delimiterSearcher.compile();
	
		// create array of field sizes & initialize them
		fieldLengths = new int[metadata.getNumFields()];
		for (int i = 0; i < metadata.getNumFields(); i++) {
			if(metadata.getField(i).isFixed()) {
				fieldLengths[i] = metadata.getField(i).getSize();
			}
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#setDataSource(java.lang.Object)
	 */
	public void setDataSource(Object inputDataSource) {
		releaseDataSource();

		fieldBuffer = new StringBuilder(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		recordBuffer = CharBuffer.allocate(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		tempReadBuffer = new StringBuilder(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		logString = new StringBuffer(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);

		decoder.reset();// reset CharsetDecoder
		byteBuffer.clear();
		charBuffer.clear();
		charBuffer.flip();
		fieldBuffer.setLength(0);
		recordBuffer.clear();
		tempReadBuffer.setLength(0);
		
		recordCounter = 0;// reset record counter

		if (inputDataSource == null) {
			reader = null;
		} else {
			if (inputDataSource instanceof ReadableByteChannel) {
				reader = ((ReadableByteChannel)inputDataSource);
			} else {
				reader = Channels.newChannel((InputStream)inputDataSource);
			}
		}
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
		if(reader != null) {
			try {
				reader.close();
			} catch(IOException ex) {
				ex.printStackTrace();
			}
		}
	}

	private DataRecord parseNext(DataRecord record) {
		int result;
		int fieldCounter;
		int character = -1;
		int mark;
		long size = 0;
		boolean inQuote;
		boolean skipBlanks = skipLeadingBlanks;
		
		recordCounter++;
		recordBuffer.clear();
		for (fieldCounter = 0; fieldCounter < metadata.getNumFields(); fieldCounter++) {
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
						
						//quotedStrings
						if (quotedStrings 
								&& metadata.getField(fieldCounter).getType() == DataFieldMetadata.STRING_FIELD) {
							if (fieldBuffer.length() == 0) {
								if (isCharacterQuote((char) character)) {
									inQuote = true;
									continue;
								}
							} else {
								if (inQuote && isCharacterQuote((char) character)) {
									if (!followFieldDelimiter(fieldCounter)) { //after ending quote can i find delimiter
										findFirstRecordDelimiter();
										return parsingErrorFound("Bad quote format", record, fieldCounter);
									}
									break;
								}
							}
						}
						//test record delimiter
						if (!inQuote && metadata.isSpecifiedRecordDelimiter()) {
							if (isRecordDelimiter()) {
								return parsingErrorFound("Unexpected record delimiter", record, fieldCounter);
							}
						}
						//fieldDelimiter update
						fieldBuffer.append((char) character);

						//test field delimiter
						if (!inQuote) {
							if(delimiterSearcher.isPattern(fieldCounter)) {
								fieldBuffer.setLength(fieldBuffer.length() - delimiterSearcher.getMatchLength());
								if(treatMultipleDelimitersAsOne)
									while(followFieldDelimiter(fieldCounter));
								break;
							}
						}
					}
				} catch (Exception ex) {
					throw new RuntimeException(getErrorMessage(ex.getMessage(),	null, fieldCounter));
				}
			} else { //fixlen data field
				fieldBuffer.setLength(0);
				mark = 0;
				try {
					for(int i = 0; i < fieldLengths[fieldCounter]; i++) {
						//end of file
						if ((character = readChar()) == -1) {
							break;
						}

						//skip leading blanks
						if(skipBlanks) 
							if(character == ' ') continue; 
							else skipBlanks = false;

						//keep track of trailing blanks
						if(character != ' ') {
							mark = i;
						} 

						fieldBuffer.append((char) character);
					}
					if(fieldBuffer.length() > 0) fieldBuffer.setLength(fieldBuffer.length() - (fieldLengths[fieldCounter] - mark - 1));
				} catch (Exception ex) {
					throw new RuntimeException(getErrorMessage(ex.getMessage(),	null, fieldCounter));
				}
			}

			// did we have EOF situation ?
			if (character == -1) {
				try {
                    //hack for data files without last row delimiter (for example last record without new-line character)
                    if(fieldCounter + 1 == metadata.getNumFields()) {
                        populateField(record, fieldCounter, fieldBuffer);
                        if((fieldCounter != 0 || recordBuffer.position() > 0)) { //hack for hack for one column table
                            //reader.close();
                            return record;
                        }
                    }
    				if(recordBuffer.position() == 0) {
                        reader.close();
    				    return null;
                    } else {
                        return parsingErrorFound("Unexpected end of file", record, fieldCounter);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
			}

			//populate field
			populateField(record, fieldCounter, fieldBuffer);
		}
		
		if(metadata.isSpecifiedRecordDelimiter()) {
			if(!followRecordDelimiter()) { //record delimiter is not found
				return parsingErrorFound("Missing record delimiter", record, fieldCounter);
			}
		}

		return record;
	}

	private DataRecord parsingErrorFound(String exceptionMessage, DataRecord record, int fieldNum) {
        if(exceptionHandler != null) {
            exceptionHandler.populateHandler("Parsing error: " + exceptionMessage, record, recordCounter, fieldNum, getLogString(), new BadDataFormatException("Parsing error: " + exceptionMessage));
            return record;
        } else {
			throw new RuntimeException("Parsing error: " + exceptionMessage + " when parsing record #" + recordCounter + " and " + fieldNum + ". field (" + recordBuffer.toString() + ")");
		}
	}
	
	private int readChar() throws IOException {
		int size;
		char character;
		CoderResult decodingResult;
		
		if(tempReadBuffer.length() > 0) {
			character = tempReadBuffer.charAt(0);
			tempReadBuffer.deleteCharAt(0);
			return character;
		}
		
		if (!charBuffer.hasRemaining()) {
			byteBuffer.clear();
			size = reader.read(byteBuffer);
			// if no more data, return -1
			if (size == -1) {
				return -1;
			}
			try {
				byteBuffer.flip();
				charBuffer.clear();
				decodingResult = decoder.decode(byteBuffer, charBuffer, true);
				charBuffer.flip();
			} catch (Exception ex) {
				throw new IOException("Exception when decoding characters: " + ex.getMessage());
			}
		}
		
		character = charBuffer.get();
		recordBuffer.put(character);

		return character;
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
	private void finishRecord(DataRecord record, int fieldNumber) {
		for(int i = fieldNumber; i < metadata.getNumFields(); i++) {
			record.getField(i).setToDefaultValue();
		}
	}

	/**
	 * Populate field.
	 * 
	 * @param record
	 * @param fieldNum
	 * @param data
	 */
	private void populateField(DataRecord record, int fieldNum,	StringBuilder data) {
        String strData = data.toString();
		try {
			record.getField(fieldNum).fromString(strData);
		} catch(BadDataFormatException bdfe) {
            if(exceptionHandler != null) {
                exceptionHandler.populateHandler(bdfe.getMessage(), record, recordCounter, fieldNum, strData, bdfe);
            } else {
                bdfe.setRecordNumber(recordCounter);
                bdfe.setFieldNumber(fieldNum);
                bdfe.setOffendingValue(strData);
                throw bdfe;
            }
		} catch(Exception ex) {
			throw new RuntimeException(getErrorMessage(ex.getMessage(), null, fieldNum));
		}
	}

	/**
	 * Is character quote?
	 * 
	 * @param character
	 * @return true if character is quote; false else
	 */
	private boolean isCharacterQuote(char character) {
		return (character == '\'' || character == '"');
	}

	/**
	 * Find first record delimiter in input channel.
	 */
	private boolean findFirstRecordDelimiter() {
        if(!metadata.isSpecifiedRecordDelimiter()) {
            return false;
        }
		int character;
		try {
			while ((character = readChar()) != -1) {
				delimiterSearcher.update((char) character);
				//test record delimiter
				if (isRecordDelimiter()) {
					return true;
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(getErrorMessage(e.getMessage(), null, -1));
		}
		//end of file
		return false;
	}

	/**
	 * Find end of record for metadata without record delimiter specified.
	 */
	private boolean findEndOfRecord(int fieldNum) {
		int character = 0;
		try {
			for(int i = fieldNum + 1; i < metadata.getNumFields(); i++) {
				while((character = readChar()) != -1) {
					delimiterSearcher.update((char) character);
					if(delimiterSearcher.isPattern(i)) {
						break;
					}
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(getErrorMessage(e.getMessage(), null, -1));
		}
		
		return (character != -1);
	}

    private void shiftToNextRecord(int fieldNum) {
        if(metadata.isSpecifiedRecordDelimiter()) {
            findFirstRecordDelimiter();
        } else {
            findEndOfRecord(fieldNum);
        }
    }
	/**
	 * Is record delimiter in the input channel?
	 * @return
	 */
	private boolean isRecordDelimiter() {
		return delimiterSearcher.isPattern(-1);
	}
	
	/**
	 * Folow field delimiter in the input channel?
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
	 * Folow record delimiter in the input channel?
	 * @return
	 */
	private boolean followRecordDelimiter() {
		int count = 1;
		int character;
		try {
			while ((character = readChar()) != -1) {
				delimiterSearcher.update((char) character);
				if(isRecordDelimiter()) {
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
	public void setSkipLeadingBlanks(boolean skipLeadingBlanks) {
		this.skipLeadingBlanks = skipLeadingBlanks;
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
	
	public String getLogString() {
        recordBuffer.flip();
		return recordBuffer.toString(); //TODO kokon
		//return logString;
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
}
/*
Default hodnoty jsou nyni definovany na urovni metadat a prirazovany do polozek pres
metodu fromString() (v metadatech ulozeno jako retezec, neni parsovano).

Pridat option umoznujici zpracovat nekolik oddelovacu za sebou
jako jeden - tedy "treat multiple delimiters as one".

Komentare.
*/