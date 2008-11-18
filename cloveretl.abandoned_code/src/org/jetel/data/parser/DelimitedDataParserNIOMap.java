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
// FILE: c:/projects/jetel/org/jetel/data/DelimitedDataParser.java

package org.jetel.data.parser;
import java.nio.*;
import java.nio.channels.*;
import java.nio.charset.*;
import java.io.*;

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.BadDataFormatExceptionHandler;
import org.jetel.exception.JetelException;
import org.jetel.metadata.*;

/**
 *  Parsing delimited text data. Datafile is memory-mapped for higher speed.
 *  Useful only with BIG data files.<br>
 *  Supports delimiters with the length of up to 32 characters. Delimiter for
 *  each individual field must be specified - through metadata definition. The
 *  maximum length of one parseable field is denoted by <b>Field_BUFFER_LENGTH
 *  </b> . Parser handles quoted strings (single or double). This class is using
 *  the new IO (NIO) features introduced in Java 1.4 - directly mapped byte
 *  buffers & character encoders/decoders
 *
 *@author     D.Pavlis
 *@since      March 27, 2002
 *@see        Parser
 *@revision    $Revision$
 */
public class DelimitedDataParserNIOMap implements Parser {
	private BadDataFormatExceptionHandler handlerBDFE;
	private ByteBuffer dataBuffer;
	private MappedByteBuffer mappedDataBuffer;
	private CharBuffer charBuffer;
	private CharBuffer fieldStringBuffer;
	private char[] delimiterCandidateBuffer;
	private DataRecordMetadata metadata;
	private FileChannel reader;
	private CharsetDecoder decoder;
	private int recordCounter;
	private char[] delimiters[];
	private char fieldTypes[];
	private int bufferSize;
	private boolean memoryMapped;
	private long filePosition;
	private long memoryMappedSize;
	private boolean isEOF;

	// this will be added as a parameter to constructor
	private boolean handleQuotedStrings = true;

	// Attributes
	private final static int MEMORY_MAPPED_CHUNK_SIZE = 16 * 1024 * 1024;
	// 16 MB
	
	// maximum length of delimiter
	private final static int DELIMITER_CANDIDATE_BUFFER_LENGTH = 32;

	// Associations

	// Operations
	/**
	 *  Constructor for the DelimitedDataParser object. With default size and
	 *  default character encoding.
	 *
	 *@since    March 28, 2002
	 */
	public DelimitedDataParserNIOMap() {
		charBuffer = CharBuffer.allocate(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		charBuffer.flip();
		fieldStringBuffer = CharBuffer.allocate(Defaults.DataParser.FIELD_BUFFER_LENGTH);
		delimiterCandidateBuffer = new char[DELIMITER_CANDIDATE_BUFFER_LENGTH];
		decoder = Charset.forName(Defaults.DataParser.DEFAULT_CHARSET_DECODER).newDecoder();
		decoder.reset();
	}


	/**
	 *  Constructor for the DelimitedDataParser object
	 *
	 *@param  charsetDecoder  Charset Decoder used for converting input data into
	 *      UNICODE chars
	 *@since                  March 28, 2002
	 */
	public DelimitedDataParserNIOMap(String charsetDecoder) {
		charBuffer = CharBuffer.allocate(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		charBuffer.flip();
		fieldStringBuffer = CharBuffer.allocate(Defaults.DataParser.FIELD_BUFFER_LENGTH);
		delimiterCandidateBuffer = new char[DELIMITER_CANDIDATE_BUFFER_LENGTH];
		decoder = Charset.forName(charsetDecoder).newDecoder();

	}


	/**
	 *  Returs next data record parsed from input stream or NULL if no more data
	 *  available
	 *
	 *@return                  The Next value
	 *@exception  IOException  Description of Exception
	 *@since                   May 2, 2002
	 */
	public DataRecord getNext() throws JetelException {
		// create a new data record
		DataRecord record = new DataRecord(metadata);

		record.init();

		record = parseNext(record);
		if(handlerBDFE != null ) {  //use handler only if configured
			while(handlerBDFE.isThrowException()) {
				handlerBDFE.handleException(record);
				//record.init();   //redundant
				record = parseNext(record);
			}
		}
		return record;
	}


	/**
	 *  Returs next data record parsed from input stream or NULL if no more data
	 *  available The specified DataRecord's fields are altered to contain new
	 *  values.
	 *
	 *@param  record           Description of Parameter
	 *@return                  The Next value
	 *@exception  IOException  Description of Exception
	 *@since                   May 2, 2002
	 */
	public DataRecord getNext(DataRecord record) throws JetelException {
		record = parseNext(record);
		if(handlerBDFE != null ) {  //use handler only if configured
			while(handlerBDFE.isThrowException()) {
				handlerBDFE.handleException(record);
				//record.init();   //redundant
				record = parseNext(record);
			}
		}
		return record;
	}


	/**
	 *  An operation that opens/initializes parser.
	 *
	 *@param  in         InputStream of delimited text data
	 *@param  _metadata  Metadata describing the structure of data
	 *@since             March 27, 2002
	 */
	public void open(Object in, DataRecordMetadata _metadata) {
		CoderResult result;
		DataFieldMetadata fieldMetadata;
		this.metadata = _metadata;

		reader = ((FileInputStream) in).getChannel();
		try {
			memoryMappedSize = reader.size() > MEMORY_MAPPED_CHUNK_SIZE ?
					MEMORY_MAPPED_CHUNK_SIZE : reader.size();
			mappedDataBuffer = reader.map(FileChannel.MapMode.READ_ONLY, 0, memoryMappedSize);
			filePosition = 0;
			isEOF = false;
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}

		// create array of delimiters & initialize them
		delimiters = new char[metadata.getNumFields()][];
		fieldTypes = new char[metadata.getNumFields()];
		for (int i = 0; i < metadata.getNumFields(); i++) {
			fieldMetadata = metadata.getField(i);
			delimiters[i] = fieldMetadata.getDelimiter().toCharArray();
			fieldTypes[i] = fieldMetadata.getType();

		}
		decoder.reset();
		// reset CharsetDecoder
		recordCounter = 1;
		// reset record counter
	}


	/**
	 *  Description of the Method
	 *
	 *@since    May 2, 2002
	 */
	public void close() {
		if (reader != null) {
			try {
				reader.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
	}


	/**
	 *  Assembles error message when exception occures during parsing
	 *
	 *@param  exceptionMessage  message from exception getMessage() call
	 *@param  recNo             recordNumber
	 *@param  fieldNo           fieldNumber
	 *@return                   error message
	 *@since                    September 19, 2002
	 */
	private String getErrorMessage(String exceptionMessage, int recNo, int fieldNo) {
		StringBuffer message = new StringBuffer();
		message.append(exceptionMessage);
		message.append(" when parsing record #");
		message.append(recordCounter);
		message.append(" field ");
		message.append(metadata.getField(fieldNo).getName());
		return message.toString();
	}


	/**
	 *  Description of the Method
	 *
	 *@return                  Description of the Returned Value
	 *@exception  IOException  Description of Exception
	 *@since                   May 13, 2002
	 */
	private int readChar() throws IOException {
		char character;
		CoderResult decodingResult;

		if ((!charBuffer.hasRemaining())) {
			if (isEOF) {
				return -1;
			}
			charBuffer.clear();
			decodingResult = decoder.decode(mappedDataBuffer, charBuffer, false);
			if (charBuffer.hasRemaining()) {
				if (filePosition + memoryMappedSize < reader.size()) {
					// remap datafile
					filePosition += memoryMappedSize;
					memoryMappedSize = reader.size() > filePosition + MEMORY_MAPPED_CHUNK_SIZE ?
							MEMORY_MAPPED_CHUNK_SIZE : reader.size() - filePosition;
					mappedDataBuffer = reader.map(FileChannel.MapMode.READ_ONLY, filePosition, memoryMappedSize);
					decodingResult = decoder.decode(mappedDataBuffer, charBuffer, false);
				} else {
					isEOF = true;
					if (charBuffer.limit() == 0) {
						return -1;
					}
				}
			}
			charBuffer.flip();
		}
		return charBuffer.get();
	}


	/**
	 *  An operation that does ...
	 *
	 *@param  record           Description of Parameter
	 *@return                  Next DataRecord (parsed from input data) or null if
	 *      no more records available
	 *@exception  IOException  Description of Exception
	 *@since                   March 27, 2002
	 */
	private DataRecord parseNext(DataRecord record) throws JetelException {
		int result;
		int fieldCounter = 0;
		int character;
		int totalCharCounter = 0;
		int delimiterPosition;
		long size = 0;
		int charCounter;

		// populate all data fields

		while (fieldCounter < metadata.getNumFields()) {
			// we clear our buffer
			fieldStringBuffer.clear();
			character = 0;
			// read data till we reach delimiter, end of file or exceed buffer size
			// exceeded buffer is indicated by BufferOverflowException
			charCounter = 0;
			delimiterPosition = 0;
			try {
				while ((character = readChar()) != -1) {
					totalCharCounter++;
					if ((result = is_delimiter((char) character, fieldCounter, delimiterPosition)) == 1) {
						/*
						 *  DELIMITER
						 */
						break;
					} else if (result == 0) {
						/*
						 *  NOT A DELIMITER
						 */
						if (delimiterPosition > 0) {
							charCounter += delimiterPosition;
							fieldStringBuffer.put(delimiterCandidateBuffer, 0, delimiterPosition);
						} else {
							fieldStringBuffer.put((char) character);
							charCounter++;
						}
						delimiterPosition = 0;
					} else {
						/*
						 *  CAN'T DECIDE DELIMITER
						 */
						delimiterCandidateBuffer[delimiterPosition] = ((char) character);
						delimiterPosition++;
					}
				}
				if ((character == -1) && (totalCharCounter > 1)) {
					//- incomplete record - do something
					throw new RuntimeException("Incomplete record");
				}
			} catch (Exception ex) {
				throw new RuntimeException(getErrorMessage(ex.getMessage(), recordCounter, fieldCounter));
			}

			// did we have EOF situation ?
			if (character == -1) {
				try {
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
					throw new JetelException(e.getMessage());
				}
				return null;
			}

			// prepare for reading
			fieldStringBuffer.flip();
			populateField(record, fieldCounter, fieldStringBuffer);
			fieldCounter++;
		}
		recordCounter++;
		return record;
	}


	/**
	 *  Description of the Method
	 *
	 *@param  record    Description of Parameter
	 *@param  fieldNum  Description of Parameter
	 *@param  data      Description of Parameter
	 *@since            March 28, 2002
	 */
	private void populateField(DataRecord record, int fieldNum, CharBuffer data) {
		try {
			record.getField(fieldNum).fromString(buffer2String(data, fieldNum, handleQuotedStrings));
		} catch (BadDataFormatException bdfe) {
			if(handlerBDFE != null ) {  //use handler only if configured
				handlerBDFE.populateFieldFailure(getErrorMessage(bdfe.getMessage(), recordCounter, fieldNum), record,fieldNum,data.toString());
			} else {
				throw new RuntimeException(getErrorMessage(bdfe.getMessage(), recordCounter, fieldNum));
			}
		} catch (Exception ex) {
			throw new RuntimeException(getErrorMessage(ex.getMessage(), recordCounter, fieldNum));
		}
	}


	/**
	 *  Transfers CharBuffer into string and handles quoting of strings (removes
	 *  quotes)
	 *
	 *@param  buffer        Character buffer to work on
	 *@param  removeQuotes  true/false remove quotation characters
	 *@param  fieldNum      Description of the Parameter
	 *@return               String with quotes removed if specified
	 */
	private String buffer2String(CharBuffer buffer, int fieldNum, boolean removeQuotes) {
		if (removeQuotes && buffer.hasRemaining() &&
				metadata.getField(fieldNum).getType() == DataFieldMetadata.STRING_FIELD) {
			/*
			 *  if first & last characters are quotes (and quoted is at least one character, remove quotes
			 */
			if (buffer.charAt(0) == '\'') {
				if (buffer.charAt(buffer.limit() - 1) == '\'') {
					if (buffer.remaining() > 2) {
						return buffer.subSequence(1, buffer.limit() - 1).toString();
					} else {
						return "";
						//empty string after quotes removed
					}
				}
			} else if (buffer.charAt(0) == '"') {
				if (buffer.charAt(buffer.limit() - 1) == '"') {
					if (buffer.remaining() > 2) {
						return buffer.subSequence(1, buffer.limit() - 1).toString();
					} else {
						return "";
						//empty string after quotes removed
					}
				}
			}
		}
		return buffer.toString();
	}


	/**
	 *  Decides whether delimiter was encountered
	 *
	 *@param  character          character to compare with delimiter
	 *@param  fieldCounter       delimiter for which field
	 *@param  delimiterPosition  current position within delimiter string
	 *@return                    1 if delimiter matched; -1 if can't decide yet; 0
	 *      if not part of delimiter
	 */
	private int is_delimiter(char character, int fieldCounter, int delimiterPosition) {
		if (character == delimiters[fieldCounter][delimiterPosition]) {
			if (delimiterPosition == delimiters[fieldCounter].length - 1) {
				return 1;
				// whole delimiter matched
			} else {
				return -1;
				// can't decide
			}
		} else {
			return 0;
			// not a match
		}
	}


	/* (non-Javadoc)
	 * @see org.jetel.data.DataParser#addBDFHandler(org.jetel.exception.BadDataFormatExceptionHandler)
	 */
	public void addBDFHandler(BadDataFormatExceptionHandler handler) {
		this.handlerBDFE = handler;
	}

}
/*
 *  end class DelimitedDataParser
 */

