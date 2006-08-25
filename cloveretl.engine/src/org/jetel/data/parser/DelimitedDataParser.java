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
import java.io.IOException;
import java.io.InputStream;
import java.nio.BufferOverflowException;
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
import org.jetel.exception.BadDataFormatExceptionHandler;
import org.jetel.exception.JetelException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.StringUtils;

/**
 *  Parsing delimited text data. Supports delimiters with the length of up to 32
 *  characters. Delimiter for each individual field must be specified - through
 *  metadata definition. The maximum length of one parseable field is denoted by
 *  <b>FIELD_BUFFER_LENGT</b> . Parser handles quoted strings (single or double).
 *  This class is using the new IO (NIO) features
 *  introduced in Java 1.4 - directly mapped byte buffers & character
 *  encoders/decoders
 *
 *@author     D.Pavlis
 *@since      March 27, 2002
 *@see        Parser
 *@see 	      org.jetel.data.Defaults
 * @revision    $Revision$
 */
public class DelimitedDataParser implements Parser {
	private String charSet = null;
	private BadDataFormatExceptionHandler handlerBDFE;
	private ByteBuffer dataBuffer;
	private CharBuffer charBuffer;
	private CharBuffer fieldStringBuffer;
	private char[] delimiterCandidateBuffer;
	private DataRecordMetadata metadata;
	private ReadableByteChannel reader;
	private CharsetDecoder decoder;
	private int recordCounter;
	private char[][] delimiters;
	private char[] fieldTypes;
	private boolean isEof;
    private boolean skipRows=false;

	// this will be added as a parameter to constructor
	private boolean handleQuotedStrings = true;

	// Attributes
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
	public DelimitedDataParser() {
		this(Defaults.DataParser.DEFAULT_CHARSET_DECODER);
	}


	/**
	 *  Constructor for the DelimitedDataParser object
	 *
	 *@param  charsetDecoder  Charset Decoder used for converting input data into
	 *      UNICODE chars
	 *@since                  March 28, 2002
	 */
	public DelimitedDataParser(String charsetDecoder) {
		this.charSet = charsetDecoder;
		dataBuffer = ByteBuffer.allocateDirect(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
        charBuffer = CharBuffer.allocate(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		fieldStringBuffer = CharBuffer.allocate(Defaults.DataParser.FIELD_BUFFER_LENGTH);
		delimiterCandidateBuffer = new char [DELIMITER_CANDIDATE_BUFFER_LENGTH];
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
	public void open(Object in, DataRecordMetadata metadata) {
        
        DataFieldMetadata fieldMetadata;
		this.metadata = metadata;

		
		reader = Channels.newChannel((InputStream) in);

		// create array of delimiters & initialize them
		delimiters = new char[metadata.getNumFields()][];
		fieldTypes = new char[metadata.getNumFields()];
		for (int i = 0; i < metadata.getNumFields(); i++) {
			fieldMetadata = metadata.getField(i);
			delimiters[i] = fieldMetadata.getDelimiter().toCharArray();
			fieldTypes[i] = fieldMetadata.getType();
			// we handle only one character delimiters
		}
		decoder.reset();// reset CharsetDecoder
		dataBuffer.clear();
        dataBuffer.flip();
		charBuffer.clear();
		charBuffer.flip();
		recordCounter = 1;// reset record counter
		isEof=false;

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
	private String getErrorMessage(String exceptionMessage,CharSequence value, int recNo, int fieldNo) {
		StringBuffer message = new StringBuffer();
		message.append(exceptionMessage);
		message.append(" when parsing record #");
		message.append(recordCounter);
		message.append(" field ");
		message.append(metadata.getField(fieldNo).getName());
		if (value!=null){
			message.append(" value \"").append(value).append("\"");
		}
		return message.toString();
	}


	/**
     * Description of the Method
     * 
     * @return Description of the Returned Value
     * @exception IOException
     *                Description of Exception
     * @since May 13, 2002
     */
    private int readChar() throws IOException {
        CoderResult result;

        if (charBuffer.hasRemaining()) {
            return charBuffer.get();
        }

        if (isEof)
            return -1;

        charBuffer.clear();
        if (dataBuffer.hasRemaining())
            dataBuffer.compact();
        else
            dataBuffer.clear();

        if (reader.read(dataBuffer) == -1) {
            isEof = true;
        }
        dataBuffer.flip();

        result = decoder.decode(dataBuffer, charBuffer, isEof);
        if (result == CoderResult.UNDERFLOW) {
            // try to load additional data
            dataBuffer.compact();

            if (reader.read(dataBuffer) == -1) {
                isEof = true;
            }
            dataBuffer.flip();
            decoder.decode(dataBuffer, charBuffer, isEof);
        } else if (result.isError()) {
            throw new IOException(result.toString()+" when converting from "+decoder.charset());
        }
        if (isEof) {
            result = decoder.flush(charBuffer);
            if (result.isError()) {
                throw new IOException(result.toString()+" when converting from "+decoder.charset());
            }
        }
        charBuffer.flip();
        return charBuffer.hasRemaining() ? charBuffer.get() : -1;

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
		boolean isWithinQuotes;
		char quoteChar=' ';

		// populate all data fields

		while (fieldCounter < metadata.getNumFields()) {
			// we clear our buffer
			fieldStringBuffer.clear();
			character = 0;
			isWithinQuotes=false;
			// read data till we reach delimiter, end of file or exceed buffer size
			// exceeded buffer is indicated by BufferOverflowException
			charCounter = 0;
			delimiterPosition = 0;
			try {
				while ((character = readChar()) != -1) {
//					causes problem when composed delimiter "\r\n" is used
//					if(character=='\r')  //fix for new line being \r\n
//						continue;
					totalCharCounter++;
					// handle quoted strings
					if (handleQuotedStrings && StringUtils.isQuoteChar((char)character)){
					    if (!isWithinQuotes){
					        if (charCounter==0){
					        quoteChar=(char)character;
					        isWithinQuotes=true;
					        }
					    }else if (quoteChar==(char)character){
					        isWithinQuotes=false;
					    }
					}
					if ((result = is_delimiter((char) character, fieldCounter, delimiterPosition,isWithinQuotes)) == 1) {
						/*
						 *  DELIMITER
						 */
						break;
					} else if (result == 0) {
						/*
						 *  NOT A DELIMITER
						 */
						if (delimiterPosition > 0) {
							fieldStringBuffer.put(delimiterCandidateBuffer,0,delimiterPosition);
						} else {
                            try{
                                fieldStringBuffer.put((char) character);
                            }catch(BufferOverflowException ex){
                                throw new IOException("Field too long or can not find delimiter ["+String.valueOf(delimiters[fieldCounter])+"]");
                            }
						}
						delimiterPosition = 0;
					} else {
						/*
						 *  CAN'T DECIDE DELIMITER
						 */
						delimiterCandidateBuffer[delimiterPosition]=((char) character);
						delimiterPosition++;
					}
					charCounter++;
				}
				if ((character == -1) && (totalCharCounter > 1)) {
					//- incomplete record - do something
					throw new RuntimeException("Incomplete record");
				}
			} catch (Exception ex) {
                ex.printStackTrace();
				throw new RuntimeException(getErrorMessage(ex.getClass().getName()+":"+ex.getMessage(),null, 
				        	recordCounter, fieldCounter),ex);
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

			// set field's value
			// are we skipping this row/field ?
			if (!skipRows){
			    fieldStringBuffer.flip();
			    populateField(record, fieldCounter, fieldStringBuffer);
			}
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
			record.getField(fieldNum).fromString(buffer2String(data, fieldNum,handleQuotedStrings));
		} catch (BadDataFormatException bdfe) {
			if(handlerBDFE != null ) {  //use handler only if configured
				handlerBDFE.populateFieldFailure(getErrorMessage(bdfe.getMessage(),data,recordCounter, fieldNum), record,fieldNum,data.toString());
			} else {
				throw new RuntimeException(getErrorMessage(bdfe.getMessage(),data,recordCounter, fieldNum));
			}
		} catch (Exception ex) {
			throw new RuntimeException(getErrorMessage(ex.getMessage(),null,recordCounter, fieldNum),ex);
		}
	}


	/**
	 *  Transfers CharBuffer into string and handles quoting of strings (removes quotes)
	 *
	 *@param  buffer        Character buffer to work on
	 *@param  removeQuotes  true/false remove quotation characters
	 *@return               String with quotes removed if specified
	 */
	private String buffer2String(CharBuffer buffer,int fieldNum, boolean removeQuotes) {
	    if (removeQuotes && buffer.hasRemaining() &&
			metadata.getField(fieldNum).getType()== DataFieldMetadata.STRING_FIELD) {
			/* if first & last characters are quotes (and quoted is at least one character, remove quotes */
			if (StringUtils.isQuoteChar(buffer.charAt(0))) { 
				if (StringUtils.isQuoteChar(buffer.charAt(buffer.limit()-1))) {
					if (buffer.remaining()>2){
						return buffer.subSequence(1, buffer.limit() - 1).toString();
					}else{
						return ""; //empty string after quotes removed
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
	 *@return                    1 if delimiter matched; -1 if can't decide yet; 0 if not part of delimiter
	 */
	private int is_delimiter(char character, int fieldCounter, int delimiterPosition, boolean isWithinQuotes) {
		if (isWithinQuotes){
		    return 0;
		}
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


	/**
	 * @param handler
	 */
	public void addBDFHandler(BadDataFormatExceptionHandler handler) {
		this.handlerBDFE = handler;
	}

	/**
	 * Returns charset name of this parser
	 * @return Returns name of the charset used to construct or null if none was specified
	 */
	public String getCharsetName() {
		return(this.charSet);
	}
	
	/**
	 * Returns data policy type for this parser
	 * @return Data policy type or null if none was specified
	 */
	public String getBDFHandlerPolicyType() {
		if (this.handlerBDFE != null) {
			return(this.handlerBDFE.getPolicyType());
		} else {
			return(null);
		}
			
	}


    /**
     * @return Returns the skipRows.
     */
    public boolean isSkipRows() {
        return skipRows;
    }


    /**
     * @param skipRows The skipRows to set.
     */
    public void setSkipRows(boolean skipRows) {
        this.skipRows = skipRows;
    }
	
}	
/*
 *  end class DelimitedDataParser
 */

