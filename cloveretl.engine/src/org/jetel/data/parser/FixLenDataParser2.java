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
// FILE: c:/projects/jetel/org/jetel/data/FixLenDataParser2.java

package org.jetel.data.parser;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.InvalidMarkException;
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

/**
 *  Parsing fix length data. It should be used in cases where new lines 
 *  separate records.  Supports fields up to <tt><b>FIELD_BUFFER_LENGTH</b></tt> long.
 *  Size of each individual field must be specified - through metadata definition.
 *
 *  This class is using the new IO (NIO) features introduced in Java 1.4 - directly mapped
 *  byte buffers & character encoders/decoders.  
 *
 * @author     David Pavlis,Wes Maciorowski     
 * @since    August 21, 2002
 * @see        Parser
 * @see      Defaults
 * @revision    $Revision$
 */
public class FixLenDataParser2 implements Parser {
	private boolean oneRecordPerLinePolicy;
	private BadDataFormatExceptionHandler handlerBDFE;
	private ByteBuffer dataBuffer;
//	private ByteBuffer fieldBuffer;
	private CharBuffer charBuffer;
	private CharBuffer fieldStringBuffer;
	private DataRecordMetadata metadata;
	private int recordCounter;
	private int fieldLengths[];
	private int recordLength;
	private ReadableByteChannel reader = null;
	private CharsetDecoder decoder;
	/**
	 *Constructor for the FixLenDataParser object
	 *
	 * @since    August 21, 2002
	 */
	public FixLenDataParser2() {
		charBuffer = CharBuffer.allocate(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		charBuffer.flip(); // initially empty 
		fieldStringBuffer = CharBuffer.allocate(Defaults.DataParser.FIELD_BUFFER_LENGTH);
		dataBuffer = ByteBuffer.allocateDirect(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
//		fieldBuffer = ByteBuffer.allocateDirect(Defaults.DataParser.FIELD_BUFFER_LENGTH);
		decoder = Charset.forName(Defaults.DataParser.DEFAULT_CHARSET_DECODER).newDecoder();
	}
	/**
	 *Constructor for the FixLenDataParser object
	 *
	 * @param  charsetDecoder  Description of Parameter
	 * @since    August 21, 2002
	 */
	public FixLenDataParser2(String charsetDecoder) {
		charBuffer = CharBuffer.allocate(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		charBuffer.flip(); // initially empty 
		fieldStringBuffer = CharBuffer.allocate(Defaults.DataParser.FIELD_BUFFER_LENGTH);

		dataBuffer = ByteBuffer.allocateDirect(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
//		fieldBuffer = ByteBuffer.allocateDirect(Defaults.DataParser.FIELD_BUFFER_LENGTH);
		decoder = Charset.forName(charsetDecoder).newDecoder();
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
		
		if (_metadata.getRecType()!=DataRecordMetadata.FIXEDLEN_RECORD){
			throw new RuntimeException("Invalid record format - is not FIXLEN !");
		}
		
		reader = ((FileInputStream) in).getChannel();
		recordLength=0;
		// create array of field sizes & initialize them
		fieldLengths = new int[metadata.getNumFields()];
		for (int i = 0; i < metadata.getNumFields(); i++) {
			fieldLengths[i] = metadata.getField(i).getSize();
			recordLength += fieldLengths[i];
		}
		decoder.reset();
		// reset CharsetDecoder
		recordCounter = 0;
		// reset record counter
//		charBuffer.clear();
//		dataBuffer.clear();

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
		int fieldCounter = 0;
		int posCounter = 0;
		boolean isDataAvailable;
		int remaining=charBuffer.remaining();
				
		if (charBuffer.remaining() < recordLength){
			// need to get some data
			try {
				isDataAvailable = readRecord();
				if (!isDataAvailable){
					reader.close();
					if (remaining > 0){
						//- incomplete record - do something
						throw new RuntimeException("Incomplete record at the end of the stream");
					}
					return null; // end of data stream 
				}
			} catch (IOException e) {
				e.printStackTrace();
				throw new JetelException(e.getMessage());
			}
		}

		// process the line 
		// populate all data fields
		try {
			while (fieldCounter < metadata.getNumFields()) {
				fieldStringBuffer.clear();
				char c;
				boolean skippingLeadingBlanks = true;
				boolean trackingTrailingBlanks = false;
				for(int i=0; i < fieldLengths[fieldCounter] ; i++) {
					// skip leading blanks
					c = charBuffer.get();
					if(skippingLeadingBlanks && c == ' ') {
						continue;
					} 
					
					if(skippingLeadingBlanks && c != ' ') {
						skippingLeadingBlanks  = false;
					}
					//keep track of trailing blanks
					fieldStringBuffer.put( c );

					if( c != ' ') {
						fieldStringBuffer.mark();
					} 
					
				}
				try {
					fieldStringBuffer.reset();
					//fieldStringBuffer.limit(fieldStringBuffer.position()-1);
				} catch (InvalidMarkException e) {
				}
				// prepare for reading
				fieldStringBuffer.flip();
				populateField(record, fieldCounter, fieldStringBuffer);
				posCounter += fieldLengths[fieldCounter];
				fieldCounter++;
			}
		} catch (Exception ex) {

			throw new RuntimeException(ex.getMessage());
		}

		recordCounter++;
		return record;
	}


	/**
	 * Fills the charBuffer with at least records worth of chars.
	 * 
	 * @return false if there is nothing left to read; true at least one record's worth is available
	 */
	private boolean readRecord() throws IOException {
		CoderResult decodingResult;
		int size;
		int remaining=charBuffer.remaining();
		
		dataBuffer.clear();
		if(remaining>0) {
			charBuffer.compact();
			dataBuffer.limit(dataBuffer.capacity()-remaining);
		} else {
			charBuffer.clear();
		}
		size = reader.read(dataBuffer);
		//System.out.println( "Read: " + size);
		dataBuffer.flip();

		// if no more data 
		if ( size <= 0 ) {
			return false;
		}

		decodingResult=decoder.decode(dataBuffer,charBuffer,true);
		charBuffer.flip();
		
		// check if \r or \n ; if yes discard
		charBuffer.mark();
		char c = charBuffer.get();
		while (c=='\r' || c=='\n') {
			charBuffer.mark();
			c = charBuffer.get();
		}
		charBuffer.reset();
		return true;
	}

	/**
	 *  Assembles error message when exception occures during parsing
	 *
	 * @param  exceptionMessage  message from exception getMessage() call
	 * @param  recNo             recordNumber
	 * @param  fieldNo           fieldNumber
	 * @return                   error message
	 * @since                    September 19, 2002
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
	 *  Assembles error message when exception occures during parsing
	 *
	 * @param  exceptionMessage  message from exception getMessage() call
	 * @param  recNo             recordNumber
	 * @param  fieldNo           fieldNumber
	 * @return                   error message
	 * @since                    September 19, 2002
	 */
	private String getErrorMessage(String exceptionMessage,CharSequence  value, int recNo, int fieldNo) {
		StringBuffer message = new StringBuffer();
		message.append(exceptionMessage);
		message.append(" when parsing record #").append(recordCounter);
		message.append(" field ").append(metadata.getField(fieldNo).getName());
		message.append(" value \"").append(value).append("\"");
		return message.toString();
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
				//record.init();  redundant
				record = parseNext(record);
			}
		}
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
			record.getField(fieldNum).fromString(buffer2String(data, fieldNum, false));
		} catch (BadDataFormatException bdfe) {
			if(handlerBDFE != null ) {  //use handler only if configured
				handlerBDFE.populateFieldFailure(record,fieldNum,data.toString());
			} else {
				throw new RuntimeException(getErrorMessage(bdfe.getMessage(),data,recordCounter, fieldNum));
			}
		} catch (Exception ex) {
			throw new RuntimeException(getErrorMessage(ex.getMessage(), recordCounter, fieldNum));
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
			if (buffer.charAt(0) == '\'') { 
				if (buffer.charAt(buffer.limit()-1) == '\'') {
					if (buffer.remaining()>2){
						return buffer.subSequence(1, buffer.limit() - 1).toString();
					}else{
						return ""; //empty string after quotes removed
					}
				}
			} else if (buffer.charAt(0) == '"' ) {
				if (buffer.charAt(buffer.limit()-1) == '"') {
					if ( buffer.remaining()>2){
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
	 *  Gets the Next attribute of the FixLenDataParser object
	 *
	 * @return                  The Next value
	 * @exception  IOException  Description of Exception
	 * @since                   August 21, 2002
	 */
	public DataRecord getNext() throws JetelException {
		// create a new data record
		DataRecord record = new DataRecord(metadata);
		record.init();
		return parseNext(record);
	}

	/**
	 *  Description of the Method
	 *
	 * @since    August 21, 2002
	 */
	public void close() {
		if (reader != null) {
			try {
				reader.close();
			}
			catch (IOException ex) {
				ex.printStackTrace();
			}
		}
	}



	/** 
	 * Adds BadDataFormatExceptionHandler to behave according to DataPolicy.
	 * @param handler
	 */
	public void addBDFHandler(BadDataFormatExceptionHandler handler) {
		this.handlerBDFE = handler;
	}

	/**
	 *  Sets OneRecordPerLinePolicy.
	 * @see org.jetel.data.formatter.Formatter#setOneRecordPerLinePolicy(boolean)
	 */
	public void setOneRecordPerLinePolicy(boolean b) {
		oneRecordPerLinePolicy = b;
	}

}
