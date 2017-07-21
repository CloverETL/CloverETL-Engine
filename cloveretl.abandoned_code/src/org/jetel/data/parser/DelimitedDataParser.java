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
import java.io.*;

import org.jetel.data.DataRecord;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.BadDataFormatExceptionHandler;
import org.jetel.exception.JetelException;
import org.jetel.metadata.*;

/**
 *  Parsing delimited text data. Supports delimiters with the length of ONE character.
 *  Delimiter for each individual field must be specified - through metadata definition
 *  The maximum length of one parseable field is denoted by <b>INTERNAL_BUFFER_SIZE</b>.
 *
 * @author     D.Pavlis
 * @since    March 27, 2002
 * @see        OtherClasses
 */
public class DelimitedDataParser implements Parser {
	private BadDataFormatExceptionHandler handlerBDFE;
	private CharBuffer buffer;
	private DataRecordMetadata metadata;
	private Reader reader;
	private char delimiters[];

	// Attributes
	private final static int DEFAULT_INTERNAL_BUFFER_SIZE = 8192;


	// Associations

	// Operations
	/**
	 * Constructor for the DelimitedDataParser object. With default size of INTERNAL_BUFFER_SIZE (8192 bytes)
	 *
	 * @since    March 28, 2002
	 */
	public DelimitedDataParser() {
		buffer = CharBuffer.allocate(DEFAULT_INTERNAL_BUFFER_SIZE);
	}


	/**
	 *Constructor for the DelimitedDataParser object
	 *
	 * @param  _bufferSize  Maximum length of one field in bytes
	 * @since               March 28, 2002
	 */
	public DelimitedDataParser(int _bufferSize) {
		if (_bufferSize < DEFAULT_INTERNAL_BUFFER_SIZE) {
			_bufferSize = DEFAULT_INTERNAL_BUFFER_SIZE;
		}
		buffer = CharBuffer.allocate(_bufferSize);
	}


	/**
	 *  Returs next data record parsed from input stream or NULL if no more data available.
	 *  The DataRecord object is internally created.
	 *
	 * @return                  next data record
	 * @exception  IOException  Description of Exception
	 * @since                   May 2, 2002
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
	 *  Returs next data record parsed from input stream or NULL if no more data available
	 *  The specified DataRecord's fields are altered to contain new values.  
	 *
	 * @param  record           Description of Parameter
	 * @return                  The Next value
	 * @exception  IOException  Description of Exception
	 * @since                   May 2, 2002
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
	 * @param  in         InputStream of delimited text data
	 * @param  _metadata  Metadata describing the structure of data
	 * @since             March 27, 2002
	 */
	public void open(Object in, DataRecordMetadata _metadata) {
		this.metadata = _metadata;

		// create buffered input stream reader
		reader = new BufferedReader(new InputStreamReader((InputStream)in),DEFAULT_INTERNAL_BUFFER_SIZE) ;

		// create array of delimiters & initialize them
		delimiters = new char[metadata.getNumFields()];
		for (int i = 0; i < metadata.getNumFields(); i++) {
			delimiters[i] = metadata.getField(i).getDelimiter().charAt(0);
			// we handle only one character delimiters
		}

	}


	/**
	 *  Closes the input data stream
	 *
	 * @since    May 2, 2002
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
	 *  An operation that does parse next DataRecord
	 *
	 * @param  record           Description of Parameter
	 * @return                  Next DataRecord (parsed from input data) or null if no more records available
	 * @exception  IOException  Description of Exception
	 * @since                   March 27, 2002
	 */
	private DataRecord parseNext(DataRecord record) throws JetelException {
		int fieldCounter = 0;
		int character;

		// populate all data fields

		while (fieldCounter < metadata.getNumFields()) {
			buffer.clear();
			// we clear our buffer
			character = 0;
			// read data till we reach delimiter, end of file or exceed buffer size
			// exceeded buffer is indicated by BufferOverflowException
			try {
				while ((character = reader.read()) != -1) {
					// patch for Windows platform, this will have to be improved ;-)
					if (character == '\r') continue;  
					if (character == delimiters[fieldCounter]) {
						break;
					} else {
						try {
							buffer.put((char) character);
						}
						catch (BufferOverflowException e) {
							System.err.println(e.getMessage());
							e.printStackTrace();
							break;
						}
					}
				}
			// did we have EOF situation ?
			if (character == -1) {
				reader.close();
				return null;
			}
			} catch (IOException e1) {
				e1.printStackTrace();
				throw new JetelException(e1.getMessage());
			}
			buffer.flip();
			// prepare for reading
			populateField(record, fieldCounter, buffer.toString());
			fieldCounter++;
		}
		return record;
	}


	/**
	 *  Populates the specified field (by field number) with the value represented by string
	 *
	 * @param  record    Description of Parameter
	 * @param  fieldNum  Description of Parameter
	 * @param  data      Description of Parameter
	 * @since            March 28, 2002
	 */
	private void populateField(DataRecord record, int fieldNum, String data) {
		try {
			record.getField(fieldNum).fromString(data);
		} catch (BadDataFormatException bdfe) {
			if(handlerBDFE != null ) {  //use handler only if configured
				handlerBDFE.populateFieldFailure("Parse error:",record,fieldNum,data.toString());
			} else {
				throw new RuntimeException(bdfe.getMessage() + ":" + bdfe.getOffendingValue() );
			}
		}
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

